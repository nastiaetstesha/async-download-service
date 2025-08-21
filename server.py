import asyncio
import aiofiles
import contextlib
import logging
import os

from aiohttp import web
from pathlib import Path
from urllib.parse import quote


def env_bool(name: str, default: bool) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    return str(v).strip().lower() in ("1", "true", "yes", "on")


def build_config() -> dict:
    base_dir = Path(__file__).resolve().parent

    photos_dir = os.getenv("PHOTOS_DIR", "photos")
    photos_path = Path(photos_dir)
    if not photos_path.is_absolute():
        photos_path = (base_dir / photos_dir).resolve()

    try:
        throttle_kbps = float(os.getenv("THROTTLE_KBPS", "0") or 0.0)
    except ValueError:
        throttle_kbps = 0.0

    log_enabled = env_bool("LOG", True)
    log_level = os.getenv("LOG_LEVEL", "INFO").upper()

    return {
        "photos_path": photos_path,
        "throttle_kbps": throttle_kbps,
        "log_enabled": log_enabled,
        "log_level": log_level,
    }


async def stop_proc(proc: asyncio.subprocess.Process):
    if not proc:
        return
    if proc.returncode is None:
        proc.kill()
    with contextlib.suppress(Exception):
        await proc.communicate()


async def archive(request: web.Request) -> web.StreamResponse:
    cfg = request.app["cfg"]
    log: logging.Logger = request.app["log"]

    archive_hash = request.match_info["archive_hash"]
    target_dir = cfg["photos_path"] / archive_hash

    if not target_dir.is_dir():
        raise web.HTTPNotFound(
            text="Архив не существует или был удалён",
            content_type="text/plain"
            )

    filename = f"photos-{archive_hash}.zip"
    cd = f"attachment; filename={filename}; filename*=UTF-8''{quote(filename)}"

    resp = web.StreamResponse(
        status=200,
        headers={
            "Content-Type": "application/zip",
            "Content-Disposition": cd,
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
        },
    )
    resp.enable_chunked_encoding()
    await resp.prepare(request)

    cmd = ["zip", "-r", "-", "."]

    proc = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        cwd=str(target_dir),
        start_new_session=True,
    )

    kbps = cfg["throttle_kbps"]
    try:
        kbps = float(request.rel_url.query.get("kbps", kbps))
        if kbps < 0:
            kbps = 0.0
    except ValueError:
        pass

    total = 0
    try:
        inj = request.rel_url.query.get("raise")
        if inj == "index":
            raise IndexError("Injected IndexError for testing")
        if inj == "systemexit":
            raise SystemExit("Injected SystemExit for testing")

        assert proc.stdout is not None
        while chunk := await proc.stdout.read(64 * 1024):

            total += len(chunk)
            log.debug("Sending archive chunk ... (%d bytes)", len(chunk))

            if kbps > 0:
                await asyncio.sleep(len(chunk) / (kbps * 1024.0))

            if request.transport is None or request.transport.is_closing():
                log.info(
                        "Download was interrupted (transport closing). sent=%d",
                        total
                        )
                await stop_proc(proc)
                return resp

            try:
                await resp.write(chunk)
            except (ConnectionResetError, BrokenPipeError):
                log.info(
                        "Download was interrupted (write failed). sent=%d",
                        total
                        )
                await stop_proc(proc)
                return resp

        rc = await proc.wait()
        if rc != 0:
            err = (await proc.stderr.read()).decode(errors="ignore")
            log.error("zip failed rc=%s stderr=%r", rc, err)
            await stop_proc(proc)
            raise web.HTTPInternalServerError(text="Ошибка архивации")

        await resp.write_eof()
        log.info(
                "Finished hash=%s total_sent=%d bytes", archive_hash, total
                )
        return resp

    except asyncio.CancelledError:
        log.warning(
                "Download was interrupted (handler cancelled). sent=%d", total
                )
        await stop_proc(proc)
        raise
    except Exception as e:
        log.exception(
                "Unhandled error (Exception). sent=%d: %s", total, e
                )
        await stop_proc(proc)
        raise
    except BaseException as e:
        log.warning(
                "Unhandled BaseException %s; sent=%d", type(e).__name__, total
                )
        await stop_proc(proc)
        raise
    finally:
        with contextlib.suppress(Exception):
            await resp.write_eof()
        with contextlib.suppress(Exception):
            await stop_proc(proc)


async def handle_index_page(request):
    async with aiofiles.open('index.html', mode='r') as index_file:
        index_contents = await index_file.read()
    return web.Response(text=index_contents, content_type='text/html')


def main():
    cfg = build_config()

    logging.basicConfig(
        level=(getattr(logging, cfg["log_level"], logging.INFO)
               if cfg["log_enabled"] else logging.CRITICAL),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    if not cfg["log_enabled"]:
        logging.disable(logging.CRITICAL)

    log = logging.getLogger("archive")
    access_log = None if not cfg["log_enabled"] else logging.getLogger("aiohttp.access")

    app = web.Application()
    app["cfg"] = cfg
    app["log"] = log
    app.add_routes([
        web.get('/', handle_index_page),
        web.get('/archive/{archive_hash}/', archive),
        web.get('/archive/{archive_hash}', archive),
        web.get('/favicon.ico', lambda r: web.Response(status=204)),
    ])
    web.run_app(app, access_log=access_log)


if __name__ == "__main__":
    main()
