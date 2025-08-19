import asyncio
import aiofiles
import contextlib
import logging
import os
import signal

from aiohttp import web
from pathlib import Path
from urllib.parse import quote


def env_bool(name: str, default: bool) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    return str(v).strip().lower() in ("1", "true", "yes", "on")


BASE_DIR = Path(__file__).resolve().parent
PHOTOS_DIR = os.getenv("PHOTOS_DIR", "photos")
PHOTOS_PATH = (
    Path(PHOTOS_DIR) if Path(PHOTOS_DIR).is_absolute() else (BASE_DIR / PHOTOS_DIR)
    ).resolve()

THROTTLE_KBPS = 0.0
try:
    THROTTLE_KBPS = float(os.getenv("THROTTLE_KBPS", "0") or 0.0)
except ValueError:
    THROTTLE_KBPS = 0.0

LOG_ENABLED = env_bool("LOG", True)
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

if LOG_ENABLED:
    logging.basicConfig(
        level=getattr(logging, LOG_LEVEL, logging.INFO),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
else:
    logging.basicConfig(level=logging.CRITICAL)
    logging.disable(logging.CRITICAL)

log = logging.getLogger("archive")


async def _stop_zip(proc: asyncio.subprocess.Process, grace: float = 1.5):
    if not proc or proc.returncode is not None:
        return
    try:
        pgid = os.getpgid(proc.pid)
    except Exception:
        pgid = None

    if pgid and pgid > 0:
        os.killpg(pgid, signal.SIGTERM)
    else:
        proc.terminate()

    try:
        await asyncio.wait_for(proc.communicate(), timeout=grace)
    except asyncio.TimeoutError:
        if pgid and pgid > 0:
            os.killpg(pgid, signal.SIGKILL)
        else:
            proc.kill()
        await proc.communicate()


async def archive(request: web.Request) -> web.StreamResponse:
    archive_hash = request.match_info["archive_hash"]
    target_dir = PHOTOS_PATH / archive_hash

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

    cmd = [
        "zip", "-r", "-", ".", "-x", "*/__pycache__/*", "*.pyc", ".DS_Store", "*/.git/*"
        ]
    proc = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        cwd=str(target_dir),
        start_new_session=True,
    )

    # задержка может быть в URL: ?kbps=...
    kbps = THROTTLE_KBPS
    if "kbps" in request.rel_url.query:
        try:
            kbps = float(request.rel_url.query["kbps"])
        except ValueError:
            kbps = THROTTLE_KBPS

    total = 0
    try:
        inj = request.rel_url.query.get("raise")
        if inj == "index":
            raise IndexError("Injected IndexError for testing")
        if inj == "systemexit":
            raise SystemExit("Injected SystemExit for testing")

        assert proc.stdout is not None
        while True:
            chunk = await proc.stdout.read(64 * 1024)
            if not chunk:
                break

            total += len(chunk)
            if LOG_ENABLED:
                log.debug("Sending archive chunk ... (%d bytes)", len(chunk))

            if kbps > 0:
                await asyncio.sleep(len(chunk) / (kbps * 1024.0))

            if request.transport is None or request.transport.is_closing():
                if LOG_ENABLED:
                    log.info(
                        "Download was interrupted (transport closing). sent=%d",
                        total
                        )
                await _stop_zip(proc)
                return resp

            try:
                await resp.write(chunk)
            except (ConnectionResetError, BrokenPipeError):
                if LOG_ENABLED:
                    log.info(
                        "Download was interrupted (write failed). sent=%d",
                        total
                        )
                await _stop_zip(proc)
                return resp

        rc = await proc.wait()
        if rc != 0:
            err = (await proc.stderr.read()).decode(errors="ignore")
            if LOG_ENABLED:
                log.error("zip failed rc=%s stderr=%r", rc, err)
            await _stop_zip(proc)
            raise web.HTTPInternalServerError(text="Ошибка архивации")

        await resp.write_eof()
        if LOG_ENABLED:
            log.info(
                "Finished hash=%s total_sent=%d bytes", archive_hash, total
                )
        return resp

    except asyncio.CancelledError:
        if LOG_ENABLED:
            log.warning(
                "Download was interrupted (handler cancelled). sent=%d", total
                )
        await _stop_zip(proc)
        raise
    except Exception as e:
        if LOG_ENABLED:
            log.exception(
                "Unhandled error (Exception). sent=%d: %s", total, e
                )
        await _stop_zip(proc)
        raise
    except BaseException as e:
        if LOG_ENABLED:
            log.warning(
                "Unhandled BaseException %s; sent=%d", type(e).__name__, total
                )
        await _stop_zip(proc)
        raise
    finally:
        with contextlib.suppress(Exception):
            await resp.write_eof()
        with contextlib.suppress(Exception):
            await _stop_zip(proc)


async def handle_index_page(request):
    async with aiofiles.open('index.html', mode='r') as index_file:
        index_contents = await index_file.read()
    return web.Response(text=index_contents, content_type='text/html')


def main():
    app = web.Application()
    app.add_routes([
        web.get('/', handle_index_page),
        web.get('/archive/{archive_hash}/', archive),
        web.get('/archive/{archive_hash}', archive),
        web.get('/favicon.ico', lambda r: web.Response(status=204)),
    ])
    access_log = None if not LOG_ENABLED else logging.getLogger("aiohttp.access")
    web.run_app(app, access_log=access_log)


if __name__ == "__main__":
    main()
