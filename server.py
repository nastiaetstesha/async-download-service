import asyncio
import aiofiles
import contextlib
import logging
import os
import signal

from aiohttp import web
from pathlib import Path
from urllib.parse import quote

# INFO
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
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

    base_dir = Path(__file__).resolve().parent
    photos_root = base_dir / "photos"
    target_dir = photos_root / archive_hash  # photos/<hash>/

    if not target_dir.is_dir():
        raise web.HTTPNotFound(
            text="Архив не существует или был удалён",
            content_type="text/plain",
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
        "zip", "-r", "-", ".",
        "-x", "*/__pycache__/*", "*.pyc", ".DS_Store", "*/.git/*"
    ]

    proc = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        cwd=str(target_dir),
        start_new_session=True,
    )

    total = 0
    try:
        # ТЕСТ ИСКЛЮЧЕНИЙ
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

            log.debug("Sending archive chunk ... (%d bytes)", len(chunk))

            if request.transport is None or request.transport.is_closing():
                log.info(
                    "Download was interrupted (transport closing) hash=%s sent=%d bytes",
                    archive_hash, total
                )
                await _stop_zip(proc)
                return resp

            try:
                await resp.write(chunk)
            except (ConnectionResetError, BrokenPipeError):
                log.info(
                    "Download was interrupted - Write failed (client disconnect) hash=%s sent=%d bytes",
                    archive_hash, total
                )
                await _stop_zip(proc)
                return resp

        rc = await proc.wait()
        if rc != 0:
            err = (await proc.stderr.read()).decode(errors="ignore")
            log.error(
                "zip failed rc=%s hash=%s stderr=%r", rc, archive_hash, err
                )
            await _stop_zip(proc)
            raise web.HTTPInternalServerError(text=f"zip failed: {err}")

        await resp.write_eof()
        log.info("Finished hash=%s total_sent=%d bytes", archive_hash, total)
        return resp

    except asyncio.CancelledError:
        log.info(
            "Request cancelled hash=%s sent=%d bytes", archive_hash, total
            )
        await _stop_zip(proc)
        raise

    except Exception as e:
        log.exception(
            "Unhandled error hash=%s after_sent=%d bytes: %s",
            archive_hash, total, e
        )
        await _stop_zip(proc)
        raise

    except BaseException as e:
        log.warning(
            "Unhandled BaseException %s hash=%s sent=%d bytes",
            type(e).__name__, archive_hash, total
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


if __name__ == '__main__':
    app = web.Application()
    app.add_routes([
        web.get('/', handle_index_page),
        web.get('/archive/{archive_hash}/', archive),
        web.get('/archive/{archive_hash}', archive),
        web.get('/favicon.ico', lambda r: web.Response(status=204)),
    ])
    web.run_app(app)
