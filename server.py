import asyncio
from datetime import datetime
from aiohttp import web
import aiofiles
import contextlib


async def archive(request: web.Request) -> web.StreamResponse:
    # /archive/<hash>/ 
    resp = web.StreamResponse(
        status=200,
        headers={
            "Content-Type": "text/plain; charset=utf-8",
            "Cache-Control": "no-cache",
        },
    )
    resp.enable_chunked_encoding()
    await resp.prepare(request)

    try:
        while True:
            if request.transport is None or request.transport.is_closing():
                break

            line = datetime.now().strftime("%Y-%m-%d %H:%M:%S") + "\n"
            try:
                await resp.write(line.encode("utf-8"))
            except (ConnectionResetError, BrokenPipeError):
                break

            await asyncio.sleep(1)

    except asyncio.CancelledError:
        raise

    finally:
        with contextlib.suppress(Exception):
            await resp.write_eof()

    return resp


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
