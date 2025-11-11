import logging


from typing import Any


def cmd_serve(args: Any) -> None:
    """Serve the built static site directory over HTTP for local preview.

    Args:
        args: argparse namespace with .host, .port, .directory
    """
    import http.server
    import socketserver
    from pathlib import Path

    logger = logging.getLogger("ednews.cli.serve")

    directory = (
        Path(args.directory) if getattr(args, "directory", None) else Path("build")
    )
    if not directory.exists():
        logger.error("Build directory does not exist: %s", str(directory))
        return

    host = args.host if getattr(args, "host", None) else "127.0.0.1"
    port = int(args.port) if getattr(args, "port", None) else 8000

    handler_class = http.server.SimpleHTTPRequestHandler

    try:
        handler = lambda *p, directory=str(directory), **kw: handler_class(
            *p, directory=directory, **kw
        )
    except TypeError:
        import os

        os.chdir(str(directory))
        handler = handler_class

    with socketserver.TCPServer((host, port), handler) as httpd:
        sa = httpd.socket.getsockname()
        logger.info("Serving %s on http://%s:%d", str(directory), sa[0], sa[1])
        try:
            httpd.serve_forever()
        except KeyboardInterrupt:
            logger.info("Shutting down server")
            httpd.shutdown()
