from .common import get_conn
import logging
from typing import Any
from ednews import embeddings

logger = logging.getLogger("ednews.cli.embed")


def cmd_embed(args: Any) -> None:
    """Generate and insert embeddings for articles and/or headlines.

    Args:
        args: argparse namespace with .model, .batch_size, .articles, .headlines
    """
    conn = get_conn()
    embeddings.create_database(conn)
    want_articles = getattr(args, 'articles', False)
    want_headlines = getattr(args, 'headlines', False)

    if not want_articles and not want_headlines:
        want_articles = True
        want_headlines = True

    if want_articles:
        try:
            embeddings.generate_and_insert_embeddings_local(conn, model=args.model, batch_size=args.batch_size)
        except Exception:
            logger.exception("Failed to generate article embeddings")

    if want_headlines:
        try:
            embeddings.create_headlines_vec(conn)
            embeddings.generate_and_insert_headline_embeddings(conn, model=args.model, batch_size=args.batch_size)
        except Exception:
            logger.exception("Failed to generate headline embeddings")
    conn.close()
