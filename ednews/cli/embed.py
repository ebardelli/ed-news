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
            # support --force and --ids
            force = getattr(args, 'force', False)
            ids_arg = getattr(args, 'ids', None)
            if ids_arg:
                try:
                    ids = [int(x.strip()) for x in str(ids_arg).split(',') if x.strip()]
                except Exception:
                    ids = []
                if ids:
                    embeddings.generate_and_insert_embeddings_for_ids(conn, ids, model=args.model)
                    # still generate missing ones for others if force is set
                    if force:
                        embeddings.generate_and_insert_article_embeddings(conn, model=args.model, batch_size=args.batch_size, force=True)
                    else:
                        embeddings.generate_and_insert_article_embeddings(conn, model=args.model, batch_size=args.batch_size, force=False)
                else:
                    # no valid ids parsed; fall back to normal behavior
                    embeddings.generate_and_insert_embeddings_local(conn, model=args.model, batch_size=args.batch_size)
            else:
                if force:
                    embeddings.generate_and_insert_article_embeddings(conn, model=args.model, batch_size=args.batch_size, force=True)
                else:
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
