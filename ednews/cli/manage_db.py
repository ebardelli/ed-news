from .common import get_conn, start_maintenance_run, finalize_maintenance_run
import logging

logger = logging.getLogger("ednews.cli.manage_db")


def cmd_manage_db_cleanup(args):
    from ..db import manage_db
    conn = get_conn()
    started, run_id = start_maintenance_run(conn, "cleanup-empty-articles", {"args": vars(args)})
    status = "failed"
    details = {}
    try:
        if getattr(args, 'dry_run', False):
            cur = conn.cursor()
            params = []
            where_clauses = ["(COALESCE(title, '') = '' AND COALESCE(abstract, '') = '')"]
            if getattr(args, 'older_than_days', None) is not None:
                from datetime import datetime, timezone, timedelta
                cutoff = (datetime.now(timezone.utc) - timedelta(days=int(args.older_than_days))).isoformat()
                where_clauses.append("(COALESCE(fetched_at, '') != '' AND COALESCE(fetched_at, '') < ? OR COALESCE(published, '') != '' AND COALESCE(published, '') < ?)")
                params.extend([cutoff, cutoff])
            where_sql = " AND ".join(where_clauses)
            cur.execute(f"SELECT COUNT(1) FROM articles WHERE {where_sql}", tuple(params))
            row = cur.fetchone()
            count_empty = row[0] if row and row[0] else 0

            try:
                count_filtered = manage_db.cleanup_filtered_titles(conn, filters=None, dry_run=True)
            except Exception:
                count_filtered = 0

            print(f"dry-run: would delete {count_empty} empty rows and {count_filtered} filtered-title rows")
            status = "dry-run"
            details = {"would_delete_empty": count_empty, "would_delete_filtered": count_filtered}
        else:
            deleted_empty = manage_db.cleanup_empty_articles(conn, older_than_days=getattr(args, 'older_than_days', None))
            deleted_filtered = manage_db.cleanup_filtered_titles(conn, filters=None, dry_run=False)
            total_deleted = (deleted_empty or 0) + (deleted_filtered or 0)
            print(f"deleted {total_deleted} rows ({deleted_empty} empty, {deleted_filtered} filtered-title)")
            status = "ok"
            details = {"deleted_empty": deleted_empty, "deleted_filtered": deleted_filtered, "total_deleted": total_deleted}
    except Exception as e:
        status = "failed"
        details = {"error": str(e)}
        raise
    finally:
        finalize_maintenance_run(conn, "cleanup-empty-articles", run_id, started, status, details)
        conn.close()


def cmd_manage_db_cleanup_filtered_title(args):
    from ..db import manage_db
    conn = get_conn()
    started, run_id = start_maintenance_run(conn, "cleanup-filtered-title", {"args": vars(args)})
    status = "failed"
    details = {}
    try:
        filters = None
        if getattr(args, 'filter', None):
            filters = list(args.filter)
        elif getattr(args, 'filters', None):
            filters = [f.strip() for f in str(args.filters).split(',') if f.strip()]

        if getattr(args, 'dry_run', False):
            count = manage_db.cleanup_filtered_titles(conn, filters=filters, dry_run=True)
            print(f"dry-run: would delete {count} rows")
            status = "dry-run"
            details = {"would_delete_filtered": count}
        else:
            deleted = manage_db.cleanup_filtered_titles(conn, filters=filters, dry_run=False)
            print(f"deleted {deleted} rows")
            status = "ok"
            details = {"deleted_filtered": deleted}
    except Exception as e:
        status = "failed"
        details = {"error": str(e)}
        raise
    finally:
        finalize_maintenance_run(conn, "cleanup-filtered-title", run_id, started, status, details)
        conn.close()


def cmd_manage_db_vacuum(args):
    from ..db import manage_db
    conn = get_conn()
    started, run_id = start_maintenance_run(conn, "vacuum", {})
    status = "failed"
    details = {}
    try:
        ok = manage_db.vacuum_db(conn)
        print("vacuum: ok" if ok else "vacuum: failed")
        status = "ok" if ok else "failed"
        details = {}
    except Exception as e:
        status = "failed"
        details = {"error": str(e)}
        raise
    finally:
        finalize_maintenance_run(conn, "vacuum", run_id, started, status, details)
        conn.close()


def cmd_manage_db_migrate(args):
    from ..db import manage_db
    conn = get_conn()
    started, run_id = start_maintenance_run(conn, "migrate", {})
    status = "failed"
    details = {}
    try:
        ok = manage_db.migrate_db(conn)
        print("migrate: ok" if ok else "migrate: failed")
        status = "ok" if ok else "failed"
        details = {}
    except Exception as e:
        status = "failed"
        details = {"error": str(e)}
        raise
    finally:
        finalize_maintenance_run(conn, "migrate", run_id, started, status, details)
        conn.close()


def cmd_manage_db_rematch(args):
    from ..db import manage_db
    from ednews import feeds

    feed_keys = list(args.feed) if getattr(args, 'feed', None) else None
    publication_id = getattr(args, 'publication_id', None)

    conn = get_conn()
    started, run_id = start_maintenance_run(conn, "rematch-dois", {"args": vars(args)})
    status = "failed"
    details = {}
    try:
        res = manage_db.rematch_publication_dois(
            conn,
            publication_id=publication_id,
            feed_keys=feed_keys,
            dry_run=getattr(args, 'dry_run', False),
            remove_orphan_articles=getattr(args, 'remove_orphan_articles', False),
            only_wrong=getattr(args, 'only_wrong', False),
        )

        if getattr(args, 'dry_run', False):
            print(f"dry-run: would clear DOIs for feeds: {', '.join(res.get('feeds', {}).keys())}")
        else:
            print(
                f"cleared {res.get('total_cleared', 0)} item DOIs; postprocessor updates: {res.get('postprocessor_results', {})}; removed_orphan_articles={res.get('removed_orphan_articles', 0)}; articles_created={res.get('articles_created_total', 0)}; articles_updated={res.get('articles_updated_total', 0)}"
            )
        status = "ok"
        details = res
    except Exception as e:
        status = "failed"
        details = {"error": str(e)}
        raise
    finally:
        finalize_maintenance_run(conn, "rematch-dois", run_id, started, status, details)
        conn.close()


def cmd_manage_db_sync_publications(args):
    from ..db import manage_db
    from ednews import feeds
    feeds_list = feeds.load_feeds()
    if not feeds_list:
        print("No feeds found; nothing to sync")
        return
    conn = get_conn()
    started, run_id = start_maintenance_run(conn, "sync-publications", {"feed_count": len(feeds_list)})
    status = "failed"
    details = {}
    try:
        count = manage_db.sync_publications_from_feeds(conn, feeds_list)
        print(f"synced {count} publications")
        status = "ok"
        details = {"synced": count}
    except Exception as e:
        status = "failed"
        details = {"error": str(e)}
        raise
    finally:
        finalize_maintenance_run(conn, "sync-publications", run_id, started, status, details)
        conn.close()


def cmd_manage_db_run_all(args):
    from ednews.db import manage_db
    from ednews import feeds

    print("Running migrations...")
    conn = get_conn()
    try:
        mig_ok = manage_db.migrate_db(conn)
        print("migrate: ok" if mig_ok else "migrate: failed")
    finally:
        conn.close()

    print("Syncing publications from feeds...")
    try:
        conn = get_conn()
        try:
            feeds_list = feeds.load_feeds()
            if feeds_list:
                synced = manage_db.sync_publications_from_feeds(conn, feeds_list)
                print(f"synced {synced} publications")
            else:
                print("no feeds found; skipping sync-publications")
        finally:
            conn.close()
    except Exception:
        logger.exception("sync-publications failed")

    print("Running cleanup steps...")
    try:
        conn = get_conn()
        try:
            if getattr(args, 'dry_run', False):
                cur = conn.cursor()
                params = []
                where_clauses = ["(COALESCE(title, '') = '' AND COALESCE(abstract, '') = '')"]
                if getattr(args, 'older_than_days', None) is not None:
                    from datetime import datetime, timezone, timedelta
                    cutoff = (datetime.now(timezone.utc) - timedelta(days=int(args.older_than_days))).isoformat()
                    where_clauses.append("(COALESCE(fetched_at, '') != '' AND COALESCE(fetched_at, '') < ? OR COALESCE(published, '') != '' AND COALESCE(published, '') < ?)")
                    params.extend([cutoff, cutoff])
                where_sql = " AND ".join(where_clauses)
                cur.execute(f"SELECT COUNT(1) FROM articles WHERE {where_sql}", tuple(params))
                row = cur.fetchone()
                count_empty = row[0] if row and row[0] else 0
                count_filtered = manage_db.cleanup_filtered_titles(conn, filters=None, dry_run=True)
                print(f"dry-run: would delete {count_empty} empty rows and {count_filtered} filtered-title rows")
            else:
                deleted_empty = manage_db.cleanup_empty_articles(conn, older_than_days=getattr(args, 'older_than_days', None))
                deleted_filtered = manage_db.cleanup_filtered_titles(conn, filters=None, dry_run=False)
                total_deleted = (deleted_empty or 0) + (deleted_filtered or 0)
                print(f"deleted {total_deleted} rows ({deleted_empty} empty, {deleted_filtered} filtered-title)")
        finally:
            conn.close()
    except Exception:
        logger.exception("cleanup steps failed")

    print("Running VACUUM...")
    try:
        conn = get_conn()
        try:
            ok = manage_db.vacuum_db(conn)
            print("vacuum: ok" if ok else "vacuum: failed")
        finally:
            conn.close()
    except Exception:
        logger.exception("vacuum failed in run-all")
