
import argparse
import logging
from datetime import datetime, timezone

from sqlalchemy.orm import Session

from database import engine
from models import RSSArticle, ArticleToRemove

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("removal.log", encoding="utf-8"),
    ],
)
log = logging.getLogger("ArticleRemoval")

def remove_flagged(dry_run: bool = False, reason_filter: str | None = None):
    """
    Delete flagged articles from rss_articles and clean up articles_to_remove.

    dry_run       — if True, print what would be deleted but change nothing
    reason_filter — if set, only process rows with this specific reason
    """
    started = datetime.now(timezone.utc)

    log.info("=" * 55)
    log.info("  Article Removal Script")
    log.info(f"  Started  : {started.strftime('%Y-%m-%d %H:%M:%S')} UTC")
    log.info(f"  Dry run  : {dry_run}")
    log.info(f"  Filter   : {reason_filter or 'all reasons'}")
    log.info("=" * 55)

    with Session(engine) as session:

        query = session.query(ArticleToRemove)
        if reason_filter:
            query = query.filter(ArticleToRemove.reason == reason_filter)

        flagged = query.order_by(ArticleToRemove.flagged_at.asc()).all()

        if not flagged:
            log.info("  No flagged articles found — nothing to do.")
            return

        log.info(f"  Found {len(flagged)} flagged article(s)\n")

        by_reason: dict[str, list] = {}
        for row in flagged:
            by_reason.setdefault(row.reason, []).append(row)

        for reason, rows in by_reason.items():
            log.info(f"  Reason: {reason}  ({len(rows)} articles)")

        log.info("")

        deleted_count = 0
        missing_count = 0
        cleaned_count = 0

        for flag in flagged:
            article = session.get(RSSArticle, flag.id)

            if article:
                log.info(
                    f"  {'[DRY RUN] Would delete' if dry_run else 'Deleting'}: "
                    f"{flag.id[:8]}…  outlet={flag.outlet}  reason={flag.reason}"
                )
                log.info(f"    title : {article.title[:80]}")
                log.info(f"    url   : {flag.url}")

                if not dry_run:
                    session.delete(article)
                    deleted_count += 1
            else:
                log.warning(
                    f"  [MISSING] {flag.id[:8]}… not found in rss_articles — "
                    f"cleaning up flag only"
                )
                missing_count += 1

            if not dry_run:
                session.delete(flag)
                cleaned_count += 1

        if not dry_run:
            session.commit()
            log.info("")
            log.info("  Committed.")

        elapsed = (datetime.now(timezone.utc) - started).seconds
        log.info("=" * 55)
        if dry_run:
            log.info(f"  DRY RUN — no changes made")
            log.info(f"  Would delete : {len(flagged)} articles")
        else:
            log.info(f"  Deleted from rss_articles   : {deleted_count}")
            log.info(f"  Already missing (cleaned)   : {missing_count}")
            log.info(f"  Flags removed               : {cleaned_count}")
        log.info(f"  Elapsed : {elapsed}s")
        log.info("=" * 55)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Delete articles flagged in articles_to_remove from rss_articles."
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview what would be deleted without making any changes.",
    )
    parser.add_argument(
        "--reason",
        type=str,
        default=None,
        choices=["invalid_url", "failed_3_times"],
        help="Only process articles flagged with this specific reason.",
    )
    args = parser.parse_args()

    if not args.dry_run:
        print("\n⚠  This will PERMANENTLY delete articles from rss_articles.")
        print(f"   Filter: {args.reason or 'all reasons'}")
        confirm = input("   Type YES to continue: ").strip().upper()
        if confirm != "YES":
            print("   Aborted.")
            exit(0)

    remove_flagged(dry_run=args.dry_run, reason_filter=args.reason)
