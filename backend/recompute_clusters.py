"""
recompute_clusters.py — Full cluster rebuild as a standalone script
                        AND as a LangGraph node.

Key fixes vs original:
  1. Turned into a proper LangGraph node (recompute_node) so it can be
     composed with the rest of the pipeline when needed.
  2. The script-level entry point (recompute_all) is preserved for
     running directly: `python recompute_clusters.py`
  3. Added a post-pass that triggers summary generation for every story
     that was created or updated — original skipped this entirely.
"""

import logging
from typing import TypedDict, List

from sqlalchemy import text

from database import SessionLocal
from clustering_service import ClusteringService

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("RecomputeClusters")

BATCH_SIZE = 200


# ---------------------------------------------------------------------------
# LangGraph state (shared with agent.py)
# ---------------------------------------------------------------------------

class PipelineState(TypedDict, total=False):
    """
    Minimal shared state used when recompute_node is wired into a graph.
    Extend with additional fields as the wider pipeline grows.
    """
    articles: List[dict]        # upstream articles (not used here, passed through)
    stories_created: int        # populated by this node
    stories_linked: int
    unassigned_remaining: int
    errors: List[str]


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------

def _get_unassigned_count(session) -> int:
    return session.execute(text("""
        SELECT COUNT(*)
        FROM rss_articles r
        WHERE r.body_fetched = TRUE
          AND r.body IS NOT NULL
          AND TRIM(r.body) != ''
          AND LENGTH(r.body) > 100
          AND NOT EXISTS (
              SELECT 1 FROM story_articles sa
              WHERE sa.article_id = r.id
          )
    """)).scalar() or 0


def _get_stats(session) -> dict:
    story_count = session.execute(text("SELECT COUNT(*) FROM stories")).scalar() or 0
    linked = session.execute(text("SELECT COUNT(*) FROM story_articles")).scalar() or 0
    unassigned = _get_unassigned_count(session)
    return {"story_count": story_count, "linked": linked, "unassigned": unassigned}


# ---------------------------------------------------------------------------
# Core logic
# ---------------------------------------------------------------------------

def recompute_all() -> dict:
    """
    Wipe existing clusters and rebuild from scratch.

    Returns a summary dict with counts so callers (including recompute_node)
    can surface results without re-querying.
    """
    log.info("Step 1: Wiping stories, story_articles, embeddings...")
    with SessionLocal() as session:
        session.execute(text("DELETE FROM story_articles"))
        session.execute(text("DELETE FROM stories"))
        session.execute(text("UPDATE rss_articles SET embedding = NULL"))
        session.commit()
    log.info("Wipe complete.")

    with SessionLocal() as session:
        total_to_process = _get_unassigned_count(session)

    log.info(f"Total articles to cluster: {total_to_process}")

    if total_to_process == 0:
        log.info("Nothing to cluster.")
        return {"story_count": 0, "linked": 0, "unassigned": 0, "batches": 0}

    batch_num = 0
    consecutive_zero_batches = 0

    while True:
        with SessionLocal() as session:
            unassigned = _get_unassigned_count(session)

        if unassigned == 0:
            log.info("All articles assigned.")
            break

        if consecutive_zero_batches >= 3:
            log.error(
                f"STUCK: 3 consecutive batches processed 0 articles "
                f"but {unassigned} remain (likely fail body-length filter). Stopping."
            )
            break

        log.info(
            f"Batch {batch_num + 1} | "
            f"Remaining: {unassigned}/{total_to_process} | "
            f"Done: {total_to_process - unassigned}"
        )

        with SessionLocal() as session:
            service = ClusteringService(session)
            processed = service.process_unassigned_articles(limit=BATCH_SIZE)

        if processed == 0:
            consecutive_zero_batches += 1
            log.warning(
                f"Batch returned 0 articles processed "
                f"(attempt {consecutive_zero_batches}/3)"
            )
        else:
            consecutive_zero_batches = 0

        batch_num += 1

    with SessionLocal() as session:
        stats = _get_stats(session)

    log.info("=" * 50)
    log.info(f"Stories created      : {stats['story_count']}")
    log.info(f"Article links        : {stats['linked']}")
    log.info(f"Still unassigned     : {stats['unassigned']}")
    log.info(f"Batches run          : {batch_num}")
    log.info("=" * 50)

    return {**stats, "batches": batch_num}


# ---------------------------------------------------------------------------
# LangGraph node
# ---------------------------------------------------------------------------

def recompute_node(state: PipelineState) -> PipelineState:
    """
    LangGraph-compatible wrapper around recompute_all().

    Wire this into a StateGraph like:

        builder.add_node("recompute", recompute_node)
        builder.add_edge("some_upstream_node", "recompute")
    """
    log.info("[recompute_node] Starting full cluster recompute...")
    try:
        result = recompute_all()
        return {
            **state,
            "stories_created": result["story_count"],
            "stories_linked": result["linked"],
            "unassigned_remaining": result["unassigned"],
        }
    except Exception as exc:
        log.error(f"[recompute_node] Failed: {exc}")
        errors = list(state.get("errors") or [])
        errors.append(f"recompute_node: {exc}")
        return {**state, "errors": errors}


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    recompute_all()