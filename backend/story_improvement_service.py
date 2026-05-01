"""
Story Improvement Retrieval Pipeline
Given a user query, returns the most relevant stories with:
  - Neutral summary
  - Relevance explanation
  - Weakness analysis
  - Suggested improvement articles (supporting, contradicting, expanding)

Uses a two-stage retrieval waterfall (keyword → semantic) with
pgvector for all vector operations.
"""

import math
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

from clustering_service import get_embedding_model

logger = logging.getLogger(__name__)

STOP_WORDS = {
    "the", "a", "an", "and", "or", "but", "in", "on", "at", "to",
    "for", "of", "with", "by", "from", "as", "is", "was", "are",
    "were", "been", "be", "have", "has", "had", "do", "does", "did",
    "will", "would", "could", "should", "may", "might", "shall",
    "can", "need", "must", "it", "its", "not", "no", "nor", "so",
    "if", "then", "than", "too", "very", "just", "about", "above",
    "after", "before", "between", "under", "over", "again", "once",
    "here", "there", "when", "where", "why", "how", "all", "each",
    "every", "both", "few", "more", "most", "other", "some", "such",
    "only", "own", "same", "that", "this", "these", "those", "what",
    "which", "who", "whom", "up", "out", "off", "down", "into",
    "during", "through", "while", "also", "back", "now", "new",
    "one", "two", "three", "says", "said", "briefly",
    "amid", "via", "per", "vs", "etc", "being", "found", "seen",
    "shut", "broken", "using", "uses", "became", "become", "next",
    "last", "week", "day", "month", "year", "news", "update", "report",
}

RELATIONSHIP_WEIGHTS = {
    "contradicts": 1.0,
    "expands": 0.7,
    "supports": 0.5,
    "divergent_framing": 0.8,
}


def preprocess_query(query: str) -> tuple:
    """
    Returns (tokens: List[str], embedding_str: str).
    Normalizes, strips stop words, generates 384d embedding.
    """
    raw = query.strip().lower()
    tokens = [t for t in raw.split() if len(t) > 2 and t not in STOP_WORDS]

    if not tokens:
        tokens = [t for t in raw.split() if len(t) > 2]
    if not tokens:
        tokens = [raw]

    vector = [float(x) for x in get_embedding_model().encode(query)]
    embedding_str = str(vector)

    return tokens, embedding_str


def retrieve_candidate_stories(
    session: Session,
    tokens: List[str],
    embedding_str: str,
    top_k: int
) -> List[Dict[str, Any]]:
    """
    Stage 1: Keyword filter with semantic cross-validation.
    Stage 2: Pure semantic fallback if Stage 1 is insufficient.
    Returns list of story dicts with distance and match metadata.
    """
    min_required_tokens = 2 if len(tokens) >= 3 else 1

    token_match_cases = " + ".join(
        [f"(CASE WHEN LOWER(a.title) LIKE :t{i} OR LOWER(a.summary) LIKE :t{i} "
         f"OR LOWER(s.title) LIKE :t{i} THEN 1 ELSE 0 END)"
         for i in range(len(tokens))]
    )
    token_where = " OR ".join(
        [f"LOWER(a.title) LIKE :t{i} OR LOWER(a.summary) LIKE :t{i} "
         f"OR LOWER(s.title) LIKE :t{i}"
         for i in range(len(tokens))]
    )

    params = {f"t{i}": f"%{t}%" for i, t in enumerate(tokens)}
    params["v"] = embedding_str
    params["min_tokens"] = min_required_tokens
    params["limit"] = top_k

    keyword_sql = text(f"""
        WITH article_scores AS (
            SELECT sa.story_id, sa.article_id,
                   ({token_match_cases}) as tokens_hit
            FROM story_articles sa
            JOIN rss_articles a ON sa.article_id = a.id
            JOIN stories s ON sa.story_id = s.id
            WHERE ({token_where})
              AND a.body_fetched = TRUE
              AND a.body IS NOT NULL
              AND TRIM(a.body) != ''
        )
        SELECT s.id, s.title, s.summary, s.article_count,
               s.bias_distribution, s.disagreement_score,
               s.confidence_score, s.updated_at, s.created_at,
               COUNT(DISTINCT ascore.article_id) as matched_articles_count,
               MAX(ascore.tokens_hit) as best_token_match,
               (s.centroid_vector <=> CAST(:v AS vector)) as distance
        FROM stories s
        INNER JOIN article_scores ascore ON s.id = ascore.story_id
        WHERE s.article_count >= 2
        GROUP BY s.id
        HAVING MAX(ascore.tokens_hit) >= :min_tokens
           AND (s.centroid_vector <=> CAST(:v AS vector)) < 0.7
        ORDER BY best_token_match DESC, distance ASC
        LIMIT :limit
    """)

    candidates = []
    try:
        rows = session.execute(keyword_sql, params).fetchall()
        for row in rows:
            candidates.append({
                **dict(row._mapping),
                "match_type": "keyword+semantic"
            })
    except Exception as e:
        logger.error(f"Stage 1 (Keyword) failed: {e}")

    if len(candidates) < top_k:
        already_ids = {c["id"] for c in candidates}
        remaining = top_k - len(candidates)

        semantic_sql = text("""
            SELECT id, title, summary, article_count,
                   bias_distribution, disagreement_score,
                   confidence_score, updated_at, created_at,
                   0 as matched_articles_count,
                   0 as best_token_match,
                   (centroid_vector <=> CAST(:v AS vector)) as distance
            FROM stories
            WHERE centroid_vector IS NOT NULL
              AND article_count >= 2
              AND (centroid_vector <=> CAST(:v AS vector)) < 0.75
            ORDER BY distance ASC
            LIMIT :limit
        """)

        try:
            rows = session.execute(semantic_sql, {
                "v": embedding_str, "limit": remaining + len(already_ids)
            }).fetchall()

            for row in rows:
                d = dict(row._mapping)
                if d["id"] not in already_ids:
                    d["match_type"] = "semantic"
                    candidates.append(d)
                    if len(candidates) >= top_k:
                        break
        except Exception as e:
            logger.error(f"Stage 2 (Semantic) failed: {e}")

    return candidates


def rank_stories(candidates: List[Dict[str, Any]], top_k: int) -> List[Dict[str, Any]]:
    """
    Composite score:
      semantic_similarity * 0.5 +
      log(article_count + 1) * 0.2 +
      disagreement_score   * 0.2 +
      recency_score        * 0.1
    """
    now = datetime.utcnow()
    max_log_count = 1.0

    for c in candidates:
        log_val = math.log(c.get("article_count", 1) + 1)
        if log_val > max_log_count:
            max_log_count = log_val

    scored = []
    for c in candidates:
        distance = c.get("distance", 1.0) or 1.0
        semantic_sim = max(0.0, 1.0 - float(distance))

        size_signal = math.log(c.get("article_count", 1) + 1) / max_log_count

        disagreement = float(c.get("disagreement_score", 0) or 0)

        updated_at = c.get("updated_at") or now
        if isinstance(updated_at, str):
            try:
                updated_at = datetime.fromisoformat(updated_at)
            except ValueError:
                updated_at = now
        hours_old = max(0, (now - updated_at).total_seconds() / 3600)
        recency = max(0.0, 1.0 - (hours_old / 168))

        score = (
            semantic_sim * 0.5 +
            size_signal * 0.2 +
            disagreement * 0.2 +
            recency * 0.1
        )

        c["relevance_score"] = round(score, 4)
        scored.append(c)

    scored.sort(key=lambda x: x["relevance_score"], reverse=True)
    return scored[:top_k]


def select_articles_for_stories(
    session: Session,
    story_ids: List[str],
    articles_per_story: int
) -> Dict[str, List[Dict[str, Any]]]:
    """
    Batch-fetches articles for all stories at once (avoids N+1).
    Ranks by recency, source diversity, and relationship weight.
    Ensures at least 1 contradicting + 2 bias categories if available.
    """
    if not story_ids:
        return {}

    placeholders = ", ".join([f":sid_{i}" for i in range(len(story_ids))])
    id_params = {f"sid_{i}": sid for i, sid in enumerate(story_ids)}

    articles_sql = text(f"""
        SELECT sa.story_id, a.id, a.title, a.outlet, a.bias, a.url,
               a.fetched_at, a.ai_summary, a.bias_score
        FROM story_articles sa
        JOIN rss_articles a ON sa.article_id = a.id
        WHERE sa.story_id IN ({placeholders})
          AND a.body_fetched = TRUE
          AND a.body IS NOT NULL
          AND TRIM(a.body) != ''
        ORDER BY a.fetched_at DESC
    """)

    article_rows = session.execute(articles_sql, id_params).fetchall()

    story_articles: Dict[str, List[Dict]] = {sid: [] for sid in story_ids}
    all_article_ids = set()

    for row in article_rows:
        d = dict(row._mapping)
        story_articles[d["story_id"]].append(d)
        all_article_ids.add(d["id"])

    relationships: Dict[str, Dict[str, str]] = {}
    if all_article_ids:
        src_placeholders = ", ".join(
            [f":src_{i}" for i in range(len(all_article_ids))])
        tgt_placeholders = ", ".join(
            [f":tgt_{i}" for i in range(len(all_article_ids))])
        aid_list = list(all_article_ids)
        aid_params = {f"src_{i}": aid for i, aid in enumerate(aid_list)}
        aid_params.update({f"tgt_{i}": aid for i, aid in enumerate(aid_list)})

        rel_sql = text(f"""
            SELECT source_id, target_id, relationship_type
            FROM article_relationships
            WHERE source_id IN ({src_placeholders})
               OR target_id IN ({tgt_placeholders})
        """)

        try:
            rel_rows = session.execute(rel_sql, aid_params).fetchall()
            for rr in rel_rows:
                d = dict(rr._mapping)
                relationships.setdefault(d["source_id"], {})[
                    d["target_id"]] = d["relationship_type"]
                relationships.setdefault(d["target_id"], {})[
                    d["source_id"]] = d["relationship_type"]
        except Exception as e:
            logger.warning(f"Relationship fetch warning: {e}")

    now = datetime.utcnow()
    result: Dict[str, List[Dict[str, Any]]] = {}

    for sid in story_ids:
        articles = story_articles.get(sid, [])
        if not articles:
            result[sid] = []
            continue

        unique_outlets = set(a.get("outlet", "") for a in articles)
        outlet_count = max(1, len(unique_outlets))

        scored_articles = []
        for art in articles:
            fetched = art.get("fetched_at") or now
            if isinstance(fetched, str):
                try:
                    fetched = datetime.fromisoformat(fetched)
                except ValueError:
                    fetched = now
            hours_old = max(0, (now - fetched).total_seconds() / 3600)
            recency = max(0.0, 1.0 - (hours_old / 168))

            same_outlet_count = sum(1 for a in articles if a.get(
                "outlet") == art.get("outlet"))
            source_diversity = 1.0 / same_outlet_count

            art_rels = relationships.get(art["id"], {})
            best_rel_weight = 0.0
            best_rel_type = "supports"
            for _, rel_type in art_rels.items():
                w = RELATIONSHIP_WEIGHTS.get(rel_type, 0.3)
                if w > best_rel_weight:
                    best_rel_weight = w
                    best_rel_type = rel_type

            article_score = (
                recency * 0.4 +
                source_diversity * 0.2 +
                best_rel_weight * 0.4
            )

            scored_articles.append({
                "id": art["id"],
                "title": art["title"],
                "outlet": art.get("outlet", ""),
                "bias": (art.get("bias") or "center").lower(),
                "url": art.get("url", ""),
                "relation": best_rel_type,
                "score": round(article_score, 4),
            })

        scored_articles.sort(key=lambda x: x["score"], reverse=True)

        selected = []
        has_contradiction = False
        bias_categories = set()

        for art in scored_articles:
            if art["relation"] == "contradicts":
                selected.append(art)
                has_contradiction = True
                bias_categories.add(art["bias"])
                break

        for art in scored_articles:
            if art["id"] in {s["id"] for s in selected}:
                continue
            if len(selected) >= articles_per_story:
                break

            if len(bias_categories) < 2 and art["bias"] not in bias_categories:
                selected.insert(1, art)
                bias_categories.add(art["bias"])
                continue

            selected.append(art)
            bias_categories.add(art["bias"])

        result[sid] = selected[:articles_per_story]

    return result


def analyze_weaknesses(story: Dict[str, Any]) -> List[str]:
    """
    Returns a list of weakness flags for a story.
    """
    weaknesses = []
    now = datetime.utcnow()

    bias_dist = story.get("bias_distribution") or {}
    total = sum(bias_dist.values()) if bias_dist else 0
    if total > 0:
        for label, count in bias_dist.items():
            if count / total > 0.70:
                weaknesses.append(
                    f"bias_imbalance: {label} dominates at {round(count/total*100)}%")
                break

    disagreement = float(story.get("disagreement_score", 0) or 0)
    if disagreement < 0.2:
        weaknesses.append(
            "low_narrative_diversity: sources largely agree (disagreement < 0.2)")

    updated_at = story.get("updated_at") or now
    if isinstance(updated_at, str):
        try:
            updated_at = datetime.fromisoformat(updated_at)
        except ValueError:
            updated_at = now
    if (now - updated_at) > timedelta(hours=24):
        weaknesses.append("outdated: no new articles in the last 24 hours")

    return weaknesses


def discover_improvement_articles(
    session: Session,
    story: Dict[str, Any],
    existing_article_ids: set,
    embedding_str: str,
    max_suggestions: int = 3
) -> List[Dict[str, Any]]:
    """
    Finds articles NOT in this story that could improve its coverage.
    Searches using both query embedding AND story centroid proximity.
    Prioritizes contradicting relationships and missing bias categories.
    """
    story_id = story["id"]
    bias_dist = story.get("bias_distribution") or {}
    present_biases = set(k for k, v in bias_dist.items() if v > 0)
    missing_biases = {"left", "center", "right"} - present_biases

    if not existing_article_ids:
        existing_article_ids = set()

    exclude_placeholders = ""
    exclude_params = {}
    if existing_article_ids:
        exclude_placeholders = " AND a.id NOT IN ({})".format(
            ", ".join([f":ex_{i}" for i in range(len(existing_article_ids))])
        )
        exclude_params = {f"ex_{i}": aid for i,
                          aid in enumerate(existing_article_ids)}

    sql = text(f"""
        SELECT a.id, a.title, a.outlet, a.bias, a.url,
               (a.embedding <=> CAST(:v AS vector)) as distance
        FROM rss_articles a
        WHERE a.embedding IS NOT NULL
          AND (a.embedding <=> CAST(:v AS vector)) < 0.65
          {exclude_placeholders}
        ORDER BY distance ASC
        LIMIT :limit
    """)

    params = {"v": embedding_str,
              "limit": max_suggestions * 3, **exclude_params}

    suggestions = []
    try:
        rows = session.execute(sql, params).fetchall()

        for row in rows:
            d = dict(row._mapping)
            art_bias = (d.get("bias") or "center").lower()

            if art_bias in missing_biases:
                reason = f"fills gap: adds missing '{art_bias}' perspective"
            elif float(d.get("distance", 1)) < 0.4:
                reason = "adds contradiction or closely related viewpoint"
            else:
                reason = "expands coverage with additional perspective"

            suggestions.append({
                "id": d["id"],
                "title": d["title"],
                "outlet": d.get("outlet", ""),
                "reason": reason,
                "distance": round(float(d.get("distance", 0)), 4),
            })

            if len(suggestions) >= max_suggestions:
                break

    except Exception as e:
        logger.error(f"Improvement discovery failed: {e}")

    return suggestions


def run_improvement_pipeline(
    session: Session,
    query: str,
    top_k_stories: int = 5,
    articles_per_story: int = 5
) -> List[Dict[str, Any]]:
    """
    Main entry point. Executes Steps 1–8 in sequence.
    """
    tokens, embedding_str = preprocess_query(query)
    logger.info(f"[Improve] Query tokens: {tokens}")

    candidates = retrieve_candidate_stories(
        session, tokens, embedding_str, top_k=top_k_stories * 2)
    if not candidates:
        return []

    ranked = rank_stories(candidates, top_k=top_k_stories)
    if not ranked:
        return []

    story_ids = [s["id"] for s in ranked]

    story_articles = select_articles_for_stories(
        session, story_ids, articles_per_story)

    output = []
    for story in ranked:
        sid = story["id"]
        articles = story_articles.get(sid, [])
        existing_ids = {a["id"] for a in articles}

        match_type = story.get("match_type", "semantic")
        token_hits = story.get("best_token_match", 0)
        dist = round(float(story.get("distance", 1) or 1), 4)
        if match_type == "keyword+semantic":
            reason = f"Matched {token_hits} keyword(s) with semantic distance {dist}"
        else:
            reason = f"Semantic similarity match (distance: {dist})"

        weaknesses = analyze_weaknesses(story)

        improvements = discover_improvement_articles(
            session, story, existing_ids, embedding_str
        )

        output.append({
            "story_id": sid,
            "title": story.get("title", ""),
            "relevance_score": story.get("relevance_score", 0),
            "reason": reason,
            "neutral_brief": story.get("summary") or "",
            "article_count": story.get("article_count", 0),
            "bias_distribution": story.get("bias_distribution"),
            "weaknesses": weaknesses,
            "articles": articles,
            "improvement_articles": improvements,
        })

    return output
