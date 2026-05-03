"""
schemas.py — Pydantic models for data validation and serialization.

This module defines the structured data formats used for internal AI processing
and external API responses. It ensures type safety and provides a clear contract
between the backend services and the frontend.
"""

from typing import List, Dict, Literal, Optional, Any
from pydantic import BaseModel, Field

# ---------------------------------------------------------------------------
# AI Analysis Schemas
# ---------------------------------------------------------------------------

class BiasReport(BaseModel):
    """Structured bias evaluation for a single article."""
    emotional_language_used: bool
    loaded_terms: List[str]
    missing_viewpoints: List[str]
    bias_score: int # Range 1-10
    political_alignment: Literal["Left", "Center", "Right"]
    bias_reasoning: str
    confidence: float # Range 0.0-1.0
    ambiguity_detected: bool

class BatchArticleAnalysis(BaseModel):
    """The result of an LLM analysis for one article in a batch."""
    url: str
    summary: str
    bias_report: BiasReport

class BatchAnalysisResult(BaseModel):
    """The root object expected from the LLM during batch processing."""
    articles: List[BatchArticleAnalysis]

# ---------------------------------------------------------------------------
# Discourse & Relationship Schemas
# ---------------------------------------------------------------------------

class RelationshipLink(BaseModel):
    """Models a discursive link between two news sources."""
    source_url: str
    target_url: str
    relationship_type: Literal["supports", "contradicts", "expands", "divergent_framing"]
    strength: float
    evidence: str

class CrossExaminationResult(BaseModel):
    """The result of an LLM analysis comparing multiple sources."""
    links: List[RelationshipLink]

# ---------------------------------------------------------------------------
# API Response Schemas
# ---------------------------------------------------------------------------

class AnalysisResultSchema(BaseModel):
    """The complete payload returned to the frontend after a deep analysis."""
    balanced_brief: str
    comparison: str
    visualization_path: str
    metrics: Dict[str, Any]
    relationships: List[Dict[str, Any]]
    bias_reports: Dict[str, BiasReport]
    summaries: Dict[str, str]
    errors: List[str]
