
from typing import List, Dict, Literal, Optional, Any
from pydantic import BaseModel, Field

class BiasReport(BaseModel):
    emotional_language_used: bool
    loaded_terms: List[str]
    missing_viewpoints: List[str]
    bias_score: int
    political_alignment: Literal["Left", "Center", "Right"]
    bias_reasoning: str
    confidence: float
    ambiguity_detected: bool

class BatchArticleAnalysis(BaseModel):
    url: str
    summary: str
    bias_report: BiasReport

class BatchAnalysisResult(BaseModel):
    articles: List[BatchArticleAnalysis]

class RelationshipLink(BaseModel):
    source_url: str
    target_url: str
    relationship_type: Literal["supports", "contradicts", "expands", "divergent_framing"]
    strength: float
    evidence: str

class CrossExaminationResult(BaseModel):
    links: List[RelationshipLink]

class AnalysisResultSchema(BaseModel):
    balanced_brief: str
    comparison: str
    visualization_path: str
    metrics: Dict[str, Any]
    relationships: List[Dict[str, Any]]
    bias_reports: Dict[str, BiasReport]
    summaries: Dict[str, str]
    errors: List[str]
