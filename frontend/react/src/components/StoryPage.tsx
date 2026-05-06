import { useEffect, useState } from "react";
import { useParams, Link } from "react-router-dom";
import { API_BASE } from "../config";
import {
  BiasTag,
  ConfidenceBadge,
  BiasBar,
  LoadingState,
  formatDate,
} from "./Shared";
import {
  ArrowLeft,
  ExternalLink,
  Brain,
  Newspaper,
  AlertTriangle,
  Loader2,
} from "lucide-react";

interface BiasReport {
  emotional_language_used: boolean;
  loaded_terms: string[];
  missing_viewpoints: string[];
  bias_score: number;
  political_alignment: string;
  bias_reasoning: string;
  confidence: number;
  ambiguity_detected: boolean;
}

interface Relationship {
  source_url: string;
  target_url: string;
  relationship_type:
    | "supports"
    | "contradicts"
    | "expands"
    | "divergent_framing";
  strength: number;
  evidence: string;
}

interface Article {
  id: string;
  title: string;
  outlet: string;
  bias: string;
  url: string;
  published: string;
  ai_summary: string | null;
}

interface Story {
  id: string;
  title: string;
  summary: string | null;
  article_count: number;
  bias_distribution: Record<string, number> | null;
  disagreement_score: number | null;
  confidence_score: number | null;
  articles: Article[];
}

interface AnalysisResult {
  balanced_brief: string;
  comparison: string;
  visualization_path: string;
  metrics: {
    diversity: number;
    confidence: number;
    agreement: number;
    is_polarized: boolean;
  };
  relationships: Relationship[];
  bias_reports: Record<string, BiasReport>;
  summaries: Record<string, string>;
  errors: string[];
}

export default function StoryPage() {
  const { id } = useParams();
  const [story, setStory] = useState<Story | null>(null);
  const [analysis, setAnalysis] = useState<AnalysisResult | null>(null);
  const [loading, setLoading] = useState(true);
  const [analyzing, setAnalyzing] = useState(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    if (!id) return;
    setLoading(true);
    fetch(`${API_BASE}/stories/${id}`)
      .then((res) => {
        if (!res.ok) throw new Error("Story not found");
        return res.json();
      })
      .then(setStory)
      .catch((e) => setError(e.message))
      .finally(() => setLoading(false));
  }, [id]);

  useEffect(() => {
    if (!id || !story) return;
    setAnalyzing(true);
    fetch(`${API_BASE}/stories/${id}/analysis`)
      .then((res) => res.json())
      .then(setAnalysis)
      .catch(() => {})
      .finally(() => setAnalyzing(false));
  }, [id, story]);

  if (loading) return <LoadingState message="Loading story details..." />;

  if (error || !story)
    return (
      <div className="container mx-auto px-4 py-12 text-center">
        <p className="text-red-500 mb-4">{error || "Story not found"}</p>
        <Link to="/" className="btn btn-outline inline-flex">
          <ArrowLeft size={14} /> Back to Home
        </Link>
      </div>
    );

  const disagreement = Math.round((story.disagreement_score || 0) * 100);

  return (
    <div className="container mx-auto px-4 py-8 max-w-5xl">
      <Link
        to="/"
        className="inline-flex items-center gap-1.5 text-sm mb-8 no-underline transition-colors"
        style={{ color: "var(--muted-foreground)" }}
      >
        <ArrowLeft size={16} /> Back to Dashboard
      </Link>

      <div className="space-y-8">
        {/* Header Section */}
        <header className="space-y-4">
          <h1 className="text-3xl md:text-4xl font-bold tracking-tight leading-tight">
            {story.title}
          </h1>
          <div
            className="flex flex-wrap items-center gap-4 text-sm"
            style={{ color: "var(--muted-foreground)" }}
          >
            <div className="flex items-center gap-1.5">
              <Newspaper size={16} />
              <span>{story.article_count} Sources</span>
            </div>
            <span>•</span>
            <div className="flex items-center gap-1.5">
              <AlertTriangle
                size={16}
                style={{ color: disagreement > 60 ? "#dc2626" : "inherit" }}
              />
              <span
                style={{
                  color: disagreement > 60 ? "#dc2626" : "inherit",
                  fontWeight: disagreement > 60 ? 600 : 400,
                }}
              >
                {disagreement}% Disagreement
              </span>
            </div>
            {story.confidence_score && (
              <>
                <span>•</span>
                <ConfidenceBadge score={story.confidence_score} />
              </>
            )}
          </div>
        </header>

        {/* Top Metrics Cards - hidden while metrics logic is being refined
        {analysis && (
          <div className="grid grid-cols-2 lg:grid-cols-4 gap-4">
            <MetricCard
              label="Diversity"
              value={`${(analysis.metrics.diversity * 100).toFixed(0)}%`}
              good={analysis.metrics.diversity > 0.5}
            />
            <MetricCard
              label="Consensus"
              value={
                analysis.metrics.agreement > 0.8
                  ? "High"
                  : analysis.metrics.agreement > 0.5
                    ? "Moderate"
                    : "Low"
              }
              good={analysis.metrics.agreement > 0.5}
            />
            <MetricCard
              label="AI Confidence"
              value={`${(analysis.metrics.confidence * 100).toFixed(0)}%`}
              good={analysis.metrics.confidence > 0.7}
            />
            <MetricCard
              label="Narrative"
              value={analysis.metrics.is_polarized ? "Polarized" : "Balanced"}
              good={!analysis.metrics.is_polarized}
            />
          </div>
        )}
        */}

        <div className="grid grid-cols-1 lg:grid-cols-3 gap-8">
          {/* Main Content */}
          <div className="lg:col-span-2 space-y-8">
            {/* 1. Neutral Synthesis */}
            <div
              className="card"
              style={{
                background: "var(--secondary)",
                borderTop: "4px solid var(--primary)",
              }}
            >
              <div className="card-header flex items-center justify-between">
                <h2 className="card-title flex items-center gap-2 font-bold">
                  <Brain size={18} className="text-primary" />
                  Neutral Synthesis
                </h2>
                {analyzing && (
                  <Loader2 size={16} className="spinner text-primary" />
                )}
              </div>
              <div className="card-content">
                {analysis ? (
                  <div
                    className="prose-body italic"
                    style={{ fontSize: "1.125rem", lineHeight: "1.7" }}
                  >
                    {analysis.balanced_brief}
                  </div>
                ) : analyzing ? (
                  <div className="py-8 text-center">
                    <p
                      className="text-sm animate-pulse"
                      style={{ color: "var(--muted-foreground)" }}
                    >
                      Processing cross-source analysis...
                    </p>
                  </div>
                ) : (
                  <p
                    className="text-sm py-4 text-center"
                    style={{ color: "var(--muted-foreground)" }}
                  >
                    Synthesis currently unavailable.
                  </p>
                )}
              </div>
            </div>

            {/* 2. Relationships (Cross-Examination) */}
            {analysis && analysis.relationships?.length > 0 && (
              <div className="card">
                <div className="card-header">
                  <h3
                    className="card-title text-sm uppercase tracking-widest font-bold"
                    style={{ color: "var(--muted-foreground)" }}
                  >
                    Narrative Connections
                  </h3>
                </div>
                <div className="card-content">
                  <div className="grid grid-cols-1 gap-3">
                    {analysis.relationships.map((rel, i) => {
                      const isContra = rel.relationship_type === "contradicts";
                      return (
                        <div
                          key={i}
                          className="rounded-lg p-3 border-l-4"
                          style={{
                            borderColor: isContra ? "#dc2626" : "#16a34a",
                            background: isContra ? "#fef2f2" : "#f0fdf4",
                          }}
                        >
                          <div className="flex justify-between items-start mb-1.5">
                            <span
                              className="text-xs font-bold uppercase"
                              style={{
                                color: isContra ? "#991b1b" : "#166534",
                              }}
                            >
                              {rel.relationship_type}
                            </span>
                            <span
                              className="text-[10px] font-medium"
                              style={{ color: "var(--muted-foreground)" }}
                            >
                              Strength: {Math.round(rel.strength * 100)}%
                            </span>
                          </div>
                          <p className="text-xs leading-relaxed">
                            {rel.evidence}
                          </p>
                        </div>
                      );
                    })}
                  </div>
                </div>
              </div>
            )}

            {/* 3. Perspective Comparison */}
            {analysis && (
              <div className="card">
                <div className="card-header">
                  <h3
                    className="card-title text-sm uppercase tracking-widest font-bold"
                    style={{ color: "var(--muted-foreground)" }}
                  >
                    Perspective Comparison
                  </h3>
                </div>
                <div className="card-content">
                  <div className="prose-body whitespace-pre-wrap text-sm leading-relaxed">
                    {analysis.comparison}
                  </div>
                </div>
              </div>
            )}

            {/* 4. Detailed Source Breakdown */}
            <div className="space-y-4">
              <h2 className="text-xl font-bold">Source-by-Source Analysis</h2>
              <div className="grid gap-6">
                {story.articles.map((article) => {
                  const biasReport = analysis?.bias_reports[article.url];
                  return (
                    <div key={article.id} className="card overflow-visible">
                      <div className="card-content p-5">
                        <div className="flex justify-between items-start gap-4 mb-3">
                          <div className="flex items-center gap-3">
                            <div className="flex items-center gap-2 px-3 py-1 rounded-full border bg-muted/20 text-xs font-bold">
                              <BiasTag
                                bias={
                                  biasReport?.political_alignment ||
                                  article.bias
                                }
                              />
                              <span className="opacity-40">|</span>
                              <span className="tracking-tight">
                                {article.outlet}
                              </span>
                            </div>
                            <span
                              className="text-[10px]"
                              style={{ color: "var(--muted-foreground)" }}
                            >
                              {formatDate(article.published)}
                            </span>
                          </div>
                          <div className="flex gap-2">
                            <Link
                              to={`/BrowseArticles/${article.id}`}
                              className="btn btn-ghost btn-sm text-xs border"
                            >
                              Read
                            </Link>
                            <a
                              href={article.url}
                              target="_blank"
                              rel="noreferrer"
                              className="btn btn-outline btn-sm p-1.5"
                            >
                              <ExternalLink size={14} />
                            </a>
                          </div>
                        </div>

                        <h3 className="font-bold text-lg mb-3 leading-tight">
                          {article.title}
                        </h3>

                        <p
                          className="text-sm mb-4 leading-relaxed italic"
                          style={{
                            color: "var(--muted-foreground)",
                            fontFamily: "var(--font-serif)",
                          }}
                        >
                          {article.ai_summary || "No AI summary available."}
                        </p>

                        {/* Extended Bias Metrics */}
                        {biasReport && (
                          <div className="mt-4 pt-4 border-t space-y-3">
                            <div className="flex flex-wrap gap-4 text-xs">
                              <div>
                                <strong>Bias Score:</strong>{" "}
                                {biasReport.bias_score}/10
                              </div>
                              <div>
                                <strong>Confidence:</strong>{" "}
                                {Math.round(biasReport.confidence * 100)}%
                              </div>
                              <div>
                                <strong>Emotional Language:</strong>{" "}
                                {biasReport.emotional_language_used
                                  ? "Detected"
                                  : "None"}
                              </div>
                            </div>

                            <div className="text-xs rounded p-3 bg-muted/30 border-l-2 border-primary/20">
                              <p className="font-medium mb-1">AI Reasoning:</p>
                              <p className="leading-relaxed opacity-80">
                                {biasReport.bias_reasoning}
                              </p>
                            </div>

                            {biasReport.missing_viewpoints?.length > 0 && (
                              <div className="text-xs">
                                <p className="font-bold text-destructive mb-1">
                                  Perspective Gaps:
                                </p>
                                <ul className="list-disc pl-4 space-y-1 opacity-80">
                                  {biasReport.missing_viewpoints.map(
                                    (vp, i) => (
                                      <li key={i}>{vp}</li>
                                    ),
                                  )}
                                </ul>
                              </div>
                            )}
                          </div>
                        )}
                      </div>
                    </div>
                  );
                })}
              </div>
            </div>

            {/* Errors */}
            {analysis && analysis.errors?.length > 0 && (
              <div className="card" style={{ borderColor: "#fecaca" }}>
                <div className="card-content" style={{ background: "#fef2f2" }}>
                  <h3
                    className="text-sm font-semibold mb-2"
                    style={{ color: "#991b1b" }}
                  >
                    Analysis Warnings
                  </h3>
                  <ul
                    className="text-xs list-disc pl-4"
                    style={{ color: "#dc2626" }}
                  >
                    {analysis.errors.map((e, i) => (
                      <li key={i}>{e}</li>
                    ))}
                  </ul>
                </div>
              </div>
            )}
          </div>

          {/* Sidebar */}
          <div className="space-y-6">
            <div className="card">
              <div className="card-header">
                <h3 className="card-title text-sm">Bias Distribution</h3>
              </div>
              <div className="card-content">
                <BiasBar distribution={story.bias_distribution} />
                <p
                  className="text-xs mt-3"
                  style={{ color: "var(--muted-foreground)" }}
                >
                  Distribution of political alignment across all{" "}
                  {story.article_count} sources in this story cluster.
                </p>
              </div>
            </div>

            {analysis?.visualization_path && (
              <div className="card">
                <div className="card-header">
                  <h3 className="card-title text-sm">Sentiment Visual</h3>
                </div>
                <div className="card-content">
                  <img
                    src={`${analysis.visualization_path}`}
                    alt="Bias Chart"
                    className="w-full rounded border"
                    style={{ background: "#fff" }}
                  />
                </div>
              </div>
            )}

            <div className="card" style={{ background: "var(--secondary)" }}>
              <div className="card-header">
                <h3 className="card-title text-sm">Core Questions</h3>
              </div>
              <div className="card-content">
                <ul className="text-xs space-y-3 list-disc pl-4 opacity-80">
                  <li>What are the key confirmed facts across all reports?</li>
                  <li>
                    How do specific outlets differ in their emotional framing?
                  </li>
                  <li>
                    Are there significant contradictions in reported data?
                  </li>
                  <li>Which perspectives are consistently omitted?</li>
                </ul>
              </div>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}

function MetricCard({
  label,
  value,
  good,
}: {
  label: string;
  value: string;
  good: boolean;
}) {
  return (
    <div className="card">
      <div className="card-content py-4 text-center">
        <div
          className="text-[10px] uppercase tracking-wider font-bold mb-1"
          style={{ color: "var(--muted-foreground)" }}
        >
          {label}
        </div>
        <div
          className="text-xl font-bold"
          style={{ color: good ? "#16a34a" : "#ea580c" }}
        >
          {value}
        </div>
      </div>
    </div>
  );
}
