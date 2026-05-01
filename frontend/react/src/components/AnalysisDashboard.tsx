import { useState, type FormEvent } from "react";
import { API_BASE } from "../config";
import { Brain, Loader2 } from "lucide-react";

interface BiasReport {
  emotional_language_used: boolean;
  loaded_terms: string[];
  missing_viewpoints: string[];
  bias_score: number;
  bias_reasoning: string;
  confidence: number;
  ambiguity_detected: boolean;
}

interface Relationship {
  source_url: string;
  target_url: string;
  relationship_type: "supports" | "contradicts" | "expands" | "divergent_framing";
  strength: number;
  evidence: string;
}

interface AnalysisResult {
  summaries: Record<string, string>;
  bias_reports: Record<string, BiasReport>;
  comparison: string;
  balanced_brief: string;
  visualization_path: string;
  metrics: {
    diversity: number;
    confidence: number;
    agreement: number;
    is_polarized: boolean;
  };
  relationships: Relationship[];
  errors: string[];
}

export default function AnalysisDashboard() {
  const [topic, setTopic] = useState("");
  const [loading, setLoading] = useState(false);
  const [result, setResult] = useState<AnalysisResult | null>(null);
  const [error, setError] = useState<string | null>(null);

  const handleAnalyze = async (e: FormEvent) => {
    e.preventDefault();
    if (!topic.trim()) return;
    setLoading(true);
    setError(null);
    setResult(null);
    try {
      const res = await fetch(`${API_BASE}/analyze`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ topic: topic.trim(), urls: [] }),
      });
      if (!res.ok) throw new Error("Analysis failed");
      setResult(await res.json());
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to analyze");
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="container mx-auto px-4 py-8 max-w-5xl">
      <div className="space-y-6">
        {/* Header */}
        <div>
          <h1 className="text-3xl font-bold tracking-tight">Deep Analysis</h1>
          <p className="mt-1 text-sm" style={{ color: "var(--muted-foreground)" }}>
            AI-powered analysis of narratives, claims, and perspectives
          </p>
        </div>

        {/* Query input */}
        <div className="card">
          <div className="card-header">
            <h3 className="card-title flex items-center gap-2">
              <Brain size={18} />
              Query Input
            </h3>
          </div>
          <div className="card-content">
            <form onSubmit={handleAnalyze} className="space-y-4">
              <textarea
                placeholder="Enter your analysis query (e.g., 'Compare perspectives on the climate agreement' or 'Analyze AI regulation debate')"
                value={topic}
                onChange={(e) => setTopic(e.target.value)}
                rows={4}
                className="textarea"
                style={{ resize: "none" }}
              />
              <button
                type="submit"
                disabled={!topic.trim() || loading}
                className="btn btn-primary btn-lg w-full"
              >
                {loading ? (
                  <>
                    <Loader2 size={16} className="spinner" />
                    Analyzing…
                  </>
                ) : (
                  <>
                    <Brain size={16} />
                    Analyze
                  </>
                )}
              </button>
              {error && (
                <p className="text-sm font-medium" style={{ color: "#dc2626" }}>{error}</p>
              )}
            </form>
          </div>
        </div>

        {/* Results */}
        {result && (
          <div className="space-y-6">
            {/* Metrics */}
            <div className="grid grid-cols-2 lg:grid-cols-4 gap-4">
              <MetricCard
                label="Confidence"
                value={`${(result.metrics.confidence * 100).toFixed(0)}%`}
                good={result.metrics.confidence > 0.7}
              />
              <MetricCard
                label="Consensus"
                value={result.metrics.agreement > 0.8 ? "High" : result.metrics.agreement > 0.5 ? "Moderate" : "Low"}
                good={result.metrics.agreement > 0.5}
              />
              <MetricCard
                label="Diversity"
                value={`${(result.metrics.diversity * 100).toFixed(0)}%`}
                good={result.metrics.diversity > 0.5}
              />
              <MetricCard
                label="Narrative"
                value={result.metrics.is_polarized ? "Polarized" : "Balanced"}
                good={!result.metrics.is_polarized}
              />
            </div>

            {/* Neutral Brief */}
            <div className="card" style={{ background: "var(--secondary)" }}>
              <div className="card-header">
                <h3 className="card-title">Neutral Synthesis</h3>
              </div>
              <div className="card-content">
                <div
                  className="prose-body"
                  dangerouslySetInnerHTML={{ __html: result.balanced_brief.replace(/\n/g, "<br/>") }}
                />
              </div>
            </div>

            {/* Relationships */}
            {result.relationships?.length > 0 && (
              <div className="card">
                <div className="card-header">
                  <h3 className="card-title">Cross-Examination: Contradictions & Supports</h3>
                </div>
                <div className="card-content">
                  <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
                    {result.relationships.map((rel, i) => {
                      let srcDomain = rel.source_url;
                      let tgtDomain = rel.target_url;
                      try { srcDomain = new URL(rel.source_url).hostname; } catch {}
                      try { tgtDomain = new URL(rel.target_url).hostname; } catch {}
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
                            <span className="text-xs font-medium" style={{ color: "var(--muted-foreground)" }}>
                              {srcDomain} → {tgtDomain}
                            </span>
                            <span
                              className="badge text-xs"
                              style={{
                                background: isContra ? "#fecaca" : "#bbf7d0",
                                color: isContra ? "#991b1b" : "#166534",
                              }}
                            >
                              {rel.relationship_type}
                            </span>
                          </div>
                          <p className="text-xs leading-relaxed">{rel.evidence}</p>
                        </div>
                      );
                    })}
                  </div>
                </div>
              </div>
            )}

            {/* Bias Comparison */}
            <div className="card">
              <div className="card-header">
                <h3 className="card-title">Perspective Comparison</h3>
              </div>
              <div className="card-content">
                <div
                  className="prose-body whitespace-pre-wrap"
                  style={{ fontSize: "0.9375rem" }}
                >
                  {result.comparison}
                </div>
              </div>
            </div>

            {/* Visualization */}
            {result.visualization_path && (
              <div className="card">
                <div className="card-header">
                  <h3 className="card-title">Bias Analysis Chart</h3>
                </div>
                <div className="card-content">
                  <img
                    src={result.visualization_path}
                    alt="Bias Chart"
                    className="w-full rounded-lg"
                    style={{ background: "#fff" }}
                  />
                </div>
              </div>
            )}

            {/* Per-Source Breakdown */}
            <div className="card">
              <div className="card-header">
                <h3 className="card-title">Per-Source Breakdown</h3>
              </div>
              <div className="card-content space-y-4">
                {Object.entries(result.summaries).map(([url, summary], idx) => {
                  const bias = result.bias_reports[url];
                  let domain = url;
                  try { domain = new URL(url).hostname; } catch {}
                  return (
                    <div key={idx} className="rounded-lg border p-4" style={{ background: "var(--secondary)" }}>
                      <div className="flex flex-wrap items-center justify-between gap-2 mb-3 pb-2 border-b">
                        <a
                          href={url}
                          target="_blank"
                          rel="noreferrer"
                          className="text-sm font-semibold hover:underline"
                          style={{ color: "#2563eb" }}
                        >
                          {domain}
                        </a>
                        <div className="flex gap-2">
                          <span className="badge badge-secondary">
                            Conf: {(bias?.confidence * 100).toFixed(0)}%
                          </span>
                          <span
                            className={`badge ${(bias?.bias_score || 0) > 5 ? "badge-destructive" : "badge-secondary"}`}
                          >
                            Bias: {bias?.bias_score || "?"}/10
                          </span>
                        </div>
                      </div>
                      <p className="text-sm mb-3" style={{ color: "var(--muted-foreground)", fontFamily: "var(--font-serif)" }}>
                        {summary}
                      </p>
                      {bias && (
                        <div className="rounded border p-3 text-xs space-y-2" style={{ background: "var(--background)" }}>
                          <p className="italic" style={{ color: "var(--muted-foreground)" }}>
                            {bias.bias_reasoning || "No reasoning provided."}
                          </p>
                          <div className="flex flex-wrap gap-3">
                            <span>
                              <strong>Emotional:</strong>{" "}
                              {bias.emotional_language_used ? "Detected" : "Not detected"}
                            </span>
                            <span>
                              <strong>Loaded terms:</strong>{" "}
                              {bias.loaded_terms?.join(", ") || "None"}
                            </span>
                          </div>
                          {bias.missing_viewpoints?.length > 0 && (
                            <div>
                              <strong style={{ color: "#dc2626" }}>Missing viewpoints:</strong>
                              <ul className="list-disc pl-4 mt-1">
                                {bias.missing_viewpoints.map((vp, i) => (
                                  <li key={i}>{vp}</li>
                                ))}
                              </ul>
                            </div>
                          )}
                        </div>
                      )}
                    </div>
                  );
                })}
              </div>
            </div>

            {/* Errors */}
            {result.errors?.length > 0 && (
              <div className="card" style={{ borderColor: "#fecaca" }}>
                <div className="card-content" style={{ background: "#fef2f2" }}>
                  <h3 className="text-sm font-semibold mb-2" style={{ color: "#991b1b" }}>
                    Warnings/Errors
                  </h3>
                  <ul className="text-xs list-disc pl-4" style={{ color: "#dc2626" }}>
                    {result.errors.map((e, i) => <li key={i}>{e}</li>)}
                  </ul>
                </div>
              </div>
            )}
          </div>
        )}

        {/* Empty state */}
        {!result && !loading && (
          <div className="card" style={{ borderStyle: "dashed" }}>
            <div className="card-content py-12 text-center" style={{ color: "var(--muted-foreground)" }}>
              <Brain size={48} className="mx-auto mb-4 opacity-40" />
              <p className="font-medium">Enter a query above to start deep analysis</p>
              <p className="text-sm mt-2">
                The system will search relevant stories, extract claims,
                identify contradictions, and provide a balanced synthesis.
              </p>
            </div>
          </div>
        )}
      </div>
    </div>
  );
}

function MetricCard({ label, value, good }: { label: string; value: string; good: boolean }) {
  return (
    <div className="card">
      <div className="card-content py-4 text-center">
        <div className="text-xs font-medium mb-1" style={{ color: "var(--muted-foreground)" }}>
          {label}
        </div>
        <div className="text-xl font-bold" style={{ color: good ? "#16a34a" : "#ea580c" }}>
          {value}
        </div>
      </div>
    </div>
  );
}
