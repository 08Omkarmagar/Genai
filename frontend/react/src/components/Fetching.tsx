import { useState, useEffect, useRef } from "react";
import { API_BASE } from "../config";
import { Settings, Play, CheckCircle, XCircle, Loader2, RotateCcw, Trash2 } from "lucide-react";

let _isRunning = false;

type Status = "active" | "pending" | "loading" | "done" | "error";

interface FetchBtn {
  label: string;
  name: string;
  description: string;
  icon: React.ReactNode;
  status: Status;
  endpoint: string;
}

interface LogEntry {
  id: string;
  time: string;
  stage: string;
  outcome: "success" | "error" | "running";
  run_id?: string;
  elapsed_sec?: number;
  articles_new?: number;
  articles_skip?: number;
  failed_outlets?: string[];
  outlets_total?: number;
  error?: string;
}

const LS_BUTTONS = "pipeline_buttons";
const LS_LOG = "pipeline_log_v2";

const statusConfig: Record<Status, { dot: string; text: string; label: string }> = {
  active: { dot: "#0a0a0b", text: "var(--foreground)", label: "Ready" },
  pending: { dot: "#d4d4d8", text: "var(--muted-foreground)", label: "Waiting" },
  loading: { dot: "#eab308", text: "#ca8a04", label: "Running…" },
  done: { dot: "#16a34a", text: "#16a34a", label: "Complete" },
  error: { dot: "#dc2626", text: "#dc2626", label: "Failed" },
};

export default function Fetching() {
  const makeInitialButtons = (): FetchBtn[] => [
    {
      label: "Stage 01",
      name: "Fetch RSS Feeds",
      description: "Pull latest headlines from all configured RSS sources.",
      icon: <Settings size={24} />,
      status: "active",
      endpoint: "rss",
    },
    {
      label: "Stage 02",
      name: "Fetch Article Bodies",
      description: "Scrape and store full body text for unfetched articles.",
      icon: <Settings size={24} />,
      status: "pending",
      endpoint: "body",
    },
  ];

  const [buttons, setButtons] = useState<FetchBtn[]>(() => {
    try {
      const saved = localStorage.getItem(LS_BUTTONS);
      if (saved) {
        const parsed = JSON.parse(saved);
        return parsed.map((b: any) => ({
          ...makeInitialButtons()[parsed.indexOf(b)] || makeInitialButtons()[0],
          ...b,
          status: b.status === "loading" ? "active" : b.status,
        }));
      }
    } catch {}
    return makeInitialButtons();
  });

  const [runLog, setRunLog] = useState<LogEntry[]>(() => {
    try {
      const saved = localStorage.getItem(LS_LOG);
      return saved ? JSON.parse(saved) : [];
    } catch {
      return [];
    }
  });

  const [isRunning, setIsRunning] = useState(() => _isRunning);
  const lockRef = useRef(_isRunning);

  const setLock = (val: boolean) => {
    _isRunning = val;
    lockRef.current = val;
    setIsRunning(val);
  };

  useEffect(() => { setIsRunning(_isRunning); }, []);
  useEffect(() => { localStorage.setItem(LS_BUTTONS, JSON.stringify(buttons)); }, [buttons]);
  useEffect(() => { localStorage.setItem(LS_LOG, JSON.stringify(runLog)); }, [runLog]);

  const pushLog = (entry: Omit<LogEntry, "id" | "time">) => {
    const time = new Date().toLocaleTimeString("en-US", { hour: "2-digit", minute: "2-digit", second: "2-digit" });
    const id = `${Date.now()}-${Math.random().toString(36).slice(2, 6)}`;
    setRunLog((prev) => [{ ...entry, id, time }, ...prev].slice(0, 30));
  };

  const updateLastLog = (patch: Partial<LogEntry>) => {
    setRunLog((prev) => {
      if (prev.length === 0) return prev;
      return [{ ...prev[0], ...patch }, ...prev.slice(1)];
    });
  };

  const handleFetch = async (index: number) => {
    const btn = buttons[index];
    if (btn.status === "loading" || btn.status === "pending" || _isRunning) return;

    setLock(true);
    setButtons((prev) => prev.map((b, i) => i === index ? { ...b, status: "loading" } : b));
    pushLog({ stage: btn.name, outcome: "running" });

    try {
      const res = await fetch(`${API_BASE}/fetch/${btn.endpoint}`, { method: "POST" });
      const data = await res.json();
      const success = data.status === "started";

      updateLastLog(
        success
          ? {
              outcome: "success",
              run_id: data.run_id,
              elapsed_sec: data.elapsed_sec,
              articles_new: data.articles_new,
              articles_skip: data.articles_skip,
              failed_outlets: data.failed_outlets ?? [],
              outlets_total: data.outlets_total,
            }
          : { outcome: "error", error: data.detail ?? "Unexpected response" }
      );

      setButtons((prev) =>
        prev.map((b, i) => {
          if (i === index) return { ...b, status: success ? "done" : "error" };
          if (i === index + 1 && success) return { ...b, status: "active" };
          return b;
        })
      );
    } catch (err: unknown) {
      updateLastLog({
        outcome: "error",
        error: err instanceof Error ? err.message : "Network error",
      });
      setButtons((prev) => prev.map((b, i) => i === index ? { ...b, status: "error" } : b));
    } finally {
      setLock(false);
    }
  };

  const handleReset = () => {
    setLock(false);
    setButtons(makeInitialButtons());
    setRunLog([]);
    localStorage.removeItem(LS_BUTTONS);
    localStorage.removeItem(LS_LOG);
  };

  const allDone = buttons.every((b) => b.status === "done");
  const anyError = buttons.some((b) => b.status === "error");
  const totalNew = runLog.filter((e) => e.outcome === "success").reduce((s, e) => s + (e.articles_new ?? 0), 0);
  const totalRuns = runLog.filter((e) => e.outcome !== "running").length;

  return (
    <div className="container mx-auto px-4 py-8 max-w-5xl">
      <div className="space-y-6">
        {/* Header */}
        <div>
          <h1 className="text-3xl font-bold tracking-tight">Pipeline Dashboard</h1>
          <p className="mt-1 text-sm" style={{ color: "var(--muted-foreground)" }}>
            Fetch and process articles through the ingestion pipeline
          </p>
        </div>

        {/* Status banner */}
        {(allDone || anyError) && (
          <div
            className="flex items-center justify-between px-4 py-3 rounded-lg border-l-4"
            style={{
              borderColor: allDone ? "#16a34a" : "#dc2626",
              background: allDone ? "#f0fdf4" : "#fef2f2",
            }}
          >
            <div className="flex items-center gap-2">
              {allDone ? <CheckCircle size={16} style={{ color: "#16a34a" }} /> : <XCircle size={16} style={{ color: "#dc2626" }} />}
              <span className="text-sm font-medium" style={{ color: allDone ? "#166534" : "#991b1b" }}>
                {allDone ? "Pipeline completed — all stages done" : "One or more stages failed"}
              </span>
            </div>
            <button onClick={handleReset} className="btn btn-ghost text-xs" style={{ color: "var(--muted-foreground)" }}>
              <RotateCcw size={12} />
              Reset
            </button>
          </div>
        )}

        {/* Main grid */}
        <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
          {/* Pipeline stages */}
          <div className="space-y-4">
            <h2 className="text-xs font-semibold uppercase tracking-wider" style={{ color: "var(--muted-foreground)" }}>
              Pipeline Stages
            </h2>

            {buttons.map((btn, index) => {
              const cfg = statusConfig[btn.status];
              return (
                <button
                  key={index}
                  onClick={() => handleFetch(index)}
                  disabled={btn.status === "loading" || btn.status === "pending" || isRunning}
                  className="card w-full text-left p-5 transition-all"
                  style={{
                    cursor: btn.status === "active" && !isRunning ? "pointer" : btn.status === "loading" ? "wait" : "default",
                    opacity: btn.status === "pending" ? 0.5 : 1,
                  }}
                >
                  <div className="flex items-center justify-between mb-3">
                    <span className="text-xs font-semibold uppercase tracking-wider" style={{ color: "var(--muted-foreground)" }}>
                      {btn.label}
                    </span>
                    <div className="flex items-center gap-1.5">
                      <span
                        className="w-2 h-2 rounded-full inline-block"
                        style={{
                          background: cfg.dot,
                          animation: btn.status === "loading" ? "pulse 1.5s ease infinite" : undefined,
                        }}
                      />
                      <span className="text-xs font-medium" style={{ color: cfg.text }}>
                        {cfg.label}
                      </span>
                    </div>
                  </div>

                  <div className="flex items-center gap-3 mb-2">
                    <div style={{ color: btn.status === "loading" ? "#ca8a04" : "var(--foreground)" }}>
                      {btn.status === "loading" ? (
                        <Loader2 size={24} className="spinner" />
                      ) : btn.status === "done" ? (
                        <CheckCircle size={24} style={{ color: "#16a34a" }} />
                      ) : btn.status === "error" ? (
                        <XCircle size={24} style={{ color: "#dc2626" }} />
                      ) : (
                        btn.icon
                      )}
                    </div>
                    <h3 className="text-lg font-bold">{btn.name}</h3>
                  </div>

                  <p className="text-sm" style={{ color: "var(--muted-foreground)" }}>
                    {btn.description}
                  </p>

                  {btn.status === "active" && !isRunning && (
                    <div className="mt-3 pt-3 border-t flex items-center justify-between text-xs" style={{ color: "var(--muted-foreground)" }}>
                      <span className="flex items-center gap-1">
                        <Play size={10} />
                        Click to run
                      </span>
                      <span>→</span>
                    </div>
                  )}
                </button>
              );
            })}
          </div>

          {/* Run log */}
          <div className="card flex flex-col overflow-hidden">
            <div className="flex items-center justify-between px-4 py-3 border-b" style={{ background: "var(--secondary)" }}>
              <div className="flex items-center gap-3">
                <span className="text-xs font-semibold uppercase tracking-wider" style={{ color: "var(--muted-foreground)" }}>
                  Run Log
                </span>
                {totalRuns > 0 && (
                  <span className="text-xs" style={{ color: "var(--muted-foreground)" }}>
                    {totalRuns} run{totalRuns !== 1 ? "s" : ""} · {totalNew} total new
                  </span>
                )}
              </div>
              {runLog.length > 0 && (
                <button
                  onClick={() => { setRunLog([]); localStorage.removeItem(LS_LOG); }}
                  className="btn btn-ghost p-1"
                  style={{ color: "var(--muted-foreground)" }}
                >
                  <Trash2 size={12} />
                </button>
              )}
            </div>

            <div className="flex flex-col min-h-[200px] max-h-[420px] overflow-y-auto">
              {runLog.length === 0 ? (
                <div className="flex-1 flex flex-col items-center justify-center py-10 gap-2">
                  <Settings size={24} style={{ color: "var(--muted-foreground)", opacity: 0.3 }} />
                  <p className="text-xs" style={{ color: "var(--muted-foreground)" }}>No runs yet</p>
                </div>
              ) : (
                runLog.map((entry) => <LogCard key={entry.id} entry={entry} />)
              )}
            </div>

            <div className="border-t px-4 py-2" style={{ background: "var(--secondary)" }}>
              <span className="text-xs" style={{ color: "var(--muted-foreground)" }}>
                Persisted across navigation · Latest first
              </span>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}

function LogCard({ entry }: { entry: LogEntry }) {
  const [expanded, setExpanded] = useState(false);
  const isSuccess = entry.outcome === "success";
  const isRunning = entry.outcome === "running";

  const borderColor = isSuccess ? "#16a34a" : isRunning ? "#eab308" : "#dc2626";

  return (
    <div className="border-b px-4 py-3 transition-colors" style={{ borderLeftWidth: "3px", borderLeftColor: borderColor }}>
      <div className="flex items-center justify-between mb-1.5">
        <div className="flex items-center gap-2">
          <span className="text-xs font-bold" style={{ color: borderColor }}>
            {isSuccess ? "✓" : isRunning ? "↻" : "✕"}
          </span>
          <span className="text-sm font-medium">{entry.stage}</span>
        </div>
        <span className="text-xs font-mono" style={{ color: "var(--muted-foreground)" }}>{entry.time}</span>
      </div>

      {isSuccess && entry.articles_new !== undefined && (
        <div className="grid grid-cols-3 gap-2 mt-2">
          <MiniStat label="New" value={entry.articles_new ?? 0} color="#16a34a" />
          <MiniStat label="Skipped" value={entry.articles_skip ?? 0} color="var(--muted-foreground)" />
          <MiniStat label="Duration" value={`${entry.elapsed_sec}s`} color="var(--muted-foreground)" />
        </div>
      )}

      {isRunning && (
        <p className="text-xs mt-1" style={{ color: "#ca8a04", animation: "pulse 2s ease infinite" }}>
          Pipeline in progress…
        </p>
      )}

      {!isSuccess && !isRunning && entry.error && (
        <p className="text-xs mt-1 font-mono" style={{ color: "#dc2626" }}>{entry.error}</p>
      )}

      {isSuccess && (entry.failed_outlets?.length ?? 0) > 0 && (
        <button
          onClick={() => setExpanded(!expanded)}
          className="text-xs mt-1.5"
          style={{ color: "var(--muted-foreground)" }}
        >
          {expanded ? "Hide ▴" : `${entry.failed_outlets!.length} failed ▾`}
        </button>
      )}

      {expanded && entry.failed_outlets && (
        <div className="mt-2 pt-2 border-t flex flex-wrap gap-1">
          {entry.failed_outlets.map((o) => (
            <span key={o} className="badge badge-destructive text-xs">{o}</span>
          ))}
        </div>
      )}
    </div>
  );
}

function MiniStat({ label, value, color }: { label: string; value: number | string; color: string }) {
  return (
    <div className="rounded-md p-2 text-center" style={{ background: "var(--secondary)" }}>
      <div className="text-sm font-bold" style={{ color }}>{value}</div>
      <div className="text-xs" style={{ color: "var(--muted-foreground)" }}>{label}</div>
    </div>
  );
}
