import { useEffect, useState } from "react";
import type { FetchLog } from "./types";
import { API_BASE } from "../config";
import { LoadingState } from "./Shared";
import { ClipboardList, CheckCircle, XCircle, AlertCircle } from "lucide-react";

function formatDateTime(dateStr: string | null) {
  if (!dateStr) return "—";
  return new Date(dateStr).toLocaleString("en-US", {
    month: "short",
    day: "numeric",
    year: "numeric",
    hour: "2-digit",
    minute: "2-digit",
  });
}

export default function Logs() {
  const [logs, setLogs] = useState<FetchLog[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [filter, setFilter] = useState<string>("all");

  useEffect(() => {
    fetch(`${API_BASE}/logs`)
      .then((res) => {
        if (!res.ok) throw new Error("Failed to fetch logs");
        return res.json();
      })
      .then(setLogs)
      .catch((e) => setError(e.message))
      .finally(() => setLoading(false));
  }, []);

  if (loading) return <LoadingState message="Retrieving records…" />;

  if (error)
    return (
      <div className="container mx-auto px-4 py-8">
        <div className="text-center py-12">
          <p className="text-sm" style={{ color: "#dc2626" }}>{error}</p>
        </div>
      </div>
    );

  const totalNew = logs.reduce((s, l) => s + (l.articles_new ?? 0), 0);
  const totalSkipped = logs.reduce((s, l) => s + (l.articles_skip ?? 0), 0);
  const totalErrors = logs.filter((l) => l.status?.toLowerCase() === "error").length;
  const totalRuns = logs.length;

  const statuses = ["all", ...Array.from(new Set(logs.map((l) => l.status ?? "unknown")))];
  const filtered = filter === "all" ? logs : logs.filter((l) => l.status === filter);

  return (
    <div className="container mx-auto px-4 py-8 max-w-5xl">
      <div className="space-y-6">
        {/* Header */}
        <div>
          <h1 className="text-3xl font-bold tracking-tight">Pipeline Ledger</h1>
          <p className="mt-1 text-sm" style={{ color: "var(--muted-foreground)" }}>
            Fetch run history and audit log
          </p>
        </div>

        {/* Stats */}
        <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
          <StatCard label="Total Runs" value={totalRuns} icon={<ClipboardList size={18} />} />
          <StatCard label="Articles Added" value={totalNew} icon={<CheckCircle size={18} />} color="#16a34a" />
          <StatCard label="Skipped" value={totalSkipped} icon={<AlertCircle size={18} />} />
          <StatCard label="Errors" value={totalErrors} icon={<XCircle size={18} />} color={totalErrors > 0 ? "#dc2626" : undefined} />
        </div>

        {/* Filter tabs */}
        <div className="tabs-list">
          {statuses.map((s) => (
            <button
              key={s}
              onClick={() => setFilter(s)}
              className={`tab-trigger ${filter === s ? "active" : ""}`}
            >
              {s}
            </button>
          ))}
          <span className="ml-auto text-xs py-2 px-2" style={{ color: "var(--muted-foreground)" }}>
            {filtered.length} record{filtered.length !== 1 ? "s" : ""}
          </span>
        </div>

        {/* Table */}
        {filtered.length === 0 ? (
          <div className="card">
            <div className="card-content py-12 text-center" style={{ color: "var(--muted-foreground)" }}>
              <ClipboardList size={40} className="mx-auto mb-3 opacity-40" />
              <p className="font-medium">No records found</p>
            </div>
          </div>
        ) : (
          <div className="card overflow-hidden">
            {/* Table header */}
            <div
              className="grid grid-cols-[2fr_1.5fr_1fr_0.8fr_0.8fr_1fr] px-4 py-2.5 border-b text-xs font-semibold uppercase tracking-wider"
              style={{ background: "var(--secondary)", color: "var(--muted-foreground)" }}
            >
              <span>Outlet</span>
              <span>Run At</span>
              <span>Status</span>
              <span>New</span>
              <span>Skipped</span>
              <span>Run ID</span>
            </div>

            {/* Table rows */}
            {filtered.map((log, i) => (
              <div
                key={log.id}
                className="grid grid-cols-[2fr_1.5fr_1fr_0.8fr_0.8fr_1fr] px-4 py-3 border-b items-center transition-colors"
                style={{
                  background: i % 2 === 0 ? "var(--background)" : "var(--secondary)",
                }}
                onMouseEnter={(e) => (e.currentTarget.style.background = "var(--accent)")}
                onMouseLeave={(e) => (e.currentTarget.style.background = i % 2 === 0 ? "var(--background)" : "var(--secondary)")}
              >
                <div className="flex flex-col gap-0.5 min-w-0">
                  <span className="text-sm font-medium truncate">
                    {log.outlet ?? "—"}
                  </span>
                  {log.error_message && (
                    <span className="text-xs truncate" style={{ color: "#dc2626" }} title={log.error_message}>
                      {log.error_message}
                    </span>
                  )}
                </div>

                <span className="text-xs tabular-nums" style={{ color: "var(--muted-foreground)" }}>
                  {formatDateTime(log.run_at)}
                </span>

                <div>
                  <StatusBadge status={log.status} />
                </div>

                <span
                  className="text-sm font-bold tabular-nums"
                  style={{ color: (log.articles_new ?? 0) > 0 ? "#16a34a" : "var(--muted-foreground)" }}
                >
                  {log.articles_new ?? 0}
                </span>

                <span className="text-sm tabular-nums" style={{ color: "var(--muted-foreground)" }}>
                  {log.articles_skip ?? 0}
                </span>

                <span
                  className="text-xs font-mono truncate"
                  style={{ color: "var(--muted-foreground)" }}
                  title={log.run_id ?? ""}
                >
                  {log.run_id ? log.run_id.slice(0, 8) + "…" : "—"}
                </span>
              </div>
            ))}
          </div>
        )}
      </div>
    </div>
  );
}

function StatusBadge({ status }: { status: string | null }) {
  if (!status) return <span className="text-xs" style={{ color: "var(--muted-foreground)" }}>—</span>;

  const isSuccess = status.toLowerCase() === "success";
  const isError = status.toLowerCase() === "error";

  let bg = "var(--secondary)";
  let color = "var(--secondary-foreground)";
  if (isSuccess) { bg = "#dcfce7"; color = "#166534"; }
  if (isError) { bg = "#fee2e2"; color = "#991b1b"; }

  return (
    <span className="badge" style={{ background: bg, color }}>
      {status}
    </span>
  );
}

function StatCard({
  label,
  value,
  icon,
  color,
}: {
  label: string;
  value: number;
  icon: React.ReactNode;
  color?: string;
}) {
  return (
    <div className="card">
      <div className="card-content flex items-center gap-3 py-4">
        <div
          className="w-10 h-10 rounded-lg flex items-center justify-center"
          style={{ background: "var(--secondary)", color: color || "var(--muted-foreground)" }}
        >
          {icon}
        </div>
        <div>
          <div className="text-2xl font-bold" style={{ color: color || "var(--foreground)" }}>
            {value.toLocaleString()}
          </div>
          <div className="text-xs" style={{ color: "var(--muted-foreground)" }}>
            {label}
          </div>
        </div>
      </div>
    </div>
  );
}
