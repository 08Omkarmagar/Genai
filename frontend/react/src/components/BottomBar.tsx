import { useState } from "react";
import { API_BASE } from "../config";

type FetchStage = "idle" | "pipeline" | "reloading" | "done" | "error";

export default function BottomBar() {
  const [stage, setStage] = useState<FetchStage>("idle");
  const [newCount, setNewCount] = useState<number | null>(null);

  const handleFetch = async () => {
    setStage("pipeline");
    setNewCount(null);
    try {
      const [rssRes, bodyRes] = await Promise.all([
        fetch(`${API_BASE}/fetch/rss`),
        fetch(`${API_BASE}/fetch/body`),
      ]);
      if (!rssRes.ok || !bodyRes.ok) throw new Error("Pipeline failed");

      setStage("reloading");
      await new Promise((r) => setTimeout(r, 800));

      setNewCount(0);
      setStage("done");
      setTimeout(() => {
        setStage("idle");
        setNewCount(null);
      }, 4000);
    } catch {
      setStage("error");
      setTimeout(() => setStage("idle"), 4000);
    }
  };

  const isWorking = stage === "pipeline" || stage === "reloading";
  const label =
    stage === "idle"
      ? "Fetch Latest"
      : stage === "pipeline"
        ? "Running pipeline…"
        : stage === "reloading"
          ? "Refreshing…"
          : stage === "done"
            ? newCount && newCount > 0
              ? `${newCount} new`
              : "Up to date"
            : "Error — retry";

  // This component is intentionally hidden/removed from the Layout.
  // Pipeline controls now live in the Fetching/Dashboard page.
  return null;
}
