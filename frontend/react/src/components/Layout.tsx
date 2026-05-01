import { Outlet } from "react-router-dom";
import Navbar from "./Navbar";
import ErrorBoundary from "./ErrorBoundary";

export default function Layout() {
  return (
    <div className="min-h-screen flex flex-col" style={{ background: "var(--background)" }}>
      <Navbar />
      <main className="flex-1">
        <ErrorBoundary>
          <Outlet />
        </ErrorBoundary>
      </main>
      <footer className="border-t py-6 mt-12">
        <div className="container mx-auto px-4 text-center text-sm" style={{ color: "var(--muted-foreground)" }}>
          <p className="font-medium">NewsHere — Story-Centric Intelligence Platform</p>
          <p className="mt-1 text-xs">Analyzing global news through multiple perspectives</p>
        </div>
      </footer>
    </div>
  );
}
