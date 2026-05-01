import { Link } from "react-router-dom";
import { Home } from "lucide-react";

export default function NotFoundPage() {
  return (
    <div
      className="min-h-screen flex items-center justify-center"
      style={{ background: "var(--background)" }}
    >
      <div className="text-center space-y-4">
        <div className="text-6xl font-bold" style={{ color: "var(--muted-foreground)", opacity: 0.3 }}>
          404
        </div>
        <h1 className="text-2xl font-bold">Page not found</h1>
        <p className="text-sm" style={{ color: "var(--muted-foreground)" }}>
          The page you're looking for doesn't exist or has been moved.
        </p>
        <Link to="/" className="btn btn-primary inline-flex mt-2">
          <Home size={16} />
          Back to Dashboard
        </Link>
      </div>
    </div>
  );
}
