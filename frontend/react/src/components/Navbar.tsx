import { useState } from "react";
import { Link, useLocation, useNavigate } from "react-router-dom";
import { Home, FileText, Brain, Search, Settings, Menu, X } from "lucide-react";

const navItems = [
  { path: "/", label: "Dashboard", icon: Home },
  { path: "/Articles", label: "Articles", icon: FileText },
  { path: "/Analyze", label: "Analysis", icon: Brain },
  { path: "/Logs", label: "Pipeline", icon: Settings },
];

export default function Navbar() {
  const location = useLocation();
  const navigate = useNavigate();
  const [searchQuery, setSearchQuery] = useState("");
  const [mobileOpen, setMobileOpen] = useState(false);

  const handleSearch = (e: React.FormEvent) => {
    e.preventDefault();
    if (searchQuery.trim()) {
      navigate(`/Search?q=${encodeURIComponent(searchQuery)}`);
      setSearchQuery("");
    }
  };

  return (
    <nav
      className="sticky top-0 z-50 border-b"
      style={{
        background: "rgba(255,255,255,0.85)",
        backdropFilter: "blur(12px)",
        WebkitBackdropFilter: "blur(12px)",
      }}
    >
      <div className="container mx-auto px-4 py-3">
        <div className="flex items-center justify-between gap-8">
          {/* Logo & Nav */}
          <div className="flex items-center gap-8">
            <Link to="/" className="flex items-center gap-2 shrink-0">
              <span
                className="text-xl font-bold tracking-tight"
                style={{ letterSpacing: "-0.03em" }}
              >
                NewsHere
              </span>
            </Link>

            <div className="hidden lg:flex items-center gap-1">
              {navItems.map((item) => {
                const Icon = item.icon;
                const isActive =
                  location.pathname === item.path ||
                  (item.path !== "/" && location.pathname.startsWith(item.path));
                return (
                  <Link
                    key={item.path}
                    to={item.path}
                    className="flex items-center gap-2 px-3 py-1.5 rounded-md text-sm transition-all duration-150"
                    style={{
                      background: isActive ? "var(--primary)" : "transparent",
                      color: isActive ? "var(--primary-foreground)" : "var(--muted-foreground)",
                      fontWeight: isActive ? 500 : 400,
                    }}
                    onMouseEnter={(e) => {
                      if (!isActive) {
                        e.currentTarget.style.background = "var(--muted)";
                        e.currentTarget.style.color = "var(--foreground)";
                      }
                    }}
                    onMouseLeave={(e) => {
                      if (!isActive) {
                        e.currentTarget.style.background = "transparent";
                        e.currentTarget.style.color = "var(--muted-foreground)";
                      }
                    }}
                  >
                    <Icon size={15} />
                    {item.label}
                  </Link>
                );
              })}
            </div>
          </div>

          {/* Search */}
          <form onSubmit={handleSearch} className="flex-1 max-w-md hidden sm:block min-w-[200px]">
            <div className="relative group">
              <Search
                size={16}
                className="absolute left-3 top-1/2 -translate-y-1/2 pointer-events-none transition-colors"
                style={{ color: "var(--muted-foreground)" }}
              />
              <input
                type="search"
                placeholder="Search stories..."
                value={searchQuery}
                onChange={(e) => setSearchQuery(e.target.value)}
                className="input input-search py-2 text-sm w-full"
                style={{ paddingLeft: "2.5rem" }}
              />
            </div>
          </form>

          {/* Mobile toggle */}
          <button
            className="md:hidden btn btn-ghost p-2 shrink-0"
            onClick={() => setMobileOpen(!mobileOpen)}
          >
            {mobileOpen ? <X size={20} /> : <Menu size={20} />}
          </button>
        </div>

        {/* Mobile menu */}
        {mobileOpen && (
          <div className="md:hidden pt-3 pb-1 border-t mt-3 flex flex-col gap-1">
            <form onSubmit={handleSearch} className="mb-2 sm:hidden">
              <div className="relative group">
                <Search
                  size={16}
                  className="absolute left-3 top-1/2 -translate-y-1/2 pointer-events-none"
                  style={{ color: "var(--muted-foreground)" }}
                />
                <input
                  type="search"
                  placeholder="Search stories..."
                  value={searchQuery}
                  onChange={(e) => setSearchQuery(e.target.value)}
                  className="input input-search py-2 text-sm w-full"
                  style={{ paddingLeft: "2.5rem" }}
                />
              </div>
            </form>
            {navItems.map((item) => {
              const Icon = item.icon;
              const isActive = location.pathname === item.path;
              return (
                <Link
                  key={item.path}
                  to={item.path}
                  onClick={() => setMobileOpen(false)}
                  className="flex items-center gap-2 px-3 py-2 rounded-md text-sm transition-colors"
                  style={{
                    background: isActive ? "var(--primary)" : "transparent",
                    color: isActive ? "var(--primary-foreground)" : "var(--foreground)",
                  }}
                >
                  <Icon size={16} />
                  {item.label}
                </Link>
              );
            })}
          </div>
        )}
      </div>
    </nav>
  );
}
