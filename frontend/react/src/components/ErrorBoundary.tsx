import { Component, type ReactNode } from "react";

interface Props {
  children: ReactNode;
}

interface State {
  error: string | null;
}

export default class ErrorBoundary extends Component<Props, State> {
  state: State = { error: null };

  static getDerivedStateFromError(e: Error): State {
    return { error: e.message };
  }

  render() {
    if (this.state.error)
      return (
        <div className="max-w-3xl mx-auto px-6 py-20 text-center">
          <p className="text-4xl mb-4">✕</p>
          <p className="text-sm text-red-500 uppercase tracking-widest">
            {this.state.error}
          </p>
        </div>
      );
    return this.props.children;
  }
}
