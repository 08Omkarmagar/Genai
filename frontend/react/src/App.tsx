import { createBrowserRouter, RouterProvider } from "react-router-dom";
import HomePage from "./components/HomePage";
import BrowseArticles from "./components/BrowseArticles";
import Layout from "./components/Layout";
import Logs from "./components/Logs";
import AnalysisDashboard from "./components/AnalysisDashboard";
import ArticleList from "./components/ArticlesList";
import InsightsPage from "./components/InsightsPage";
import SearchPage from "./components/SearchPage";
import StoryPage from "./components/StoryPage";
import NotFoundPage from "./components/NotFoundPage";

const routes = [
  {
    path: "/",
    element: <Layout />,
    errorElement: <NotFoundPage />,
    children: [
      {
        index: true,
        element: <HomePage />,
      },
      {
        path: "BrowseArticles/:id",
        element: <BrowseArticles />,
      },
      {
        path: "Articles",
        element: <ArticleList />,
      },
      {
        path: "Analyze",
        element: <AnalysisDashboard />,
      },
      {
        path: "Insights",
        element: <InsightsPage />,
      },
      {
        path: "Search",
        element: <SearchPage />,
      },
      {
        path: "Story/:id",
        element: <StoryPage />,
      },
      {
        path: "Logs",
        element: <Logs />,
      },
    ],
  },
];

const router = createBrowserRouter(routes);

function App() {
  return <RouterProvider router={router} />;
}

export default App;
