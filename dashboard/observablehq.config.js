// See https://observablehq.com/framework/config for documentation.
export default {
  title: "Cricket Intelligence: Player Portfolios",

  // Sidebar is left to default (alphabetical) so the wizard's example pages
  // stay visible as references during Phase 1. They'll be removed in M2 and
  // a proper `pages` map added in M21 (page assembly).

  head: '<link rel="icon" href="observable.png" type="image/png" sizes="32x32">',

  root: "src",

  // Dark cinematic theme per the design spec.
  theme: "dark",

  // Python data loaders run via the platform's Poetry venv.
  // PREREQUISITE: `poetry shell` must be active from repo root before `npm run dev`,
  // otherwise `import duckdb` will fail when loaders execute.
  interpreters: {
    ".py": ["python3"],
  },

  footer:
    "Built on Cricket Intelligence Platform · DuckDB · Observable Framework",
};
