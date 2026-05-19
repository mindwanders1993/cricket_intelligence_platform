// See https://observablehq.com/framework/config for documentation.
export default {
  title: "Cricket Intelligence: Player Portfolios",

  // Sidebar shows only Home (index.md) after M2 cleanup.
  // A proper `pages` map will be added in M21 (page assembly).

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
