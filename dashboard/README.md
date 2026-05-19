# Cricket Intelligence: Player Portfolios

[Observable Framework](https://observablehq.com/framework/) dashboard for
player-level cricket analytics, backed by the Gold layer in
`storage/duckdb/cricket.duckdb`.

## Prerequisites

- Node.js v18 or v20 LTS
- Python 3.11+ with the platform Poetry environment active
- The DuckDB file at `storage/duckdb/cricket.duckdb` (produced by the
  `ingest_all_match_data_gold` / `ingest_two_day_match_data_gold` DAGs)

## Running locally

From the **repo root**:

```bash
# 1. Activate Poetry env so Python data loaders can `import duckdb`
poetry shell

# 2. (First run only) Install npm dependencies
make dashboard-install

# 3. Start dev server on http://localhost:3030
make dashboard-dev
```

Stop with `Ctrl-C`. Build a static site with `make dashboard-build`
(outputs to `dashboard/dist/`).

## DuckDB lock coordination

This dashboard opens DuckDB in **read-only** mode so it coexists with
Metabase, the DuckDB UI, and other readers. However the Gold DAGs require
**exclusive write access** — before running them, stop:

1. The dashboard dev server (`Ctrl-C`)
2. Metabase (`docker stop compose-metabase-1`)
3. The DuckDB UI (`make duckdb-stop`)

See `docs/runbooks/dashboard.md` §5 for the full coordination protocol.

## Structure

```
dashboard/
├── observablehq.config.js   Site config + Python interpreter wiring
├── package.json             npm scripts (dev binds to port 3030)
├── .env                     DUCKDB_PATH (gitignored)
├── .env.example             Documentation
└── src/
    ├── index.md             Landing page (player dropdown)
    ├── components/          Chart components (D3, Plot)
    ├── data/                Python data loaders (`*.csv.py`)
    └── styles/              CSS design tokens (added in M3)
```

### Python data loaders

Files matching `src/data/*.csv.py` are executed at build/dev time by
Observable Framework via the `python3` interpreter configured in
`observablehq.config.js`. They write CSV to stdout, which Framework caches
under `src/.observablehq/cache/`.

Convention:

```python
import duckdb, os, sys

DB = os.environ.get("DUCKDB_PATH", "../storage/duckdb/cricket.duckdb")
con = duckdb.connect(DB, read_only=True)
df = con.execute("SELECT ...").df()
df.to_csv(sys.stdout, index=False)
```

## Adding a new player

The dropdown is populated from `gold.player_display_names`. To add a
new alias, edit `models/dbt/seeds/player_aliases.csv` and re-run:

```bash
cd models/dbt
poetry run dbt seed
poetry run dbt run -s player_display_names
```

## Port choice

Port 3030 is used (not Observable's default 3000) because Metabase already
binds port 3000 on this host.
