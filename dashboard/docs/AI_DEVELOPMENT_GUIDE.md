# Dashboard Development Guide — AI-Assisted Workflow

**Project:** Virat Kohli portfolio dashboard (Cricket Intelligence Platform)
**Scope:** This guide covers the **player-portfolio dashboard** only — Observable Framework, custom D3 visuals, cinematic dark theme. It is **one of two BI surfaces** in the platform.
**Stack:** Observable Framework + Python data loaders + DuckDB + D3 + Observable Plot
**AI workflow:** Claude (plan/review/teach) ⇄ mm-claude / Minimax M2.5 (execute) ⇄ You (direct/validate)

> **The two-dashboard split (revamp v2):**
> - **Player portfolio (this guide)** → Observable Framework at `dashboard/`, port 3030, reads DuckDB direct. Custom D3 visuals, cinematic theme, public-facing.
> - **Platform / ops** → Lightdash at `infra/lightdash/` (Sprint 1), port 8082, semantic-layer-driven via dbt MetricFlow. Pipeline health, FinOps cost mart, data quality. Operator-facing.
> Same Gold layer underneath; different surface for different jobs. See `docs/architecture/hld-hla.md` §7.4 and `docs/planning.md` Sprint 1.

This document is the operational playbook for building the rest of the player dashboard. Every module has its own prompt and validation criteria — copy-paste ready.

---

## Table of Contents

1. [Project goals & status](#1-project-goals--status)
2. [Architecture decisions (locked)](#2-architecture-decisions-locked)
3. [AI tool roles & decision tree](#3-ai-tool-roles--decision-tree)
4. [mm-claude session pattern](#4-mm-claude-session-pattern)
5. [Standard prompt templates](#5-standard-prompt-templates)
6. [Phase 2 — Data Layer (M4–M10)](#6-phase-2--data-layer-m4m10)
7. [Phase 3 — Component Library (M11–M20)](#7-phase-3--component-library-m11m20)
8. [Phase 4 — Page Assembly (M21)](#8-phase-4--page-assembly-m21)
9. [Phase 5 — Interactivity (M22–M23)](#9-phase-5--interactivity-m22m23)
10. [Phase 6 — Polish (M24–M27)](#10-phase-6--polish-m24m27)
11. [Common Claude review prompts](#11-common-claude-review-prompts)
12. [Data model reference](#12-data-model-reference)
13. [Verification gates](#13-verification-gates)
14. [Risk register & known pitfalls](#14-risk-register--known-pitfalls)
15. [Learning outcomes](#15-learning-outcomes)

---

## 1. Project goals & status

### Goal
Build a player-portfolio dashboard with Virat Kohli as the showcase. Architecture is generic — a dropdown selects any player from `gold.player_display_names`. Visual target: cinematic dark theme per the design spec, with D3 Timeline River, glow records wall, count-up KPIs, dismissal lab, opponent analysis.

### Design spec
`~/Downloads/virat-kohli-dashboard-FULL-DESIGN.md` (1,870 lines). Sections referenced throughout as §1.x.

### Skipped from spec (per feasibility analysis)
- **S7 Captaincy per-opponent breakdown** — Cricsheet has no captain flag
- **S8 IPL salary / trophy data** — not in any cricket stats feed (static if needed)
- **S12 Awards Timeline** — static JSON, ~30 lines hardcoded

### Status
- ✅ **M1 — Observable Framework scaffold** (commit `408894a` on branch `feat/dashboard-scaffold`)
- ✅ M2 — Project structure cleanup
- ⬜ M3 — Design system (CSS tokens) *(parked until Sprint 4 of revamp v2)*
- ⬜ M4–M10 — Python data loaders *(parked — Sprint 4)*
- ⬜ M11–M20 — Chart components *(parked — Sprint 4)*
- ⬜ M21 — Page assembly *(parked — Sprint 4)*
- ⬜ M22 — Interactivity + embedded AI chat (Chainlit iframe) *(depends on Sprint 2 — AI assistant)*
- ⬜ M23 — Cross-filter interactivity *(parked — Sprint 4)*
- ⬜ M24–M27 — Polish *(parked — Sprint 4)*

> **Sequencing note:** Dashboard M3–M27 are parked at M2 until the revamp-v2 platform deepening lands (Sprints 0–3). Reason: M22 depends on the AI assistant (Sprint 2), and the dashboard's embedded analytics depend on the Sprint 0 MetricFlow semantic layer + Sprint 1 FastAPI gateway. Resuming M3+ in Sprint 4 means the dashboard surfaces work that's already shipped, not work-in-flight. See `docs/planning.md` for the full sequence.

---

## 2. Architecture decisions (locked)

| Decision | Choice | Rationale |
|---|---|---|
| Framework | Observable Framework v1.13.4 | SQL-first, DuckDB-native, D3 built-in, static output |
| Data layer | Python `*.csv.py` loaders | Native Poetry+DuckDB; run at build time |
| Charts | Observable Plot (default) + D3 (custom: Timeline River, radial gauge) | Plot covers ~80% declaratively |
| Styling | CSS custom properties, no Tailwind | Matches design spec's `:root { --color-* }` token system |
| Theme | `theme: "dark"` (near-midnight) | Matches spec's cinematic aesthetic |
| Port | 3030 | 3000 owned by Metabase |
| DuckDB access | Read-only, env var `DUCKDB_PATH` | Coexists with Metabase / DuckDB UI |
| Python execution | Active Poetry shell required | Matches every other Python entry point in the repo |
| Player parameterization | Decided in M22 (Claude designs upfront) | Three options: build-time, dynamic params, DuckDB-WASM |
| Output | Static `dist/` via `npm run build` | Deployable anywhere |

---

## 3. AI tool roles & decision tree

### Roles

| Tool | Role | When |
|---|---|---|
| **Claude** (this assistant, opus/sonnet) | Senior consultant | Architecture, design specs, code review, teaching, debugging |
| **mm-claude** (Minimax M2.5 via Ollama Cloud) | Junior implementer | Boilerplate, JS/D3 components, CSS, markdown wiring |
| **You** (data engineer) | Tech lead | Python data loaders, SQL validation, accept/reject diffs, domain calls |

### When to use which — decision tree

```
"What's my situation?"
│
├─ I need a spec/contract/design before coding
│  └─→ Claude: "Design X — give me data shape + approach"
│
├─ I have a spec, need code written
│  └─→ mm-claude: "Implement X per [spec]. Data shape: [paste]. File: ..."
│
├─ mm-claude wrote code I don't fully understand
│  └─→ Claude: "Explain this [paste] line by line for a data engineer"
│
├─ Output looks wrong (numbers, layout, behavior)
│  ├─ Wrong NUMBERS  → You: check SQL, validate vs ground truth
│  ├─ Wrong CODE     → Claude: review + diagnose → mm-claude: apply fix
│  └─ Wrong DESIGN   → Claude: re-spec → mm-claude: re-implement
│
├─ Writing Python data loader (SQL-heavy)
│  └─→ You write it; Claude reviews the SQL
│
└─ Stuck for >15 min on something
   └─→ Claude: "I'm stuck on X. Here's what I've tried: [paste]. What now?"
```

### Golden rule
**Never accept code you don't understand.** If mm-claude generates something opaque, switch to Claude and ask for a line-by-line explanation. The teaching step is where the JS/D3/CSS skills compound.

---

## 4. mm-claude session pattern

Your alias: `mm-claude='ollama launch claude --model minimax-m2.5:cloud'`

### Standard session

```bash
# 1. From repo root, activate the Poetry env (needed if mm-claude needs to run python)
cd /Users/mrrobot/Desktop/Projects/cricket_intelligence_platform
poetry shell

# 2. Start mm-claude
mm-claude

# 3. Inside mm-claude, paste the module's prompt (from this guide)

# 4. mm-claude generates code / proposes diffs

# 5. Review the diff carefully:
#    - Does it match the spec?
#    - Are file paths correct?
#    - Any unused imports / dead code?
#    - Any "TODO" or "placeholder" comments left in?

# 6. Accept → file is edited

# 7. Validate (each module has its own validation step)

# 8. If broken → debug with Claude

# 9. Commit when working:
git add dashboard/src/...
git commit -m "feat(dashboard): [module] — [one-liner]"
```

### Commit cadence
**One commit per module.** Makes rollback trivial if mm-claude goes sideways.

### Stop signals (when to ditch mm-claude's current attempt)

- Same error after 3 retries
- Output diverges further from spec with each attempt
- Generated code references files that don't exist
- Output contains placeholder text like `// TODO: implement properly`

When you hit these → save the attempt's output, switch to Claude with: *"mm-claude tried 3× and keeps failing on X. Here's the latest output: [paste]. What's the actual problem?"*

---

## 5. Standard prompt templates

### Template A — Build a component

```
Implement [component name] for the Cricket Intelligence dashboard.

Context:
- Observable Framework v1.13.4 site at dashboard/
- Dark theme already configured (CSS variables in src/styles/theme.css)
- File to create: dashboard/src/components/[name].js
- Used by: dashboard/src/kohli.md (to be wired in M21)

Design spec: ~/Downloads/virat-kohli-dashboard-FULL-DESIGN.md §[X.Y]
[Paste relevant section text]

Data input shape (from src/data/[loader].csv):
[paste 2-3 example rows]

Requirements:
- Export a single default function: `export function [name](data, options) { ... }`
- Returns a DOM element (or SVG element)
- Use CSS variables from theme.css: var(--color-gold), var(--color-test), etc.
- Use Observable Plot for standard charts, D3 only when Plot can't do it
- No external dependencies beyond @observablehq/framework's defaults
- Keep under 100 lines
- Add a 2-line JSDoc comment at the top describing inputs and the visual

Do NOT:
- Hardcode any color values (use CSS variables)
- Add error handling for impossible states
- Add a story/example/test in the same file
```

### Template B — Build a Python data loader

You write these yourself, but the structure to follow:

```python
#!/usr/bin/env python3
"""
[Loader purpose] — feeds [which section/component].
Output schema: col1, col2, col3, ...
"""
import duckdb
import os
import sys

DB = os.environ.get("DUCKDB_PATH", "../storage/duckdb/cricket.duckdb")
PLAYER = os.environ.get("PLAYER", "V Kohli")  # parameterized in M22

con = duckdb.connect(DB, read_only=True)
df = con.execute(f"""
  SELECT ...
  FROM gold.fact_delivery
  WHERE batter = ?
""", [PLAYER]).df()

df.to_csv(sys.stdout, index=False)
```

### Template C — Claude review prompt

```
Review this code I just generated with mm-claude for the Cricket
Intelligence dashboard:

[paste full file]

Spec it should implement: §[X.Y] of ~/Downloads/virat-kohli-dashboard-FULL-DESIGN.md
[paste the relevant section]

Check for:
1. Correctness against the spec (any missing requirements?)
2. Bugs (off-by-one, wrong scale, missing null handling)
3. Anti-patterns specific to Observable Framework / Plot / D3
4. CSS variable usage (no hardcoded colors)
5. Anything I should know as a data engineer learning JS

As a data engineer learning this stack, also explain:
- [specific concept you saw and didn't fully understand]
```

---

## 6. Phase 2 — Data Layer (M4–M10)

**You own this phase.** All loaders are Python. Claude reviews SQL on request.

### M2 — Cleanup (before data loaders)
**Owner:** mm-claude
**Effort:** 20 min

**Prompt:**
```
In the dashboard/src/ directory of an Observable Framework v1.13.4 project,
do the following cleanup:

1. Delete these wizard sample files:
   - src/example-dashboard.md
   - src/example-report.md
   - src/components/timeline.js
   - src/data/launches.csv.js
   - src/data/events.json
   - src/observable.png (we'll add our own favicon later)

2. Rewrite src/index.md to a minimal landing page:
   - H1: "Cricket Intelligence: Player Portfolios"
   - One paragraph: "Player-level cricket analytics from the Cricket
     Intelligence Platform. Backed by ball-by-ball Cricsheet data via
     a DuckDB Gold layer."
   - Empty placeholder for the player dropdown (we add it in M22):
     `<!-- Player dropdown goes here in M22 -->`

3. Create empty directories (with .gitkeep files) that we'll populate later:
   - dashboard/src/components/
   - dashboard/src/data/
   - dashboard/src/styles/

4. Update dashboard/observablehq.config.js — remove the `head` favicon line
   (since we deleted observable.png).

Do not implement any charts or data loaders yet. This is a cleanup commit.
```

**Validation:**
```bash
make dashboard-dev
# Browser shows just the landing page with H1 and intro paragraph.
# Sidebar shows only "Home" (no example pages).
```

**Commit:** `chore(dashboard): remove wizard examples, prep empty src layout`

---

### M3 — Design system (CSS tokens)
**Owner:** mm-claude
**Effort:** 45 min

**Prompt:**
```
Implement the design system for the Cricket Intelligence dashboard.

File: dashboard/src/styles/theme.css

Spec: read sections 6.1 through 6.6 of
~/Downloads/virat-kohli-dashboard-FULL-DESIGN.md
[OR paste the full §6 content if mm-claude can't read files]

Requirements:
1. All color tokens from §6.1 as CSS custom properties on :root
2. Import the three Google Fonts (Bebas Neue, DM Sans, JetBrains Mono)
3. Typography scale from §6.2 (clamp() values for fluid sizing)
4. Spacing scale from §6.3
5. Border-radius tokens from §6.4
6. Shadow tokens from §6.5
7. Animation tokens from §6.6 (easing curves + durations + @keyframes)

ALSO:
- Define a body { } default applying --color-base background and DM Sans font
- Define a @media (prefers-reduced-motion: reduce) { } block disabling
  animation tokens (per §10.4)
- Import this file from dashboard/observablehq.config.js using the
  `head` config option:
    head: '<link rel="stylesheet" href="/styles/theme.css">'
  AND move theme.css to a location Observable Framework can serve as
  a static asset (e.g., dashboard/src/styles/theme.css with appropriate
  reference path — check Observable Framework docs).

Do NOT animate anything yet — components consume these tokens. Just
define them.
```

**Validation:**
```bash
make dashboard-dev
# Open browser DevTools → Inspect <body>
# Run in console: getComputedStyle(document.documentElement).getPropertyValue("--color-gold")
# Should return "#F59E0B"
```

**Claude review prompt:**
```
Review dashboard/src/styles/theme.css against §6 of the design doc.
[paste theme.css contents]
Check: are all 25+ CSS variables present? Any tokens missing? Any
fixed values that should be tokens?
```

**Commit:** `feat(dashboard): add design system CSS tokens per §6 of spec`

---

### M4 — Player resolver
**Owner:** You
**Effort:** 30 min
**File:** `dashboard/src/data/player_resolver.csv.py`

Powers the dropdown — pulls all players with display names.

```python
#!/usr/bin/env python3
"""
Player resolver — feeds the dashboard's player dropdown (M22).
Output schema: cricsheet_name, display_name, person_id
"""
import duckdb, os, sys

DB = os.environ.get("DUCKDB_PATH", "../storage/duckdb/cricket.duckdb")
con = duckdb.connect(DB, read_only=True)

df = con.execute("""
  SELECT
    cricsheet_name,
    display_name,
    person_id
  FROM gold.player_display_names
  WHERE display_name IS NOT NULL
  ORDER BY display_name
""").df()

df.to_csv(sys.stdout, index=False)
```

**Validation:**
Add a temporary cell in `src/index.md`:
````
```js
const players = FileAttachment("data/player_resolver.csv").csv({typed: true});
display(players.slice(0, 10));
```
````
`make dashboard-dev` → should show 10 rows including Virat Kohli.

**Claude prompt if stuck:**
```
Explain how Observable Framework discovers and runs my `*.csv.py`
data loaders. What's the cache behavior? Does it run on every reload?
```

---

### M5 — Career format summary
**Owner:** You
**Effort:** 45 min
**File:** `dashboard/src/data/career_summary.csv.py`

Feeds S3 Format Cards. One row per `(player, match_type)`.

```python
#!/usr/bin/env python3
"""
Career format summary — feeds S3 Format Cards.
Output: player_name, match_type, matches, innings, runs, average,
        strike_rate, hundreds, fifties, high_score
"""
import duckdb, os, sys

DB = os.environ.get("DUCKDB_PATH", "../storage/duckdb/cricket.duckdb")
PLAYER = os.environ.get("PLAYER", "V Kohli")

con = duckdb.connect(DB, read_only=True)

df = con.execute("""
  WITH per_innings AS (
    SELECT
      batter,
      match_type,
      match_id,
      innings_number,
      SUM(runs_batter) AS innings_runs,
      SUM(is_legal_ball::INT) AS innings_balls,
      MAX(CASE WHEN is_wicket AND player_out = batter THEN 1 ELSE 0 END) AS got_out
    FROM gold.fact_delivery
    WHERE batter = ?
    GROUP BY batter, match_type, match_id, innings_number
  )
  SELECT
    batter AS player_name,
    match_type,
    COUNT(DISTINCT match_id) AS matches,
    COUNT(*) AS innings,
    SUM(innings_runs) AS runs,
    ROUND(SUM(innings_runs) * 1.0 / NULLIF(SUM(got_out), 0), 2) AS average,
    ROUND(SUM(innings_runs) * 100.0 / NULLIF(SUM(innings_balls), 0), 2) AS strike_rate,
    COUNT(*) FILTER (WHERE innings_runs >= 100) AS hundreds,
    COUNT(*) FILTER (WHERE innings_runs >= 50 AND innings_runs < 100) AS fifties,
    MAX(innings_runs) AS high_score
  FROM per_innings
  GROUP BY batter, match_type
  ORDER BY match_type
""", [PLAYER]).df()

df.to_csv(sys.stdout, index=False)
```

**Validation (reference values for Kohli):**
- ODI row: ~14,797 runs, 54 centuries (design doc §3.2 KPI-F2)
- Test row: ~9,230 runs, 30 centuries
- T20I row: ~4,188 runs, 1 century

**Claude review prompt:**
```
Review this DuckDB query [paste]. Specifically check:
1. Is "got_out" correctly counted? (player_out = batter handles non-striker
   run-outs correctly)
2. Does is_legal_ball include wides correctly for strike rate?
3. Why might my hundreds count differ from ESPNcricinfo? (forfeited
   innings? super-overs?)
```

---

### M6 — Year-by-year batting
**Owner:** You
**Effort:** 45 min
**File:** `dashboard/src/data/year_wise.csv.py`

Feeds S2 Timeline River + S4 Runs Progression. One row per `(season, match_type)`.

```python
#!/usr/bin/env python3
"""
Year-by-year batting — feeds S2 Timeline River + S4 Runs Progression.
Output: season, match_type, runs, innings, hundreds, average, strike_rate
"""
import duckdb, os, sys

DB = os.environ.get("DUCKDB_PATH", "../storage/duckdb/cricket.duckdb")
PLAYER = os.environ.get("PLAYER", "V Kohli")

con = duckdb.connect(DB, read_only=True)

df = con.execute("""
  WITH per_innings AS (
    SELECT
      EXTRACT(YEAR FROM dm.match_date)::INT AS season,
      fd.batter,
      fd.match_type,
      fd.match_id,
      fd.innings_number,
      SUM(fd.runs_batter) AS innings_runs,
      SUM(fd.is_legal_ball::INT) AS innings_balls,
      MAX(CASE WHEN fd.is_wicket AND fd.player_out = fd.batter THEN 1 ELSE 0 END) AS got_out
    FROM gold.fact_delivery fd
    JOIN gold.dim_match dm USING (match_id)
    WHERE fd.batter = ?
    GROUP BY season, fd.batter, fd.match_type, fd.match_id, fd.innings_number
  )
  SELECT
    season,
    match_type,
    SUM(innings_runs) AS runs,
    COUNT(*) AS innings,
    COUNT(*) FILTER (WHERE innings_runs >= 100) AS hundreds,
    ROUND(SUM(innings_runs) * 1.0 / NULLIF(SUM(got_out), 0), 2) AS average,
    ROUND(SUM(innings_runs) * 100.0 / NULLIF(SUM(innings_balls), 0), 2) AS strike_rate
  FROM per_innings
  GROUP BY season, match_type
  ORDER BY season, match_type
""", [PLAYER]).df()

df.to_csv(sys.stdout, index=False)
```

**Validation:**
2018 ODI runs should be ~1,202 (design doc S2 table at line 908).

---

### M7 — Opponent analysis
**Owner:** You
**Effort:** 1 hour
**File:** `dashboard/src/data/opponent_stats.csv.py`

Feeds S6 Opponent Analysis + S5 opponent century heatmap.

**Tricky bit:** the player's team comes from `silver.match_players`. Ask Claude for the join pattern if your first attempt returns wrong opponents.

```python
#!/usr/bin/env python3
"""
Opponent analysis — feeds S6 Opponent Analysis + S5 opponent heatmap.
Output: opponent, match_type, runs, innings, average, hundreds, high_score
"""
import duckdb, os, sys

DB = os.environ.get("DUCKDB_PATH", "../storage/duckdb/cricket.duckdb")
PLAYER = os.environ.get("PLAYER", "V Kohli")

con = duckdb.connect(DB, read_only=True)

df = con.execute("""
  WITH player_team AS (
    -- Player's team per match (from silver.match_players)
    SELECT match_id, team AS player_team
    FROM silver.match_players
    WHERE display_name = ?
  ),
  per_innings AS (
    SELECT
      pt.player_team,
      CASE
        WHEN pt.player_team = dm.team_a THEN dm.team_b
        ELSE dm.team_a
      END AS opponent,
      fd.match_type,
      fd.match_id,
      fd.innings_number,
      SUM(fd.runs_batter) AS innings_runs,
      MAX(CASE WHEN fd.is_wicket AND fd.player_out = fd.batter THEN 1 ELSE 0 END) AS got_out
    FROM gold.fact_delivery fd
    JOIN player_team pt USING (match_id)
    JOIN gold.dim_match dm USING (match_id)
    WHERE fd.batter = ?
    GROUP BY pt.player_team, dm.team_a, dm.team_b, fd.match_type,
             fd.match_id, fd.innings_number
  )
  SELECT
    opponent,
    match_type,
    SUM(innings_runs) AS runs,
    COUNT(*) AS innings,
    ROUND(SUM(innings_runs) * 1.0 / NULLIF(SUM(got_out), 0), 2) AS average,
    COUNT(*) FILTER (WHERE innings_runs >= 100) AS hundreds,
    MAX(innings_runs) AS high_score
  FROM per_innings
  GROUP BY opponent, match_type
  ORDER BY runs DESC
""", [PLAYER, PLAYER]).df()

df.to_csv(sys.stdout, index=False)
```

**Validation:**
Sri Lanka ODI row: ~2,652 runs, 10 hundreds (design doc §5.5 heatmap).

---

### M8 — Dismissal modes
**Owner:** You
**Effort:** 45 min
**File:** `dashboard/src/data/dismissals.csv.py`

Feeds S10 Dismissal Lab.

```python
#!/usr/bin/env python3
"""
Dismissal modes — feeds S10 Dismissal Lab.
Output: match_type, dismissal_kind, count, pct_of_dismissals, top_bowler
"""
import duckdb, os, sys

DB = os.environ.get("DUCKDB_PATH", "../storage/duckdb/cricket.duckdb")
PLAYER = os.environ.get("PLAYER", "V Kohli")

con = duckdb.connect(DB, read_only=True)

df = con.execute("""
  WITH dismissals AS (
    SELECT
      match_type,
      dismissal_kind,
      bowler
    FROM gold.fact_delivery
    WHERE player_out = ?
      AND is_wicket
      AND dismissal_kind IS NOT NULL
  ),
  per_kind AS (
    SELECT
      match_type,
      dismissal_kind,
      COUNT(*) AS count
    FROM dismissals
    GROUP BY match_type, dismissal_kind
  ),
  totals AS (
    SELECT match_type, SUM(count) AS total FROM per_kind GROUP BY match_type
  ),
  top_bowlers AS (
    SELECT match_type, dismissal_kind, bowler, COUNT(*) AS dismissals_by_bowler,
      ROW_NUMBER() OVER (PARTITION BY match_type, dismissal_kind
                         ORDER BY COUNT(*) DESC) AS rn
    FROM dismissals
    GROUP BY match_type, dismissal_kind, bowler
  )
  SELECT
    pk.match_type,
    pk.dismissal_kind,
    pk.count,
    ROUND(pk.count * 100.0 / t.total, 1) AS pct_of_dismissals,
    tb.bowler AS top_bowler
  FROM per_kind pk
  JOIN totals t USING (match_type)
  LEFT JOIN top_bowlers tb
    ON tb.match_type = pk.match_type
   AND tb.dismissal_kind = pk.dismissal_kind
   AND tb.rn = 1
  ORDER BY pk.match_type, pk.count DESC
""", [PLAYER]).df()

df.to_csv(sys.stdout, index=False)
```

**Validation:**
ODI "caught" row should show ~70% of dismissals (design doc §5.10 Panel A).

---

### M9 — Innings context (chasing/setting + home/away)
**Owner:** You
**Effort:** 1 hour
**File:** `dashboard/src/data/innings_context.csv.py`

Feeds S5 sub-sections C, D (chasing vs setting, home vs away).

**You'll need:**
- `gold.fact_innings.innings_number` (1 = bat first, 2 = chase)
- `gold.dim_match.city` + a static list of India home cities
- A century flag: `runs >= 100`

Ask Claude for the India-cities list when you start — it's a 12-city lookup.

---

### M10 — Tournament editions
**Owner:** You
**Effort:** 45 min
**File:** `dashboard/src/data/tournaments.csv.py`

Feeds S9. Filter `dim_match.event_name LIKE '%T20 World Cup%'` and `'%Cricket World Cup%'`, aggregate per edition (use `EXTRACT(YEAR FROM match_date)`).

---

### Phase 2 review checkpoint
After M4–M10, run this Claude prompt:

```
I've built 7 data loaders for the dashboard. Paste each one briefly:

[paste each .csv.py file]

Review:
1. Are the SQL queries semantically equivalent to what the design doc expects?
2. Any DuckDB anti-patterns?
3. Should any loaders be merged?
4. Are column names consistent? (e.g., always `season` not sometimes `year`)
5. Are PLAYER env var defaults consistent across all files?
6. Does the Kohli output match design-doc reference values?
```

**Commit cadence:** One commit per loader. Message format:
```
feat(dashboard): add [loader-name] data loader

[1-line description of output + which spec section it feeds]
```

---

## 7. Phase 3 — Component Library (M11–M20)

### M11 — Format Cards (S3)
**Owner:** mm-claude
**Effort:** 1.5 hours
**File:** `dashboard/src/components/format-card.js`

**mm-claude prompt:**
```
Implement a Format Card component for the Cricket Intelligence dashboard.

Stack: Observable Framework v1.13.4, Observable Plot for sparklines, no React.
File to create: dashboard/src/components/format-card.js

Spec: §5.3 of ~/Downloads/virat-kohli-dashboard-FULL-DESIGN.md (Format Cards).
Three card variants:
- Test (accent var(--color-test) = #6366F1, status "RETIRED · MAY 2025")
- ODI (accent var(--color-odi) = #F59E0B, status "ACTIVE", FEATURED)
- T20I (accent var(--color-t20i) = #10B981, status "RETIRED · JUL 2024")

Each card displays:
- Top badge (status)
- Format name + matches count
- Stat rows: runs, average, centuries, half-centuries, high score, strike rate
- Bottom sparkline of runs-per-year (Observable Plot, 80×30px)

Input data shape:
{
  format: "ODI",
  accentVar: "--color-odi",
  matches: 302,
  runs: 14797,
  average: 58.71,
  hundreds: 54,
  fifties: 77,
  highScore: 183,
  strikeRate: 93.82,
  statusBadge: "ACTIVE · WORLD RECORD HOLDER",
  yearlyRuns: [{season: 2008, runs: 159}, ...]
}

Export: `export function formatCard(data)` returning a DOM element.

Use:
- CSS variables for ALL colors (no hex literals)
- Font: var(--font-display) for the format name, var(--font-body) for stats
- Observable Plot's Plot.lineY for sparkline
- document.createElement (no JSX, no template strings for DOM)

Keep under 120 lines.
```

**Validation in `src/kohli.md`:**
````
```js
import { formatCard } from "./components/format-card.js";
const summary = FileAttachment("data/career_summary.csv").csv({typed: true});
const yearly = FileAttachment("data/year_wise.csv").csv({typed: true});

const testRow = summary.find(d => d.match_type === "Test");
const testYearly = yearly.filter(d => d.match_type === "Test")
                         .map(d => ({season: d.season, runs: d.runs}));

display(formatCard({
  format: "Test",
  accentVar: "--color-test",
  matches: testRow.matches,
  runs: testRow.runs,
  // ... etc
  yearlyRuns: testYearly,
}));
```
````

**Claude teaching prompt:**
```
mm-claude generated this format-card.js [paste]. As a data engineer
learning JS:
1. What does `export function` do? (compare to Python's `def`)
2. How does document.createElement work compared to JSX?
3. Why use template literals vs string concatenation?
4. What's the difference between Plot.plot() and Plot.lineY()?
5. What would I change to add a 4th card variant?
```

---

### M12 — Runs Progression (S4)
**Owner:** mm-claude (Claude designs the data contract first)
**Effort:** 2 hours
**File:** `dashboard/src/components/runs-progression.js`

**Step 1 — Claude design prompt:**
```
Design the data contract for a multi-series area chart (Test/ODI/T20I)
with milestone annotation overlays.

My loader output (year_wise.csv) has rows like:
  season=2018, match_type="ODI", runs=1202, hundreds=6, ...

I need to render this as Observable Plot. Questions:
1. Pivot to wide format (one row per year, columns: test_runs/odi_runs/t20i_runs)
   or keep long format and use Plot's series channel?
2. How do I overlay milestone annotations (vertical dashed line + label box)?
3. What's the simplest stack — areaY with fill opacity + lineY with stroke
   on top? Or use Plot.compose?
4. Recommended axes: domain, format, gridlines?

Give me a data shape + 30-line Plot.plot({}) skeleton I can hand to mm-claude.
```

**Step 2 — mm-claude prompt:**
```
Implement multi-series Runs Progression chart per §5.4 of the design doc.

File: dashboard/src/components/runs-progression.js
Data shape (from year_wise.csv):
[paste sample rows]

Architectural design from Claude:
[paste Claude's recommendation]

Milestone annotations (10 total, from §5.4 table):
const MILESTONES = [
  {year: 2009, label: "1st ODI Century vs SL", format: "ODI"},
  {year: 2011, label: "Test Debut", format: "Test"},
  {year: 2012, label: "183 vs PAK", format: "ODI"},
  {year: 2016, label: "PEAK BEGINS", format: "Both"},
  {year: 2019, label: "254* vs SA — Career Test Best", format: "Test"},
  {year: 2020, label: "Century Drought Begins", format: "All"},
  {year: 2022, label: "122* vs AFG — Drought Ends", format: "T20I"},
  {year: 2023, label: "50th ODI 100 — Breaks Tendulkar", format: "ODI"},
  {year: 2024, label: "T20 WC Winner · Retires from T20I", format: "T20I"},
  {year: 2025, label: "Test Retirement", format: "Test"},
];

Export: `export function runsProgression(yearlyData, options)` returning
a DOM element. options.milestones defaults to MILESTONES.

Use CSS variables for colors. Plot height 480, full width.
```

**Validation:** Chart renders all 3 series, 2018 visible peak, 10 vertical dashed annotation lines.

---

### M13 — Centuries Deep Dive (S5)
**Owner:** mm-claude (5 sub-components)
**Effort:** 3 hours total
**Files:**
- M13a `dashboard/src/components/centuries-donut.js`
- M13b `dashboard/src/components/centuries-per-year.js`
- M13c `dashboard/src/components/chasing-vs-setting.js`
- M13d `dashboard/src/components/home-vs-away.js`
- M13e `dashboard/src/components/opponent-heatmap.js`

**Sub-prompt template (repeat per sub-module):**
```
Implement [sub-module] per §5.5 Sub-section [letter] of the design doc.

File: dashboard/src/components/[name].js
Spec excerpt: [paste relevant Sub-section text]
Input data shape: [paste loader output]
Export: `export function [name](data)`

Use:
- Observable Plot if appropriate (donut, bar, grouped bar all supported)
- D3 only for the heatmap (Plot doesn't do heatmaps elegantly)
- CSS variables for all colors
- Reference design tokens: var(--color-test), var(--color-odi), var(--color-t20i)

Keep each component under 80 lines.
```

**Sub-component specifics:**

| Sub | Component | Library | Key spec detail |
|---|---|---|---|
| M13a | centuries-donut | Plot | Format split: Tests 30 / ODIs 54 / T20Is 1 |
| M13b | centuries-per-year | Plot (stacked bar) | Annotate 2017+2018 (11 each, joint peak) |
| M13c | chasing-vs-setting | Plot (horizontal split bar) | Add "World Record" badge below |
| M13d | home-vs-away | Plot (grouped bar) | Test: 14 home / 16 away (highlight insight) |
| M13e | opponent-heatmap | **D3** | Color-coded grid table by opponent count |

---

### M14 — Timeline River (S2) — THE HARD ONE
**Owner:** Claude designs, mm-claude implements, you iterate
**Effort:** 4–6 hours
**File:** `dashboard/src/components/timeline-river.js`

**Step 1 — Claude design session (mandatory before mm-claude):**
```
Design the Timeline River component per §5.2 of the design doc.

Background: This is a custom D3.js SVG showing year-by-year combined runs
as a flowing river, with glow dots for centuries and phase zone labels.

For a data engineer learning D3, walk through:

1. Required D3 modules (d3-scale, d3-shape, d3-selection)
2. Data shape — I have year_wise.csv pivoted to wide. Should I aggregate
   per year first, or pass long format and aggregate in the chart?
3. The bezier-smooth top edge — d3.line().curve(d3.curveCatmullRom)?
   d3.area()? Both?
4. Glow dots above each year — separate <g> selection? size = 6 + (centuries*4)
5. Phase labels (P1-P6, year ranges, colors per §5.2). How do I avoid
   overlapping with the river itself?
6. Hover tooltip — d3-tip? Plain HTML floating div? Plot's tip?
7. SVG viewBox / responsive sizing strategy.

Give me pseudo-code (~50 lines) showing the structure and key decisions.
mm-claude will turn it into real code.
```

**Step 2 — mm-claude prompt (paste Claude's design):**
```
Implement the Career Timeline River D3 component.

File: dashboard/src/components/timeline-river.js

Spec: §5.2 of ~/Downloads/virat-kohli-dashboard-FULL-DESIGN.md
[paste §5.2 text]

Architectural design from Claude:
[paste Claude's pseudo-code]

Data input: yearWiseAggregated array, schema:
[{year, total_runs, test_runs, odi_runs, t20i_runs, centuries, dominant_format}]

Requirements:
- Pure D3 (no Plot)
- Output: SVG element via document.createElementNS
- Smooth bezier top edge via d3.line().curve(d3.curveCatmullRom.alpha(0.5))
- Glow dots above each year, size formula: 6 + (centuries * 4)
- Phase labels floating above with the colors specified in §5.2
- Hover tooltip showing per-year breakdown (per §5.2 tooltip mock)
- CSS variables for ALL colors

Export: `export function timelineRiver(data, options)` returning SVG element.

Constraints:
- No d3-tip (use plain absolute-positioned div tooltip)
- No external CSS files (use style attributes or CSS variables already in :root)
- Keep under 250 lines (this is the most complex component; that's the budget)
```

**Step 3 — Claude review (mandatory, even if it looks fine):**
```
Review this D3 Timeline River component [paste].

As a data engineer learning D3, explain each major block:
1. The scaleLinear/scaleBand setup — what do domain/range do?
2. The area path generation — what does .curve(d3.curveCatmullRom) do?
3. The event handler attachments — mouseover/mouseout pattern
4. Anything you'd flag as a bug or anti-pattern

Also check:
- Are the phase year-ranges correct (P1: 2008-10, P2: 2011-14, ...)?
- Does the dot size formula match §5.2?
- Is the tooltip positioned correctly relative to mouse?
- Will this break with 0 centuries in a year?
- Will this break if a year is missing from the data?
```

---

### M15 — Opponent Analysis (S6)
**Owner:** mm-claude
**Effort:** 1 hour
**File:** `dashboard/src/components/opponent-bars.js`

**Prompt:**
```
Implement Opponent Analysis chart per §5.6 of the design doc.

File: dashboard/src/components/opponent-bars.js
Input: opponent_stats.csv (rows: opponent, match_type, runs, innings, average, hundreds, high_score)
Type: Horizontal bar chart, sorted by runs descending

Requirements:
- Tab switcher for match_type: ODI / Test / T20I (mm-claude: use Observable's
  Inputs.radio() for the tab; the component should accept an active match_type)
- Bar fill: var(--color-odi) / var(--color-test) / var(--color-t20i) by tab
- Average overlay: small white dot per row at average value (use Plot's dot mark)
- Data labels: Runs at end of bar, average shown as separate small text

Export: `export function opponentBars(data, options)` where options.matchType = "ODI".

Use Observable Plot. Keep under 80 lines.
```

---

### M16 — Dismissal Lab (S10)
**Owner:** mm-claude
**Effort:** 1.5 hours
**Files:**
- `dashboard/src/components/dismissal-donut.js`
- `dashboard/src/components/bowler-weakness-cards.js`

**Prompt 1:**
```
Implement Dismissal Donut per §5.10 of the design doc.

File: dashboard/src/components/dismissal-donut.js
Input: dismissals.csv (rows: match_type, dismissal_kind, count, pct_of_dismissals, top_bowler)
Output: Pair of donut charts (ODI and Test, side by side)

Requirements:
- Use Observable Plot's Plot.text + Plot.dot for donut effect
  (or D3 d3.arc if Plot can't render a donut elegantly)
- Center label: total dismissal count + "Dismissals"
- Color per dismissal kind (consistent across both donuts):
  caught=#F59E0B, bowled=#EF4444, lbw=#DC2626, runOut=#9CA3AF, ...
- Tooltip on hover: count + percentage + top_bowler

Export: `export function dismissalDonut(data, options)` where
options.matchType = "ODI" | "Test".

Keep under 100 lines.
```

**Prompt 2:**
```
Implement Bowler Weakness Cards per §5.10 (the "Bowler Weakness callout
cards" below the donuts).

File: dashboard/src/components/bowler-weakness-cards.js
Input: dismissals.csv (top_bowler column gives the worst bowlers per kind)
Output: 4 callout cards in a row, each highlighting a key dismissal pattern

Requirements:
- Card layout: 4 in a row, --color-surface-1 background, border-radius var(--radius-lg)
- Auto-pick top 4 most prolific bowlers from the data (sort by their count)
- Each card: icon (emoji per bowler type), bowler name, dismissals count, kind

Export: `export function bowlerWeaknessCards(data)`.
```

---

### M17 — Tournament Excellence (S9)
**Owner:** mm-claude
**Effort:** 1 hour
**File:** `dashboard/src/components/tournament-table.js`

```
Implement Tournament Edition Table per §5.9.

File: dashboard/src/components/tournament-table.js
Input: tournaments.csv (rows: event_name, year, matches, innings, runs, average,
       hundreds, fifties, high_score, player_of_tournament, result)
Output: Two tables side by side (T20 WC + ODI WC) with row highlights

Requirements:
- Filter by event_name match: "T20 World Cup" vs "Cricket World Cup"
- Row highlight (var(--color-gold-subtle) background) when result includes
  "Winner" or "Player of Tournament"
- Below each table: badge strip showing key records from §5.9 Panel A/B

Export: `export function tournamentTable(data, options)` where
options.event = "T20 WC" | "ODI WC".
```

---

### M18 — Records Wall (S11)
**Owner:** mm-claude
**Effort:** 2 hours
**Files:**
- `dashboard/src/components/records-wall.js`
- `dashboard/src/data/records.json` (static)

```
Implement the Records & Milestones Wall per §5.11.

Files:
- dashboard/src/data/records.json (static, 12 record cards from §5.11 table)
- dashboard/src/components/records-wall.js

Records JSON shape:
[
  {"id": "R1", "stat": "54 ODI Centuries", "context": "Most ever in ODI ...",
   "glow": "gold", "size": "wide", "badge": "WORLD RECORD"},
  ...12 entries from §5.11 table
]

Component:
- 4-col CSS Grid bento layout
- "wide" cards span 2 columns
- Card background var(--color-surface-1) with colored glow border per "glow" field
  (gold = box-shadow with --color-gold-glow, etc.)
- Each card: large stat (JetBrains Mono 36px), description, context, badge

Export: `export function recordsWall(records)`.
Use CSS variables. Keep under 80 lines.
```

---

### M19 — Hero section (S1)
**Owner:** mm-claude
**Effort:** 2 hours
**File:** `dashboard/src/components/hero.js`

```
Implement the Hero section per §5.1 of the design doc.

File: dashboard/src/components/hero.js
Spec excerpt: [paste full §5.1 content]

Requirements:
- Full viewport height (100vh)
- Background: --color-base + 2 radial-gradient overlays (specified in §5.1)
- Super-headline: "THE KING OF MODERN CRICKET" (Bebas Neue 80px, --color-gold)
- 6 KPI count-up cards with requestAnimationFrame animation (1800ms duration)
- Phase scroll indicator (6 dots P1-P6) at bottom

Count-up animation:
- Use requestAnimationFrame, not CSS transition (per §5.1 spec)
- Easing: cubic-bezier(0.22, 1, 0.36, 1)
- Decimal values (e.g., 58.71) animate with 2 decimal places throughout

Input data shape:
{
  totalRuns: 28215, centuries: 85, odiAverage: 58.71,
  iccTrophies: 5, iplSeasons: 18, highestScore: 254
}

Export: `export function heroSection(kpis)`.
```

---

### M20 — Legacy Comparison (S13) + Footer (S14)
**Owner:** mm-claude
**Effort:** 45 min
**Files:**
- `dashboard/src/components/legacy-compare.js`
- `dashboard/src/data/tendulkar.json` (static reference column)

```
Implement Legacy Comparison per §5.13 + Footer per §5.14.

Files:
- dashboard/src/data/tendulkar.json — static Tendulkar reference stats
  (from §5.13 comparison table, "TENDULKAR" column)
- dashboard/src/components/legacy-compare.js

Component:
- Side-by-side comparison table (Kohli from career_summary.csv, Tendulkar from JSON)
- 9 metrics from §5.13 table
- "Edge" column highlighted with --color-gold-subtle when Kohli leads
- Below: clustered bar chart "Gap to 100 Centuries" (Kohli today: 85 vs target 100)

Export: `export function legacyCompare(kohliSummary, tendulkarRef)`.

Footer (§5.14):
- Simple text-only footer at end of page
- Two lines: data sources + author + disclaimer
- --color-base background, --color-text-muted text
```

---

## 8. Phase 4 — Page Assembly (M21)

### M21 — Assemble Kohli dashboard page
**Owner:** mm-claude
**Effort:** 2 hours
**File:** `dashboard/src/kohli.md`

```
Assemble the full Kohli dashboard page in dashboard/src/kohli.md.

Layout: stack all sections vertically in the order from §4.1 of the spec:
S1 Hero → S2 Timeline River → S3 Format Cards → S4 Runs Progression →
S5 Centuries Deep Dive → S6 Opponent Analysis → S9 Tournament Excellence →
S10 Dismissal Lab → S11 Records Wall → S13 Legacy Comparison → S14 Footer

(Skip S7 Captaincy, S8 IPL, S12 Awards — not available from Cricsheet)

Page structure:
```md
---
title: Virat Kohli — Career Portfolio
toc: false
---

```js
import { heroSection } from "./components/hero.js";
import { timelineRiver } from "./components/timeline-river.js";
import { formatCard } from "./components/format-card.js";
import { runsProgression } from "./components/runs-progression.js";
import { centuriesDonut } from "./components/centuries-donut.js";
import { centuriesPerYear } from "./components/centuries-per-year.js";
import { chasingVsSetting } from "./components/chasing-vs-setting.js";
import { homeVsAway } from "./components/home-vs-away.js";
import { opponentHeatmap } from "./components/opponent-heatmap.js";
import { opponentBars } from "./components/opponent-bars.js";
import { tournamentTable } from "./components/tournament-table.js";
import { dismissalDonut } from "./components/dismissal-donut.js";
import { bowlerWeaknessCards } from "./components/bowler-weakness-cards.js";
import { recordsWall } from "./components/records-wall.js";
import { legacyCompare } from "./components/legacy-compare.js";

const career = await FileAttachment("data/career_summary.csv").csv({typed: true});
const yearly = await FileAttachment("data/year_wise.csv").csv({typed: true});
const opponent = await FileAttachment("data/opponent_stats.csv").csv({typed: true});
const dismissals = await FileAttachment("data/dismissals.csv").csv({typed: true});
const innings = await FileAttachment("data/innings_context.csv").csv({typed: true});
const tournaments = await FileAttachment("data/tournaments.csv").csv({typed: true});
const records = await FileAttachment("data/records.json").json();
const tendulkar = await FileAttachment("data/tendulkar.json").json();
```

<section id="S1">${heroSection(...)}</section>
<section id="S2">${timelineRiver(...)}</section>
... etc through S14
```

Requirements:
- Each section wrapped in <section id="S[n]">
- Pass the correct data slice to each component
- For format cards: render 3 in a CSS Grid container
- For centuries (S5): render 5 sub-components in nested grid
- Use Observable Framework's `display()` for top-level rendering

Do NOT implement filter state yet — that's M22-M23. All data shown for V Kohli.
```

**Validation:** `make dashboard-dev` opens kohli.md, all sections render, no console errors, all 11 implemented sections visible.

---

## 9. Phase 5 — Interactivity (M22–M23)

### M22 — Player dropdown (parameterised routing)
**Owner:** Claude designs, mm-claude implements
**Effort:** 2 hours

**Step 1 — Claude design prompt:**
```
Design player switching for the dashboard.

Constraints:
- Player list comes from gold.player_display_names (~10,000+ players)
- Each player needs the same 7 data CSVs as Kohli has now
- Data loaders are Python scripts that take PLAYER env var
- We want a dropdown that switches the entire dashboard

Options:
A) Build-time: run loaders for ALL players, filter in browser
   → ~70,000 CSVs at build time. Probably too slow.

B) Observable [params] dynamic routes — one URL per player, loaders take
   the player from the URL slug
   → How does this work? Does Framework regenerate per-player on demand?

C) DuckDB-WASM: load cricket.duckdb into the browser (~6.6 GB? too big),
   or load a sliced version (~10 MB per player), query reactively
   → Smallest UX latency, but biggest engineering lift.

D) Top-K precompute: bake data for top 50 players, dropdown limited to those
   → Pragmatic. Acceptable scope cut.

Recommend the approach. Show me what mm-claude would need to implement.
```

**Step 2 — mm-claude prompt (paste Claude's recommendation):**
```
Implement player switching per Claude's recommendation [paste].

[specific files and changes per the chosen approach]
```

---

### M23 — Format tab + year range slider
**Owner:** mm-claude
**Effort:** 1.5 hours

```
Add global filters to dashboard/src/kohli.md:

1. Format tab radio (above hero section):
   const formatFilter = view(Inputs.radio(
     ["ALL", "Test", "ODI", "T20I"],
     {label: "Format", value: "ALL"}
   ));

2. Year range slider (above timeline river):
   const yearRange = view(Inputs.range(
     [2008, 2026],
     {step: 1, label: "Year range", value: [2008, 2026]}
   ));

Wire these to existing components by adding reactive filters:
- Format filter affects: S3 Format Cards (highlight active), S4 Runs Progression,
  S5 Centuries (where applicable), S6 Opponent tab, S10 Dismissal
- Year range affects: S2 Timeline River, S4 Runs Progression, S5 century bar

Per §7.2 of the design doc:
- Hero KPIs do NOT update with year filter
- Records wall (S11) does NOT filter
- Awards (S12) does NOT filter

Use Observable's reactive cells — re-deriving filtered datasets before
passing to components. Components do NOT need to change.
```

---

## 10. Phase 6 — Polish (M24–M27)

### M24 — Responsive breakpoints
**Owner:** mm-claude
**Effort:** 1 hour
**File:** Update `dashboard/src/styles/theme.css`

```
Add responsive breakpoints to dashboard/src/styles/theme.css per §8 of the spec.

Breakpoints:
--bp-sm:  480px;
--bp-md:  768px;
--bp-lg:  1024px;
--bp-xl:  1280px;

Implement the layout adaptations from §8.2 as media queries:
- Hero KPIs: 6-in-row (desktop) → 3×2 (tablet) → 2×3 (mobile)
- Format Cards: 3-col → 2+1 → 1-col
- Centuries Donut + Bar: side by side → side by side → stacked
- Records Wall: 4-col → 3-col → 2-col
- Awards Timeline: horizontal → scrollable → vertical list

Target each section by its <section id="..."> wrapper.
```

### M25 — Print stylesheet
**Owner:** mm-claude
**Effort:** 30 min
**File:** `dashboard/src/styles/print.css`

```
Create dashboard/src/styles/print.css for PDF export via window.print().

Requirements:
- All sections visible (no JS-driven hide states)
- Page breaks: avoid breaking mid-chart (page-break-inside: avoid on .chart-card)
- Force light text on white background (override dark theme)
- Hide interactive elements: dropdowns, sliders, hover-only tooltips
- Show all dropdown values as static text where they'd normally be filters

Import this stylesheet with media="print" from observablehq.config.js head.
```

### M26 — Accessibility pass
**Owner:** You + Claude
**Effort:** 1 hour

Walk through §10.1-10.4 of the design doc:
- Add `aria-label` to every chart with a 1-sentence summary
- Add `<caption>` and screen-reader tables for donut/heatmap data
- Verify keyboard navigation (Tab through dropdowns and tabs)
- Verify @media (prefers-reduced-motion: reduce) disables count-up + glow-pulse

Claude prompt:
```
Audit dashboard/src/components/*.js for accessibility against §10 of the
design doc. Specifically:
1. Are aria-labels present on every chart?
2. Is the focus visible on interactive elements?
3. Does the reduced-motion media query disable animations correctly?

Paste each component file. I'll review.
```

### M27 — README + deploy
**Owner:** You + Claude reviews
**Effort:** 1 hour
**File:** Update `dashboard/README.md` + add `dashboard/DEPLOY.md`

Document:
1. How to run locally (already in README)
2. How to add a new player (M22 design)
3. Data loader update cadence (re-run when Gold DAG completes)
4. Deployment: `npm run build` → `dist/` → serve from any static host
5. GitHub Pages deploy via GitHub Actions

---

## 11. Common Claude review prompts

### Quick code review (after every mm-claude session)
```
Review this code for a Cricket Intelligence dashboard component:

[paste full file]

Spec it should implement: §[X] of ~/Downloads/virat-kohli-dashboard-FULL-DESIGN.md
[paste relevant section]

Check for:
1. Correctness vs spec
2. Bugs / off-by-ones
3. Hardcoded values that should be CSS variables
4. Missing null handling
5. Anti-patterns specific to Observable Framework

For each issue, suggest the fix. For each clean section, say nothing.
```

### Teaching prompt (when you don't understand mm-claude output)
```
Explain this code [paste] to me as a data engineer learning JS/D3/Observable
Framework.

Specifically:
- What does each major block do?
- Compare to Python/SQL equivalents where useful
- What would I change if I wanted to [specific tweak]?
- Anything I should question or push back on?
```

### Debug prompt (when something's broken)
```
This component isn't working as expected.

Expected behavior: [what the spec says]
Actual behavior: [what you see / paste console errors]
Code: [paste]

Help me diagnose. Is this:
- A data shape mismatch?
- A library version issue?
- A logic bug in the component?
- A wiring bug at the page level?
```

### SQL review prompt
```
Review this DuckDB query I wrote for a data loader:

[paste SQL]

It's supposed to return: [expected schema]
Spec expects values like: [reference values from design doc]
Actual output for V Kohli: [paste first 10 rows]

Check:
1. Is the SQL semantically correct?
2. Will the grain (one row per X) be unique?
3. Any anti-patterns (FILTER on NULL, missing INDEX, etc.)?
4. Why might output differ from the design doc reference values?
```

---

## 12. Data model reference

### Gold tables (in DuckDB `gold` schema)

| Table | Grain | Key columns for this dashboard |
|---|---|---|
| `fact_delivery` | One row per ball | `batter, bowler, runs_batter, is_legal_ball, is_wicket, player_out, dismissal_kind, match_type, season, match_id, innings_number, over_number, delivery_number` |
| `fact_innings` | One row per innings | `match_id, innings_number, total_runs, target_runs, target_overs` |
| `fact_match_result` | One row per match | `match_id, winner, win_by_runs, win_by_wickets, toss_winner_won` |
| `fact_player_match` | One row per (match, player) | `match_id, person_id, player_name, team` |
| `fact_player_of_match` | One row per (match, player) | `match_id, player_name` |
| `dim_match` | One row per match | `match_id, match_type, match_date, season, team_a, team_b, venue, city, event_name, toss_winner, toss_decision, winner` |
| `dim_player` | One row per person | `person_id, full_name, unique_name` |
| `dim_team`, `dim_venue`, `dim_competition`, `dim_official` | Standard dimensions |
| `player_display_names` | One row per (cricsheet_name, display_name) | `cricsheet_name, display_name, person_id` |
| `mart_player_batting` | (player, match_type, season) | Pre-aggregated batting stats |
| `mart_player_bowling` | (bowler, match_type, season) | Pre-aggregated bowling stats |

### Cricsheet quirks to remember

- **Player names abbreviated**: `V Kohli` not `Virat Kohli`. The `player_display_names` table maps these.
- **`player_out` is authoritative for dismissals** (not `batter` — they differ on run-outs)
- **Multi-wicket deliveries exist**: single ball can produce 2 wickets (caught + non-striker run-out)
- **`fact_player_match.person_id` is ~1.27% populated** — Cricsheet rarely includes registry IDs. Joins on `batter` (display name) are primary.
- **Bronze append-only**: `(match_id, revision)` PK; Silver/Gold show `MAX(revision) per match_id`
- **No IPL salary, no captain flag, no ICC awards** — these are static if needed
- **`is_legal_ball`** excludes wides+noballs (correct for strike rate denominator)

### Design doc reference values for Kohli (validate against these)

| Stat | Value | Source |
|---|---|---|
| International runs | 28,215 | §1 |
| Total centuries | 85 | §1 |
| ODI matches | 302+ | §2.1 |
| ODI runs | 14,797 | §2.1 |
| ODI average | 58.71 | §2.1 |
| ODI centuries | 54 (World Record) | §2.1 |
| Test matches | 123 | §2.1 |
| Test runs | 9,230 | §2.1 |
| Test centuries | 30 | §2.1 |
| T20I matches | 125 | §2.1 |
| T20I runs | 4,188 | §2.1 |
| T20I centuries | 1 | §2.1 |
| Career HS Test | 254* | §2.1 |
| Career HS ODI | 183 | §2.1 |
| 2018 ODI runs | 1,202 | §5.2 table |
| 2017 + 2018 centuries | 11 each | §5.5 |
| Sri Lanka ODI hundreds | 10 | §5.5 |
| ODI dismissed "caught" | ~70% | §5.10 |
| 2023 ODI WC runs | 765 (Most ever) | §5.11 R3 |

If your data loader output disagrees with these, investigate before continuing.

---

## 13. Verification gates

### Per-module (every module)
- [ ] Code compiles / `make dashboard-dev` has no errors
- [ ] Output matches design spec section §X.Y visually
- [ ] No hardcoded colors (all CSS variables)
- [ ] No console errors
- [ ] Git commit clean (no `node_modules`, no `.env`)

### Per-phase
- **Phase 2 (Data Layer):** All 7 loaders produce CSV; Kohli values match design-doc reference
- **Phase 3 (Components):** All 14 components individually render with mock data
- **Phase 4 (Page Assembly):** Full kohli.md page renders without errors, scrollable, all sections visible
- **Phase 5 (Interactivity):** Player dropdown switches dashboard; format tab + year slider filter correctly
- **Phase 6 (Polish):** Mobile breakpoints work; print preview readable; Lighthouse > 85

### Final (end of project)
- [ ] `npm run dev` opens dashboard at `localhost:3030`, no console errors
- [ ] Kohli page renders all 11 implemented sections (S1, S2, S3, S4, S5, S6, S9, S10, S11, S13, S14)
- [ ] Player dropdown can switch to at least one other player (Rohit Sharma)
- [ ] All chart data matches design-doc reference values (spot-check 5 KPIs)
- [ ] Year range slider filters Timeline River + Runs Progression
- [ ] Format tab filter narrows applicable sections
- [ ] Print preview produces usable PDF-style layout
- [ ] Lighthouse Performance > 85
- [ ] `npm run build` produces `dist/` under 5 MB
- [ ] README documents how to add a new player

---

## 14. Risk register & known pitfalls

| Risk | Mitigation |
|---|---|
| **mm-claude produces opaque D3 you can't debug** | Always ask Claude for a line-by-line walkthrough before committing. Never accept code you don't understand. |
| **Data loader returns wrong numbers** | Validate every loader against the §13 reference values for Kohli. |
| **Component looks right but data is wrong** | Cross-check loader CSV in a separate cell before passing to component. |
| **Build time explodes with multi-player data** | M22 design decision — Claude flags this upfront. Falls back to top-K precompute if needed. |
| **DuckDB lock conflicts** | Stop the dev server (Ctrl-C) AND Metabase before running Gold DAGs. See dashboard/README.md. |
| **Port 3030 collision** | `lsof -iTCP:3030 -sTCP:LISTEN` — kill the offender. |
| **Poetry shell not active** | Python data loaders fail with `ModuleNotFoundError: duckdb`. Always `poetry shell` first. |
| **Scope creep on skipped sections** | S7 Captaincy, S8 IPL, S12 Awards are intentionally out of scope. Stay disciplined — don't try to source them. |
| **CSS variables missing** | Component looks unstyled / colorless. Confirm theme.css is imported (M3). |
| **Sample files re-introduced** | Don't accept mm-claude rewrites that add example-dashboard.md back. |

### Things that already burned us
- The wizard's "Initialize git repository? Yes" creates a nested `.git` inside `dashboard/`. Always answer **No**. (Fixed in M1.)
- Port 3000 conflicts with Metabase. Always use 3030. (Fixed in M1.)
- The Observable Framework `create` command is interactive and can't be piped — you have to run it manually.
- `dashboard/.gitignore` (wizard-created) handles `node_modules` and `dist`, but the root `.gitignore` should explicitly list them too for clarity.

---

## 15. Learning outcomes

By the end of this project you will have:

| Skill | Where you build it |
|---|---|
| **Observable Framework** | Built data loaders + 14 components + page assembly + interactivity |
| **Vanilla JS basics** | Reviewing every mm-claude diff; explaining unfamiliar patterns with Claude |
| **D3.js fundamentals** | M14 Timeline River + M13e heatmap |
| **Observable Plot** | M12, M13a-d, M15, M16, M17 |
| **CSS design systems** | M3 token system; M24 responsive |
| **AI orchestration** | Routing tasks between Claude (design/review/teach) and mm-claude (execute) |
| **AI output validation** | Catching wrong SQL, wrong scales, missing edge cases |
| **Spec-driven dev** | Translating a 1,870-line design doc into 27 modules with verifiable outcomes |
| **Multi-tool dev workflow** | Terminal + IDE + browser + mm-claude all in one feedback loop |

### Meta-skill being developed
You're learning to be a **technical director of AI agents**. The output isn't just a dashboard — it's the muscle memory of:
1. Knowing what to ask for (spec precision)
2. Knowing which tool to ask (workflow routing)
3. Knowing whether the answer is correct (validation)
4. Knowing when to push back (rejecting bad output)

This skill scales horizontally to every domain. In 6 months you'll be doing this for ML models, data pipelines, infra changes, microservices — anything where AI can generate code but you have to direct and verify.

---

## Appendix A — File creation index

| File | Created by | Status |
|---|---|---|
| `dashboard/observablehq.config.js` | wizard + you (M1) | ✅ |
| `dashboard/package.json` | wizard + you (M1) | ✅ |
| `dashboard/.env`, `.env.example` | you (M1) | ✅ |
| `dashboard/README.md` | you (M1) | ✅ |
| `dashboard/src/index.md` | mm-claude (M2) | ⬜ |
| `dashboard/src/styles/theme.css` | mm-claude (M3) | ⬜ |
| `dashboard/src/data/player_resolver.csv.py` | you (M4) | ⬜ |
| `dashboard/src/data/career_summary.csv.py` | you (M5) | ⬜ |
| `dashboard/src/data/year_wise.csv.py` | you (M6) | ⬜ |
| `dashboard/src/data/opponent_stats.csv.py` | you (M7) | ⬜ |
| `dashboard/src/data/dismissals.csv.py` | you (M8) | ⬜ |
| `dashboard/src/data/innings_context.csv.py` | you (M9) | ⬜ |
| `dashboard/src/data/tournaments.csv.py` | you (M10) | ⬜ |
| `dashboard/src/data/records.json` | mm-claude (M18) | ⬜ |
| `dashboard/src/data/tendulkar.json` | mm-claude (M20) | ⬜ |
| `dashboard/src/components/format-card.js` | mm-claude (M11) | ⬜ |
| `dashboard/src/components/runs-progression.js` | mm-claude (M12) | ⬜ |
| `dashboard/src/components/centuries-donut.js` | mm-claude (M13a) | ⬜ |
| `dashboard/src/components/centuries-per-year.js` | mm-claude (M13b) | ⬜ |
| `dashboard/src/components/chasing-vs-setting.js` | mm-claude (M13c) | ⬜ |
| `dashboard/src/components/home-vs-away.js` | mm-claude (M13d) | ⬜ |
| `dashboard/src/components/opponent-heatmap.js` | mm-claude (M13e) | ⬜ |
| `dashboard/src/components/timeline-river.js` | mm-claude (M14) | ⬜ |
| `dashboard/src/components/opponent-bars.js` | mm-claude (M15) | ⬜ |
| `dashboard/src/components/dismissal-donut.js` | mm-claude (M16) | ⬜ |
| `dashboard/src/components/bowler-weakness-cards.js` | mm-claude (M16) | ⬜ |
| `dashboard/src/components/tournament-table.js` | mm-claude (M17) | ⬜ |
| `dashboard/src/components/records-wall.js` | mm-claude (M18) | ⬜ |
| `dashboard/src/components/hero.js` | mm-claude (M19) | ⬜ |
| `dashboard/src/components/legacy-compare.js` | mm-claude (M20) | ⬜ |
| `dashboard/src/kohli.md` | mm-claude (M21) | ⬜ |
| `dashboard/src/styles/print.css` | mm-claude (M25) | ⬜ |
| `dashboard/DEPLOY.md` | you (M27) | ⬜ |

---

## Appendix B — Quick reference: mm-claude paste-ready snippets

### Boilerplate every component should match

```javascript
/**
 * [Component name] — [1-line description]
 * Spec: ~/Downloads/virat-kohli-dashboard-FULL-DESIGN.md §[X.Y]
 *
 * @param {Object[]} data - [shape description]
 * @param {Object} [options] - optional overrides
 * @returns {HTMLElement|SVGElement}
 */
export function componentName(data, options = {}) {
  // Implementation
}
```

### Boilerplate every data loader should match

```python
#!/usr/bin/env python3
"""
[Loader name] — feeds [section/component].
Output schema: col1, col2, col3, ...
"""
import duckdb
import os
import sys

DB = os.environ.get("DUCKDB_PATH", "../storage/duckdb/cricket.duckdb")
PLAYER = os.environ.get("PLAYER", "V Kohli")

con = duckdb.connect(DB, read_only=True)
df = con.execute("""
  SELECT ...
""", [PLAYER]).df()

df.to_csv(sys.stdout, index=False)
```

### Standard Observable Framework cell pattern (in .md files)

````md
```js
import { componentName } from "./components/component-name.js";
const data = await FileAttachment("data/loader-name.csv").csv({typed: true});
display(componentName(data));
```
````

---

*Document version: 1.0 — created post-M1 (2026-05-20)*
*Maintained alongside the dashboard source. Update as patterns evolve.*
