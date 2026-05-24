# NNNN — <Title in present tense, no leading verb>

- **Status:** Proposed | Accepted | Superseded by NNNN | Deprecated
- **Date of decision:** YYYY-MM-DD
- **Deciders:** <author + reviewers>
- **Tags:** <comma-separated, e.g. storage, observability, sprint-0>

---

## Context

What is the situation that calls for a decision? What constraints, prior art, or external forces are at play? Include only the context relevant to *this* decision — link out to other docs for background.

A reader unfamiliar with the system should be able to read this section and understand the question being asked.

---

## Decision drivers

- Driver 1 — one sentence on why this matters
- Driver 2
- …

(Drivers are the criteria you'll judge alternatives against. Be explicit. Examples: "must be open-source", "must work on a 32GB laptop", "must have a managed AWS equivalent", "must have stable Python bindings".)

---

## Considered options

### Option A — <name>

One paragraph on what this is. Then:

- **Pros:** …
- **Cons:** …

### Option B — <name>

…

### Option C — <name>

…

(2–4 options is typical. If there's only one viable option, write an ADR anyway — the doc explains why nothing else works.)

---

## Decision

State the decision crisply. One paragraph.

> *"We will use **Option A** because it best satisfies drivers 1, 2, and 4."*

---

## Consequences

What changes as a result? Be honest about negative consequences too.

### Positive

- …

### Negative / trade-offs

- …

### Neutral

- …

---

## Migration path / future swap

If the decision is reversible, what would it take to swap? If it's hard to reverse, what's the trigger that would make us reconsider?

(Especially important for revamp-v2 decisions where open-standards-first means everything must have a documented swap candidate.)

---

## References

- Related ADRs: [[NNNN]] …
- Code paths: `src/...`, `docs/...`
- External: links to RFC / vendor docs / benchmarks
