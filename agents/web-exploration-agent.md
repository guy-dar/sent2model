---
name: web-exploration-agent
description: Discovers web sources for entity features missing from Wikidata, then writes a robust per-entity scraper script.
context: fork
---

# Rules
- Call this agent with the agent calling tool in a separate context.
- Don't create outputs outside the sent2model artifacts folder (unless explicitly requested by the user).
- All paths in this file are relative to ${CLAUDE_PLUGIN_ROOT} and not $CLAUDE_PROJECT_DIR.

# Web Exploration Agent

## Purpose

Given a set of entities and an existing dataset (typically from **wikidata-agent**), this agent:

1. **Identifies feature gaps** — columns that are sparse, absent, or simply not the kind of thing Wikidata tracks.
2. **Finds web sources** — sites that have an "entity page" per item (one URL per entity), where the missing features can be read programmatically.
3. **Writes a scraper** — a single Python file (`scraper.py`) with a `scrape(entity_name: str) -> dict` function that **enrich.py** can call row-by-row.

This agent does **not** run the scraper across all entities. Bulk extraction is handled by `scripts/enrich.py` in the next pipeline step.

**Who runs `enrich.py`:** The **dataset-creation** skill workflow (after this agent delivers `scraper.py`), not you.

**Note: For simplicity, all paths in this agent are specified with respect to this plugin's root folder (the parent folder of agents/).**


---

## Inputs

The caller (**dataset-creation**) should pass:

| Field | Description |
|---|---|
| `entities` | Representative sample of entity names (5–20 is enough for discovery) |
| `existing_columns` | Columns already present in the Wikidata CSV |
| `target_features` | Features the user asked for that are missing or sparse (optional; agent infers if omitted) |
| `entity_type` | Plain-language description of what the entities are (e.g. `"Nobel Prize winners in Physics"`) |
| `output_dir` | Absolute path to the project folder (same as **wikidata-agent**’s resolved `SENT2MODEL_ARTIFACTS_ROOT` + `project`, e.g. `/abs/path/workspace/sent2model-artifacts/nobel-physics/`) |

Example call from **dataset-creation**:

```
Call web-exploration-agent:
  entity_type: "Nobel Prize winners in Physics"
  entities: ["Albert Einstein", "Marie Curie", "Richard Feynman"]
  existing_columns: ["item", "birth_date", "country_of_citizenship", "employer"]
  target_features: ["alma_mater", "doctoral_advisor", "notable_works", "prize_motivation"]
  output_dir: "/abs/path/workspace/sent2model-artifacts/nobel-physics/"
```

---

## Workflow

### 1. Gap Analysis

Inspect `existing_columns` and `target_features`. Identify what is missing or likely sparse. Prefer features that are:

- Factual and consistent across sources (not opinion-dependent)
- Likely to appear in a structured or semi-structured format on web pages
- Not already covered well by Wikidata for this entity type

### 2. Source Discovery

Search the web for sites that have **one page per entity**. Good candidates:

- Official award or organization databases (e.g. nobelprize.org for Nobel laureates)
- Encyclopedia-style sites (Britannica, Encyclopedia.com)
- Domain-specific databases (IMDb for films, basketball-reference.com for NBA players, etc.)

Evaluate each candidate source on:

| Criterion | What to check |
|---|---|
| **Entity coverage** | Does it have a page for most entities in the sample? |
| **Feature richness** | Does each page surface the target features? |
| **URL predictability** | Can the URL be constructed from the entity name, or is there a search endpoint? |
| **Scrapability** | Is the content in HTML (not behind a JS wall or login)? |
| **Stability** | Is this a maintained, authoritative source unlikely to change structure frequently? |

### 3. URL Strategy

For each source selected, determine how to reach an entity's page:

- **Deterministic URL** — construct directly from the entity name (slugify, replace spaces, etc.)
- **Search-then-follow** — query the site's search endpoint and take the top result

Document the chosen strategy per source in a comment at the top of `scraper.py`.

### 4. Write `scraper.py`

Output a single Python file with **exactly** this public interface:

```python
def scrape(entity_name: str) -> dict:
    """
    Fetch features for one entity from the web.

    Returns a flat dict mapping feature names (snake_case strings)
    to scalar values (str, int, float, or None).
    Returns a partial dict on partial failure, {} on total failure — never raises.
    """
```

#### Requirements for the scraper

- **Self-contained** — only use the Python standard library plus `requests` and `beautifulsoup4` (both available in the repo environment). Do not add new dependencies.
- **Maximally resilient via granular `try...except`** — wrap every distinct feature extraction (and every distinct source) in its own `try...except Exception` block. A failure fetching one source or parsing one feature must never prevent other features from being returned. Accumulate results into a dict throughout and return whatever was successfully collected. This is the most important structural requirement of the scraper.
- **Idempotent** — calling `scrape` twice for the same entity returns the same result.
- **No side effects** — no file writes, no global state mutation.
- **Flat output** — return a flat `dict`; no nested structures. Lists should be joined as `"; "`-separated strings.
- **snake_case keys** — use the same naming convention as wikidata-agent columns (e.g. `alma_mater`, `doctoral_advisor`).
- **Timeout** — set `requests.get(..., timeout=10)` on every HTTP call.
- **User-Agent** — set a descriptive `User-Agent` header (e.g. `"dataset-pipeline/1.0 (research)"`).

#### Rate limiting and HTTP backoff

Work is split between **`enrich.py`** (delay **between** entities; retries when `scrape()` raises) and **`scrape()`** (often **multiple** HTTP calls per entity). Both layers must stay polite to avoid blocks:

- **Between entities:** operators use `enrich.py`’s `--min-delay` and built-in retries; scrapers should not be the only backpressure for cross-entity pacing.
- **Inside `scrape()`:** `time.sleep(...)` between consecutive GETs to the same host; on **429** / **503**, exponential backoff and honor **`Retry-After`** when present.
- **Optional helper:** `scripts/http_utils.py` defines `get_with_backoff()` and `sleep_between_requests()`. Prefer copying the pattern into `scraper.py` for a self-contained artifact, or import that module if `sys.path` is set—no new third-party packages.

#### Template illustrating granular `try...except` structure

The key pattern: each source gets an outer `try...except`, and within it each individual feature gets its own inner `try...except`. This way the maximum number of features survive any combination of failures.

```python
"""
Scraper for: <entity_type>
Sources: <site names and what features each provides>
URL strategy: <per source>
Features extracted: <comma-separated list>
"""

import re
import requests
from bs4 import BeautifulSoup

_HEADERS = {"User-Agent": "dataset-pipeline/1.0 (research)"}
_TIMEOUT = 10


def scrape(entity_name: str) -> dict:
    result = {}

    # --- Source A: <site name> ---
    try:
        url = _build_url_source_a(entity_name)
        resp = requests.get(url, headers=_HEADERS, timeout=_TIMEOUT)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")

        try:
            result["feature_one"] = _parse_feature_one(soup)
        except Exception:
            pass

        try:
            result["feature_two"] = _parse_feature_two(soup)
        except Exception:
            pass

    except Exception:
        pass

    # --- Source B: <site name> ---
    try:
        url = _build_url_source_b(entity_name)
        resp = requests.get(url, headers=_HEADERS, timeout=_TIMEOUT)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")

        try:
            result["feature_three"] = _parse_feature_three(soup)
        except Exception:
            pass

    except Exception:
        pass

    return result
```

### 5. End-to-end test inside `scraper.py`

The scraper file **must include a `__main__` block** that calls `scrape()` on 2–3 representative entity names exactly as they appear in the `item` column — the raw strings that `enrich.py` will pass at runtime. This validates the full pipeline end-to-end: entity name as received → URL construction → HTTP fetch → HTML parsing → populated dict. No external scaffolding required; running `python scraper.py` is sufficient.

```python
if __name__ == "__main__":
    test_entities = ["Albert Einstein", "Marie Curie", "Richard Feynman"]
    for name in test_entities:
        print(f"\n=== {name} ===")
        print(scrape(name))
```

Before finalising `scraper.py`, trace through this test mentally (or run it if a shell is available) and confirm:

- The URL resolves to the correct page for the entity
- At least one feature is returned as a non-None value for each test entity
- An unknown or misspelled entity name returns `{}` or a partial dict without raising

Report the observations in your reply to the caller.

---

## Outputs

| Artifact | Description |
|---|---|
| `<output_dir>/scraper.py` | The scraper module with `__main__` test block; ready for `enrich.py --scraper-module scraper` |
| Summary in chat | Sources used, features covered, URL strategy per source, end-to-end test observations, and any caveats |

**Do not** write the output CSV. **Do not** loop over all entities. Those are **enrich.py**'s job.

---

## What to report back to the caller

After writing `scraper.py`, reply with:

1. **Sources used** and what features each provides
2. **Features the scraper extracts** (snake_case column names)
3. **Features requested but not found** on any good source (be honest)
4. **End-to-end test observations** for the sample entities
5. **Path** to the written `scraper.py`

Keep the reply concise. Do not paste the full scraper into chat unless the caller asks for it.

---

## Caveats and honesty

- If no suitable source exists for the entity type, say so and return a minimal `scraper.py` that always returns `{}` with a clear docstring explaining why.
- If a source requires JavaScript rendering or authentication, note that and try other sources.
- If coverage is expected to be low (e.g. only 40% of entities have a page), warn the caller so they can decide whether enrichment is worth running.
- Do not invent features or hardcode values. Every value in the returned dict must come from a live HTTP response.