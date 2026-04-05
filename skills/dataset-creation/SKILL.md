---
name: dataset-creation
description: Automatically construct comprehensive datasets from natural language queries about entities. Combines SPARQL research from Wikidata, web scraping for missing features, and autonomous enrichment to produce complete CSV datasets.
---

# Dataset Construction Skill

## Overview

Transform natural language entity queries into comprehensive CSV datasets through delegation and prioritization.

## Python environment (recommended)

Use a dedicated venv so `pip` / NumPy / SciPy stay consistent (avoids mixed Anaconda + user `site-packages` issues).

1. **Venv location:** create or reuse `venv/` inside the project’s artifact folder: `<SENT2MODEL_ARTIFACTS_ROOT>/<project>/venv/`.

Agents should use that venv when invoking `wikidata_cli.py`, `enrich.py`, and `model_trainer.py`.

## Delegation (default)

**Invoke the dedicated agents** (**wikidata-agent**, **web-exploration-agent**) so they run in **separate context**. Do not substitute reading their markdown instructions in the main chat for actually delegating to them.

Do not hand-run the full CLI pipeline from the main chat unless debugging; delegation keeps discovery CSVs, scrapers, and artifact paths consistent.

| Tool / script | Who should run it |
|----------------|-------------------|
| `wikidata_cli.py` | **wikidata-agent** (file-backed datasets under `sent2model-artifacts/`) |
| `enrich.py` | **dataset-creation** workflow after **web-exploration-agent** has written `scraper.py` |
| `model_trainer.py` | **dataset-creation** optional step only, when the user wants a quick model—not a default agent task |

**Artifacts:** Set **`SENT2MODEL_ARTIFACTS_ROOT`** to the absolute path of the user’s **`sent2model-artifacts`** directory (under their workspace or repo). Set **`SENT2MODEL_PROJECT`** (or **`--project`**) for the dataset folder name. This does **not** follow the shell’s `cwd` after `cd` into the plugin; paths must be explicit.

Paths in command examples below are illustrative; use the user’s real **`SENT2MODEL_ARTIFACTS_ROOT`** and **`project`**.

## Workflow

1. **Understand the Query** — Parse user intent.
2. **Create Dataset** — **Invoke wikidata-agent** to build the Wikidata-backed dataset. 3. **Web Exploration** — **Invoke web-exploration-agent** to find web sources and author `scraper.py`.
4. **Prioritize & Enrich** — Run **`enrich.py`** programmatically (row-by-row extraction).
5. **Deliver** — Final CSV and short summary for the user.

## Subagent delegation

### Wikidata

**Invoke wikidata-agent** with the entity / dataset goal and the correct **`SENT2MODEL_ARTIFACTS_ROOT`** / **`project`**.

### Web exploration

**Invoke web-exploration-agent** with entity sample, existing columns, optional target features, **`entity_type`**, and **`output_dir`** set to the absolute project path.

## Dataset entity-wise enrichment

Run [scripts/enrich.py](scripts/enrich.py) with absolute or correct-relative paths:

```bash
python scripts/enrich.py \
    --input /abs/path/sent2model-artifacts/nba-players/dataset_sparql.csv \
    --output /abs/path/sent2model-artifacts/nba-players/dataset_enriched.csv \
    --entity-column item \
    --scraper-module scraper \
    --scraper-function scrape \
    --scraper-dir /abs/path/sent2model-artifacts/nba-players \
    --budget 100
```

**Arguments:**

- `--input`: Path to the SPARQL-generated dataset
- `--output`: Path for the enriched dataset
- `--entity-column`: Column containing entity names (default: `item`)
- `--scraper-module`: Python module with scraper function (e.g. `scraper`)
- `--scraper-function`: Function name in the module (default: `scrape`)
- `--scraper-dir`: Directory containing `scraper.py` (prepended to `sys.path` so the module resolves without `PYTHONPATH`)
- `--sep`: CSV delimiter (**default `;`** to match Wikidata CLI exports; use `,` for normal CSV)
- `--log-file`: Verbose log path (default: `<output_stem>.enrich.log` beside the output CSV). Per-row details go here; stdout gets short progress lines every `--progress-every` fraction (default 5%).
- `--min-delay`: Seconds between entities for rate limiting (default `0.75`)
- `--scraper-retries` / `--scraper-retry-delay`: Retries when `scrape()` raises (defaults: 2 retries, 0.5s base backoff)
- `--budget`: Maximum rows to process (optional; omit to process all rows)

The enrichment process:
- Iterates through entities in sequential order
- Calls the scraper function (from `scraper.py`) for each entity
- Fills in missing values only (preserves existing data)
- Adds new columns dynamically as features are discovered
- Writes detailed logs to the log file; prints progress summaries to stdout


## Optional: Model Training

Use [scripts/model_trainer.py](scripts/model_trainer.py) in place:

```python
import pandas as pd
import sys
sys.path.insert(0, 'scripts')
from model_trainer import train_simple_model

df = pd.read_csv('sent2model-artifacts/<task>/dataset_enriched.csv')
result = train_simple_model(
    df=df,
    target_column='nationality',
    feature_columns=['birth_date', 'research_area', 'institution'],
    model_type='classification',
    algorithm='random_forest'
)
print(f"Model trained. Test accuracy: {result['test_metrics']['accuracy']:.2f}")
```

# Rules
- Call agents with the agent calling tool in a separate context, don't do their work yourself.
- Don't create outputs outside the sent2model artifacts folder (unless explicitly requested by the user).
- All paths in this file are relative to ${CLAUDE_PLUGIN_ROOT} and not $CLAUDE_PROJECT_DIR.