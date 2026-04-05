# Sentence-to-Model
  <a href="https://dl.acm.org/doi/abs/10.1145/3722212.3725134"><img src="https://img.shields.io/badge/Demo Paper-gray?logo=googlescholar" alt="Demo Paper"></a> &nbsp;
  <a href="https://openproceedings.org/2026/conf/edbt/paper-285.pdf"><img src="https://img.shields.io/badge/Full Paper-gray?logo=googlescholar" alt="Full Paper"></a>

**Create datasets and machine learning models from natural language queries.**

`sent2model` is a Claude/Cursor plugin that turns a plain-language description of entities into a fully enriched CSV dataset — and optionally trains a ML model on it. It combines structured data from Wikidata with web scraping to fill gaps, all orchestrated through a set of AI agents.

## How it works

The pipeline has four stages:

1. **Wikidata extraction** — The `wikidata-agent` queries Wikidata via SPARQL to build an initial structured dataset for your entities.
2. **Web exploration** — The `web-exploration-agent` identifies features missing from Wikidata, finds suitable web sources, and writes a `scraper.py` script for per-entity extraction.
3. **Enrichment** — `scripts/enrich.py` runs the scraper row-by-row to fill in missing values and produce an enriched CSV.
4. **Model training** *(optional)* — `scripts/model_trainer.py` trains a classification or regression model on the enriched dataset.


## Local Installation
First, clone this repo:
```
git clone https://github.com/guy-dar/sent2model
```
Then install the plugin locally.

**Option 1: Claude Code**:
To test the plugin on Claude Code:

```
claude --plugin-dir ./path/to/sent2model
```

Note that this does not install the plugin permanently.

**Option 2: Cursor**:
Copy this folder to your `.cursor` folder. In Linux, we can use symbolic link and then git pulls will be updated automatically:

```
ln -s /path/to/sent2model ~/.cursor/plugins/local/sent2model
```


After loading the plugin (on either Cursor/Claude), you can just ask your coding assistant in chat to build your datasets.

Marketplace installation will be available in the future!

## Project structure

```
sent2model/
├── README.md
├── agents/
│   ├── wikidata-agent.md        # Agent: builds datasets from Wikidata via SPARQL
│   └── web-exploration-agent.md # Agent: finds web sources & writes scraper.py
├── scripts/
│   ├── ....
└── skills/
    └── dataset-creation/
        └── SKILL.md             # Orchestration skill: end-to-end dataset pipeline
```

## Usage

The plugin is designed to be driven by AI agents in Claude Code and Cursor. Describe your entities in natural language; the **`dataset-creation`** skill orchestrates the pipeline by **delegating** to **wikidata-agent** and **web-exploration-agent**.

**Example prompt:**
> "Build a dataset for Nobel Prize winners in Physics"

Behind the scenes, the skill:
1. Invokes **wikidata-agent** to run `wikidata_cli.py` against Wikidata (with **`SENT2MODEL_ARTIFACTS_ROOT`** and **`SENT2MODEL_PROJECT`** / **`--project`** set to the user’s workspace paths).
2. Invokes **web-exploration-agent** to produce `scraper.py` in that project folder.
3. Runs **`enrich.py`** to fill missing values.
4. Delivers **`dataset_enriched.csv`** under `<SENT2MODEL_ARTIFACTS_ROOT>/<project>/`.

### Model training (Optional)

Model training is an optional step invoked by the `dataset-creation` skill when the user requests it. `model_trainer.py` supports:

- **Classification:** `random_forest`, `logistic_regression`
- **Regression:** `random_forest`, `linear_regression`


## Artifacts

All outputs go under **`SENT2MODEL_ARTIFACTS_ROOT`**, which must be an **absolute path** to your **`sent2model-artifacts`** directory (usually beside your repo or workspace).

A typical project folder:

```
sent2model-artifacts/nobel-physics/
├── search_items.csv         # Wikidata entity search results
├── search_properties.csv    # Wikidata property search results
├── dataset_sparql.csv       # Initial dataset from Wikidata
├── scraper.py               # Generated web scraper
├── dataset_enriched.csv     # Final enriched dataset
└── enrich.log               # Enrichment run log
```

## How to Cite?
This is the official implementation of the Sentence-to-Model system described in the following conference papers

**SIGMOD 2025 Demo Paper**
```bibtex
@inproceedings{einy2025sentence,
  title={Sentence to Model: Cost-Effective Data Collection LLM Agent},
  author={Einy, Yael and Dar, Guy and Novgorodov, Slava and Milo, Tova},
  booktitle={Companion of the 2025 International Conference on Management of Data},
  pages={83--86},
  year={2025}
}
```

**EDBT 2026 Full System Paper**
```bibtex
@article{einy2026automating,
  title={Automating Efficient Data Collection through the Synergy of Agentic AI and Active Learning},
  author={Einy, Yael and Dar, Guy and Novgorodov, Slava and Milo, Tova and Frost, Nave},
  year={2026}
}
```
