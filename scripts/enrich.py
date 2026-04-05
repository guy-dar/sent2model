"""
Entity-wise dataset enrichment (missing-value fill via per-row scraper).

**Who should run this:** Normally the **dataset-creation** skill workflow, after
**web-exploration-agent** has written ``scraper.py`` in the artifact folder.

**Consumes:** CSV from **wikidata-agent** (via ``wikidata_cli.py``); default
delimiter is ``;`` to match those exports. Use ``--sep ,`` for comma-separated
inputs.
"""

from __future__ import annotations

import argparse
import importlib
import logging
import sys
import time
from pathlib import Path
from typing import Any, Callable, Dict, Iterator, Optional, Protocol, Tuple

import pandas as pd

# Default: no handlers until main() configures file logging
logger = logging.getLogger(__name__)


class Prioritizer(Protocol):
    """Protocol for prioritizer implementations."""

    def generate_order(
        self, df: pd.DataFrame, budget: Optional[int] = None
    ) -> Iterator[int]:
        ...


class SequentialPrioritizer:
    """Process rows in sequential order (0, 1, 2, ...)."""

    def __init__(self) -> None:
        self.name = "Sequential"

    def generate_order(
        self, df: pd.DataFrame, budget: Optional[int] = None
    ) -> Iterator[int]:
        max_idx = len(df) if budget is None else min(budget, len(df))
        yield from range(max_idx)


def _setup_file_logger(log_path: Path) -> logging.Logger:
    """Verbose pipeline logs (per-row, per-field) — file only."""
    log_path.parent.mkdir(parents=True, exist_ok=True)
    lg = logging.getLogger("enrich.pipeline")
    lg.handlers.clear()
    lg.setLevel(logging.DEBUG)
    fh = logging.FileHandler(log_path, encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(
        logging.Formatter("%(asctime)s\t%(levelname)s\t%(message)s")
    )
    lg.addHandler(fh)
    lg.propagate = False
    return lg


def _call_scraper_with_retries(
    scraper: Callable[[str], Dict[str, Any]],
    entity_name: str,
    *,
    max_retries: int,
    base_delay: float,
    detail_logger: logging.Logger,
) -> Tuple[Optional[Dict[str, Any]], Optional[Exception]]:
    last_err: Optional[Exception] = None
    for attempt in range(max_retries + 1):
        try:
            data = scraper(entity_name)
            return data, None
        except Exception as e:
            last_err = e
            detail_logger.warning(
                "scraper exception for %r attempt %s/%s: %s",
                entity_name,
                attempt + 1,
                max_retries + 1,
                e,
            )
            if attempt < max_retries:
                wait = base_delay * (2**attempt)
                time.sleep(wait)
    return None, last_err


def enrich_dataset(
    df: pd.DataFrame,
    scraper: Callable[[str], Dict[str, Any]],
    entity_column: str = "item",
    prioritizer: Optional[Prioritizer] = None,
    budget: Optional[int] = None,
    *,
    inter_request_delay: float = 0.0,
    scraper_retries: int = 0,
    scraper_retry_base_delay: float = 0.5,
    progress_every: float = 0.05,
    detail_logger: Optional[logging.Logger] = None,
    progress_print: Callable[[str], None] = print,
) -> pd.DataFrame:
    """
    Enrich dataset by filling missing values.

    Verbose diagnostics go to ``detail_logger`` (typically a file). Callers
    should pass ``progress_print`` that writes short lines to stdout (default
    ``print``).
    """
    if prioritizer is None:
        prioritizer = SequentialPrioritizer()

    log = detail_logger or logging.getLogger(__name__)

    if entity_column not in df.columns:
        log.error("Entity column '%s' not found", entity_column)
        return df

    total_rows = len(df)
    budget_limit = budget if budget is not None else total_rows
    processed_count = 0
    ok_entities = 0
    err_entities = 0
    skip_entities = 0
    filled_cells = 0

    log.info(
        "Budget: %s rows (total in frame: %s), inter_request_delay=%ss, scraper_retries=%s",
        budget_limit,
        total_rows,
        inter_request_delay,
        scraper_retries,
    )
    progress_print(
        f"enrich: start rows={budget_limit} delay={inter_request_delay}s retries={scraper_retries}"
    )

    next_milestone = progress_every

    for idx in prioritizer.generate_order(df, budget=budget):
        processed_count += 1
        entity_name = df.at[idx, entity_column]

        if pd.isna(entity_name) or entity_name == "":
            skip_entities += 1
            log.warning("Row %s: empty entity, skip", idx)
            continue

        log.info("Processing %s/%s: %s", processed_count, budget_limit, entity_name)

        scraped_data: Optional[Dict[str, Any]] = None
        err: Optional[Exception] = None

        if scraper_retries > 0:
            scraped_data, err = _call_scraper_with_retries(
                scraper,
                str(entity_name),
                max_retries=scraper_retries,
                base_delay=scraper_retry_base_delay,
                detail_logger=log,
            )
        else:
            scraped_data = None
            try:
                scraped_data = scraper(str(entity_name))
            except Exception as e:
                err = e
                log.error("Scraper failed for %s: %s", entity_name, e)

        if err is not None and scraped_data is None:
            err_entities += 1
        elif not isinstance(scraped_data, dict):
            log.error("Scraper returned non-dict for %s: %s", entity_name, type(scraped_data))
            err_entities += 1
        else:
            ok_entities += 1
            for feature, value in scraped_data.items():
                if feature not in df.columns:
                    df[feature] = None
                current_value = df.at[idx, feature]
                if pd.isna(current_value) or current_value == "":
                    df.at[idx, feature] = value
                    filled_cells += 1
                    log.info("  set %s=%r", feature, str(value)[:200])

        if inter_request_delay > 0 and processed_count < budget_limit:
            time.sleep(inter_request_delay)

        frac = processed_count / budget_limit if budget_limit else 1.0
        while frac >= next_milestone - 1e-9:
            progress_print(
                f"enrich: progress {next_milestone:.0%} ({processed_count}/{budget_limit}) "
                f"ok={ok_entities} err={err_entities} skip={skip_entities} filled_cells={filled_cells}"
            )
            next_milestone += progress_every
            if next_milestone > 1.0 + 1e-9:
                break

    log.info("=== Final coverage ===")
    for col in df.columns:
        if col != entity_column:
            coverage = df[col].notna().sum() / len(df)
            log.info("%s: %.1f%%", col, coverage * 100)

    progress_print(
        f"enrich: done ok={ok_entities} err={err_entities} skip={skip_entities} filled_cells={filled_cells}"
    )
    return df


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Enrich dataset with missing values using a scraper function"
    )
    parser.add_argument("--input", type=str, required=True, help="Input CSV path")
    parser.add_argument("--output", type=str, required=True, help="Output CSV path")
    parser.add_argument(
        "--entity-column",
        type=str,
        default="item",
        help="Entity name column (default: item)",
    )
    parser.add_argument(
        "--scraper-module",
        type=str,
        required=True,
        help="Import path for scraper module (e.g. scraper)",
    )
    parser.add_argument(
        "--scraper-function",
        type=str,
        default="scrape",
        help="Scraper callable name (default: scrape)",
    )
    parser.add_argument(
        "--budget",
        type=int,
        default=None,
        help="Max rows to process (default: all)",
    )
    parser.add_argument(
        "--sep",
        type=str,
        default=";",
        help="CSV delimiter (default: ; for Wikidata CLI exports; use , for RFC-style CSV)",
    )
    parser.add_argument(
        "--scraper-dir",
        type=str,
        default=None,
        help="Directory prepended to sys.path before importing the scraper module",
    )
    parser.add_argument(
        "--log-file",
        type=str,
        default=None,
        help="Verbose log file (default: <output>.enrich.log next to --output)",
    )
    parser.add_argument(
        "--progress-every",
        type=float,
        default=0.05,
        help="Print progress to stdout every N fraction of rows (default: 0.05 = 5%%)",
    )
    parser.add_argument(
        "--min-delay",
        type=float,
        default=0.75,
        help="Seconds to sleep after each entity (rate limiting; default: 0.75)",
    )
    parser.add_argument(
        "--scraper-retries",
        type=int,
        default=2,
        help="Retries on scraper exception with exponential backoff (default: 2)",
    )
    parser.add_argument(
        "--scraper-retry-delay",
        type=float,
        default=0.5,
        help="Base seconds for scraper retry backoff (default: 0.5)",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    out_path = Path(args.output)
    log_path = (
        Path(args.log_file)
        if args.log_file
        else out_path.with_name(out_path.stem + ".enrich.log")
    )
    detail_logger = _setup_file_logger(log_path)

    detail_logger.info("input=%s output=%s sep=%r", args.input, args.output, args.sep)
    print(f"enrich: verbose log -> {log_path.resolve()}", flush=True)

    df = pd.read_csv(args.input, sep=args.sep)
    detail_logger.info("loaded %s rows", len(df))

    if args.scraper_dir:
        sd = Path(args.scraper_dir).expanduser().resolve()
        if str(sd) not in sys.path:
            sys.path.insert(0, str(sd))

    module = importlib.import_module(args.scraper_module)
    scraper = getattr(module, args.scraper_function)

    enrich_dataset(
        df=df,
        scraper=scraper,
        entity_column=args.entity_column,
        budget=args.budget,
        inter_request_delay=args.min_delay,
        scraper_retries=args.scraper_retries,
        scraper_retry_base_delay=args.scraper_retry_delay,
        progress_every=args.progress_every,
        detail_logger=detail_logger,
        progress_print=lambda s: print(s, flush=True),
    )

    detail_logger.info("writing %s", args.output)
    print(f"enrich: writing {out_path.resolve()}", flush=True)
    df.to_csv(args.output, index=False)
    print("enrich: finished", flush=True)


if __name__ == "__main__":
    main()
