#!/usr/bin/env python3
"""
Wikidata CLI: in-process commands that write under a caller-defined ``sent2model-artifacts`` tree.

Set ``SENT2MODEL_ARTIFACTS_ROOT`` to the absolute path of the ``sent2model-artifacts``
directory (each project is a subfolder inside it), or pass ``--artifacts-root`` on every
invocation. This avoids depending on the process current working directory (e.g. after
``cd`` into the plugin install path).

Pass ``--project NAME`` or set ``SENT2MODEL_PROJECT`` for the project subfolder.

**Who should run this:** the **wikidata-agent** (see ``wikidata-agent.md``) for dataset builds.

Optional: ``WD_VECTORDB_API_SECRET`` — vector search with keyword fallback.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

import pandas as pd
import requests

_scripts = Path(__file__).resolve().parent
if str(_scripts) not in sys.path:
    sys.path.insert(0, str(_scripts))

import wikidata_client as wd  # noqa: E402

WD_VECTORDB_API_SECRET = os.environ.get("WD_VECTORDB_API_SECRET")
VECTOR_ENABLED = wd.vectorsearch_verify_apikey(WD_VECTORDB_API_SECRET or "")


def _default_user_agent() -> str:
    return os.environ.get("WIKIDATA_CLI_USER_AGENT", "wikidata-skill-cli")


def _resolve_artifacts_root(cli_root: Path | None) -> Path:
    if cli_root is not None:
        out = cli_root.expanduser().resolve()
    else:
        env = (os.environ.get("SENT2MODEL_ARTIFACTS_ROOT") or "").strip()
        if not env:
            raise ValueError(
                "Set SENT2MODEL_ARTIFACTS_ROOT to the sent2model-artifacts directory "
                "(parent of each project folder), or pass --artifacts-root PATH."
            )
        out = Path(env).expanduser().resolve()
    out.mkdir(parents=True, exist_ok=True)
    return out


def _sanitize_project_name(name: str) -> str:
    safe = (
        name.strip()
        .replace("..", "")
        .replace("/", "_")
        .replace("\\", "_")
    )
    if not safe or safe in (".",):
        raise ValueError("Invalid --project / SENT2MODEL_PROJECT name.")
    return safe


def _resolve_project_name(cli_project: str | None) -> str:
    raw = cli_project if cli_project is not None else os.environ.get("SENT2MODEL_PROJECT")
    if raw is None or not str(raw).strip():
        raise ValueError(
            "Pass --project NAME or set SENT2MODEL_PROJECT (project folder under the artifacts root)."
        )
    return _sanitize_project_name(str(raw))


def _resolve_task_dir(artifacts_root: Path, cli_project: str | None) -> Path:
    project = _resolve_project_name(cli_project)
    base_r = artifacts_root.resolve()
    run = (base_r / project).resolve()
    try:
        run.relative_to(base_r)
    except ValueError as e:
        raise ValueError("Project path must stay under the artifacts root.") from e
    run.mkdir(parents=True, exist_ok=True)
    return run


def _resolve_write_path(task_dir: Path | None, output: Path) -> Path:
    """
    Absolute --output -> write that path (--dir optional).
    Relative --output -> require task_dir; result is task_dir / output (no .. escape).
    """
    out = output.expanduser()
    if out.is_absolute():
        path = out.resolve()
        path.parent.mkdir(parents=True, exist_ok=True)
        return path
    if task_dir is None:
        raise ValueError(
            "Relative output requires a project task directory. "
            "Use an absolute output path, or pass --project / SENT2MODEL_PROJECT."
        )
    td = task_dir.expanduser().resolve()
    td.mkdir(parents=True, exist_ok=True)
    path = (td / out).resolve()
    try:
        path.relative_to(td)
    except ValueError:
        raise ValueError("Output must not escape the task directory (no .. paths).")
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


def _resolve_read_path(task_dir: Path, file_arg: Path) -> Path:
    p = file_arg.expanduser()
    if p.is_absolute():
        return p.resolve()
    path = (task_dir / p).resolve()
    try:
        path.relative_to(task_dir.resolve())
    except ValueError:
        raise ValueError("File path must not escape the task directory.")
    return path


def _report(path: Path, rows: int | None, preview: str, *, no_preview: bool) -> None:
    print(f"path:{path.resolve()}")
    if rows is not None:
        print(f"rows:{rows}")
    if not no_preview and preview.strip():
        print("preview:")
        print(preview.rstrip())


def _search_entities(
    query: str, *, entity_type: str, lang: str, limit: int
) -> dict:
    ua = _default_user_agent()
    kw = dict(type=entity_type, lang=lang, user_agent=ua, limit=limit)

    if not (VECTOR_ENABLED and WD_VECTORDB_API_SECRET):
        try:
            return wd.keywordsearch(query, **kw)
        except requests.RequestException as e:
            raise RuntimeError("Wikidata is currently unavailable.") from e
        except Exception as e:
            raise RuntimeError(f"Wikidata entity search failed: {e!s}") from e

    try:
        return wd.vectorsearch(
            query,
            WD_VECTORDB_API_SECRET,
            type=entity_type,
            lang=lang,
            user_agent=ua,
            limit=limit,
        )
    except requests.RequestException:
        try:
            return wd.keywordsearch(query, **kw)
        except requests.RequestException as e:
            raise RuntimeError("Wikidata is currently unavailable.") from e
    except Exception:
        try:
            return wd.keywordsearch(query, **kw)
        except requests.RequestException as e:
            raise RuntimeError("Wikidata is currently unavailable.") from e
        except Exception as e:
            raise RuntimeError(f"Wikidata entity search failed: {e!s}") from e


def _results_to_df(results: dict) -> pd.DataFrame:
    if not results:
        return pd.DataFrame(columns=["id", "label", "description"])
    rows = [
        {"id": eid, "label": v.get("label", ""), "description": v.get("description", "")}
        for eid, v in results.items()
    ]
    return pd.DataFrame(rows)


def _peek_csv(path: Path, sep: str, n: int) -> str:
    if n <= 0:
        return ""
    df = pd.read_csv(path, sep=sep, nrows=n, encoding="utf-8")
    return df.to_string(index=False)


def _run_search(
    query: str,
    *,
    entity_type: str,
    lang: str,
    limit: int,
    path: Path,
    preview_rows: int,
    no_preview: bool,
) -> int:
    if not query.strip():
        print("Query cannot be empty.", file=sys.stderr)
        return 1
    results = _search_entities(query, entity_type=entity_type, lang=lang, limit=limit)
    df = _results_to_df(results)
    df.to_csv(path, index=False, encoding="utf-8")
    prev = (
        ""
        if no_preview
        else df.head(preview_rows).to_string(index=False)
    )
    _report(path, len(df), prev, no_preview=no_preview)
    return 0


def _run_get_statements(
    entity_id: str,
    *,
    include_external_ids: bool,
    lang: str,
    path: Path,
    preview_rows: int,
    no_preview: bool,
) -> int:
    if not entity_id.strip():
        print("Entity ID cannot be empty.", file=sys.stderr)
        return 1
    ua = _default_user_agent()
    try:
        result = wd.get_entities_triplets(
            [entity_id],
            external_ids=include_external_ids,
            all_ranks=False,
            qualifiers=False,
            lang=lang,
            user_agent=ua,
        )
    except requests.RequestException:
        print("Wikidata is currently unavailable.", file=sys.stderr)
        return 2
    if not result:
        print(f"Entity {entity_id} not found", file=sys.stderr)
        return 1
    text = result.get(entity_id, f"Entity {entity_id} not found")
    if text.startswith("Entity ") and "not found" in text:
        print(text, file=sys.stderr)
        return 1
    lines = [ln for ln in text.splitlines() if ln.strip()]
    df = pd.DataFrame({"statement": lines})
    df.to_csv(path, index=False, encoding="utf-8")
    prev = (
        ""
        if no_preview
        else df.head(preview_rows).to_string(index=False)
    )
    _report(path, len(df), prev, no_preview=no_preview)
    return 0


def _run_get_statement_values(
    entity_id: str,
    property_id: str,
    *,
    lang: str,
    path: Path,
    preview_rows: int,
    no_preview: bool,
) -> int:
    if not entity_id.strip() or not property_id.strip():
        print("Entity and property IDs cannot be empty.", file=sys.stderr)
        return 1
    ua = _default_user_agent()
    try:
        result = wd.get_triplet_values(
            [entity_id],
            pid=[property_id],
            external_ids=True,
            references=True,
            all_ranks=True,
            qualifiers=True,
            lang=lang,
            user_agent=ua,
        )
    except requests.RequestException:
        print("Wikidata is currently unavailable.", file=sys.stderr)
        return 2
    if not result:
        print(f"Entity {entity_id} not found", file=sys.stderr)
        return 1
    entity = result.get(entity_id)
    if not entity:
        print(f"Entity {entity_id} not found", file=sys.stderr)
        return 1
    text = wd.triplet_values_to_string(entity_id, property_id, entity)
    if not text:
        print(
            f"No statement found for {entity_id} with property {property_id}",
            file=sys.stderr,
        )
        return 1
    lines = text.splitlines()
    df = pd.DataFrame({"line": lines})
    df.to_csv(path, index=False, encoding="utf-8")
    prev = (
        ""
        if no_preview
        else df.head(preview_rows).to_string(index=False)
    )
    _report(path, len(df), prev, no_preview=no_preview)
    return 0


def _run_hierarchy(
    entity_id: str,
    *,
    max_depth: int,
    lang: str,
    path: Path,
    preview_rows: int,
    no_preview: bool,
) -> int:
    if not entity_id.strip():
        print("Entity ID cannot be empty.", file=sys.stderr)
        return 1
    try:
        raw = wd.get_hierarchy_data(entity_id, max_depth, lang=lang)
    except requests.RequestException:
        print("Wikidata is currently unavailable.", file=sys.stderr)
        return 2
    if not raw or entity_id not in raw:
        print(f"Entity {entity_id} not found", file=sys.stderr)
        return 1
    tree = wd.hierarchy_to_json(entity_id, raw, level=max_depth)
    path.write_text(json.dumps(tree, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    lines = path.read_text(encoding="utf-8").splitlines()
    prev = ""
    if not no_preview and preview_rows > 0:
        prev = "\n".join(lines[:preview_rows])
        if len(lines) > preview_rows:
            prev += f"\n... ({len(lines) - preview_rows} more lines)"
    _report(path, len(lines), prev, no_preview=no_preview)
    return 0


def _run_sparql(
    sparql: str,
    *,
    k: int,
    path: Path,
    preview_rows: int,
    no_preview: bool,
) -> int:
    if not sparql.strip():
        print("SPARQL query cannot be empty.", file=sys.stderr)
        return 1
    ua = _default_user_agent()
    try:
        df = wd.execute_sparql(sparql, K=k, user_agent=ua)
    except ValueError as e:
        print(str(e), file=sys.stderr)
        return 2
    except requests.RequestException:
        print("Wikidata is currently unavailable.", file=sys.stderr)
        return 2
    df.to_csv(path, sep=";", index=True, header=True, encoding="utf-8")
    prev = (
        ""
        if no_preview
        else df.head(preview_rows).to_string()
    )
    _report(path, len(df), prev, no_preview=no_preview)
    return 0


def _run_peek_csv(task_dir: Path, file_arg: Path, sep: str, n: int) -> int:
    path = _resolve_read_path(task_dir, file_arg)
    if not path.is_file():
        print(f"Not a file: {path}", file=sys.stderr)
        return 1
    print(_peek_csv(path, sep, n))
    return 0


def _run_distinct_features(
    td: Path, body: dict, preview_rows: int, no_preview: bool
) -> int:
    props = body.get("properties")
    if not props or not isinstance(props, list):
        print("distinct-features requires properties: list of PIDs", file=sys.stderr)
        return 1
    items = body.get("items")
    if items is not None and not isinstance(items, list):
        print("items must be a list of QIDs or omitted", file=sys.stderr)
        return 1
    item_where = body.get("item_where")
    if item_where is not None and not isinstance(item_where, str):
        print("item_where must be a string or omitted", file=sys.stderr)
        return 1
    common_kw = dict(
        items=items,
        item_where=item_where,
        item_variable=str(body.get("item_variable", "item")),
        multi_value=str(body.get("multi_value", "sample")),
        group_concat_separator=str(body.get("group_concat_separator", " | ")),
        filters=body.get("filters"),
        label_lang=str(body.get("label_lang", "en")),
    )
    if body.get("execute"):
        out_raw = body.get("output")
        if out_raw is None or str(out_raw).strip() == "":
            print(
                "distinct-features --execute requires -o / output path",
                file=sys.stderr,
            )
            return 1
        path = _resolve_write_path(td, Path(out_raw))
        ua = _default_user_agent()
        try:
            df = wd.distinct_item_features_sparql(
                props,
                user_agent=ua,
                execute=True,
                K=int(body.get("k", 10_000)),
                **common_kw,
            )
        except ValueError as e:
            print(str(e), file=sys.stderr)
            return 2
        except requests.RequestException:
            print("Wikidata is currently unavailable.", file=sys.stderr)
            return 2
        df.to_csv(path, sep=";", index=True, header=True, encoding="utf-8")
        prev = (
            ""
            if no_preview
            else df.head(preview_rows).to_string()
        )
        _report(path, len(df), prev, no_preview=no_preview)
        return 0

    try:
        q = wd.distinct_item_features_sparql(
            props,
            execute=False,
            **common_kw,
        )
    except ValueError as e:
        print(str(e), file=sys.stderr)
        return 1
    out_raw = body.get("output")
    if out_raw is None or str(out_raw).strip() == "":
        lines = len(q.splitlines())
        print(f"rows:{lines}")
        print(q.rstrip() + ("\n" if q and not q.endswith("\n") else ""))
        return 0
    path = _resolve_write_path(td, Path(out_raw))
    path.write_text(q, encoding="utf-8")
    lines = len(q.splitlines())
    prev = (
        ""
        if no_preview
        else "\n".join(q.splitlines()[: max(1, preview_rows)])
    )
    _report(path, lines, prev, no_preview=no_preview)
    return 0


def _run_command(cmd: str, body: dict, *, task_dir: Path) -> int:
    """Run one CLI subcommand; prints to stdout/stderr."""
    td = task_dir
    preview_rows = int(body.get("preview_rows", 20))
    no_preview = bool(body.get("no_preview", False))

    if cmd == "search-items":
        path = _resolve_write_path(td, Path(body["output"]))
        return _run_search(
            body["query"],
            entity_type="item",
            lang=body.get("lang", "en"),
            limit=int(body.get("limit", 10)),
            path=path,
            preview_rows=preview_rows,
            no_preview=no_preview,
        )
    if cmd == "search-properties":
        path = _resolve_write_path(td, Path(body["output"]))
        return _run_search(
            body["query"],
            entity_type="property",
            lang=body.get("lang", "en"),
            limit=int(body.get("limit", 10)),
            path=path,
            preview_rows=preview_rows,
            no_preview=no_preview,
        )
    if cmd == "get-statements":
        path = _resolve_write_path(td, Path(body["output"]))
        return _run_get_statements(
            body["entity"],
            include_external_ids=bool(body.get("include_external_ids", False)),
            lang=body.get("lang", "en"),
            path=path,
            preview_rows=preview_rows,
            no_preview=no_preview,
        )
    if cmd == "get-statement-values":
        path = _resolve_write_path(td, Path(body["output"]))
        return _run_get_statement_values(
            body["entity"],
            body["property"],
            lang=body.get("lang", "en"),
            path=path,
            preview_rows=preview_rows,
            no_preview=no_preview,
        )
    if cmd == "hierarchy":
        path = _resolve_write_path(td, Path(body["output"]))
        return _run_hierarchy(
            body["entity"],
            max_depth=int(body.get("max_depth", 5)),
            lang=body.get("lang", "en"),
            path=path,
            preview_rows=preview_rows,
            no_preview=no_preview,
        )
    if cmd == "sparql":
        if body.get("file_content") and body.get("query"):
            print("Use only one of file_content or query", file=sys.stderr)
            return 1
        if body.get("file_content"):
            sparql = body["file_content"]
        elif body.get("query"):
            sparql = body["query"]
        else:
            print("sparql requires query or file_content", file=sys.stderr)
            return 1
        path = _resolve_write_path(td, Path(body["output"]))
        return _run_sparql(
            sparql,
            k=int(body.get("k", 10)),
            path=path,
            preview_rows=preview_rows,
            no_preview=no_preview,
        )
    if cmd == "peek-csv":
        return _run_peek_csv(
            td,
            Path(body["file"]),
            body.get("sep", ","),
            int(body.get("rows", 20)),
        )
    if cmd == "distinct-features":
        return _run_distinct_features(td, body, preview_rows, no_preview)
    print(f"Unknown command: {cmd}", file=sys.stderr)
    return 1


def _add_output_flags(p: argparse.ArgumentParser) -> None:
    p.add_argument(
        "-o",
        "--output",
        type=Path,
        required=True,
        help="Output path relative to the project task directory, or absolute",
    )
    p.add_argument(
        "--preview-rows",
        type=int,
        default=20,
        help="Max rows/lines printed to stdout after write (default: 20)",
    )
    p.add_argument(
        "--no-preview",
        action="store_true",
        help="Only print path and row count",
    )


def _build_parser() -> argparse.ArgumentParser:
    cli_parent = argparse.ArgumentParser(add_help=False)
    cli_parent.add_argument(
        "--artifacts-root",
        type=Path,
        default=None,
        help=(
            "Absolute path to the sent2model-artifacts directory (parent of each "
            "--project folder). Overrides SENT2MODEL_ARTIFACTS_ROOT."
        ),
    )
    cli_parent.add_argument(
        "-n",
        "--project",
        default=None,
        help=(
            "Project subfolder under the artifacts root. Overrides SENT2MODEL_PROJECT."
        ),
    )

    p = argparse.ArgumentParser(
        parents=[cli_parent],
        description=(
            "Wikidata CLI: in-process commands. Set SENT2MODEL_ARTIFACTS_ROOT to your "
            "sent2model-artifacts path (not inferred from cwd). Use --project or "
            "SENT2MODEL_PROJECT for the dataset folder name."
        ),
    )
    sub = p.add_subparsers(dest="command", required=True)

    def add_lang_limit(sp):
        sp.add_argument("--lang", default="en", help="Language code (default: en)")
        sp.add_argument(
            "--limit",
            type=int,
            default=10,
            help="Max results for search / vector k (default: 10)",
        )

    s_items = sub.add_parser("search-items", help="Search items -> CSV (you choose -o)")
    _add_output_flags(s_items)
    s_items.add_argument("--query", "-q", required=True, help="Search text")
    add_lang_limit(s_items)

    s_props = sub.add_parser(
        "search-properties", help="Search properties -> CSV (you choose -o)"
    )
    _add_output_flags(s_props)
    s_props.add_argument("--query", "-q", required=True, help="Search text")
    add_lang_limit(s_props)

    g_st = sub.add_parser("get-statements", help="Statements -> CSV (you choose -o)")
    _add_output_flags(g_st)
    g_st.add_argument("--entity", "-e", required=True, help="QID or PID, e.g. Q42")
    g_st.add_argument(
        "--include-external-ids",
        action="store_true",
        help="Include external-id properties",
    )
    g_st.add_argument("--lang", default="en", help="Language code (default: en)")

    g_sv = sub.add_parser(
        "get-statement-values",
        help="Statement detail -> CSV (you choose -o)",
    )
    _add_output_flags(g_sv)
    g_sv.add_argument("--entity", "-e", required=True, help="QID or PID")
    g_sv.add_argument("--property", "-p", required=True, help="PID, e.g. P31")
    g_sv.add_argument("--lang", default="en", help="Language code (default: en)")

    g_h = sub.add_parser("hierarchy", help="Hierarchy -> JSON (you choose -o)")
    _add_output_flags(g_h)
    g_h.add_argument("--entity", "-e", required=True, help="QID or PID")
    g_h.add_argument("--max-depth", type=int, default=5, help="Max depth (default: 5)")
    g_h.add_argument("--lang", default="en", help="Language code (default: en)")

    g_sp = sub.add_parser("sparql", help="SPARQL -> CSV ;-sep (you choose -o)")
    _add_output_flags(g_sp)
    g_sp.add_argument("--query", "-q", default=None, help="SPARQL string")
    g_sp.add_argument("--file", "-f", type=Path, default=None, help="SPARQL UTF-8 file")
    g_sp.add_argument(
        "-k",
        "--max-rows",
        type=int,
        default=10,
        dest="k",
        help="Max rows from WDQS (default: 10)",
    )

    df_cmd = sub.add_parser(
        "distinct-features",
        help="Feature-matrix SPARQL (DISTINCT + sample/group_concat/distinct_rows); stdout or CSV",
    )
    df_cmd.add_argument(
        "-o",
        "--output",
        type=Path,
        default=None,
        help=(
            "Output path (task-relative or absolute). Required with --execute (CSV). "
            "Without --execute, omit to print SPARQL to stdout, or set to save a .rq file."
        ),
    )
    df_cmd.add_argument(
        "--preview-rows",
        type=int,
        default=20,
        help="Max data rows / query lines in the preview after a file write (default: 20)",
    )
    df_cmd.add_argument(
        "--no-preview",
        action="store_true",
        help="After writing a file, only print path: and rows:",
    )
    df_cmd.add_argument(
        "--properties",
        "-p",
        required=True,
        help="Comma-separated PIDs, e.g. P106,P27",
    )
    df_cmd.add_argument(
        "--items",
        default=None,
        help="Comma-separated QIDs (omit if --item-where-file)",
    )
    df_cmd.add_argument(
        "--item-where-file",
        type=Path,
        default=None,
        help="UTF-8 SPARQL fragment that binds --item-variable",
    )
    df_cmd.add_argument(
        "--item-variable",
        default="item",
        help="Item variable without ? (default: item)",
    )
    df_cmd.add_argument(
        "--multi-value",
        choices=("sample", "group_concat", "distinct_rows"),
        default="sample",
        help="Per-property multiplicity (default: sample)",
    )
    df_cmd.add_argument(
        "--group-separator",
        default=" | ",
        help="GROUP_CONCAT separator when --multi-value group_concat",
    )
    df_cmd.add_argument(
        "--filter",
        default=None,
        help="Extra SPARQL (triples/FILTERs) using the item variable",
    )
    df_cmd.add_argument(
        "--filter-file",
        type=Path,
        default=None,
        help="UTF-8 file with --filter content",
    )
    df_cmd.add_argument(
        "--label-lang",
        default="en",
        help="LANG() for rdfs:label (default: en)",
    )
    df_cmd.add_argument(
        "--execute",
        action="store_true",
        help="Run on WDQS -> CSV (requires -o); without it, SPARQL goes to stdout unless -o saves .rq",
    )
    df_cmd.add_argument(
        "-k",
        "--max-rows",
        type=int,
        default=10_000,
        dest="k",
        help="Max rows when --execute (default: 10000)",
    )

    peek = sub.add_parser(
        "peek-csv",
        help="Print first N rows of a CSV under the project directory (or absolute path)",
    )
    peek.add_argument("--file", "-f", type=Path, required=True, help="CSV path")
    peek.add_argument(
        "-n",
        "--rows",
        type=int,
        default=20,
        help="Rows to read from disk (default: 20)",
    )
    peek.add_argument(
        "--sep",
        default=",",
        help="Delimiter (use ';' for SPARQL exports)",
    )

    sub.add_parser(
        "project-path",
        help="Print path: resolved project directory (creates it if missing)",
    )

    return p


def _args_to_search_body(args: argparse.Namespace) -> dict:
    return {
        "query": args.query,
        "lang": args.lang,
        "limit": args.limit,
        "output": str(args.output),
        "preview_rows": args.preview_rows,
        "no_preview": args.no_preview,
    }


def _parse_comma_list(s: str) -> list[str]:
    return [x.strip() for x in s.split(",") if x.strip()]


def main(argv: list[str] | None = None) -> int:
    argv = argv if argv is not None else sys.argv[1:]
    parser = _build_parser()
    args = parser.parse_args(argv)

    try:
        artifacts_root = _resolve_artifacts_root(args.artifacts_root)
        task_dir = _resolve_task_dir(artifacts_root, args.project)
    except ValueError as e:
        print(str(e), file=sys.stderr)
        return 1

    if args.command == "project-path":
        print(f"path:{task_dir.resolve()}", flush=True)
        return 0

    try:
        if args.command == "search-items":
            return _run_command(
                "search-items", _args_to_search_body(args), task_dir=task_dir
            )
        if args.command == "search-properties":
            return _run_command(
                "search-properties", _args_to_search_body(args), task_dir=task_dir
            )
        if args.command == "get-statements":
            return _run_command(
                "get-statements",
                {
                    "entity": args.entity,
                    "include_external_ids": args.include_external_ids,
                    "lang": args.lang,
                    "output": str(args.output),
                    "preview_rows": args.preview_rows,
                    "no_preview": args.no_preview,
                },
                task_dir=task_dir,
            )
        if args.command == "get-statement-values":
            return _run_command(
                "get-statement-values",
                {
                    "entity": args.entity,
                    "property": args.property,
                    "lang": args.lang,
                    "output": str(args.output),
                    "preview_rows": args.preview_rows,
                    "no_preview": args.no_preview,
                },
                task_dir=task_dir,
            )
        if args.command == "hierarchy":
            return _run_command(
                "hierarchy",
                {
                    "entity": args.entity,
                    "max_depth": args.max_depth,
                    "lang": args.lang,
                    "output": str(args.output),
                    "preview_rows": args.preview_rows,
                    "no_preview": args.no_preview,
                },
                task_dir=task_dir,
            )
        if args.command == "sparql":
            if args.file and args.query:
                print("Use only one of --file or --query", file=sys.stderr)
                return 1
            body: dict = {
                "output": str(args.output),
                "preview_rows": args.preview_rows,
                "no_preview": args.no_preview,
                "k": args.k,
            }
            if args.file:
                body["file_content"] = args.file.read_text(encoding="utf-8")
            elif args.query:
                body["query"] = args.query
            else:
                print("sparql requires --query or --file", file=sys.stderr)
                return 1
            return _run_command("sparql", body, task_dir=task_dir)
        if args.command == "peek-csv":
            return _run_command(
                "peek-csv",
                {
                    "file": str(args.file),
                    "sep": args.sep,
                    "rows": args.rows,
                },
                task_dir=task_dir,
            )
        if args.command == "distinct-features":
            if args.items and args.item_where_file:
                print(
                    "Use only one of --items or --item-where-file",
                    file=sys.stderr,
                )
                return 1
            if args.filter is not None and args.filter_file:
                print(
                    "Use only one of --filter or --filter-file",
                    file=sys.stderr,
                )
                return 1
            item_where = None
            if args.item_where_file:
                item_where = args.item_where_file.read_text(encoding="utf-8")
            items_list = (
                _parse_comma_list(args.items) if args.items and args.items.strip() else None
            )
            if item_where is None and not items_list:
                print("Provide --items or --item-where-file", file=sys.stderr)
                return 1
            filters = args.filter
            if args.filter_file:
                filters = args.filter_file.read_text(encoding="utf-8")
            props_list = _parse_comma_list(args.properties)
            if not props_list:
                print("No PIDs in --properties / -p", file=sys.stderr)
                return 1
            if args.execute and args.output is None:
                print(
                    "distinct-features --execute requires -o (CSV output path)",
                    file=sys.stderr,
                )
                return 1
            body_df = {
                "output": str(args.output) if args.output is not None else None,
                "properties": props_list,
                "items": items_list,
                "item_where": item_where,
                "item_variable": args.item_variable,
                "multi_value": args.multi_value,
                "group_concat_separator": args.group_separator,
                "filters": filters,
                "label_lang": args.label_lang,
                "execute": args.execute,
                "k": args.k,
                "preview_rows": args.preview_rows,
                "no_preview": args.no_preview,
            }
            return _run_command("distinct-features", body_df, task_dir=task_dir)
    except ValueError as e:
        print(str(e), file=sys.stderr)
        return 1

    parser.print_help()
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
