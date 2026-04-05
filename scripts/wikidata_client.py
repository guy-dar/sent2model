"""
Wikidata HTTP helpers: wbsearchentities, wbgetentities, Textifier, Query Service,
optional Wikimedia vector search. Self-contained (no other project packages).

**Who uses this:** Imported by ``wikidata_cli.py`` and other repo code/tests.
For dataset builds, **wikidata-agent** drives ``wikidata_cli.py``; do not call
``execute_sparql`` / ``distinct_item_features_sparql(..., execute=True)`` from
one-off scripts when the CLI is the intended path (see ``wikidata-agent.md``).
"""

from __future__ import annotations

import os
import re
from typing import Any, Literal, Sequence

import pandas as pd
import requests

VECTOR_SEARCH_URI = os.environ.get(
    "VECTOR_SEARCH_URI", "https://wd-vectordb.wmcloud.org"
)
TEXTIFER_URI = os.environ.get("TEXTIFER_URI", "https://wd-textify.wmcloud.org")
WD_API_URI = os.environ.get("WD_API_URI", "https://www.wikidata.org/w/api.php")
WD_QUERY_URI = os.environ.get("WD_QUERY_URI", "https://query.wikidata.org/sparql")
USER_AGENT = os.environ.get(
    "USER_AGENT", "wikidata-skill-scripts (https://www.wikidata.org)"
)

REQUEST_TIMEOUT_SECONDS = float(os.environ.get("REQUEST_TIMEOUT_SECONDS", "15"))


def format_user_agent(suffix: str = "") -> str:
    s = suffix.strip()
    return f"{USER_AGENT} ({s})" if s else USER_AGENT


def keywordsearch(
    query: str,
    type: str = "item",
    limit: int = 10,
    lang: str = "en",
    user_agent: str = "",
) -> dict[str, dict[str, str]]:
    params = {
        "action": "wbsearchentities",
        "type": type,
        "search": query,
        "limit": limit,
        "language": lang,
        "format": "json",
        "origin": "*",
    }
    response = requests.get(
        WD_API_URI,
        params=params,
        headers={"User-Agent": format_user_agent(user_agent)},
        timeout=REQUEST_TIMEOUT_SECONDS,
    )
    response.raise_for_status()

    raw = response.json().get("search")
    if not isinstance(raw, list):
        return {}

    out: dict[str, dict[str, str]] = {}
    for x in raw:
        if not isinstance(x, dict):
            continue
        eid = x.get("id")
        if not eid:
            continue
        disp = x.get("display")
        label = ""
        desc = ""
        if isinstance(disp, dict):
            lab = disp.get("label")
            if isinstance(lab, dict):
                label = str(lab.get("value", "") or "")
            des = disp.get("description")
            if isinstance(des, dict):
                desc = str(des.get("value", "") or "")
        if not label:
            label = str(x.get("label", "") or "")
        if not desc:
            desc = str(x.get("description", "") or "")
        out[str(eid)] = {"label": label, "description": desc}
    return out


def vectorsearch_verify_apikey(x_api_key: str) -> bool:
    try:
        key = x_api_key or ""
        response = requests.get(
            f"{VECTOR_SEARCH_URI}/item/query/?query=",
            headers={
                "x-api-secret": key,
                "User-Agent": USER_AGENT,
            },
            timeout=REQUEST_TIMEOUT_SECONDS,
        )
        return response.status_code != 401
    except OSError:
        return False


def vectorsearch(
    query: str,
    x_api_key: str,
    type: str = "item",
    limit: int = 10,
    lang: str = "en",
    user_agent: str = "",
) -> dict[str, dict[str, str]]:
    id_name = "QID" if type == "item" else "PID"

    response = requests.get(
        f"{VECTOR_SEARCH_URI}/{type}/query/",
        params={
            "query": query,
            "k": limit,
        },
        headers={
            "x-api-secret": x_api_key,
            "User-Agent": format_user_agent(user_agent),
        },
        timeout=REQUEST_TIMEOUT_SECONDS,
    )
    response.raise_for_status()

    vectordb_result = response.json()

    ids = [x[id_name] for x in vectordb_result]
    return get_entities_labels_and_descriptions(ids, lang=lang)


def execute_sparql(
    sparql_query: str,
    K: int = 10,
    user_agent: str = "",
) -> pd.DataFrame:
    result = requests.get(
        WD_QUERY_URI,
        params={
            "query": sparql_query,
            "format": "json",
        },
        headers={"User-Agent": format_user_agent(user_agent)},
        timeout=REQUEST_TIMEOUT_SECONDS,
    )

    if result.status_code == 400:
        error_message = result.text.split("\tat ")[0]
        raise ValueError(error_message)
    result.raise_for_status()

    result_bindings = result.json()["results"]["bindings"]
    df = pd.json_normalize(result_bindings)

    value_cols = {c: c.split(".")[0] for c in df.columns if c.endswith(".value")}
    if not value_cols:
        return pd.DataFrame().head(K)
    df = df[list(value_cols)].rename(columns=value_cols)

    def shorten(val: str) -> str:
        if not isinstance(val, str):
            return val
        uri_re = re.compile(r"http://www\.wikidata\.org/entity/([A-Z]\d+)$")
        match = uri_re.match(val)
        return match.group(1) if match else val

    # pandas >= 2.2: DataFrame.map; older: applymap
    if hasattr(df, "map"):
        df = df.map(shorten)
    else:
        df = df.applymap(shorten)
    return df.head(K)


MultiValueMode = Literal["sample", "group_concat", "distinct_rows"]


def _coerce_multi_value(mode: str) -> MultiValueMode:
    m = mode.strip()
    if m == "sample":
        return "sample"
    if m == "group_concat":
        return "group_concat"
    if m == "distinct_rows":
        return "distinct_rows"
    raise ValueError(
        "multi_value must be 'sample', 'group_concat', or 'distinct_rows'."
    )


def _sparql_string_literal(s: str) -> str:
    return '"' + s.replace("\\", "\\\\").replace('"', '\\"') + '"'


def normalize_wikidata_qid(q: str) -> str:
    """Return canonical ``wd:Q…`` term for VALUES / Turtle-style use."""
    q = q.strip()
    if q.startswith("http://www.wikidata.org/entity/"):
        tail = q.rsplit("/", 1)[-1]
        if re.match(r"^Q\d+$", tail, re.I):
            return "wd:" + tail.upper()
        raise ValueError(f"Not an item id in entity URL: {q!r}")
    if q.startswith("wd:"):
        tail = q[3:].strip()
        if re.match(r"^Q\d+$", tail, re.I):
            return "wd:" + tail.upper()
        raise ValueError(f"Invalid wd: item: {q!r}")
    if re.match(r"^Q\d+$", q, re.I):
        return "wd:" + q.upper()
    raise ValueError(f"Not a QID or wd: term: {q!r}")


def normalize_wikidata_pid(p: str) -> str:
    p = p.strip().upper()
    if not re.match(r"^P\d+$", p):
        raise ValueError(f"Not a PID: {p!r}")
    return p


def _slug_property_label_for_column(label: str, pid: str) -> str:
    """ASCII-ish snake_case from a Wikidata property label; fallback to lower PID."""
    label = (label or "").strip()
    raw = re.sub(r"[^a-zA-Z0-9]+", "_", label.lower()).strip("_")
    if not raw:
        return pid.lower()
    return raw[:120]


def rename_distinct_feature_dataframe_columns(
    df: pd.DataFrame,
    pids: Sequence[str],
    *,
    label_lang: str = "en",
) -> pd.DataFrame:
    """
    Rename ``feat_P…`` columns from ``distinct_item_features_sparql`` / WDQS to
    snake_case **property labels** (English by default), with disambiguation when
    two labels collide.
    """
    if df.empty:
        return df
    ordered = [normalize_wikidata_pid(p) for p in pids]
    meta = get_entities_labels_and_descriptions(list(dict.fromkeys(ordered)), lang=label_lang)
    taken: set[str] = {"item"}
    rename_map: dict[str, str] = {}
    for pid in ordered:
        old = f"feat_{pid}"
        if old not in df.columns:
            continue
        base = _slug_property_label_for_column(meta.get(pid, {}).get("label", ""), pid)
        new = base
        if new in taken:
            new = f"{base}_{pid.lower()}"
        if new in taken:
            new = pid.lower()
        taken.add(new)
        rename_map[old] = new
    return df.rename(columns=rename_map)


def _validate_item_variable(name: str) -> str:
    n = name.strip()
    if not n.startswith("?"):
        n = "?" + n
    bare = n[1:]
    if not re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", bare):
        raise ValueError(f"Invalid SPARQL variable name: {name!r}")
    return n


def distinct_item_features_sparql(
    properties: Sequence[str],
    *,
    items: Sequence[str] | None = None,
    item_variable: str = "item",
    item_where: str | None = None,
    multi_value: str = "sample",
    group_concat_separator: str = " | ",
    filters: str | None = None,
    label_lang: str = "en",
    execute: bool = False,
    user_agent: str = "",
    K: int = 10_000,
) -> str | pd.DataFrame:
    """
    Build a WDQS query that pulls one row per item (for ``sample`` / ``group_concat``)
    or distinct bindings per value combination (``distinct_rows``), so multi-valued
    statements do not duplicate the whole matrix unless you opt in.

    Uses ``SELECT DISTINCT`` (outer wrap for aggregated modes; native ``DISTINCT`` for
    ``distinct_rows``). Default ``multi_value`` is ``sample`` (``SAMPLE`` per column);
    ``group_concat`` joins values; ``distinct_rows`` keeps one result row per distinct
    binding (multiple rows per item when a property has several values).

    ``filters`` is raw SPARQL (triples / FILTERs) using the same ``item_variable``,
    e.g. ``?item wdt:P31 wd:Q5 .``

    Returns the SPARQL string when ``execute`` is False; otherwise runs the query and
    returns a DataFrame like ``execute_sparql`` but with ``feat_P…`` columns renamed
    to snake_case **property labels** in ``label_lang`` (via the Wikidata API). The
    ``item`` column is the entity label in ``label_lang``, falling back to the QID
    when no label is available.
    """
    if not properties:
        raise ValueError("properties must be non-empty.")
    mv = _coerce_multi_value(multi_value.strip())

    iv = _validate_item_variable(item_variable)

    if item_where is not None and items is not None:
        raise ValueError("Pass only one of items or item_where.")
    if item_where is not None:
        item_block = item_where.strip()
        if not item_block:
            raise ValueError("item_where must be non-empty.")
    elif items is not None:
        if len(items) == 0:
            raise ValueError("items must be non-empty when item_where is not used.")
        terms = " ".join(normalize_wikidata_qid(x) for x in items)
        item_block = f"VALUES {iv} {{ {terms} }}"
    else:
        raise ValueError("Provide items or item_where.")

    pids = [normalize_wikidata_pid(p) for p in properties]
    filter_block = filters.strip() if filters else ""
    lang_lit = _sparql_string_literal(label_lang)
    sep_lit = _sparql_string_literal(group_concat_separator)

    prefixes = "\n".join(
        [
            "PREFIX wd: <http://www.wikidata.org/entity/>",
            "PREFIX wdt: <http://www.wikidata.org/prop/direct/>",
            "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>",
        ]
    )

    optionals: list[str] = []
    for pid in pids:
        wdt = f"wdt:{pid}"
        v = f"?__v_{pid}"
        lb = f"?__lb_{pid}"
        optionals.append(
            "\n".join(
                [
                    "OPTIONAL {",
                    f"  {iv} {wdt} {v} .",
                    "  OPTIONAL {",
                    f"    {v} rdfs:label {lb} .",
                    f"    FILTER(LANG({lb}) = {lang_lit})",
                    "  }",
                    "}",
                ]
            )
        )
        # col used below in SELECT

    item_lb = "?__lb_item"
    item_label_optional = "\n".join(
        [
            "OPTIONAL {",
            f"  {iv} rdfs:label {item_lb} .",
            f"  FILTER(LANG({item_lb}) = {lang_lit})",
            "}",
        ]
    )
    item_projection = (
        f"COALESCE({item_lb}, REPLACE(STR({iv}), \"^.*/\", \"\"))"
    )

    where_body = [item_block]
    if filter_block:
        where_body.append(filter_block)
    where_body.append(item_label_optional)
    where_body.extend(optionals)
    where_clause = "\n    ".join(where_body)

    if mv == "distinct_rows":
        projections: list[str] = [f"({item_projection} AS ?item)"]
        for pid in pids:
            v = f"?__v_{pid}"
            lb = f"?__lb_{pid}"
            col = f"?feat_{pid}"
            projections.append(
                f"(COALESCE({lb}, STR({v})) AS {col})"
            )
        select_list = " ".join(projections)
        query = (
            f"{prefixes}\n\n"
            f"SELECT DISTINCT {select_list}\n"
            f"WHERE {{\n    {where_clause}\n}}\n"
        )
    else:
        inner_projections: list[str] = [
            f"(SAMPLE({item_projection}) AS ?item)"
        ]
        for pid in pids:
            v = f"?__v_{pid}"
            lb = f"?__lb_{pid}"
            col = f"?feat_{pid}"
            expr = f"COALESCE({lb}, STR({v}))"
            if mv == "sample":
                inner_projections.append(f"(SAMPLE({expr}) AS {col})")
            else:
                inner_projections.append(
                    f"(GROUP_CONCAT(DISTINCT {expr}; separator={sep_lit}) AS {col})"
                )
        inner_select = " ".join(inner_projections)
        query = (
            f"{prefixes}\n\n"
            "SELECT DISTINCT *\n"
            "WHERE {\n"
            "  {\n"
            f"    SELECT {inner_select}\n"
            "    WHERE {\n"
            f"    {where_clause}\n"
            "    }\n"
            f"    GROUP BY {iv}\n"
            "  }\n"
            "}\n"
        )

    if execute:
        df = execute_sparql(query, K=K, user_agent=user_agent)
        return rename_distinct_feature_dataframe_columns(
            df, pids, label_lang=label_lang
        )
    return query


def get_lang_specific(data: dict, langs: list[str] | None = None) -> str:
    if langs is None:
        langs = ["en", "mul"]
    for lang in langs:
        if lang in data:
            if data[lang].get("value"):
                return data[lang].get("value", "")
    return ""


def get_entities_labels_and_descriptions(
    ids: list[str],
    lang: str = "en",
) -> dict[str, dict[str, str]]:
    if not ids:
        return {}

    entities_data: dict[str, Any] = {}

    for chunk_idx in range(0, len(ids), 50):
        ids_chunk = ids[chunk_idx : chunk_idx + 50]
        params = {
            "action": "wbgetentities",
            "ids": "|".join(ids_chunk),
            "languages": lang + "|mul|en",
            "props": "labels|descriptions",
            "format": "json",
            "origin": "*",
        }
        response = requests.get(
            WD_API_URI,
            params=params,
            headers={"User-Agent": USER_AGENT},
            timeout=REQUEST_TIMEOUT_SECONDS,
        )
        response.raise_for_status()
        chunk_data = response.json().get("entities", {})
        entities_data.update(chunk_data)

    return {
        eid: {
            "label": get_lang_specific(
                val.get("labels", {}), langs=[lang, "mul", "en"]
            ),
            "description": get_lang_specific(
                val.get("descriptions", {}), langs=[lang, "mul", "en"]
            ),
        }
        for eid, val in entities_data.items()
    }


def get_entities_triplets(
    ids: list[str],
    external_ids: bool = False,
    all_ranks: bool = False,
    qualifiers: bool = True,
    lang: str = "en",
    user_agent: str = "",
) -> dict:
    if not ids:
        return {}

    params = {
        "id": ",".join(ids),
        "external_ids": external_ids,
        "all_ranks": all_ranks,
        "qualifiers": qualifiers,
        "lang": lang,
        "format": "triplet",
    }
    response = requests.get(
        TEXTIFER_URI,
        params=params,
        headers={"User-Agent": format_user_agent(user_agent)},
        timeout=REQUEST_TIMEOUT_SECONDS,
    )
    response.raise_for_status()
    return response.json()


def get_triplet_values(
    ids: list[str],
    pid: list[str],
    external_ids: bool = False,
    all_ranks: bool = False,
    references: bool = False,
    qualifiers: bool = True,
    lang: str = "en",
    user_agent: str = "",
) -> dict:
    if not ids:
        return {}

    params = {
        "id": ",".join(ids),
        "external_ids": external_ids,
        "all_ranks": all_ranks,
        "references": references,
        "qualifiers": qualifiers,
        "lang": lang,
        "pid": ",".join(pid),
        "format": "json",
    }
    response = requests.get(
        TEXTIFER_URI,
        params=params,
        headers={"User-Agent": format_user_agent(user_agent)},
        timeout=REQUEST_TIMEOUT_SECONDS,
    )
    response.raise_for_status()
    return response.json()


def get_hierarchy_data(qid: str, max_depth: int = 5, lang: str = "en") -> dict:
    qids = [qid]
    hierarchical_data: dict[str, Any] = {}
    label_data: dict[str, str] = {}
    level = 0

    while qids and level <= max_depth:
        new_qids: set[str] = set()

        current_data = get_triplet_values(qids, pid=["P31", "P279"], lang=lang)

        for q in qids:
            if q not in current_data:
                continue

            instanceof = [
                c["values"]
                for c in current_data[q]["claims"]
                if c["PID"] == "P31"
            ]
            instanceof = [v["value"] for v in instanceof[0]] if instanceof else []

            subclassof = [
                c["values"]
                for c in current_data[q]["claims"]
                if c["PID"] == "P279"
            ]
            subclassof = [v["value"] for v in subclassof[0]] if subclassof else []

            instanceof_qids = [v.get("QID", v.get("PID")) for v in instanceof]
            subclassof_qids = [v.get("QID", v.get("PID")) for v in subclassof]

            hierarchical_data[q] = {
                "instanceof": instanceof_qids,
                "subclassof": subclassof_qids,
            }

            new_qids |= set(instanceof_qids) | set(subclassof_qids)

            for v in instanceof + subclassof:
                if "QID" in v:
                    label_data[v["QID"]] = v.get("label", "")
                elif "PID" in v:
                    label_data[v["PID"]] = v.get("label", "")
            label_data[q] = current_data[q].get("label", "")

        qids = list(new_qids - set(hierarchical_data.keys()) - {None})
        level += 1

    for hqid, label in label_data.items():
        if hqid in hierarchical_data:
            hierarchical_data[hqid]["label"] = label

    return hierarchical_data


def hierarchy_to_json(qid: str, data: dict, level: int = 5) -> Any:
    if level <= 0:
        return f"{data[qid]['label']} ({qid})"

    return {
        f"{data[qid]['label']} ({qid})": {
            "instance of (P31)": [
                hierarchy_to_json(i_qid, data, level - 1)
                for i_qid in data[qid]["instanceof"]
                if i_qid in data
            ],
            "subclass of (P279)": [
                hierarchy_to_json(i_qid, data, level - 1)
                for i_qid in data[qid]["subclassof"]
                if i_qid in data
            ],
        }
    }


def stringify(value: Any) -> str:
    if isinstance(value, dict):
        if "values" in value:
            return ", ".join(
                stringify(v.get("value", {})) for v in value["values"]
            )
        if "value" in value:
            return stringify(value["value"])
        if "string" in value:
            return value["string"]
        if "QID" in value:
            return f"{value.get('label')} ({value.get('QID')})"
        if "PID" in value:
            return f"{value.get('label')} ({value.get('PID')})"
        if "amount" in value:
            return f"{value.get('amount')} {value.get('unit', '')}".strip()
    return str(value)


def triplet_values_to_string(
    entity_id: str, property_id: str, entity: dict
) -> str | None:
    claims = entity.get("claims")
    if not claims:
        return None

    output = ""
    for claim in claims:
        for claim_value in claim.get("values", []):
            if output:
                output += "\n"

            output += f"{entity['label']} ({entity_id}): "
            claim_pid = claim.get("PID", property_id)
            output += f"{claim['property_label']} ({claim_pid}): "
            output += f"{stringify(claim_value['value'])}\n"

            output += f"  Rank: {claim_value.get('rank', 'normal')}\n"

            qualifiers = claim_value.get("qualifiers", [])
            if qualifiers:
                output += "  Qualifier:\n"
                for qualifier in qualifiers:
                    output += (
                        f"    - {qualifier['property_label']} "
                        f"({qualifier['PID']}): "
                    )
                    output += stringify(qualifier)
                    output += "\n"

            references = claim_value.get("references", [])
            if references:
                i = 1
                for reference in references:
                    output += f"  Reference {i}:\n"
                    for reference_claim in reference:
                        output += (
                            f"    - {reference_claim['property_label']} "
                            f"({reference_claim['PID']}): "
                        )
                        output += stringify(reference_claim)
                        output += "\n"
                    i += 1
    return output.strip()
