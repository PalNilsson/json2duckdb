#!/usr/bin/env python3
"""
json_to_duckdb.py

Read a top-level JSON dictionary (e.g., queuedata) and load it into a DuckDB table.
Keys of the top-level dict become "record_id".

Example:
  python3 json_to_duckdb.py --json queuedata.json
  # creates queuedata.db (DuckDB) with table 'queuedata'
"""

import argparse
import json
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple, Union

try:
    import duckdb  # type: ignore
except Exception as e:
    raise SystemExit("This script requires the 'duckdb' Python package. Install it with: pip install duckdb") from e


TYPE_ORDER = ["BIGINT", "DOUBLE", "TEXT"]  # escalation order for numeric->text


def _is_int(x: Any) -> bool:
    return isinstance(x, bool) is False and isinstance(x, int)


def _is_float(x: Any) -> bool:
    return isinstance(x, float)


def _to_cell_value(v: Any) -> Tuple[Any, str]:
    """
    Convert arbitrary Python value into a DB-storable scalar and return (value, inferred_type).
    Non-scalar / structured values are JSON-serialized into TEXT.
    """
    if v is None:
        return None, "TEXT"  # type unknown; default TEXT for NULL-only columns

    if _is_int(v):
        return v, "BIGINT"
    if _is_float(v):
        return v, "DOUBLE"

    if isinstance(v, str):
        return v, "TEXT"

    # everything else -> JSON string
    try:
        return json.dumps(v, ensure_ascii=False), "TEXT"
    except Exception:
        return str(v), "TEXT"


def _merge_type(t1: str, t2: str) -> str:
    if t1 == t2:
        return t1
    order = {t: i for i, t in enumerate(TYPE_ORDER)}
    return TYPE_ORDER[max(order.get(t1, 2), order.get(t2, 2))]


def _infer_schema(rows: Iterable[Dict[str, Any]]) -> Dict[str, str]:
    """
    Given materialized rows, infer DuckDB column types.
    Mutates the rows to store coerced values.
    """
    col_types: Dict[str, str] = {}
    for row in rows:
        for k, v in row.items():
            val, t = _to_cell_value(v)
            row[k] = val
            prev = col_types.get(k)
            col_types[k] = t if prev is None else _merge_type(prev, t)
    return col_types


def _create_or_replace_table(con: "duckdb.DuckDBPyConnection", table: str, schema: Dict[str, str]) -> None:
    cols_sql = ", ".join([f'"{c}" {t}' for c, t in schema.items()])
    con.execute(f'CREATE OR REPLACE TABLE "{table}" ({cols_sql})')


def _insert_rows(con: "duckdb.DuckDBPyConnection", table: str, rows: List[Dict[str, Any]]) -> None:
    if not rows:
        return
    cols = list(rows[0].keys())
    placeholders = ", ".join(["?"] * len(cols))
    col_list = ", ".join([f'"{c}"' for c in cols])
    data: List[Tuple[Any, ...]] = [tuple(r.get(c) for c in cols) for r in rows]
    con.executemany(f'INSERT INTO "{table}" ({col_list}) VALUES ({placeholders})', data)


def dict_to_duckdb(
    db_path: Union[str, Path],
    table_name: str,
    dictionary: Dict[str, Any],
) -> None:
    """
    Load a JSON dictionary into a DuckDB table. Keys become 'record_id'.
    """
    rows: List[Dict[str, Any]] = []
    for rec_id, payload in dictionary.items():
        row: Dict[str, Any] = {"record_id": rec_id}
        if isinstance(payload, dict):
            for k, v in payload.items():
                row[k] = v
        else:
            # Non-dict payload is stored as JSON in 'data'
            row["data"] = payload
        rows.append(row)

    rows = [dict(r) for r in rows]
    schema = _infer_schema(rows)

    # Ensure record_id exists and is TEXT
    if "record_id" not in schema:
        schema["record_id"] = "TEXT"
        for r in rows:
            r.setdefault("record_id", None)
    else:
        schema["record_id"] = "TEXT"

    con = duckdb.connect(str(db_path))
    try:
        _create_or_replace_table(con, table_name, schema)
        _insert_rows(con, table_name, rows)
        con.commit()
    finally:
        con.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Load queuedata JSON into DuckDB (queuedata.db).")
    parser.add_argument("--json", required=True, help="Path to queuedata.json (top-level dictionary).")
    parser.add_argument("--db", default="queuedata.db",
                        help="Path to DuckDB database file (default: queuedata.db).")
    parser.add_argument("--table", default="queuedata",
                        help="Target table name (default: queuedata).")
    args = parser.parse_args()

    p = Path(args.json)
    if not p.exists():
        raise FileNotFoundError(f"JSON file not found: {p}")
    data = json.loads(p.read_text(encoding="utf-8"))

    if not isinstance(data, dict):
        raise SystemError("Input JSON must be a top-level dictionary of {record_id: { ...row... }}.")

    dict_to_duckdb(args.db, args.table, data)
    print(f"Loaded '{args.json}' into {args.db}:{args.table}")


if __name__ == "__main__":
    main()
