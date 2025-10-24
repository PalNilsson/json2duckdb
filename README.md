# json2duckdb

A simple tool that creates and populates a DuckDB database from a JSON file with assumed format:

`{ outer_key: { col: value, ... }, ... }`

**Usage**

`python3 json_to_duckdb.py [-h] --db DB --table TABLE [--json JSON]`

where

* --db: Path to SQLite DB file (will be created if missing).
* --table: Target table name, e.g., 'queuedata'.
* --json: Path to JSON file.




