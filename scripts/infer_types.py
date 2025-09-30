"""Memory-efficient inference of Wikidata types using SQLite.

This module builds a SQLite database containing Wikidata instance-of (P31) and
subclass-of (P279) edges along with their transitive closure. The CLI requires
the caller to specify the input edge files and the output SQLite database path.
"""

import argparse
import sqlite3
from collections import defaultdict
from pathlib import Path


def prepare_db(db_path, inst_path, sub_path):
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    # Drop existing tables
    cur.execute("DROP TABLE IF EXISTS instance;")
    cur.execute("DROP TABLE IF EXISTS subclass;")
    # Create tables with UNIQUE constraints to avoid duplicate inserts
    cur.execute(
        """
        CREATE TABLE instance(
            item TEXT,
            class TEXT,
            UNIQUE(item, class)
        );
    """
    )
    cur.execute("CREATE INDEX IF NOT EXISTS idx_inst_item ON instance(item);")
    cur.execute(
        """
        CREATE TABLE subclass(
            subclass TEXT,
            superclass TEXT,
            UNIQUE(subclass, superclass)
    );
    """
    )
    cur.execute("CREATE INDEX IF NOT EXISTS idx_sub_sub ON subclass(subclass);")
    conn.commit()

    # Bulk insert P31 edges (instances), ignoring duplicates
    with open(inst_path, "r", encoding="utf-8") as f:
        cur.executemany(
            "INSERT OR IGNORE INTO instance(item, class) VALUES (?, ?);",
            (line.strip().split("\t") for line in f if line.strip()),
        )

    # Bulk insert P279 edges (subclasses), ignoring duplicates
    with open(sub_path, "r", encoding="utf-8") as f:
        cur.executemany(
            "INSERT OR IGNORE INTO subclass(subclass, superclass) VALUES (?, ?);",
            (line.strip().split("\t") for line in f if line.strip()),
        )

    conn.commit()
    conn.close()
    print(f"Database prepared at {db_path}")


def materialize(db_path):
    # Connect and tune for bulk operations
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    # Speed up writes and temp storage
    cur.execute("PRAGMA journal_mode = MEMORY;")
    cur.execute("PRAGMA synchronous = OFF;")
    cur.execute("PRAGMA temp_store = MEMORY;")

    # Create the closure table with UNIQUE constraint
    cur.execute(
        """
    CREATE TABLE IF NOT EXISTS subclass_closure (
        subclass   TEXT,
        superclass TEXT,
        UNIQUE(subclass, superclass)
    );
    """
    )
    conn.commit()

    # Ensure raw subclass table is indexed for the CTE
    cur.execute("CREATE INDEX IF NOT EXISTS idx_sub_sub   ON subclass(subclass);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_sub_super ON subclass(superclass);")
    conn.commit()

    # Populate the closure table using a recursive CTE
    cur.execute(
        """
    INSERT OR IGNORE INTO subclass_closure(subclass, superclass)
    WITH RECURSIVE
      closure(sub, sup) AS (
        SELECT subclass, superclass FROM subclass
        UNION
        SELECT c.sub, s.superclass
          FROM closure AS c
          JOIN subclass AS s
            ON c.sup = s.subclass
      )
    SELECT sub, sup FROM closure;
    """
    )
    conn.commit()

    # Index the closure table for fast lookups
    cur.execute("CREATE INDEX IF NOT EXISTS idx_closure_sub ON subclass_closure(subclass);")
    conn.commit()

    conn.close()
    print(f"Transitive closure materialized in 'subclass_closure' in {db_path}")


def infer_all(db_path, items: list[str] | None = None):
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    # Query distinct items
    if items is None:
        cur.execute("SELECT DISTINCT item FROM instance;")
        items = [row[0] for row in cur.fetchall()]

    vals = ",".join(f"('{item}')" for item in items)

    # Recursive CTE: join direct classes and their ancestors
    # query = f"""
    # WITH RECURSIVE
    # items(item) AS (
    #     VALUES {vals}
    # ),
    # initial(item, sup) AS (
    #     SELECT i.item, i.class
    #     FROM instance AS i
    #     JOIN items    AS its ON i.item     = its.item
    #     UNION
    #     SELECT s.subclass, s.superclass
    #     FROM subclass AS s
    #     JOIN items    AS its ON s.subclass = its.item
    # ),
    # closure(item, sup) AS (
    #     SELECT item, sup FROM initial
    #     UNION
    #     SELECT c.item, s.superclass
    #     FROM closure AS c
    #     JOIN subclass AS s ON c.sup = s.subclass
    # )
    # SELECT DISTINCT
    # item,
    # sup AS superclass
    # FROM closure
    # ORDER BY item, superclass;
    # """
    query = f"""
        WITH RECURSIVE
        items(item) AS (
            VALUES {vals}
        ),
        initial(item, sup) AS (
            -- only direct subclass_of edges for each seed
            SELECT s.subclass, s.superclass
            FROM subclass AS s
            JOIN items     AS its ON s.subclass = its.item
        ),
        closure(item, sup) AS (
            -- walk up the P279 chain
            SELECT item, sup FROM initial
            UNION
            SELECT c.item, s.superclass
            FROM closure AS c
            JOIN subclass AS s
                ON c.sup = s.subclass
        )
        SELECT DISTINCT
        item,
        sup     AS superclass
        FROM closure
    """
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute(query)
    results = cur.fetchall()
    conn.close()
    out = {}
    for item, superclass in results:
        if item not in out:
            out[item] = set()
        out[item].add(superclass)
    return out


def linear_infer_all(db_path, items):
    """
    items: List of QIDs, e.g. ["Q1","Q2","Q3"]
    Returns: dict mapping each item -> set(superclasses)
    """
    if not items:
        return {}
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    # build a parameter placeholder for each item
    placeholders = ",".join("?" for _ in items)
    sql = f"""
        SELECT subclass, superclass
          FROM subclass_closure
         WHERE subclass IN ({placeholders})
    """
    cur.execute(sql, items)

    out = defaultdict(set)
    for sub, sup in cur.fetchall():
        out[sub].add(sup)

    # ensure every requested item appears (even if it has no superclasses)
    return {item: out.get(item, set()) for item in items}


def transitive_closure_from_db_update(db_path, items):
    """Get transitive closure from SQLite database,
    using a pre-materialized closure table if available."""
    if not items:
        return {}
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    vals = ",".join(f"('{item}')" for item in items)

    cur.execute(
        """
            SELECT name
            FROM sqlite_master
            WHERE type='table'
            AND name='subclass_closure'
        """
    )
    if cur.fetchone():
        # 1) Check if the fast lookup table exists
        query = f"""
                SELECT subclass, superclass
                FROM subclass_closure
                WHERE subclass IN ({vals})
            """
    else:
        # 2) Fallback: recursive‐CTE
        query = f"""
                WITH RECURSIVE
                items(item) AS (
                    VALUES {vals}
                ),
                initial(item, sup) AS (
                    SELECT s.subclass, s.superclass
                    FROM subclass AS s
                    JOIN items     AS its ON s.subclass = its.item
                ),
                closure(item, sup) AS (
                    SELECT item, sup FROM initial
                    UNION
                    SELECT c.item, s.superclass
                    FROM closure AS c
                    JOIN subclass AS s
                        ON c.sup = s.subclass
                )
                SELECT DISTINCT
                    item,
                    sup     AS superclass
                FROM closure
            """
    cur.execute(query)
    out = defaultdict(set)
    for item, superclass in cur.fetchall():
        out[item].add(superclass)
    return {item: out.get(item, set()) for item in items}


def parse_args() -> argparse.Namespace:
    """Parse command line arguments for building the types database."""

    parser = argparse.ArgumentParser(
        description="Build a SQLite database with transitive closure for Wikidata types",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    parser.add_argument(
        "--instance-of",
        type=Path,
        required=True,
        help="Path to TSV file containing P31 (instance of) edges",
    )
    parser.add_argument(
        "--subclass-of",
        type=Path,
        required=True,
        help="Path to TSV file containing P279 (subclass of) edges",
    )
    parser.add_argument(
        "--output-db",
        type=Path,
        required=True,
        help="Destination SQLite database file (will be created/overwritten)",
    )

    parser.add_argument(
        "--no-closure",
        action="store_true",
        help="Skip computing the transitive closure after loading the edges",
    )

    return parser.parse_args()


def main() -> None:
    """Entry point for the CLI interface."""

    args = parse_args()

    instance_path = args.instance_of.expanduser().resolve()
    subclass_path = args.subclass_of.expanduser().resolve()
    output_path = args.output_db.expanduser().resolve()

    missing_inputs = [path for path in (instance_path, subclass_path) if not path.is_file()]
    if missing_inputs:
        missing = "\n - ".join(str(path) for path in missing_inputs)
        raise FileNotFoundError(
            "The following required input files were not found:\n - " + missing
        )

    output_path.parent.mkdir(parents=True, exist_ok=True)

    print(f"Preparing SQLite database at {output_path}")
    prepare_db(str(output_path), str(instance_path), str(subclass_path))

    if args.no_closure:
        print("Skipping transitive closure materialization per --no-closure flag")
        return

    print("Materializing transitive closure (this may take a while)...")
    materialize(str(output_path))
    print("Done.")


if __name__ == "__main__":
    main()
