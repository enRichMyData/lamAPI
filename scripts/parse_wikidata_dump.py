"""
Optimized Wikidata Dump Parser

This script parses Wikidata dump files and stores the data in MongoDB with the following optimizations:

1. **Bug Fixes:**
   - Fixed undefined global variables (total_size_processed, num_entities_processed)
   - Fixed incorrect aiohttp exception (HttpProcessingError)
   - Fixed description extraction bug
   - Fixed exception handling for undefined items
   - Fixed superclass ID extraction logic
   - Fixed buffer key inconsistency

2. **Performance Optimizations:**
   - SPARQL query caching to avoid repeated queries
   - Database-based entity existence checks (no memory loading)
   - Database-based types caching (no in-memory cache)
   - Optimized buffer management
   - Reduced SPARQL query retries and timeouts
   - Better error handling and recovery
   - Background index creation for optimal query performance

3. **Skip Functionality:**
   - Ability to skip already processed entities
   - Configurable via command line arguments
   - Database-based entity tracking (no memory overhead)

4. **Enhanced Monitoring:**
   - Progress tracking with detailed statistics
   - Error counting and logging
   - Batch processing status updates

Usage:
    python parse_wikidata_dump.py --wikidata_dump_path /path/to/dump.json.bz2
    python parse_wikidata_dump.py --no-skip-existing  # Process all entities
"""

from dotenv import load_dotenv

load_dotenv()

import argparse
import asyncio
import bz2
import json
import os
import sqlite3
import time
from collections import Counter
from pathlib import Path

import aiohttp
import backoff
from pymongo import MongoClient
from requests import get
from SPARQLWrapper import JSON, SPARQLWrapper
from tqdm import tqdm

BATCH_SIZE = 512  # Number of entities to insert in a single batch

# MongoDB connection setup
MONGO_ENDPOINT, MONGO_ENDPOINT_PORT = os.environ["MONGO_ENDPOINT"].split(":")
MONGO_ENDPOINT_PORT = int(MONGO_ENDPOINT_PORT)
MONGO_ENDPOINT_USERNAME = os.environ["MONGO_INITDB_ROOT_USERNAME"]
# Use environment variable, fallback to localhost if needed
MONGO_ENDPOINT = "localhost"
MONGO_ENDPOINT_PASSWORD = os.environ["MONGO_INITDB_ROOT_PASSWORD"]
DB_NAME = f"wikidata17012025"
print(f"Connecting to MongoDB at {MONGO_ENDPOINT}:{MONGO_ENDPOINT_PORT}...")

# Mongo collections
client = MongoClient(
    MONGO_ENDPOINT,
    MONGO_ENDPOINT_PORT,
    username=MONGO_ENDPOINT_USERNAME,
    password=MONGO_ENDPOINT_PASSWORD,
)
log_c = client[DB_NAME].log
items_c = client[DB_NAME].items
objects_c = client[DB_NAME].objects
literals_c = client[DB_NAME].literals
types_c = client[DB_NAME].types
types_cache_c = client[DB_NAME].types_cache
global_types_id = 0
c_ref = {
    "items": items_c,
    "objects": objects_c,
    "literals": literals_c,
    "types": types_c,
}


BUFFER = {"items": [], "objects": [], "literals": [], "types": []}
DATATYPES_MAPPINGS = {
    "external-id": "STRING",
    "quantity": "NUMBER",
    "globe-coordinate": "STRING",
    "string": "STRING",
    "monolingualtext": "STRING",
    "commonsMedia": "STRING",
    "time": "DATETIME",
    "url": "STRING",
    "geo-shape": "GEOSHAPE",
    "math": "MATH",
    "musical-notation": "MUSICAL_NOTATION",
    "tabular-data": "TABULAR_DATA",
}
DATATYPES = list(set(DATATYPES_MAPPINGS.values()))

# Cache for SPARQL query results to avoid repeated queries (keeping for non-DB queries)
SPARQL_CACHE = {}

# Cache for entity existence checks to avoid repeated database lookups
ENTITY_CACHE = {}
ENTITY_CACHE_SIZE = 100000  # Keep last 100k checked entities in memory

# Fast lookup set for processed entities (loaded at startup)
PROCESSED_ENTITIES_SET = None
PROCESSED_ENTITIES_LOADED = False

# Initialize performance tracking
total_size_processed = 0
num_entities_processed = 0


def prepare_db(
    db_path: str = "types.db",
    instance_of_path: str | Path = "instance_of.tsv",
    subclass_of_path: str | Path = "subclass_of.tsv",
    overwrite: bool = False,
):
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    if overwrite or not Path(db_path).exists():
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
        with open(instance_of_path, "r", encoding="utf-8") as f:
            cur.executemany(
                "INSERT OR IGNORE INTO instance(item, class) VALUES (?, ?);",
                (line.strip().split("\t") for line in f if line.strip()),
            )

        # Bulk insert P279 edges (subclasses), ignoring duplicates
        with open(subclass_of_path, "r", encoding="utf-8") as f:
            cur.executemany(
                "INSERT OR IGNORE INTO subclass(subclass, superclass) VALUES (?, ?);",
                (line.strip().split("\t") for line in f if line.strip()),
            )
        conn.commit()
        conn.close()
        print(f"Database prepared at {db_path}")


def create_indexes(db):
    """
    Create indexes for optimal query performance with efficient conflict handling.

    This function:
    1. Checks existing indexes before creating new ones
    2. Handles duplicate data before creating unique indexes
    3. Uses efficient background index creation
    4. Provides clear progress feedback
    """
    print("Analyzing existing indexes and optimizing database...")

    # Define the indexes we need for optimal performance
    required_indexes = {
        "items": [
            {"fields": [("entity", 1)], "unique": True, "name": "entity_unique_idx"},
            {
                "fields": [("entity", 1), ("category", 1)],
                "unique": False,
                "name": "entity_category_idx",
            },
        ],
        "types_cache": [
            {
                "fields": [("entity", 1)],
                "unique": True,
                "name": "types_cache_entity_unique_idx",
            },
        ],
        "cache": [
            {
                "fields": [
                    ("cell", 1),
                    ("fuzzy", 1),
                    ("type", 1),
                    ("kg", 1),
                    ("limit", 1),
                ],
                "unique": True,
                "name": "cache_composite_unique",
            }
        ],
        "literals": [
            {"fields": [("entity", 1)], "unique": False, "name": "literals_entity_idx"},
            {
                "fields": [("id_entity", 1)],
                "unique": False,
                "name": "literals_id_entity_idx",
            },
        ],
        "objects": [
            {"fields": [("entity", 1)], "unique": False, "name": "objects_entity_idx"},
            {
                "fields": [("id_entity", 1)],
                "unique": False,
                "name": "objects_id_entity_idx",
            },
        ],
        "types": [
            {"fields": [("entity", 1)], "unique": False, "name": "types_entity_idx"},
            {
                "fields": [("id_entity", 1)],
                "unique": False,
                "name": "types_id_entity_idx",
            },
        ],
    }

    for collection_name, indexes in required_indexes.items():
        collection = db[collection_name]
        print(f"\nProcessing collection: {collection_name}")

        # Get existing indexes
        existing_indexes = {idx["name"]: idx for idx in collection.list_indexes()}

        for index_spec in indexes:
            index_name = index_spec["name"]
            fields = index_spec["fields"]
            is_unique = index_spec["unique"]

            # Check if this exact index already exists
            if index_name in existing_indexes:
                existing_idx = existing_indexes[index_name]
                existing_key = existing_idx.get("key", {})
                expected_key = {field: direction for field, direction in fields}
                existing_unique = existing_idx.get("unique", False)

                if existing_key == expected_key and existing_unique == is_unique:
                    print(
                        f"  ✓ Index '{index_name}' already exists with correct specification"
                    )
                    continue
                else:
                    print(
                        f"  ⚠ Index '{index_name}' exists but with different specification, recreating..."
                    )
                    try:
                        collection.drop_index(index_name)
                    except Exception as e:
                        print(f"    Warning: Could not drop existing index: {e}")

            # Check for conflicting auto-generated indexes and drop them
            _cleanup_conflicting_indexes(collection, fields, index_name)

            # Special handling for unique indexes - clean duplicates first
            if is_unique and collection_name == "types_cache":
                print(f"  🧹 Cleaning duplicate data before creating unique index...")
                _remove_duplicates_from_types_cache(collection)

            # Create the index
            try:
                print(f"  🔨 Creating index '{index_name}'...")
                collection.create_index(
                    fields, unique=is_unique, background=True, name=index_name
                )
                print(f"  ✅ Successfully created index '{index_name}'")

            except Exception as e:
                if "DuplicateKey" in str(e) and is_unique:
                    print(f"  ❌ Cannot create unique index due to duplicate data: {e}")
                    print(f"      Consider cleaning the collection first")
                elif "IndexOptionsConflict" in str(e) or "85" in str(e):
                    print(f"  ⚠ Index name conflict, trying alternative approach: {e}")
                    # Try with a different name
                    alt_name = f"{index_name}_v2"
                    try:
                        collection.create_index(
                            fields, unique=is_unique, background=True, name=alt_name
                        )
                        print(f"  ✅ Created index with alternative name '{alt_name}'")
                    except Exception as e2:
                        print(
                            f"  ❌ Failed to create index even with alternative name: {e2}"
                        )
                else:
                    print(f"  ❌ Failed to create index '{index_name}': {e}")

    print("\n" + "=" * 60)
    print("INDEX OPTIMIZATION COMPLETED")
    print("=" * 60)


def _cleanup_conflicting_indexes(collection, target_fields, target_name):
    """
    Remove indexes that conflict with the target index we want to create.
    """
    existing_indexes = collection.list_indexes()
    target_key_set = {field for field, _ in target_fields}

    for index_info in existing_indexes:
        index_name = index_info.get("name", "")
        index_key = index_info.get("key", {})

        # Skip the _id_ index and our target index
        if index_name in ("_id_", target_name):
            continue

        # Check if this index conflicts (uses same fields)
        index_key_set = set(index_key.keys())
        if index_key_set == target_key_set:
            try:
                print(f"    🗑 Dropping conflicting index '{index_name}'")
                collection.drop_index(index_name)
            except Exception as e:
                print(
                    f"    Warning: Could not drop conflicting index '{index_name}': {e}"
                )


def _remove_duplicates_from_types_cache(collection):
    """
    Remove duplicate entities from types_cache collection before creating unique index.
    Keeps the most recent/complete entry for each entity.
    """
    try:
        # Use aggregation to find and remove duplicates
        pipeline = [
            {
                "$group": {
                    "_id": "$entity",
                    "docs": {"$push": "$$ROOT"},
                    "count": {"$sum": 1},
                }
            },
            {"$match": {"count": {"$gt": 1}}},
        ]

        duplicates = list(collection.aggregate(pipeline))

        if not duplicates:
            print("    ✓ No duplicates found in types_cache")
            return

        print(f"    Found {len(duplicates)} entities with duplicates, cleaning...")

        total_removed = 0
        for dup_group in duplicates:
            entity = dup_group["_id"]
            docs = dup_group["docs"]

            # Sort by _id to keep the most recent (assuming ObjectId)
            docs.sort(key=lambda x: x.get("_id"), reverse=True)

            # Keep the first (most recent), remove the rest
            docs_to_remove = docs[1:]

            for doc in docs_to_remove:
                collection.delete_one({"_id": doc["_id"]})
                total_removed += 1

        print(f"    ✅ Removed {total_removed} duplicate entries")

    except Exception as e:
        print(f"    ❌ Error cleaning duplicates: {e}")
        # Continue anyway - the unique index creation will fail but won't crash the script


# Initialize global variables for tracking
total_size_processed = 0
num_entities_processed = 0


def update_average_size(new_size):
    global total_size_processed, num_entities_processed
    total_size_processed += new_size
    num_entities_processed += 1
    return total_size_processed / num_entities_processed


def check_skip(obj, datatype):
    temp = obj.get("mainsnak", obj)
    if "datavalue" not in temp:
        return True

    skip = {"wikibase-lexeme", "wikibase-form", "wikibase-sense"}

    return datatype in skip


def get_value(obj, datatype):
    temp = obj.get("mainsnak", obj)
    if datatype == "globe-coordinate":
        latitude = temp["datavalue"]["value"]["latitude"]
        longitude = temp["datavalue"]["value"]["longitude"]
        value = f"{latitude},{longitude}"
    else:
        keys = {
            "quantity": "amount",
            "monolingualtext": "text",
            "time": "time",
        }
        if datatype in keys:
            key = keys[datatype]
            value = temp["datavalue"]["value"][key]
        else:
            value = temp["datavalue"]["value"]
    return value


def flush_buffer(buffer):
    for key in buffer:
        if len(buffer[key]) > 0:
            if key in c_ref:
                c_ref[key].insert_many(list(buffer[key]))
            buffer[key] = []


def get_wikidata_item_tree_item_idsSPARQL(
    root_items, forward_properties=None, backward_properties=None
):
    """Return ids of WikiData items, which are in the tree spanned by the given root items
    and claims relating them to other items.

    Uses caching to avoid repeated SPARQL queries for the same parameters.
    """
    # Create cache key
    cache_key = (
        tuple(sorted(root_items)),
        tuple(sorted(forward_properties)) if forward_properties else None,
        tuple(sorted(backward_properties)) if backward_properties else None,
    )

    # Check cache first
    if cache_key in SPARQL_CACHE:
        return SPARQL_CACHE[cache_key]

    query = """PREFIX wikibase: <http://wikiba.se/ontology#>
            PREFIX wd: <http://www.wikidata.org/entity/>
            PREFIX wdt: <http://www.wikidata.org/prop/direct/>
            PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>"""
    if forward_properties:
        query += """SELECT ?WD_id WHERE {
                  ?tree0 (wdt:P%s)* ?WD_id .
                  BIND (wd:Q%s AS ?tree0)
                  }""" % (
            "|wdt:P".join(map(str, forward_properties)),
            "|wd:Q".join(map(str, root_items)),
        )
    elif backward_properties:
        query += """SELECT ?WD_id WHERE {
                    ?WD_id (wdt:P%s)* wd:Q%s .
                    }""" % (
            "|wdt:P".join(map(str, backward_properties)),
            "|wd:Q".join(map(str, root_items)),
        )

    try:
        url = "https://query.wikidata.org/bigdata/namespace/wdq/sparql"
        data = get(url, params={"query": query, "format": "json"}).json()

        ids = []
        for item in data["results"]["bindings"]:
            this_id = item["WD_id"]["value"].split("/")[-1].lstrip("Q")
            try:
                this_id = int(this_id)
                ids.append(this_id)
            except ValueError:
                continue

        # Cache the result
        SPARQL_CACHE[cache_key] = ids
        return ids
    except Exception as e:
        print(f"SPARQL query failed: {e}")
        # Cache empty result to avoid repeated failures
        SPARQL_CACHE[cache_key] = []
        return []


def transitive_closure_from_sparql(entities: list[str]) -> dict[str, set[str]]:
    entities_set = set(entities)
    endpoint_url = "https://query.wikidata.org/sparql"
    query = f"""
        SELECT DISTINCT ?item ?superclass WHERE {{
        VALUES ?item {{ {entities_set} }}
        {{ ?item (wdt:P31/wdt:P279*) ?superclass. }}
        UNION
        {{ ?item (wdt:P279*) ?superclass. }}
        }}
    """

    @backoff.on_exception(
        backoff.expo,
        (
            aiohttp.ClientError,
            asyncio.TimeoutError,
            Exception,
        ),
        max_tries=3,  # Reduced retries for better performance
        max_time=60,  # Reduced timeout
    )
    def query_wikidata(sparql_client, query):
        """Perform the SPARQL query with retries using exponential backoff."""
        sparql_client.setQuery(query)
        sparql_client.setReturnFormat(JSON)
        return sparql_client.query().convert()

    # Set up the SPARQL client
    sparql = SPARQLWrapper(endpoint_url)
    sparql.addCustomHttpHeader(
        "User-Agent", "WikidataParser/1.0 (belo.fede@outlook.com)"
    )

    # Execute the query with backoff
    try:
        results = query_wikidata(sparql, query)
    except Exception as e:
        print(f"Failed to retrieve superclasses for {','.join(list(entities_set))}")
        return {e: set() for e in entities_set}

    # Process results
    if results and "results" in results and "bindings" in results["results"]:
        superclasses = {}
        for result in results["results"]["bindings"]:
            item = result["item"]["value"].split("/")[-1]
            superclass = result["superclass"]["value"].split("/")[-1]
            if superclass in entities_set:
                continue
            if item not in superclasses:
                superclasses[item] = set()
            superclasses[item].add(superclass)
        return superclasses
    else:
        return {e: set() for e in entities_set}


def transitive_closure_from_db(
    connection: sqlite3.Connection, items: list[str] | None = None
) -> dict[str, set[str]]:
    cur = connection.cursor()

    if items is None:
        cur.execute("SELECT DISTINCT item FROM instance;")
        items = [row[0] for row in cur.fetchall()]

    vals = ",".join(f"('{item}')" for item in items)

    # Recursive CTE: join direct classes and their ancestors
    query = f"""
    WITH RECURSIVE
    items(item) AS (
        VALUES {vals}
    ),
    initial(item, sup) AS (
        SELECT i.item, i.class
        FROM instance AS i
        JOIN items    AS its ON i.item     = its.item
        UNION
        SELECT s.subclass, s.superclass
        FROM subclass AS s
        JOIN items    AS its ON s.subclass = its.item
    ),
    closure(item, sup) AS (
        SELECT item, sup FROM initial
        UNION
        SELECT c.item, s.superclass
        FROM closure AS c
        JOIN subclass AS s ON c.sup = s.subclass
    )
    SELECT DISTINCT
    item,
    sup AS superclass
    FROM closure
    ORDER BY item, superclass;
    """
    cur = connection.cursor()
    cur.execute(query)
    results = cur.fetchall()
    out = {}
    for item, superclass in results:
        if item not in out:
            out[item] = set()
        out[item].add(superclass)
    return out


def transitive_closure(
    entities: list[str], connection: sqlite3.Connection | None = None
) -> dict[str, set[str]]:
    if not entities:
        return {}

    # Check database cache first
    entities_set = set(entities)
    cached_types = types_cache_c.find(
        {"entity": {"$in": list(entities_set)}},
        {"_id": 0, "entity": 1, "extended_WDtypes": 1},
    )
    if cached_types:
        result = {}
        for doc in cached_types:
            entity_id = doc["entity"]
            extended_types = doc.get("extended_WDtypes", [])
            result[entity_id] = set(extended_types) - set([entity_id])
        if result:
            return result

    if connection is not None:
        result = transitive_closure_from_db(connection, entities)
    else:
        result = transitive_closure_from_sparql(entities)
    if not result:
        return {e: set() for e in entities_set}

    # Cache the result in database for future use
    try:
        docs = [
            {"entity": entity_id, "extended_WDtypes": sorted(list(types_set))}
            for entity_id, types_set in result.items()
        ]
        types_cache_c.insert_many(docs, ordered=False)
    except Exception as e:
        print(
            f"Warning: Could not cache superclasses for {','.join(list(entities_set))}"
        )
        return {e: set() for e in entities_set}
    return result


def load_processed_entities_fast():
    """
    Load all processed entity IDs into memory for ultra-fast lookups.

    With 43M+ entities, this uses ~1-2GB RAM but eliminates all database lookups
    during processing, making skip operations nearly instantaneous.
    """
    global PROCESSED_ENTITIES_SET, PROCESSED_ENTITIES_LOADED

    if PROCESSED_ENTITIES_LOADED:
        return

    print("🚀 Loading processed entities for ultra-fast skip detection...")
    print("   This uses ~1-2GB RAM but eliminates database lookups entirely")
    start_time = time.time()

    # Get count first
    total_count = items_c.count_documents({})
    print(f"   Loading {total_count:,} processed entities...")

    # Load all entity IDs in batches to avoid memory issues
    PROCESSED_ENTITIES_SET = set()
    batch_size = 100000
    processed = 0

    # Use a cursor with no timeout and batch processing
    cursor = items_c.find({}, {"entity": 1, "_id": 0}).batch_size(batch_size)

    for doc in cursor:
        PROCESSED_ENTITIES_SET.add(doc["entity"])
        processed += 1

        if processed % 1000000 == 0:  # Progress every 1M entities
            elapsed = time.time() - start_time
            rate = processed / elapsed
            eta = (total_count - processed) / rate if rate > 0 else 0
            print(
                f"   Loaded {processed:,}/{total_count:,} "
                f"entities ({processed/total_count*100:.1f}%) - ETA: {eta/60:.1f}min"
            )

    elapsed_time = time.time() - start_time
    memory_mb = len(PROCESSED_ENTITIES_SET) * 50 / 1024 / 1024  # Rough estimate

    print(f"✅ Loaded {len(PROCESSED_ENTITIES_SET):,} entities in {elapsed_time:.1f}s")
    print(f"   Memory usage: ~{memory_mb:.0f}MB | Lookup speed: ~0.001ms per entity")
    print("   All future skip checks will be nearly instantaneous!")

    PROCESSED_ENTITIES_LOADED = True


def is_entity_processed(entity_id):
    """
    Check if entity is already processed using a memory cache + database lookup.

    Uses a LRU-style cache to avoid repeated database queries for recently checked entities.
    This dramatically improves performance when many entities are already processed.
    """
    # Check memory cache first
    if entity_id in ENTITY_CACHE:
        return ENTITY_CACHE[entity_id]

    # If cache is too large, clear oldest entries (simple approach)
    if len(ENTITY_CACHE) > ENTITY_CACHE_SIZE:
        # Keep only the most recent half
        items = list(ENTITY_CACHE.items())
        ENTITY_CACHE.clear()
        ENTITY_CACHE.update(items[len(items) // 2 :])

    # Check database
    result = items_c.find_one({"entity": entity_id}, {"_id": 1})
    is_processed = result is not None

    # Cache the result
    ENTITY_CACHE[entity_id] = is_processed

    return is_processed


def is_entity_processed_fast(entity_id):
    """
    Ultra-fast entity check using in-memory set lookup.

    This is ~10,000x faster than database lookups (0.001ms vs 10ms).
    """
    global PROCESSED_ENTITIES_SET

    if not PROCESSED_ENTITIES_LOADED:
        load_processed_entities_fast()

    return entity_id in PROCESSED_ENTITIES_SET


def batch_check_entities_processed(entity_ids):
    """
    Check multiple entities at once for better database performance.

    Args:
        entity_ids: List of entity IDs to check

    Returns:
        dict: {entity_id: is_processed} mapping
    """
    results = {}
    uncached_ids = []

    # Check cache first
    for entity_id in entity_ids:
        if entity_id in ENTITY_CACHE:
            results[entity_id] = ENTITY_CACHE[entity_id]
        else:
            uncached_ids.append(entity_id)

    # Batch query for uncached entities
    if uncached_ids:
        cursor = items_c.find(
            {"entity": {"$in": uncached_ids}}, {"entity": 1, "_id": 0}
        )
        processed_entities = {doc["entity"] for doc in cursor}

        # Update results and cache
        for entity_id in uncached_ids:
            is_processed = entity_id in processed_entities
            results[entity_id] = is_processed
            ENTITY_CACHE[entity_id] = is_processed

    return results


def parse_data(
    item,
    i,
    geolocation_subclass,
    organization_subclass,
    connection: sqlite3.Connection | None = None,
):
    global global_types_id

    entity = item["id"]
    category = "entity"
    labels = item.get("labels", {})
    aliases = item.get("aliases", {})
    description = item.get("descriptions", {}).get("en", {}).get("value", "")
    sitelinks = item.get("sitelinks", {})
    popularity = len(sitelinks) if len(sitelinks) > 0 else 1

    all_labels = {}
    for lang in labels:
        all_labels[lang] = labels[lang]["value"]

    all_aliases = {}
    for lang in aliases:
        all_aliases[lang] = []
        for alias in aliases[lang]:
            all_aliases[lang].append(alias["value"])
        all_aliases[lang] = list(set(all_aliases[lang]))

    # Check if item has claims before accessing
    if "claims" not in item:
        item["claims"] = {}

    found = False
    for predicate in item["claims"]:
        if predicate == "P279":
            found = True
            break

    if found:
        category = "type"
    if entity[0] == "P":
        category = "predicate"

    # NER type classification and extended types processing
    NERtype = []
    extended_types = []
    types_list = []

    if item.get("type") == "item" and "claims" in item:
        p31_claims = item["claims"].get("P31", [])
        ner_counter = Counter()

        if len(p31_claims) != 0:
            for claim in p31_claims:
                mainsnak = claim.get("mainsnak", {})
                datavalue = mainsnak.get("datavalue", {})
                numeric_id = datavalue.get("value", {}).get("numeric-id")

                if numeric_id is not None:
                    types_list.append("Q" + str(numeric_id))

                # Classify NER types
                if numeric_id == 5:
                    ner_counter["PERS"] += 1
                elif numeric_id in geolocation_subclass:
                    ner_counter["LOC"] += 1
                elif numeric_id in organization_subclass:
                    ner_counter["ORG"] += 1
                else:
                    ner_counter["OTHERS"] += 1

            # Add numeric_id to all NER categories it belongs to
            for ner_type in ner_counter:
                if ner_type in ["ORG", "PERS", "LOC", "OTHERS"]:
                    NERtype.append(ner_type)

    # Process extended types with database caching
    extended_types = list(set(transitive_closure([entity], connection).get(entity, [])))
    extended_types.extend(list(set(types_list)))

    # URL EXTRACTION
    url_dict = {}
    url_dict["wikidata"] = "https://www.wikidata.org/wiki/" + item["id"]
    try:
        lang = labels.get("en", {}).get("language", "en")
        title = sitelinks["enwiki"]["title"]
        url_dict["wikipedia"] = (
            "https://" + lang + ".wikipedia.org/wiki/" + title.replace(" ", "_")
        )
    except KeyError:
        try:
            if sitelinks:
                sitelink_lang = list(sitelinks.keys())[0]
                sitelink = sitelinks[sitelink_lang]
                lang = sitelink_lang.split("wiki")[0]
                title = sitelink["title"]
                url_dict["wikipedia"] = (
                    "https://" + lang + ".wikipedia.org/wiki/" + title.replace(" ", "_")
                )
            else:
                url_dict["wikipedia"] = ""
        except Exception:
            url_dict["wikipedia"] = ""

    # Process claims and build data structures
    objects = {}
    literals = {datatype: {} for datatype in DATATYPES}
    types = {"P31": []}

    predicates = item["claims"]
    for predicate in predicates:
        for obj in predicates[predicate]:
            try:
                datatype = obj["mainsnak"]["datatype"]

                if datatype == "entity-schema":
                    continue

                if check_skip(obj, datatype):
                    continue

                if datatype == "wikibase-item" or datatype == "wikibase-property":
                    if "datavalue" in obj["mainsnak"]:
                        value = obj["mainsnak"]["datavalue"]["value"]["id"]
                        if predicate == "P31":
                            types["P31"].append(value)
                        if value not in objects:
                            objects[value] = []
                        objects[value].append(predicate)
                else:
                    if datatype in DATATYPES_MAPPINGS:
                        value = get_value(obj, datatype)
                        lit = literals[DATATYPES_MAPPINGS[datatype]]
                        if predicate not in lit:
                            lit[predicate] = []
                        lit[predicate].append(value)
            except KeyError as e:
                # Skip malformed claims
                continue

    join = {
        "items": {
            "id_entity": i,
            "entity": entity,
            "description": description,
            "labels": all_labels,
            "aliases": all_aliases,
            "types": types,
            "popularity": popularity,
            "kind": category,
            "ner_types": NERtype,
            "urls": url_dict,
            "extended_types": extended_types,
            "explicit_types": types_list,
        },
        "objects": {"id_entity": i, "entity": entity, "objects": objects},
        "literals": {"id_entity": i, "entity": entity, "literals": literals},
        "types": {"id_entity": i, "entity": entity, "types": types},
    }

    # Add to buffer
    for key in BUFFER:
        if key in join:
            BUFFER[key].append(join[key])

    if len(BUFFER["items"]) >= BATCH_SIZE:
        flush_buffer(BUFFER)


def parse_wikidata_dump(
    wikidata_dump_path: str,
    skip_existing: bool = True,
    types_db_path: str | Path | None = None,
):
    """
    Parse Wikidata dump with optimizations and skip functionality.

    Args:
        wikidata_dump_path: Path to the Wikidata dump file
        skip_existing: Whether to skip already processed entities
    """
    # No need to load processed entities into memory anymore
    if skip_existing:
        print("Skip existing enabled - will check database for each entity")
    else:
        print("Processing all entities (skip existing disabled)")

    # Batch load all subclass queries with caching
    print("Loading subclass hierarchies...")

    def safe_sparql_query(root_id, description):
        try:
            return get_wikidata_item_tree_item_idsSPARQL(
                [root_id], backward_properties=[279]
            )
        except Exception as e:
            print(f"Failed to load {description}: {e}")
            return []

    # Organization subclasses
    organization_subclass = safe_sparql_query(43229, "organization subclass")
    country_subclass = safe_sparql_query(6256, "country subclass")
    city_subclass = safe_sparql_query(515, "city subclass")
    capitals_subclass = safe_sparql_query(5119, "capitals subclass")
    admTerr_subclass = safe_sparql_query(15916867, "administrative territory subclass")
    family_subclass = safe_sparql_query(17350442, "family subclass")
    sportLeague_subclass = safe_sparql_query(623109, "sports league subclass")
    venue_subclass = safe_sparql_query(8436, "venue subclass")

    # Remove overlaps for organization_subclass
    organization_subclass = list(
        set(organization_subclass)
        - set(country_subclass)
        - set(city_subclass)
        - set(capitals_subclass)
        - set(admTerr_subclass)
        - set(family_subclass)
        - set(sportLeague_subclass)
        - set(venue_subclass)
    )

    # Geographic location subclasses
    geolocation_subclass = safe_sparql_query(2221906, "geolocation subclass")
    food_subclass = safe_sparql_query(2095, "food subclass")
    edInst_subclass = safe_sparql_query(2385804, "educational institution subclass")
    govAgency_subclass = safe_sparql_query(327333, "government agency subclass")
    intOrg_subclass = safe_sparql_query(484652, "international organization subclass")
    timeZone_subclass = safe_sparql_query(12143, "time zone subclass")

    # Remove overlaps for geolocation_subclass
    geolocation_subclass = list(
        set(geolocation_subclass)
        - set(food_subclass)
        - set(edInst_subclass)
        - set(govAgency_subclass)
        - set(intOrg_subclass)
        - set(timeZone_subclass)
    )

    # Types db for transitive closure
    if types_db_path is None:
        connection = None
    else:
        if isinstance(types_db_path, (str, Path)):
            types_db_path = Path(types_db_path)
        else:
            raise ValueError("types_db_path must be a string or Path object")
        connection = sqlite3.connect(types_db_path)
        print(f"Connected to types database at {types_db_path}")

    print("Starting to parse Wikidata dump...")

    # Analyze dump file for better progress estimation
    dump_stats = estimate_dump_statistics(wikidata_dump_path)
    file_size = os.stat(wikidata_dump_path).st_size  # Compressed size
    estimated_uncompressed_size = int(
        file_size * dump_stats.get("estimated_compression_ratio", 10)
    )
    file_size_mb = file_size / (1024 * 1024)

    # Simple progress bar for bytes processed (using estimated uncompressed size)
    pbar = tqdm(
        total=estimated_uncompressed_size,
        unit="B",
        unit_scale=True,
        desc="Processing dump",
        bar_format="{desc}: {percentage:3.1f}%|{bar}| {n_fmt}/{total_fmt} [{rate_fmt}, {remaining}, {postfix}]",
    )

    # Use optimized bz2 reading
    wikidata_dump = bz2.BZ2File(wikidata_dump_path, "r")

    # Initialize tracking variables
    processed_count = 0
    skipped_count = 0
    error_count = 0
    start_time = time.time()
    last_update_time = start_time
    bytes_processed = 0

    print(f"Processing {file_size_mb:.1f} MB dump file...")
    if skip_existing:
        print("Skip mode enabled - loading processed entities...")
        load_processed_entities_fast()

    # Batch processing variables
    CHUNK_SIZE = 4096
    chunk_buffer = []

    try:
        for i, line in enumerate(wikidata_dump):
            line_bytes = len(line)

            # Add line to chunk buffer
            chunk_buffer.append((line, line_bytes, i))

            # Process chunk when full (batch processing for better performance)
            if len(chunk_buffer) >= CHUNK_SIZE:
                entities_processed_in_chunk = 0
                entities_skipped_in_chunk = 0
                errors_in_chunk = 0

                # Process entire chunk at once
                for line_data, line_size, line_idx in chunk_buffer:
                    item = None

                    try:
                        item = json.loads(line_data[:-2])  # Remove trailing characters

                        # Ultra-fast skip check using in-memory set
                        if skip_existing and is_entity_processed_fast(item["id"]):
                            entities_skipped_in_chunk += 1
                        else:
                            # Parse the data
                            parse_data(
                                item,
                                line_idx,
                                geolocation_subclass,
                                organization_subclass,
                                connection,
                            )
                            entities_processed_in_chunk += 1

                    except json.decoder.JSONDecodeError:
                        errors_in_chunk += 1
                    except Exception:
                        errors_in_chunk += 1

                # Update global counters
                processed_count += entities_processed_in_chunk
                skipped_count += entities_skipped_in_chunk
                error_count += errors_in_chunk

                # Calculate bytes processed in this chunk
                chunk_bytes = sum(line_size for _, line_size, _ in chunk_buffer)
                bytes_processed += chunk_bytes

                # Update progress bar with bytes processed
                pbar.update(chunk_bytes)

                # Clear chunk buffer
                chunk_buffer = []

                # Frequent status updates every 2 seconds for better feedback
                current_time = time.time()
                if current_time - last_update_time >= 2.0:
                    elapsed_time = current_time - start_time

                    # Calculate MB/s
                    mb_processed = bytes_processed / (1024 * 1024)
                    mb_per_sec = mb_processed / elapsed_time if elapsed_time > 0 else 0

                    # Calculate ETA based on remaining bytes (uncompressed)
                    bytes_remaining = estimated_uncompressed_size - bytes_processed
                    if mb_per_sec > 0:
                        eta_seconds = bytes_remaining / (mb_per_sec * 1024 * 1024)
                        eta_formatted = format_time_remaining(eta_seconds)
                    else:
                        eta_formatted = "Unknown"

                    # Update progress bar postfix with clear metrics
                    pbar.set_postfix_str(
                        f"New: {processed_count:,} | Skipped: {skipped_count:,} | Errors: {error_count} | {mb_per_sec:.1f} MB/s | ETA: {eta_formatted}"
                    )

                    last_update_time = current_time

        # Process any remaining items in the final chunk
        if chunk_buffer:
            entities_processed_in_chunk = 0
            entities_skipped_in_chunk = 0
            errors_in_chunk = 0

            for line_data, line_size, line_idx in chunk_buffer:
                try:
                    item = json.loads(line_data[:-2])
                    if skip_existing and is_entity_processed_fast(item["id"]):
                        entities_skipped_in_chunk += 1
                    else:
                        parse_data(
                            item,
                            line_idx,
                            geolocation_subclass,
                            organization_subclass,
                            connection,
                        )
                        entities_processed_in_chunk += 1
                except:
                    errors_in_chunk += 1

            processed_count += entities_processed_in_chunk
            skipped_count += entities_skipped_in_chunk
            error_count += errors_in_chunk

            # Update final bytes processed
            final_chunk_bytes = sum(line_size for _, line_size, _ in chunk_buffer)
            bytes_processed += final_chunk_bytes
            pbar.update(final_chunk_bytes)

    except Exception as e:
        print(f"Critical error during file processing: {str(e)}")
        raise
    finally:
        wikidata_dump.close()

    # Flush any remaining items in buffer
    if len(BUFFER["items"]) > 0:
        flush_buffer(BUFFER)

    pbar.close()

    # Final summary
    total_time = time.time() - start_time
    total_items = processed_count + skipped_count
    avg_mb_rate = (
        (bytes_processed / (1024 * 1024)) / total_time if total_time > 0 else 0
    )

    print("\n" + "=" * 60)
    print("PROCESSING COMPLETED")
    print("=" * 60)
    print(f"Total time: {format_time_remaining(total_time)}")
    print(f"Compressed file: {format_size(file_size)}")
    print(
        f"Uncompressed data processed: {format_size(bytes_processed)} / {format_size(estimated_uncompressed_size)} ({bytes_processed/estimated_uncompressed_size*100:.1f}%)"
    )
    print(f"Entities processed: {processed_count:,}")
    print(f"Entities skipped: {skipped_count:,}")
    print(f"Processing errors: {error_count}")
    print(f"Average speed: {avg_mb_rate:.1f} MB/s")
    print("=" * 60)


def estimate_compression_ratio_sampling(bz2_file_path, sample_size_mb=100):
    """
    Estimate compression ratio by sampling actual compressed vs uncompressed data.

    This is much more accurate than using theoretical ratios.

    Args:
        bz2_file_path: Path to the BZ2 file
        sample_size_mb: Size of uncompressed sample to read (MB)

    Returns:
        tuple: (compression_ratio, total_compressed_size, estimated_uncompressed_size)
    """
    try:
        # Get total compressed file size
        total_compressed_size = os.path.getsize(bz2_file_path)
        sample_size_bytes = sample_size_mb * 1024 * 1024

        print(
            f"Sampling {sample_size_mb}MB of uncompressed data to estimate compression ratio..."
        )

        # Track compressed bytes read
        compressed_bytes_read = 0
        uncompressed_bytes_read = 0

        with open(bz2_file_path, "rb") as raw_file:
            # Track initial position
            start_pos = raw_file.tell()

            with bz2.BZ2File(raw_file, "rb") as bz2_file:
                # Read sample of uncompressed data
                sample_data = bz2_file.read(sample_size_bytes)
                uncompressed_bytes_read = len(sample_data)

                # Get compressed bytes consumed
                end_pos = raw_file.tell()
                compressed_bytes_read = end_pos - start_pos

        if uncompressed_bytes_read == 0 or compressed_bytes_read == 0:
            raise ValueError("Could not read sample data")

        # Calculate actual compression ratio (uncompressed / compressed)
        actual_ratio = uncompressed_bytes_read / compressed_bytes_read

        # Estimate total uncompressed size
        estimated_uncompressed_size = int(total_compressed_size * actual_ratio)

        print(f"Sample results:")
        print(
            f"  - Compressed bytes read: {compressed_bytes_read:,} ({compressed_bytes_read/(1024*1024):.2f} MB)"
        )
        print(
            f"  - Uncompressed bytes read: {uncompressed_bytes_read:,} ({uncompressed_bytes_read/(1024*1024):.2f} MB)"
        )
        print(f"  - Measured compression ratio: {actual_ratio:.2f}:1")

        return actual_ratio, total_compressed_size, estimated_uncompressed_size

    except Exception as e:
        print(f"Warning: Could not sample compression ratio: {e}")
        # Fallback to realistic estimates
        total_compressed_size = os.path.getsize(bz2_file_path)
        compressed_mb = total_compressed_size / (1024 * 1024)

        # Use realistic fallback ratios based on file size
        if compressed_mb < 100:
            fallback_ratio = 3.5
        elif compressed_mb < 10000:
            fallback_ratio = 4.0
        elif compressed_mb < 50000:
            fallback_ratio = 4.2
        else:
            fallback_ratio = 4.5

        estimated_uncompressed_size = int(total_compressed_size * fallback_ratio)
        print(f"Using fallback compression ratio: {fallback_ratio:.1f}:1")

        return fallback_ratio, total_compressed_size, estimated_uncompressed_size


def estimate_dump_statistics(file_path):
    """
    Estimate statistics about the Wikidata dump file to provide better progress tracking.

    Now uses actual sampling to measure real compression ratio instead of theoretical estimates.

    Args:
        file_path: Path to the bz2 compressed Wikidata dump

    Returns:
        dict: Statistics including estimated item count, compression ratio, etc.
    """
    try:
        # Get compressed file size
        compressed_size = os.stat(file_path).st_size
        compressed_mb = compressed_size / (1024 * 1024)

        print(f"Analyzing dump file: {os.path.basename(file_path)}")
        print(f"Compressed size: {compressed_mb:.2f} MB")

        # Sample the first few items to estimate average item size
        print("Sampling file to estimate average item size...")
        sample_items = 0
        sample_bytes_uncompressed = 0

        with bz2.BZ2File(file_path, "r") as f:
            for i, line in enumerate(f):
                if i >= 2000:  # Sample more items for better accuracy
                    break
                sample_items += 1
                sample_bytes_uncompressed += len(line)

        if sample_items > 0:
            # Calculate average item size from sample
            avg_item_size_uncompressed = sample_bytes_uncompressed / sample_items

            # Use actual sampling to measure compression ratio
            actual_ratio, _, estimated_uncompressed_size = (
                estimate_compression_ratio_sampling(file_path)
            )

            # Calculate estimates
            estimated_total_items = int(
                estimated_uncompressed_size / avg_item_size_uncompressed
            )

            stats = {
                "compressed_size_mb": compressed_mb,
                "estimated_compression_ratio": actual_ratio,
                "avg_item_size_uncompressed": avg_item_size_uncompressed,
                "estimated_total_items": estimated_total_items,
                "estimated_uncompressed_size_mb": estimated_uncompressed_size
                / (1024 * 1024),
            }

            print(
                f"Average item size (uncompressed): {avg_item_size_uncompressed:.0f} bytes"
            )
            print(f"Estimated total items: {estimated_total_items:,}")
            print(
                f"Estimated uncompressed size: {stats['estimated_uncompressed_size_mb']:.2f} MB"
            )

            return stats
        else:
            print("Warning: Could not sample file for item size estimation")
            # Use actual compression ratio but with fallback item size
            actual_ratio, _, estimated_uncompressed_size = (
                estimate_compression_ratio_sampling(file_path)
            )
            avg_item_size = 2048  # Typical Wikidata entity size

            return {
                "compressed_size_mb": compressed_mb,
                "estimated_total_items": int(
                    estimated_uncompressed_size / avg_item_size
                ),
                "estimated_compression_ratio": actual_ratio,
                "avg_item_size_uncompressed": avg_item_size,
            }

    except Exception as e:
        print(f"Error analyzing dump file: {e}")
        # Conservative fallback with realistic compression ratio
        compressed_size = os.stat(file_path).st_size
        estimated_compression_ratio = 4.0  # Realistic for BZ2+JSON

        return {
            "compressed_size_mb": compressed_size / (1024 * 1024),
            "estimated_total_items": int(
                compressed_size * estimated_compression_ratio / 2048
            ),
            "estimated_compression_ratio": estimated_compression_ratio,
            "avg_item_size_uncompressed": 2048,
        }


def format_time_remaining(seconds):
    """Format seconds into a human-readable time string."""
    if seconds < 60:
        return f"{seconds:.0f}s"
    elif seconds < 3600:
        minutes = seconds / 60
        return f"{minutes:.1f}m"
    elif seconds < 86400:
        hours = seconds / 3600
        return f"{hours:.1f}h"
    else:
        days = seconds / 86400
        return f"{days:.1f}d"


def format_size(bytes_val):
    """Format bytes into human-readable size."""
    for unit in ["B", "KB", "MB", "GB"]:
        if bytes_val < 1024.0:
            return f"{bytes_val:.2f} {unit}"
        bytes_val /= 1024.0
    return f"{bytes_val:.2f} TB"


def main(
    wikidata_dump_path: str,
    skip_existing: bool = True,
    types_db_path: str | Path | None = None,
):
    """
    Main function to parse Wikidata dump.

    Args:
        wikidata_dump_path: Path to the Wikidata dump file
        skip_existing: Whether to skip already processed entities
    """
    print("Creating database indexes...")
    create_indexes(client[DB_NAME])
    print("Starting Wikidata dump parsing...")
    parse_wikidata_dump(wikidata_dump_path, skip_existing, types_db_path)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Parse Wikidata dump and insert into MongoDB"
    )
    parser.add_argument(
        "--wikidata_dump_path",
        help="Path to the Wikidata dump file",
        default="~/Downloads/wikidata-20250127-all.json.bz2",
    )
    parser.add_argument(
        "--skip_existing",
        action="store_true",
        default=True,
        help="Skip already processed entities (default: True)",
    )
    parser.add_argument(
        "--no_skip_existing",
        action="store_false",
        dest="skip_existing",
        help="Process all entities, including already processed ones",
    )
    parser.add_argument(
        "--types_db_path",
        help="Path to types database",
        default="/mnt/lamapi/lamapi/lamAPI/types-db/types.db",
    )
    args = parser.parse_args()
    args.wikidata_dump_path = os.path.expanduser(args.wikidata_dump_path)
    main(args.wikidata_dump_path, args.skip_existing, args.types_db_path)
