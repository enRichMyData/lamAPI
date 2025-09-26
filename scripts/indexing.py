import dotenv

dotenv.load_dotenv(override=True)

import argparse
import json
import os
import re
import sys
import time
import traceback
from multiprocessing import Pool, cpu_count

from elasticsearch import Elasticsearch
from elasticsearch.helpers import BulkIndexError, bulk
from pymongo import MongoClient
from tqdm import tqdm


def index_documents(es_host, es_port, buffer, max_retries=5, localhost=False):
    if localhost:
        es_host = "localhost"
    es = Elasticsearch(
        hosts=f"http://{es_host}:{es_port}",
        request_timeout=60,
        max_retries=10,
        retry_on_timeout=True,
    )
    for attempt in range(max_retries):
        try:
            bulk(es, buffer)
            break  # Exit the loop if the bulk operation was successful
        except BulkIndexError as e:
            print(f"Bulk indexing error on attempt {attempt + 1}: {e.errors}")
            for error_detail in e.errors:
                action, error_info = list(error_detail.items())[0]
                print(f"Failed action: {action}")
                print(f"Error details: {error_info}")
            time.sleep(5)
        except Exception as e:
            print(
                f"An unexpected error occurred during indexing on attempt {attempt + 1}: {str(e)}"
            )
            traceback.print_exc()
            time.sleep(5)
    else:
        print("Max retries exceeded. Failed to index some documents.")


def generate_dot_notation_options(name):
    words = name.split()
    num_words = len(words)
    options = []

    for i in range(num_words):
        abbreviated_parts = []
        for j in range(num_words - 1):
            if j < i:
                abbreviated_parts.append(words[j][0] + ".")
            else:
                abbreviated_parts.append(words[j])

        option = " ".join(abbreviated_parts + [words[-1]])
        options.append(option)

    return options


def create_elasticsearch_client(endpoint, port, localhost: bool = False):
    if localhost:
        endpoint = "localhost"
    return Elasticsearch(
        hosts=f"http://{endpoint}:{port}",
        request_timeout=60,
        max_retries=10,
        retry_on_timeout=True,
    )


def create_mongo_client(endpoint, port, localhost: bool = False):
    if localhost:
        endpoint = "localhost"
    MONGO_ENDPOINT_USERNAME = os.environ["MONGO_INITDB_ROOT_USERNAME"]
    MONGO_ENDPOINT_PASSWORD = os.environ["MONGO_INITDB_ROOT_PASSWORD"]

    return MongoClient(
        endpoint,
        int(port),
        username=MONGO_ENDPOINT_USERNAME,
        password=MONGO_ENDPOINT_PASSWORD,
    )


def create_argument_parser():
    """Create and configure the argument parser."""
    parser = argparse.ArgumentParser(
        description="Elasticsearch indexing tool for MongoDB collections",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
            Examples:
            python indexing.py index mydb mycoll mapping.json
            python indexing.py index mydb mycoll mapping.json --batch-size 8192 --max-threads 4
            python indexing.py status
            python indexing.py list-databases
            python indexing.py list-collections mydb
        """,
    )

    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # Index command
    index_parser = subparsers.add_parser("index", help="Index MongoDB collection to Elasticsearch")
    index_parser.add_argument("db_name", help="MongoDB database name", default="wikidata17012025")
    index_parser.add_argument("collection_name", help="MongoDB collection name", default="items")
    index_parser.add_argument(
        "mapping_file",
        help="Path to Elasticsearch mapping JSON file",
        default="scripts/index_confs/kg_schema.json",
    )
    index_parser.add_argument(
        "--batch-size", type=int, default=16384, help="Batch size for indexing (default: 16384)"
    )
    index_parser.add_argument(
        "--max-threads",
        type=int,
        default=None,
        help="Maximum number of threads (default: CPU count - 1)",
    )
    index_parser.add_argument(
        "--localhost", action="store_true", help="Force use localhost for MongoDB connection"
    )

    # Status command
    subparsers.add_parser("status", help="Show MongoDB and Elasticsearch status")

    # List databases command
    subparsers.add_parser("list-databases", help="List available MongoDB databases")

    # List collections command
    collections_parser = subparsers.add_parser(
        "list-collections", help="List collections in a MongoDB database"
    )
    collections_parser.add_argument("db_name", help="MongoDB database name")

    return parser


def process_batch(args):
    es_host, es_port, batch, use_localhost = args
    index_documents(es_host, es_port, batch, localhost=use_localhost)


def index_data(
    es_host,
    es_port,
    mongo_client,
    db_name,
    collection_name,
    mapping,
    batch_size=16384,
    max_threads=None,
    use_localhost=False,
):
    if max_threads is None:
        max_threads = cpu_count() - 1

    documents_c = mongo_client[db_name][collection_name]

    documents_c.create_index([("popularity", -1)])
    max_popularity_doc = documents_c.find_one(
        filter={},
        sort=[("popularity", -1)],
        projection={"popularity": 1, "_id": 0},
    )
    if max_popularity_doc:
        max_popularity = max_popularity_doc["popularity"]
        print(f"The maximum popularity is: {max_popularity}")
    else:
        raise Exception("No documents found in the collection or popularity field is missing.")

    index_name = re.sub(r"\d+$", "", db_name)
    es_client = create_elasticsearch_client(es_host, es_port, localhost=use_localhost)

    # Create index if it doesn't exist
    if not es_client.indices.exists(index=index_name):
        print(f"Creating index {index_name}...")
        es_client.indices.create(
            index=index_name, settings=mapping["settings"], mappings=mapping["mappings"]
        )
    else:
        print(f"Index {index_name} already exists. Will check for existing documents.")

    # Get all existing entity IDs in the index
    print("Retrieving existing entity IDs from Elasticsearch...")
    existing_ids = set()
    try:
        # Use scan to get all existing IDs efficiently
        from elasticsearch.helpers import scan

        scan_results = scan(
            es_client,
            query={"query": {"match_all": {}}, "_source": ["id"]},
            index=index_name,
            size=1024,
        )
        for doc in tqdm(scan_results):
            entity_id = doc["_source"].get("id")
            if entity_id:
                existing_ids.add(entity_id)
        print(f"Found {len(existing_ids)} existing entities in index.")

    except Exception as e:
        print(f"Warning: Could not retrieve existing IDs: {e}")
        existing_ids = set()  # Continue with empty set if retrieval fails

    # Disable refresh interval and replicas temporarily
    es_client.indices.put_settings(
        index=index_name,
        settings={"index": {"refresh_interval": "-1", "number_of_replicas": 0}},
    )

    total_docs = documents_c.estimated_document_count()
    results = documents_c.find({})

    buffer = []
    batches = []
    _id = 0
    skipped_count = 0
    processed_count = 0
    pbar = tqdm(total=total_docs, desc="Indexing documents")

    for item in results:
        try:
            # Handle both "entity" and "id_entity" fields for compatibility
            id_entity = item.get("entity") or item.get("id_entity")

            # Skip if entity already exists in Elasticsearch
            if str(id_entity) in existing_ids:
                skipped_count += 1
                pbar.update(1)
                continue

            processed_count += 1
            labels = item.get("labels", {})
            aliases = item.get("aliases", {})

            # Handle description - could be string or dict with "value" key
            desc = item.get("description")
            if isinstance(desc, dict):
                description = desc.get("value")
            else:
                description = desc

            # Handle NERtype - check both "NERtype" and "ner_types"
            NERtype = item.get("NERtype")
            if NERtype is None:
                ner_types = item.get("ner_types")
                if isinstance(ner_types, list) and ner_types:
                    NERtype = ner_types[0]

            explicit_types = item.get("explicit_types", None)
            extended_types = item.get("extended_types", None)
            types = item.get("types", {}).get("P31", [])
            types.extend(item.get("types", {}).get("P279", []))
            kind = item.get("kind", None)
            popularity = int(item.get("popularity", 0))

            if max_popularity > 0:
                popularity_norm = round(popularity / max_popularity, 2)
            else:
                popularity_norm = 0.0
            unique_labels = {}

            for lang, name in labels.items():
                if not isinstance(name, str):
                    continue  # Skip non-string labels
                key = name.lower()
                if key not in unique_labels:
                    unique_labels[key] = {"name": name, "languages": [], "is_alias": False}
                unique_labels[key]["languages"].append(lang)

            for lang, alias_list in aliases.items():
                if not isinstance(alias_list, list):
                    continue  # Skip non-list aliases
                for alias in alias_list:
                    if not isinstance(alias, str):
                        continue  # Skip non-string aliases
                    key = alias.lower()
                    if (
                        key in unique_labels and not unique_labels[key]["is_alias"]
                    ):  # Skip if the alias is already a label
                        continue
                    if key not in unique_labels:
                        unique_labels[key] = {
                            "name": alias,
                            "languages": [],
                            "is_alias": True,
                        }
                    unique_labels[key]["languages"].append(lang)

            all_names = []
            for _, value in unique_labels.items():
                name = value["name"]
                languages = value["languages"]
                is_alias = value["is_alias"]
                all_names.append({"name": name, "language": languages, "is_alias": is_alias})

            if NERtype == "PERS":
                name = labels.get("en")
                if name is not None:
                    name_abbreviations = generate_dot_notation_options(name)
                    for abbrev in name_abbreviations:
                        all_names.append({"name": abbrev, "language": ["en"], "is_alias": True})

            for name_entry in all_names:
                name = name_entry["name"]
                language = name_entry["language"]
                is_alias = name_entry["is_alias"]
                doc = {
                    "_op_type": "index",
                    "_index": index_name,
                    "_id": _id,
                    "id": str(id_entity) if id_entity is not None else None,  # Ensure string type
                    "name": name,
                    "language": language,
                    "is_alias": is_alias,
                    "description": description,
                    "kind": kind,
                    "NERtype": NERtype,
                    "explicit_types": [t for t in explicit_types if t is not None],
                    "extended_types": [t for t in extended_types if t is not None],
                    "types": " ".join([t for t in types if t is not None]),
                    "length": len(name),
                    "ntoken": len(name.split(" ")),
                    "popularity": popularity_norm,  # Use the normalized value
                }
                _id += 1
                buffer.append(doc)

                if len(buffer) >= batch_size:
                    batches.append(buffer)
                    buffer = []

                    if len(batches) >= max_threads:
                        with Pool(max_threads) as pool:
                            pool.map(
                                process_batch,
                                [(es_host, es_port, batch, use_localhost) for batch in batches],
                            )
                        batches = []

            pbar.update(1)
        except Exception:
            print("An error occurred while processing documents")
            print(item)
            traceback.print_exc()

    if len(buffer) > 0:
        batches.append(buffer)

    if len(batches) > 0:
        with Pool(max_threads) as pool:
            pool.map(
                process_batch, [(es_host, es_port, batch, use_localhost) for batch in batches]
            )

    pbar.close()
    print(
        f"Processing complete: {processed_count} new documents processed, "
        f"{skipped_count} existing documents skipped."
    )

    # Enable refresh interval
    es_client.indices.put_settings(
        index=index_name, settings={"index": {"refresh_interval": "1s"}}
    )


def show_status(mongo_client, es):
    print("MongoDB Status:")
    print(mongo_client.server_info())
    print("\nElasticsearch Status:")
    print(es.info())


def list_databases(mongo_client):
    print("Available Databases:")
    for db in mongo_client.list_database_names():
        print(f"  - {db}")


def list_collections(mongo_client, db_name):
    if db_name in mongo_client.list_database_names():
        print(f"Collections in database '{db_name}':")
        for coll in mongo_client[db_name].list_collection_names():
            print(f"  - {coll}")
    else:
        print(f"Database '{db_name}' not found.")


def main():
    parser = create_argument_parser()
    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    # Get environment variables
    try:
        print(os.environ["ELASTIC_ENDPOINT"])
        ELASTIC_ENDPOINT, ELASTIC_PORT = os.environ["ELASTIC_ENDPOINT"].split(":")
        MONGO_ENDPOINT, MONGO_ENDPOINT_PORT = os.environ["MONGO_ENDPOINT"].split(":")
    except KeyError as e:
        print(f"Error: Environment variable {e} is not set.")
        sys.exit(1)
    except ValueError:
        print(
            "Error: ELASTIC_ENDPOINT or MONGO_ENDPOINT format is incorrect. Expected format: 'host:port'"
        )
        sys.exit(1)

    # Create clients
    use_localhost = getattr(args, "localhost", True)
    es = create_elasticsearch_client(
        ELASTIC_ENDPOINT,
        ELASTIC_PORT,
        localhost=use_localhost,
    )
    mongo_client = create_mongo_client(
        MONGO_ENDPOINT,
        MONGO_ENDPOINT_PORT,
        localhost=use_localhost,
    )

    try:
        if args.command == "index":
            # Validate mapping file exists
            if not os.path.exists(args.mapping_file):
                print(f"Error: Mapping file '{args.mapping_file}' not found.")
                sys.exit(1)

            try:
                with open(args.mapping_file, "r") as file:
                    mapping = json.load(file)
            except json.JSONDecodeError as e:
                print(f"Error: Invalid JSON in mapping file '{args.mapping_file}': {e}")
                sys.exit(1)

            print("Starting indexing process...")
            print(f"  Database: {args.db_name}")
            print(f"  Collection: {args.collection_name}")
            print(f"  Mapping file: {args.mapping_file}")
            print(f"  Batch size: {args.batch_size}")
            print(f"  Max threads: {args.max_threads or 'CPU count - 1'}")
            print(f"  MongoDB localhost: {use_localhost}")

            # Perform indexing
            index_data(
                ELASTIC_ENDPOINT,
                ELASTIC_PORT,
                mongo_client,
                args.db_name,
                args.collection_name,
                mapping,
                batch_size=args.batch_size,
                max_threads=args.max_threads,
                use_localhost=use_localhost,
            )

            print("All Finished")

        elif args.command == "status":
            show_status(mongo_client, es)

        elif args.command == "list-databases":
            list_databases(mongo_client)

        elif args.command == "list-collections":
            list_collections(mongo_client, args.db_name)

    except KeyboardInterrupt:
        print("\nOperation interrupted by user.")
        sys.exit(1)
    except Exception as e:
        print(f"Error: {e}")
        traceback.print_exc()
        print("An error occurred. Exiting...")
        sys.exit(1)


if __name__ == "__main__":
    main()
