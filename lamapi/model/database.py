import os
import time
from datetime import datetime

from pymongo import MongoClient
from pymongo.errors import PyMongoError, ServerSelectionTimeoutError


def _resolve_mongo_host_port():
    raw_endpoint = os.environ.get("MONGO_ENDPOINT", "mongo:27017")
    if ":" in raw_endpoint:
        host, port = raw_endpoint.split(":", 1)
    else:
        host = raw_endpoint
        port = os.environ.get("MONGO_PORT", "27017")

    host = host.strip()
    if host in {"localhost", "127.0.0.1", "0.0.0.0"}:
        host = os.environ.get("MONGO_SERVICE_HOST", "mongo")

    port = int(port)
    return host, port


MONGO_HOST, MONGO_PORT = _resolve_mongo_host_port()
MONGO_ENDPOINT_USERNAME = os.environ["MONGO_INITDB_ROOT_USERNAME"]
MONGO_ENDPOINT_PASSWORD = os.environ["MONGO_INITDB_ROOT_PASSWORD"]
SUPPORTED_KGS = os.environ["SUPPORTED_KGS"].split(",")
MONGO_MAX_RETRIES = int(os.environ.get("MONGO_MAX_RETRIES", "6"))
MONGO_RETRY_DELAY = int(os.environ.get("MONGO_RETRY_DELAY", "5"))
MONGO_SERVER_TIMEOUT_MS = int(os.environ.get("MONGO_SERVER_TIMEOUT_MS", "5000"))


class Database:
    def __init__(self):
        self.mongo = self._connect_with_retry()
        self.mappings = {kg.lower(): None for kg in SUPPORTED_KGS}
        self.update_mappings()
        self.create_indexes()

    def _connect_with_retry(self):
        attempt = 0
        last_exception = None
        while attempt < MONGO_MAX_RETRIES:
            try:
                client = MongoClient(
                    MONGO_HOST,
                    MONGO_PORT,
                    username=MONGO_ENDPOINT_USERNAME,
                    password=MONGO_ENDPOINT_PASSWORD,
                    serverSelectionTimeoutMS=MONGO_SERVER_TIMEOUT_MS,
                )
                # Trigger a lightweight command to ensure connectivity
                client.admin.command("ping")
                return client
            except (ServerSelectionTimeoutError, PyMongoError) as exc:
                last_exception = exc
                attempt += 1
                print(
                    f"Mongo connection attempt {attempt} failed: {exc}. "
                    f"Retrying in {MONGO_RETRY_DELAY}s...",
                    flush=True,
                )
                time.sleep(MONGO_RETRY_DELAY)
        raise last_exception

    def update_mappings(self):
        history = {}
        try:
            database_names = self.mongo.list_database_names()
        except (ServerSelectionTimeoutError, PyMongoError) as exc:
            print(f"Unable to list Mongo databases: {exc}", flush=True)
            return

        for db in database_names:
            # Handle real databases
            doc = self.mongo[db]["metadata"].find_one()
            if doc is not None and doc.get("status") == "DOING":
                continue
            kg_name = "".join(filter(str.isalpha, db))
            date = "".join(filter(str.isdigit, db))
            if kg_name in self.mappings:
                parsed_date = datetime.now()
                if date != "":
                    parsed_date = datetime.strptime(date, "%d%m%Y")
                if kg_name not in history:
                    history[kg_name] = parsed_date
                    self.mappings[kg_name] = db
                elif parsed_date > history[kg_name]:
                    history[kg_name] = parsed_date
                    self.mappings[kg_name] = db

    def create_indexes(self):
        print("Ensuring Mongo indexes...", flush=True)
        # Specify the collections and their respective fields to be indexed
        index_specs = {
            "cache": [
                "name",
                "lastAccessed",
                "limit",
            ],
            "items": ["id_entity", "entity", "category", "popularity"],
            "literals": ["id_entity", "entity"],
            "objects": ["id_entity", "entity"],
            "types": ["id_entity", "entity"],
            "bow": ["id"],
        }
        for db_name in self.mappings.values():
            if db_name is None:
                continue
            db = self.mongo[db_name]
            for collection, fields in index_specs.items():
                collection_handler = db[collection]
                existing_indexes = list(collection_handler.list_indexes())
                existing_keys = {
                    tuple(index_info["key"].items()): index_info for index_info in existing_indexes
                }

                if collection == "cache":
                    index_key = [
                        ("name", 1),
                        ("limit", 1),
                        ("kg", 1),
                        ("fuzzy", 1),
                        ("types", 1),
                        ("kind", 1),
                        ("NERtype", 1),
                        ("language", 1),
                    ]
                    index_name = "cache_query_key"
                    key_tuple = tuple(index_key)
                    existing_index = existing_keys.get(key_tuple)
                    if existing_index is None:
                        collection_handler.create_index(
                            index_key,
                            name=index_name,
                            unique=True,
                            background=True,
                        )
                    elif existing_index.get("unique") is not True:
                        print(
                            f"Warning: cache index {existing_index['name']} is not unique; "
                            "skipping recreation.",
                            flush=True,
                        )
                elif collection == "items":
                    index_key = [("entity", 1), ("kind", 1)]
                    index_name = "items_entity_kind"
                    key_tuple = tuple(index_key)
                    existing_index = existing_keys.get(key_tuple)
                    if existing_index is None:
                        collection_handler.create_index(
                            index_key,
                            name=index_name,
                            unique=True,
                            background=True,
                        )
                    elif existing_index.get("unique") is not True:
                        print(
                            f"Warning: items index {existing_index['name']} is not unique; "
                            "skipping recreation.",
                            flush=True,
                        )
                elif collection == "bow":
                    index_key = [("text", 1), ("id", 1)]
                    index_name = "bow_text_id"
                    key_tuple = tuple(index_key)
                    existing_index = existing_keys.get(key_tuple)
                    if existing_index is None:
                        collection_handler.create_index(
                            index_key,
                            name=index_name,
                            unique=True,
                            background=True,
                        )
                    elif existing_index.get("unique") is not True:
                        print(
                            f"Warning: bow index {existing_index['name']} is not unique; "
                            "skipping recreation.",
                            flush=True,
                        )
                for field in fields:
                    index_name = f"{collection}_{field}_idx"
                    key_tuple = ((field, 1),)
                    if key_tuple in existing_keys:
                        continue
                    collection_handler.create_index([(field, 1)], name=index_name, background=True)
        print("Mongo indexes ensured.", flush=True)

    def get_supported_kgs(self):
        return self.mappings

    def get_url_kgs(self):  # hard-coded for now
        return {
            "wikidata": "https://www.wikidata.org/wiki/",
            "crunchbase": "https://www.crunchbase.com/organization/",
        }

    def get_requested_collection(self, collection, kg="wikidata"):
        self.update_mappings()
        print(f"KG: {kg}", collection, self.mappings, flush=True)
        if kg in self.mappings and self.mappings[kg] is not None:
            return self.mongo[self.mappings[kg]][collection]
        else:
            raise ValueError(f"KG {kg} is not supported.")
