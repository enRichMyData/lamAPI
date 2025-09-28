# LamAPI

LamAPI provides semantic access to Wikidata: you can index dumps into MongoDB, compute rich type hierarchies, and expose everything through a REST API or directly from Python. This document covers how to run the service, use the CLI tooling, and build the search index.

---

## Architecture at a Glance

- **Python library (`lamapi/`)** – core retrieval logic (types, literals, objects, summaries, etc.).
- **REST API (`lamapi/server.py`)** – Flask application wrapping the library and serving JSON responses.
- **Data pipeline scripts (`scripts/`)** – tooling for extracting edges, building SQLite closures, parsing Wikidata dumps, and materialising Elasticsearch/MongoDB indices.
- **Docker assets** – `docker-compose*.yml` files wire MongoDB, Elasticsearch, and the LamAPI service for local development or production.

---

## Quick Start

### Prerequisites

- Docker + Docker Compose (recommended for API usage).
- Python 3.9+ with `pip` if you plan to run scripts locally.
- Sufficient disk space (> 300 GB recommended) for dumps, SQLite DB, and MongoDB.

### Option A – Run the REST API via Docker Compose

- API base URL: `http://localhost:5000` (Swagger UI available at root).
- MongoDB exposed on `localhost:27017`, Elasticsearch on `localhost:9200`.
- Environment variables can be customised through `.env` or compose overrides.

### Option B – Run the REST API locally

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -e .[dev]
python lamapi/server.py
```

Set the required environment variables first:

```bash
# Cluster Configuration
CLUSTER_NAME=lamapi
LICENSE=basic
STACK_VERSION=8.8.1

# Elasticsearch Configuration
ELASTICSEARCH_USERNAME=lamapi
ELASTIC_PASSWORD=<elastic_username>
ELASTIC_ENDPOINT=es01:9200
ELASTIC_FINGERPRINT=
ELASTIC_PORT=9200

# Kibana Configuration
KIBANA_PASSWORD=<kibana_password>
KIBANA_PORT=5601

# MongoDB Configuration
MONGO_ENDPOINT=mongo:27017
MONGO_INITDB_ROOT_USERNAME=<mongo_username>
MONGO_INITDB_ROOT_PASSWORD=<mongo_password>
MONGO_PORT=27017
MONGO_VERSION=6.0


# Other Configuration
THREADS=8
PYTHON_VERSION=3.11
LAMAPI_TOKEN=<your_token>
LAMAPI_PORT=5000
SUPPORTED_KGS=WIKIDATA
MEM_LIMIT=2G

# Connection Strategy
# - Set `LAMAPI_RUNTIME=docker` inside containers (Dockerfile already does this).
# - Leave it unset or `auto` when running locally: LamAPI will rewrite known service
#   hosts such as `mongo` or `es01` to `localhost` so CLI commands work out of the box.
# - Override `LAMAPI_LOCAL_MONGO_HOST` / `LAMAPI_LOCAL_ELASTIC_HOST` if your databases
#   live on a different machine.
```

---

## Using LamAPI as a Library

Import the package when you need programmatic access to retrievers:

```python
from lamapi import Database, TypesRetriever

db = Database()
retriever = TypesRetriever(db)
types = retriever.get_types_output(["Q30"], kg="wikidata")
```

Scripts in `scripts/` provide CLI utilities for data preparation and indexing.

---

## REST API Overview

LamAPI ships with multiple namespaces (Swagger UI shows schemas and payloads):

| Namespace | Endpoint | Method | Description |
|-----------|----------|--------|-------------|
| `info`    | `/info/kgs` | GET | List supported knowledge graphs. |
| `entity`  | `/entity/types` | POST | Retrieve explicit + extended types for entities. |
| `entity`  | `/entity/objects` | POST | Fetch object neighbours for entities. |
| `entity`  | `/entity/literals` | POST | Fetch literal attributes. |
| `entity`  | `/entity/predicates` | POST | Retrieve predicates and relations. |
| `lookup`  | `/lookup/search` | GET | Free-text entity lookup. |
| `lookup`  | `/lookup/sameas` | POST | Same-as entity discovery. |
| `sti`     | `/sti/column-analysis` | POST | Semantic table interpretation helpers. |
| `sti`     | `/sti/ner` | POST | Named-entity recognition utilities. |
| `classify`| `/classify/literals` | POST | Literal classifier outputs. |
| `summary` | `/summary/statistics` | GET | Dataset-level statistics. |

Confirm contracts via Swagger at `http://localhost:5000` once the API is running.

---

## Building the Knowledge Graph Index

Transform an official Wikidata dump into the artefacts LamAPI expects. Run the steps from the repository root (`emd-lamapi/`).

1. **Download the Wikidata JSON dump**
   ```bash
   mkdir -p data/wikidata
   curl -o data/wikidata/latest-all.json.bz2 \
     https://dumps.wikimedia.org/wikidatawiki/entities/latest-all.json.bz2
   ```

2. **Extract type hierarchy edges** using `scripts/extract_type_hierarchy.py` to produce `instance_of.tsv` (P31) and `subclass_of.tsv` (P279).
   ```bash
   python3 scripts/extract_type_hierarchy.py \
     --input data/wikidata/latest-all.json.bz2 \
     --output-instance data/wikidata/instance_of.tsv \
     --output-subclass data/wikidata/subclass_of.tsv

   # Stream with pbzip2 for better throughput
   pbzip2 -dc data/wikidata/latest-all.json.bz2 | \
     python3 scripts/extract_type_hierarchy.py --stdin-json \
     --output-instance data/wikidata/instance_of.tsv \
     --output-subclass data/wikidata/subclass_of.tsv
   ```

3. **Compute the type transitive closure** with `scripts/infer_types.py`, creating `types.db`.
   ```bash
   python3 scripts/infer_types.py \
     --instance-of data/wikidata/instance_of.tsv \
     --subclass-of data/wikidata/subclass_of.tsv \
     --output-db data/wikidata/types.db
   ```
   Add `--no-closure` to skip materialising the closure if you only need raw edges.

4. **Parse the Wikidata dump into MongoDB** using the parallel ingestion script.
   ```bash
   # Stream with pbzip2 for better throughput
   pbzip2 -dc data/wikidata/latest-all.json.bz2 | python3 scripts/parse_wikidata_dump_parallel.py \
     --input data/wikidata/latest-all.json.bz2 \
     --types-db-path types.db \
     --threads 16
   ```
   Inspect `python3 scripts/parse_wikidata_dump_parallel.py --help` for batching and worker options.

5. **Create Elasticsearch/MongoDB indices** using `scripts/indexing.py` with a configuration under `scripts/index_confs/`.
   ```bash
   python3 ./scripts/indexing.py index \
    --db_name mydb \
    --collection_name mycoll \
    --mapping_file ./scripts/mapping.json \
    --batch-size 8192 \
    --max-threads 4
   ```
   Tweak the JSON config to control retrievers, filters, and indexed fields.

After these steps LamAPI can answer type, lookup, literal, and object queries against the populated databases.

---

## Useful Scripts & Utilities

| Script | Purpose |
|--------|---------|
| `scripts/extract_type_hierarchy.py` | Multi-threaded extractor for P31/P279 edges; supports streaming input. |
| `scripts/infer_types.py` | Builds a SQLite DB with transitive closure to accelerate type lookups. |
| `scripts/parse_wikidata_dump_parallel.py` | Ingests Wikidata entities into MongoDB using a threaded pipeline. |
| `scripts/indexing.py` | Materialises search indices based on JSON configs. |
| `scripts/experiments.py`, `scripts/summary.py`, etc. | Additional analytics helpers and evaluations. |

Run `python3 <script> --help` for the complete argument list.

---

## Troubleshooting & Tips

- Prefer streaming the dump through `pbzip2` to keep CPUs saturated while avoiding disk bottlenecks.
- `extract_type_hierarchy.py` and `infer_types.py` deduplicate edges with `INSERT OR IGNORE`, so re-running them is safe.
- Store TSVs and SQLite DBs inside a dedicated `data/` directory; they can reach tens of GB.
- Never commit `.env` files or credentials—`.gitignore` already excludes them.
- Swagger UI at `http://localhost:5000/docs` is the quickest way to try endpoints once the stack is live.

---

Feel free to explore the code and examples in this repository for a deeper understanding of how the entity types are defined, extended, and mapped to NER categories.
