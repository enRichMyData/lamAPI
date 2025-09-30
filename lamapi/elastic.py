import os
from time import sleep

from elasticsearch import ConnectionError, Elasticsearch

from lamapi.utils import _runtime_mode

_LOCAL_ADDRS = {"localhost", "127.0.0.1", "0.0.0.0"}


def _resolve_elastic_host_port():
    raw_endpoint = os.environ.get("ELASTIC_ENDPOINT", "localhost:9200")
    if ":" in raw_endpoint:
        host, port = raw_endpoint.split(":", 1)
    else:
        host = raw_endpoint
        port = os.environ.get("ELASTIC_PORT", "9200")

    runtime = _runtime_mode()
    host = host.strip()
    host_lower = host.lower()

    if runtime == "docker":
        if host_lower in _LOCAL_ADDRS:
            host = os.environ.get("ELASTIC_SERVICE_HOST", "es01")
    elif runtime == "local":
        host = os.environ.get("LOCAL_ELASTIC_SERVICE_HOST", "localhost")

    return host, int(port)


class Elastic:
    def __init__(self, timeout=120, max_popularity: int | None = None):
        self._timeout = timeout
        self.max_popularity = max_popularity
        self._host, self._port = _resolve_elastic_host_port()
        self._elastic = self.connect_to_elasticsearch()

    def connect_to_elasticsearch(self, max_retry=5, delay=10):
        retry = 0
        while retry < max_retry:
            try:
                host_url = f"http://{self._host}:{self._port}"
                es = Elasticsearch(
                    hosts=[host_url],
                    request_timeout=self._timeout,
                    retry_on_timeout=True,
                    max_retries=5,
                    connections_per_node=20,
                    http_compress=True,
                )
                if es.ping():
                    print("Connected to Elasticsearch")
                    return es
                else:
                    print("Unable to ping Elasticsearch")
            except ConnectionError as e:
                print(f"Connection error: {e}", flush=True)
            print(f"Retrying in {delay} seconds...")
            retry += 1
            sleep(delay)
        raise Exception("Failed to connect to Elasticsearch after multiple attempts")

    def search(self, body, kg="wikidata", limit=1000, normalize_score=True):
        try:
            if "_source" not in body:
                body["_source"] = {"excludes": ["language"]}

            query_result = self._elastic.search(
                index=kg,
                query=body["query"],
                source_excludes=body["_source"]["excludes"],
                size=limit,
            )
            hits = query_result["hits"]["hits"]
            max_score = query_result["hits"]["max_score"]

            if len(hits) == 0:
                return []

            new_hits = []

            for i, hit in enumerate(hits):
                new_hit = {
                    "id": hit["_source"]["id"],
                    "name": hit["_source"]["name"],
                    "description": hit["_source"]["description"],
                    "types": hit["_source"]["types"],
                    "popularity": (
                        hit["_source"]["popularity"]
                        if normalize_score
                        else (
                            hit["_source"]["popularity"] * self.max_popularity
                            if self.max_popularity is not None
                            else hit["_source"]["popularity"]
                        )
                    ),
                    "pos_score": (i + 1) / len(hits),
                    "es_score": hit["_score"] / max_score if normalize_score else hit["_score"],
                    "ntoken_entity": hit["_source"]["ntoken"],
                    "length_entity": hit["_source"]["length"],
                }
                if "kind" in hit["_source"]:
                    new_hit["kind"] = hit["_source"]["kind"]
                    new_hit["NERtype"] = hit["_source"]["NERtype"]
                new_hits.append(new_hit)
            return new_hits
        except ConnectionError as e:
            print(f"Search connection error: {e}", flush=True)
            return []
