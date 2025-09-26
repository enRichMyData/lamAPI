import os
from time import sleep

from elasticsearch import ConnectionError, Elasticsearch

# Extract environment variables
ELASTIC_ENDPOINT, ELASTIC_PORT = os.environ["ELASTIC_ENDPOINT"].split(":")
ELASTIC_PORT = int(ELASTIC_PORT)


class Elastic:
    def __init__(self, timeout=120):
        self._timeout = timeout
        self._elastic = self.connect_to_elasticsearch()

    def connect_to_elasticsearch(self, max_retry=5, delay=10):
        retry = 0
        while retry < max_retry:
            try:
                hosts = [
                    {
                        "host": ELASTIC_ENDPOINT,
                        "port": ELASTIC_PORT,
                        "scheme": "http",
                    }
                ]
                es = Elasticsearch(
                    hosts=hosts,
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

    def search(self, body, kg="wikidata", limit=128):
        if body is None:
            raise ValueError("Search body cannot be None")

        try:
            request_kwargs = dict(body)
            request_kwargs.setdefault("_source", {"excludes": ["language"]})

            # Honour explicit size in the request otherwise fallback to provided limit
            size = request_kwargs.pop("size", None)
            if size is not None:
                request_kwargs["size"] = size
            elif limit is not None:
                request_kwargs["size"] = limit

            query = request_kwargs.pop("query", None)
            if query is None:
                raise ValueError("Search body must include a 'query' section")

            _source = request_kwargs.pop("_source", None)

            sort_clause = request_kwargs.get("sort")
            if sort_clause is not None and request_kwargs.get("track_scores") is None:
                request_kwargs["track_scores"] = True

            search_params = {"index": kg, "query": query, **request_kwargs}
            if isinstance(_source, dict):
                includes = _source.get("includes")
                excludes = _source.get("excludes")
                if includes is not None:
                    search_params["_source_includes"] = includes
                if excludes is not None:
                    search_params["_source_excludes"] = excludes
            elif _source is not None:
                search_params["_source"] = _source

            query_result = self._elastic.search(
                **search_params,
                request_timeout=self._timeout,
            )
            hits_info = query_result.get("hits", {})
            hits = hits_info.get("hits", [])
            max_score = hits_info.get("max_score")
            if max_score in (None, 0):
                max_score = None

            if len(hits) == 0:
                return []

            new_hits = []

            for i, hit in enumerate(hits):
                source = hit.get("_source", {})
                raw_score = hit.get("_score")
                if raw_score is None:
                    es_score = 0
                elif max_score:
                    es_score = round(raw_score / max_score, 3)
                else:
                    es_score = round(raw_score, 3)

                new_hit = {
                    "id": source.get("id"),
                    "name": source.get("name"),
                    "description": source.get("description"),
                    "types": source.get("types", ""),
                    "popularity": source.get("popularity"),
                    "pos_score": round((i + 1) / len(hits), 3),
                    "es_score": es_score,
                    "ntoken_entity": source.get("ntoken"),
                    "length_entity": source.get("length"),
                }
                if "kind" in source:
                    new_hit["kind"] = source.get("kind")
                    new_hit["NERtype"] = source.get("NERtype")
                if "explicit_types" in source:
                    new_hit["explicit_types"] = source.get("explicit_types")
                if "extended_types" in source:
                    new_hit["extended_types"] = source.get("extended_types")
                new_hits.append(new_hit)

            new_hits.sort(key=lambda item: item.get("es_score", 0), reverse=True)
            return new_hits
        except ConnectionError as e:
            print(f"Search connection error: {e}", flush=True)
            return []
