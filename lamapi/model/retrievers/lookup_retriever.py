import datetime
import json

from lamapi.model.elastic import Elastic
from lamapi.model.utils import clean_str, compute_similarity_between_string, editdistance


class LookupRetriever:
    def __init__(self, database):
        self.database = database
        self.elastic_retriever = Elastic()

    def search(
        self,
        name,
        limit=1000,
        kg="wikidata",
        fuzzy=False,
        types=None,
        kind=None,
        ner_type=None,
        extended_types=None,
        language=None,
        ids=None,
        query=None,
        cache=True,
        soft_filtering=False,
    ):
        self.candidate_cache_collection = self.database.get_requested_collection("cache", kg=kg)
        cleaned_name = clean_str(name)
        query_result = self._exec_query(
            cleaned_name,
            limit=limit,
            kg=kg,
            fuzzy=fuzzy,
            types=types,
            kind=kind,
            ner_type=ner_type,
            extended_types=extended_types,
            language=language,
            ids=ids,
            query=query,
            cache=cache,
            soft_filtering=soft_filtering,
        )
        return query_result

    def _exec_query(
        self,
        cleaned_name,
        limit,
        kg,
        fuzzy,
        types,
        kind,
        ner_type,
        extended_types,
        language,
        ids,
        query,
        cache=True,
        soft_filtering=False,
    ):
        self.candidate_cache_collection = self.database.get_requested_collection("cache", kg=kg)

        types_values = self._normalise_filter_values(types, preserve_case=True)
        extended_types_values = self._normalise_filter_values(extended_types, preserve_case=True)
        ner_types_values = self._normalise_filter_values(ner_type, preserve_case=True)
        ner_type_value = ner_types_values[0] if ner_types_values else None

        types_serialised = self._serialise_values(types_values)
        explicit_serialised = types_serialised
        extended_serialised = self._serialise_values(extended_types_values)

        ids_serialised = ids
        if ids is not None and not isinstance(ids, str):
            normalised_ids = self._normalise_filter_values(ids, preserve_case=True)
            ids_serialised = self._serialise_values(normalised_ids)

        ntoken_mention = len(cleaned_name.split(" "))
        length_mention = len(cleaned_name)
        ambiguity_mention, corrects_tokens = self._get_ambiguity_mention(cleaned_name, kg, limit)

        if query is not None:
            query = json.loads(query)
            result = self.elastic_retriever.search(query, kg, limit)
            result = self._get_final_candidates_list(
                result,
                cleaned_name,
                kg,
                ambiguity_mention,
                corrects_tokens,
                ntoken_mention,
                length_mention,
            )
            return result

        if not cache:
            query = self.create_query(
                cleaned_name,
                fuzzy=fuzzy,
                types=types_values,
                kind=kind,
                ner_type=ner_type_value,
                extended_types=extended_types_values,
                language=language,
                soft_filtering=soft_filtering,
            )
            result = self.elastic_retriever.search(query, kg, limit)
            final_result = self._get_final_candidates_list(
                result,
                cleaned_name,
                kg,
                ambiguity_mention,
                corrects_tokens,
                ntoken_mention,
                length_mention,
            )
            final_result = self._check_ids(
                cleaned_name,
                kg,
                ids_serialised,
                ntoken_mention,
                length_mention,
                ambiguity_mention,
                corrects_tokens,
                final_result,
            )
            return final_result

        body = {
            "name": cleaned_name,
            "limit": {"$gte": limit},
            "kg": kg,
            "fuzzy": fuzzy,
            "types": types_serialised,
            "kind": kind,
            "NERtype": ner_type_value,
            "explicit_types": explicit_serialised,
            "extended_types": extended_serialised,
            "language": language,
            "soft_filtering": soft_filtering,
        }

        result = self.candidate_cache_collection.find_one_and_update(
            body,
            {"$set": {"lastAccessed": datetime.datetime.now(datetime.timezone.utc)}},
        )

        if result is not None:
            final_result = result["candidates"][0:limit]
            limit = result["limit"]
            result = self._check_ids(
                cleaned_name,
                kg,
                ids_serialised,
                ntoken_mention,
                length_mention,
                ambiguity_mention,
                corrects_tokens,
                final_result,
            )
            if result is not None:
                final_result = result
                self.add_or_update_cache(body, final_result, limit)
            return final_result

        query = self.create_query(
            cleaned_name,
            fuzzy=fuzzy,
            types=types_values,
            kind=kind,
            ner_type=ner_type_value,
            extended_types=extended_types_values,
            language=language,
            soft_filtering=soft_filtering,
        )
        final_result = []

        result = self.elastic_retriever.search(query, kg, limit)
        final_result = self._get_final_candidates_list(
            result,
            cleaned_name,
            kg,
            ambiguity_mention,
            corrects_tokens,
            ntoken_mention,
            length_mention,
        )
        final_result = self._check_ids(
            cleaned_name,
            kg,
            ids_serialised,
            ntoken_mention,
            length_mention,
            ambiguity_mention,
            corrects_tokens,
            final_result,
        )
        self.add_or_update_cache(body, final_result, limit)

        return final_result

    def _get_ambiguity_mention(self, cleaned_name, kg, limit=1000):
        query_token = self.create_token_query(name=cleaned_name)
        result_to_discard = self.elastic_retriever.search(query_token, kg, limit)
        ambiguity_mention, corrects_tokens = (0, 0)
        history_labels, tokens_set = (set(), set())
        for entity in result_to_discard:
            label_clean = clean_str(entity["name"])
            tokens = label_clean.split(" ")
            for token in tokens:
                tokens_set.add(token)
            if cleaned_name == label_clean and entity["id"] not in history_labels:
                ambiguity_mention += 1
            history_labels.add(entity["id"])
        tokens_mention = set(cleaned_name.split(" "))
        ambiguity_mention = (
            ambiguity_mention / len(history_labels) if len(history_labels) > 0 else 0
        )
        ambiguity_mention = round(ambiguity_mention, 3)
        corrects_tokens = round(
            len(tokens_mention.intersection(tokens_set)) / len(tokens_mention), 3
        )
        return ambiguity_mention, corrects_tokens

    def _get_final_candidates_list(
        self,
        result,
        name,
        kg,
        ambiguity_mention,
        corrects_tokens,
        ntoken_mention,
        length_mention,
    ):
        def _types_to_tokens(raw_types):
            if isinstance(raw_types, str):
                return [token for token in raw_types.split(" ") if token]
            if isinstance(raw_types, list):
                tokens = []
                for item in raw_types:
                    if isinstance(item, dict):
                        type_id = item.get("id")
                        if type_id:
                            tokens.append(type_id)
                    elif isinstance(item, str):
                        tokens.extend(token for token in item.split(" ") if token)
                return tokens
            if raw_types is None:
                return []
            return [str(raw_types)]

        ids = set()
        normalised_entity_types = []
        for entity in result:
            tokens = _types_to_tokens(entity.get("types", ""))
            normalised_entity_types.append(tokens)
            ids.update(tokens)
        types_id_to_name = self._get_types_id_to_name(list(ids), kg)

        history = {}
        for entity, entity_types_tokens in zip(result, normalised_entity_types):
            id_entity = entity["id"]
            label_clean = clean_str(entity["name"])
            ed_score = round(editdistance(label_clean, name), 2)
            jaccard_score = round(compute_similarity_between_string(label_clean, name), 2)
            jaccard_ngram_score = round(compute_similarity_between_string(label_clean, name, 3), 2)
            obj = {
                "id": entity["id"],
                "name": entity["name"],
                "description": entity.get("description", ""),
                "types": [
                    {"id": id_type, "name": types_id_to_name.get(id_type, id_type)}
                    for id_type in entity_types_tokens
                ],
                "kind": entity.get("kind", None),
                "NERtype": entity.get("NERtype", None),
                "explicit_types": entity.get("explicit_types", None),
                "extended_types": entity.get("extended_types", None),
                "ambiguity_mention": ambiguity_mention,
                "corrects_tokens": corrects_tokens,
                "ntoken_mention": ntoken_mention,
                "ntoken_entity": entity["ntoken_entity"],
                "length_mention": length_mention,
                "length_entity": entity["length_entity"],
                "popularity": entity["popularity"],
                "pos_score": entity["pos_score"],
                "es_score": entity["es_score"],
                "ed_score": ed_score,
                "jaccard_score": jaccard_score,
                "jaccardNgram_score": jaccard_ngram_score,
            }

            if id_entity not in history:
                history[id_entity] = obj
            elif (ed_score + jaccard_score) > (
                history[id_entity]["ed_score"] + history[id_entity]["jaccard_score"]
            ):
                history[id_entity] = obj

        return list(history.values())

    def add_or_update_cache(self, body, final_result, limit):
        """
        Add or update an element in the cache.

        Parameters:
        - body (dict): The query body to identify the cache element.
        - final_result (list): The final result to cache if the element does not exist.
        """
        query = {
            "name": body["name"],
            "limit": limit,
            "kg": body["kg"],
            "fuzzy": body["fuzzy"],
            "types": body.get("types"),
            "kind": body.get("kind"),
            "NERtype": body.get("NERtype"),
            "explicit_types": body.get("explicit_types"),
            "extended_types": body.get("extended_types"),
            "language": body.get("language"),
            "soft_filtering": body.get("soft_filtering", False),
        }

        update = {
            "$set": {
                "candidates": final_result,
                "lastAccessed": datetime.datetime.now(datetime.timezone.utc),
            },
            "$setOnInsert": query,
        }

        try:
            self.candidate_cache_collection.update_one(query, update, upsert=True)
        except Exception as e:
            print(f"Error inserting or updating in cache: {e}")

    def _check_ids(
        self,
        name,
        kg,
        ids,
        ntoken_mention,
        length_mention,
        ambiguity_mention,
        corrects_tokens,
        result,
    ):
        if ids is None:
            return result

        result = result or []
        ids_list = self._normalise_filter_values(ids, preserve_case=True)

        for item in result:
            if item["id"] in ids_list:
                ids_list.remove(item["id"])

        if len(ids_list) == 0:
            return result

        fetched_candidates = []
        for entity_id in ids_list:
            query = self.create_ids_query(entity_id)
            fetched = self.elastic_retriever.search(query, kg, limit=1)
            if fetched:
                fetched_candidates.extend(fetched)

        if not fetched_candidates:
            return result

        result_by_id = self._get_final_candidates_list(
            fetched_candidates,
            name,
            kg,
            ambiguity_mention,
            corrects_tokens,
            ntoken_mention,
            length_mention,
        )
        new_result = result + result_by_id
        return new_result

    def _get_types_id_to_name(self, ids, kg):
        items_collection = self.database.get_requested_collection("items", kg=kg)
        results = items_collection.find({"kind": "type", "entity": {"$in": ids}})
        types_id_to_name = {result["entity"]: result["labels"].get("en") for result in results}
        return types_id_to_name

    def create_token_query(self, name):
        query = {
            "query": {"match": {"name": name}},
            "_source": {"excludes": ["language"]},
        }
        return query

    # Create a query to search for a list of ids (string separated by space)
    def create_ids_query(self, ids, language="en", allow_alias=False):
        ids_list = self._normalise_filter_values(ids, preserve_case=True)
        if not ids_list:
            raise ValueError("Unable to build id query without ids")

        must_clauses = []
        if len(ids_list) == 1:
            must_clauses.append({"match": {"id": ids_list[0]}})
        else:
            must_clauses.append({"terms": {"id": ids_list}})

        if language:
            must_clauses.append({"match": {"language": language}})
        if not allow_alias:
            must_clauses.append({"match": {"is_alias": False}})

        query = {
            "query": {
                "bool": {
                    "must": must_clauses,
                }
            },
            "_source": {"excludes": ["language"]},
            "size": len(ids_list),
        }
        return query

    @staticmethod
    def _normalise_filter_values(values, preserve_case=False):
        if values is None:
            return []

        tokens = []
        if isinstance(values, str):
            tokens = values.replace(",", " ").split()
        elif isinstance(values, (list, tuple, set)):
            for item in values:
                if item is None:
                    continue
                if isinstance(item, str):
                    tokens.extend(item.replace(",", " ").split())
                else:
                    tokens.append(str(item))
        else:
            tokens = [str(values)]

        normalised_tokens = []
        for token in tokens:
            if not token:
                continue
            cleaned = token.strip()
            if not cleaned:
                continue
            if not preserve_case:
                cleaned = cleaned.casefold()
            normalised_tokens.append(cleaned)

        # Preserve deterministic order without duplicates by sorting tokens
        seen = set()
        unique_tokens = []
        for token in normalised_tokens:
            if token in seen:
                continue
            seen.add(token)
            unique_tokens.append(token)
        return sorted(unique_tokens)

    @staticmethod
    def _serialise_values(values):
        if not values:
            return None
        return " ".join(values)

    def create_query(
        self,
        name,
        fuzzy=False,
        types=None,
        kind=None,
        ner_type=None,
        extended_types=None,
        language=None,
        soft_filtering=False,
    ):
        # Base query
        query_base = {
            "query": {"bool": {"must": [], "should": [], "filter": []}},
            "sort": [{"popularity": {"order": "desc"}}],
            "_source": {"excludes": ["language"]},
        }

        penalty_functions = []

        # Add name to the query
        if fuzzy:
            query_base["query"]["bool"]["must"].append(
                {"match": {"name": {"query": name, "fuzziness": "auto"}}}
            )
        else:
            query_base["query"]["bool"]["must"].append(
                {"match": {"name": {"query": name, "boost": 2}}}
            )

        # Normalise incoming filters into token lists
        def ensure_list(value):
            if value is None:
                return []
            if isinstance(value, str):
                return [token for token in value.split() if token]
            if isinstance(value, (list, tuple, set)):
                return [str(token) for token in value if token]
            return [str(value)]

        types_tokens = ensure_list(types)
        extended_tokens = ensure_list(extended_types)
        ner_tokens = ensure_list(ner_type)

        explicit_tokens = list(types_tokens)

        # Add kind filter if provided
        if kind:
            query_base["query"]["bool"]["filter"].append({"term": {"kind": kind}})

        # Handle NER soft/hard filtering
        if ner_tokens:
            if soft_filtering:
                for value in ner_tokens:
                    query_base["query"]["bool"]["should"].append(
                        {"term": {"NERtype": {"value": value, "boost": 2.0}}}
                    )
            else:
                if len(ner_tokens) == 1:
                    query_base["query"]["bool"]["filter"].append(
                        {"term": {"NERtype": ner_tokens[0]}}
                    )
                else:
                    query_base["query"]["bool"]["filter"].append(
                        {"terms": {"NERtype": ner_tokens}}
                    )

        # Handle explicit types
        if explicit_tokens:
            if soft_filtering:
                for value in explicit_tokens:
                    query_base["query"]["bool"]["should"].append(
                        {"term": {"explicit_types": {"value": value, "boost": 1.5}}}
                    )
                penalty_functions.append(
                    {
                        "filter": {
                            "bool": {"must_not": [{"terms": {"explicit_types": explicit_tokens}}]}
                        },
                        "weight": 0.1,
                    }
                )
            else:
                query_base["query"]["bool"]["filter"].append(
                    {"terms": {"explicit_types": explicit_tokens}}
                )

        # Handle extended types
        if extended_tokens:
            if soft_filtering:
                for value in extended_tokens:
                    query_base["query"]["bool"]["should"].append(
                        {"term": {"extended_types": {"value": value, "boost": 1.2}}}
                    )
                penalty_functions.append(
                    {
                        "filter": {
                            "bool": {"must_not": [{"terms": {"extended_types": extended_tokens}}]}
                        },
                        "weight": 0.2,
                    }
                )
            else:
                query_base["query"]["bool"]["filter"].append(
                    {"terms": {"extended_types": extended_tokens}}
                )

        # Add language filter if provided
        if language:
            query_base["query"]["bool"]["filter"].append({"term": {"language": language}})

        if soft_filtering and penalty_functions:
            wrapped_query = {
                "query": {
                    "function_score": {
                        "query": query_base["query"],
                        "functions": penalty_functions,
                        "boost_mode": "multiply",
                        "score_mode": "multiply",
                    }
                }
            }
            if "sort" in query_base:
                wrapped_query["sort"] = query_base["sort"]
            if "_source" in query_base:
                wrapped_query["_source"] = query_base["_source"]
            return wrapped_query

        return query_base
