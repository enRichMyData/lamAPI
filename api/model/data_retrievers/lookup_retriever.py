import datetime
import json

from model.elastic import Elastic
from model.utils import clean_str, compute_similarity_between_string, editdistance
from pymongo import ReturnDocument


class LookupRetriever:
    def __init__(self, database):
        self.database = database
        self.elastic_retriever = Elastic()

    def search(
        self,
        name,
        limit=128,
        kg="wikidata",
        fuzzy=False,
        types=None,
        kind=None,
        NERtype=None,
        explicit_types=None,
        extended_types=None,
        language=None,
        ids=None,
        query=None,
        cache=True,
        soft_filtering=False,
    ):
        self.candidate_cache_collection = self.database.get_requested_collection("cache", kg=kg)
        # Normalize name to ensure lowercase in order to avoid case-sensitive issues in the cache
        cleaned_name = clean_str(name)
        query_result = self._exec_query(
            cleaned_name,
            limit=limit,
            kg=kg,
            fuzzy=fuzzy,
            types=types,
            kind=kind,
            NERtype=NERtype,
            explicit_types=explicit_types,
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
        NERtype,
        explicit_types,
        extended_types,
        language,
        ids,
        query,
        cache=True,
        soft_filtering=False,
    ):
        self.candidate_cache_collection = self.database.get_requested_collection("cache", kg=kg)

        types_values = self._normalise_filter_values(types, preserve_case=True)
        explicit_types_values = self._normalise_filter_values(explicit_types, preserve_case=True)
        extended_types_values = self._normalise_filter_values(extended_types, preserve_case=True)
        ner_types_values = self._normalise_filter_values(NERtype, preserve_case=True)
        ner_type_value = ner_types_values[0] if ner_types_values else None

        types_cache_value = self._serialise_values(types_values)
        explicit_types_cache_value = self._serialise_values(explicit_types_values)
        extended_types_cache_value = self._serialise_values(extended_types_values)
        ner_types_cache_value = ner_type_value

        requested_limit = limit
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
                NERtype=ner_type_value,
                explicit_types=explicit_types_values,
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
                ids,
                ntoken_mention,
                length_mention,
                ambiguity_mention,
                corrects_tokens,
                language,
                final_result,
            )
            return final_result

        body = {
            "name": cleaned_name,
            "limit": {"$gte": requested_limit},
            "kg": kg,
            "fuzzy": fuzzy,
            "types": types_cache_value,
            "kind": kind,
            "NERtype": ner_types_cache_value,
            "explicit_types": explicit_types_cache_value,
            "extended_types": extended_types_cache_value,
            "language": language,
            "soft_filtering": soft_filtering,
        }

        result = self.candidate_cache_collection.find_one_and_update(
            body,
            {"$set": {"lastAccessed": datetime.datetime.now(datetime.timezone.utc)}},
            sort=[("limit", -1)],
            return_document=ReturnDocument.BEFORE,
        )

        if result is not None:
            cached_candidates = result.get("candidates", [])
            cached_limit = result.get("limit", requested_limit)

            enriched_candidates = self._check_ids(
                cleaned_name,
                kg,
                ids,
                ntoken_mention,
                length_mention,
                ambiguity_mention,
                corrects_tokens,
                language,
                list(cached_candidates),
            )

            if enriched_candidates is None:
                enriched_candidates = list(cached_candidates)

            if enriched_candidates != cached_candidates:
                self.add_or_update_cache(body, enriched_candidates, cached_limit)

            response_candidates = enriched_candidates[0:requested_limit]

            if ids:
                requested_ids_set = set(self._normalise_filter_values(ids, preserve_case=True))
                returned_ids = {
                    candidate.get("id")
                    for candidate in response_candidates
                    if candidate.get("id") is not None
                }
                missing_ids = requested_ids_set - returned_ids
                if missing_ids:
                    additional_candidates = [
                        candidate
                        for candidate in enriched_candidates
                        if candidate.get("id") in missing_ids
                    ]
                    response_candidates.extend(additional_candidates)

            return response_candidates

        query = self.create_query(
            cleaned_name,
            fuzzy=fuzzy,
            types=types_values,
            kind=kind,
            NERtype=ner_type_value,
            explicit_types=explicit_types_values,
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
            ids,
            ntoken_mention,
            length_mention,
            ambiguity_mention,
            corrects_tokens,
            language,
            final_result,
        )
        self.add_or_update_cache(body, final_result, requested_limit)

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
        type_ids_set = set()
        normalised_types_per_entity = []

        for entity in result:
            raw_types = entity.get("types", [])
            current_type_ids = []

            if isinstance(raw_types, str):
                current_type_ids = [token for token in raw_types.split() if token]
            elif isinstance(raw_types, list):
                for item in raw_types:
                    if isinstance(item, dict) and item.get("id"):
                        current_type_ids.append(item["id"])
                    elif isinstance(item, str):
                        current_type_ids.append(item)
            elif raw_types:
                current_type_ids = [str(raw_types)]

            type_ids_set.update(current_type_ids)
            normalised_types_per_entity.append(current_type_ids)

        types_id_to_name = self._get_types_id_to_name(list(type_ids_set), kg)

        history = {}
        for entity, entity_type_ids in zip(result, normalised_types_per_entity):
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
                    for id_type in entity_type_ids
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
        language,
        result,
    ):
        if ids is None:
            return result

        result = result or []
        requested_ids = self._normalise_filter_values(ids, preserve_case=True)

        if not requested_ids:
            return result

        found_ids = {item.get("id") for item in result if item.get("id") is not None}
        remaining_ids = [entity_id for entity_id in requested_ids if entity_id not in found_ids]

        if not remaining_ids:
            return result

        merged_results = list(result)

        for entity_id in remaining_ids:
            query = self.create_ids_query(entity_id, language=language)
            fetched = self.elastic_retriever.search(query, kg, limit=1)

            if not fetched:
                alias_query = self.create_ids_query(entity_id, language=language, allow_alias=True)
                fetched = self.elastic_retriever.search(alias_query, kg, limit=1)

            if fetched:
                merged_results.extend(fetched)

        if len(merged_results) == len(result):
            return result

        final_result = self._get_final_candidates_list(
            merged_results,
            name,
            kg,
            ambiguity_mention,
            corrects_tokens,
            ntoken_mention,
            length_mention,
        )

        return final_result

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
    def create_ids_query(self, ids, language=None, allow_alias=False):
        ids_list = self._normalise_filter_values(ids, preserve_case=True)
        if not ids_list:
            raise ValueError("Unable to build id query without ids")

        if len(ids_list) == 1:
            id_value = ids_list[0]
            query = {
                "query": {
                    "bool": {
                        "must": [
                            {"match": {"id": id_value}},
                        ]
                    }
                },
                "_source": {"excludes": ["language"]},
            }
            if language:
                query["query"]["bool"]["must"].append({"match": {"language": language}})
            if not allow_alias:
                query["query"]["bool"]["must"].append({"match": {"is_alias": False}})
            query["size"] = 1
            return query

        query = {
            "query": {
                "bool": {
                    "filter": [
                        {"terms": {"id": ids_list}},
                    ]
                }
            },
            "_source": {"excludes": ["language"]},
        }
        if language:
            query["query"]["bool"]["filter"].append({"term": {"language": language}})
        if not allow_alias:
            query["query"]["bool"]["filter"].append({"term": {"is_alias": False}})
        query["size"] = len(ids_list)
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
            token = token.strip()
            if not preserve_case:
                token = token.casefold()
            normalised_tokens.append(token)

        normalised_tokens = [token for token in normalised_tokens if token]
        return sorted(set(normalised_tokens))

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
        NERtype=None,
        explicit_types=None,
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

        if explicit_types is None and types is not None:
            explicit_types = types

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

        # Add kind filter if provided
        if kind:
            query_base["query"]["bool"]["filter"].append({"term": {"kind": kind}})

        if NERtype:
            ner_values = list(NERtype) if isinstance(NERtype, (list, tuple, set)) else [NERtype]
            if soft_filtering:
                for value in ner_values:
                    query_base["query"]["bool"]["should"].append(
                        {"term": {"NERtype": {"value": value, "boost": 2.0}}}
                    )
            else:
                if len(ner_values) == 1:
                    query_base["query"]["bool"]["filter"].append(
                        {"term": {"NERtype": ner_values[0]}}
                    )
                else:
                    query_base["query"]["bool"]["filter"].append(
                        {"terms": {"NERtype": ner_values}}
                    )

        if explicit_types:
            if soft_filtering:
                for value in explicit_types:
                    query_base["query"]["bool"]["should"].append(
                        {"term": {"explicit_types": {"value": value, "boost": 1.5}}}
                    )
                penalty_functions.append(
                    {
                        "filter": {
                            "bool": {"must_not": [{"terms": {"explicit_types": explicit_types}}]}
                        },
                        "weight": 0.1,
                    }
                )
            else:
                query_base["query"]["bool"]["filter"].append(
                    {"terms": {"explicit_types": explicit_types}}
                )

        if extended_types:
            if soft_filtering:
                for value in extended_types:
                    query_base["query"]["bool"]["should"].append(
                        {"term": {"extended_types": {"value": value, "boost": 1.2}}}
                    )
                penalty_functions.append(
                    {
                        "filter": {
                            "bool": {"must_not": [{"terms": {"extended_types": extended_types}}]}
                        },
                        "weight": 0.2,
                    }
                )
            else:
                query_base["query"]["bool"]["filter"].append(
                    {"terms": {"extended_types": extended_types}}
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
