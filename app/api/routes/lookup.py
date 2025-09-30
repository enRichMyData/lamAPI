import traceback
from typing import List, Optional

from fastapi import APIRouter, Depends, Query

from app.api.utils import error_response
from app.core.task_queue import TaskQueue
from app.dependencies import get_lamapi, get_task_queue
from lamapi import LamAPI
from lamapi.utils import build_error

router = APIRouter(prefix="/lookup", tags=["lookup"])


@router.get("/entity-retrieval")
async def entity_retrieval(
    name: str = Query(..., description="Name to look for (e.g., Batman Begins)."),
    limit: Optional[int] = Query(None, description="Number of entities to retrieve."),
    token: str = Query(..., description="Private token to access the API."),
    kind: Optional[str] = Query(None, description="Kind of Named Entity to be matched."),
    kg: Optional[str] = Query(None, description="Knowledge Graph to query."),
    fuzzy: Optional[str] = Query(None, description="Enable fuzzy search."),
    soft_filtering: Optional[str] = Query(
        None, alias="softFiltering", description="Enable soft filtering."
    ),
    types: Optional[List[str]] = Query(None, description="Explicit types to match."),
    extended_types: Optional[List[str]] = Query(
        None, alias="extendedTypes", description="Extended types for soft filtering."
    ),
    ner_type: Optional[str] = Query(None, alias="nerType", description="NER type to match."),
    ids: Optional[List[str]] = Query(None, description="IDs to include."),
    language: Optional[str] = Query(None, description="Language filter."),
    query: Optional[str] = Query(None, description="Raw Elasticsearch query."),
    cache: Optional[str] = Query(None, description="Use cached result."),
    normalize_score: Optional[str] = Query(
        None, description="Whether or not normalize the ElasticSearch score."
    ),
    lamapi_service: LamAPI = Depends(get_lamapi),
    task_queue: TaskQueue = Depends(get_task_queue),
):
    token_valid, token_error = lamapi_service.validate_token(token)
    if not token_valid:
        return error_response(token_error)

    fuzzy_valid, fuzzy_value = lamapi_service.validate_bool(fuzzy)
    if not fuzzy_valid:
        return error_response(fuzzy_value)

    soft_valid, soft_value = lamapi_service.validate_bool(soft_filtering)
    if not soft_valid:
        return error_response(soft_value)

    kg_valid, kg_value = lamapi_service.validate_kg(kg)
    if not kg_valid:
        return error_response(kg_value)

    limit_valid, limit_value = lamapi_service.validate_limit(limit)
    if not limit_valid:
        return error_response(limit_value)

    ner_valid, ner_value = lamapi_service.validate_ner_type(ner_type)
    if not ner_valid:
        return error_response(ner_value)

    normalize_score_valid, normalize_score_value = lamapi_service.validate_bool(normalize_score)
    if not normalize_score_valid:
        return error_response(normalize_score_value)

    types_values = lamapi_service.parse_multi_values(types)
    extended_types_values = lamapi_service.parse_multi_values(extended_types)
    ids_values = lamapi_service.parse_multi_values(ids)
    ids_value = " ".join(ids_values) if ids_values else None
    cache_value = cache in (None, "true", "True")

    kwargs = {
        "limit": limit_value,
        "kg": kg_value,
        "fuzzy": fuzzy_value,
        "types": types_values or None,
        "kind": kind,
        "ner_type": ner_value,
        "extended_types": extended_types_values or None,
        "language": language,
        "ids": ids_value,
        "query": query,
        "cache": cache_value,
        "soft_filtering": soft_value,
        "normalize_score": normalize_score_value,
    }

    try:
        result = await task_queue.submit(lamapi_service.lookup, name, **kwargs)
        return result
    except Exception as exc:  # noqa: BLE001
        return error_response(build_error(str(exc), 500, traceback.format_exc()))
