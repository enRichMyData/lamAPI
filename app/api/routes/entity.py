import traceback
from typing import Any, Dict, Optional

from fastapi import APIRouter, Body, Depends, Query

from app.api.utils import error_response, extract_json
from app.core.task_queue import TaskQueue
from app.dependencies import get_lamapi, get_task_queue
from lamapi import LamAPI
from lamapi.utils import build_error

router = APIRouter(prefix="/entity", tags=["entity"])


@router.post("/types")
async def entity_types(
    payload: Dict[str, Any] = Body(...),
    token: str = Query(...),
    kg: Optional[str] = Query(None),
    lamapi_service: LamAPI = Depends(get_lamapi),
    task_queue: TaskQueue = Depends(get_task_queue),
):
    token_valid, token_error = lamapi_service.validate_token(token)
    if not token_valid:
        return error_response(token_error)

    kg_valid, kg_value = lamapi_service.validate_kg(kg)
    if not kg_valid:
        return error_response(kg_value)

    data, error = extract_json(payload)
    if error:
        return error

    try:
        result = await task_queue.submit(lamapi_service.get_types, data, kg=kg_value)
        return result
    except Exception as exc:  # noqa: BLE001
        return error_response(build_error(str(exc), 500, traceback.format_exc()))


@router.post("/objects")
async def entity_objects(
    payload: Dict[str, Any] = Body(...),
    token: str = Query(...),
    kg: Optional[str] = Query(None),
    lamapi_service: LamAPI = Depends(get_lamapi),
    task_queue: TaskQueue = Depends(get_task_queue),
):
    token_valid, token_error = lamapi_service.validate_token(token)
    if not token_valid:
        return error_response(token_error)

    kg_valid, kg_value = lamapi_service.validate_kg(kg)
    if not kg_valid:
        return error_response(kg_value)

    data, error = extract_json(payload)
    if error:
        return error_response(build_error("Invalid Data", 400))

    try:
        result = await task_queue.submit(lamapi_service.get_objects, data, kg=kg_value)
        return result
    except Exception as exc:  # noqa: BLE001
        return error_response(build_error(str(exc), 500, traceback.format_exc()))


@router.post("/bow")
async def entity_bow(
    payload: Dict[str, Any] = Body(...),
    token: str = Query(...),
    kg: Optional[str] = Query(None),
    lamapi_service: LamAPI = Depends(get_lamapi),
    task_queue: TaskQueue = Depends(get_task_queue),
):
    token_valid, token_error = lamapi_service.validate_token(token)
    if not token_valid:
        return error_response(token_error)

    kg_valid, kg_value = lamapi_service.validate_kg(kg)
    if not kg_valid:
        return error_response(kg_value)

    wrapper, error = extract_json(payload)
    if error:
        return error_response(build_error("Invalid Data", 400))

    if not isinstance(wrapper, dict):
        return error_response(build_error("Invalid Data", 400))

    row_text = wrapper.get("text")
    qids = wrapper.get("qids", [])
    if row_text is None or not isinstance(qids, list):
        return error_response(build_error("Invalid Data", 400))

    try:
        result = await task_queue.submit(lamapi_service.get_bow, row_text, qids, kg=kg_value)
        return result
    except Exception as exc:  # noqa: BLE001
        return error_response(build_error(str(exc), 500, traceback.format_exc()))


@router.post("/predicates")
async def entity_predicates(
    payload: Dict[str, Any] = Body(...),
    token: str = Query(...),
    kg: Optional[str] = Query(None),
    lamapi_service: LamAPI = Depends(get_lamapi),
    task_queue: TaskQueue = Depends(get_task_queue),
):
    token_valid, token_error = lamapi_service.validate_token(token)
    if not token_valid:
        return error_response(token_error)

    kg_valid, kg_value = lamapi_service.validate_kg(kg)
    if not kg_valid:
        return error_response(kg_value)

    data, error = extract_json(payload)
    if error:
        return error

    try:
        result = await task_queue.submit(lamapi_service.get_predicates, data, kg=kg_value)
        return result
    except Exception as exc:  # noqa: BLE001
        return error_response(build_error(str(exc), 500, traceback.format_exc()))


@router.post("/labels")
async def entity_labels(
    payload: Dict[str, Any] = Body(...),
    token: str = Query(...),
    kg: Optional[str] = Query(None),
    lang: Optional[str] = Query(None),
    category: Optional[str] = Query(None),
    lamapi_service: LamAPI = Depends(get_lamapi),
    task_queue: TaskQueue = Depends(get_task_queue),
):
    token_valid, token_error = lamapi_service.validate_token(token)
    if not token_valid:
        return error_response(token_error)

    kg_valid, kg_value = lamapi_service.validate_kg(kg)
    if not kg_valid:
        return error_response(kg_value)

    data, error = extract_json(payload)
    if error:
        return error

    try:
        result = await task_queue.submit(
            lamapi_service.get_labels, data, kg=kg_value, lang=lang, category=category
        )
        return result
    except Exception as exc:  # noqa: BLE001
        return error_response(build_error(str(exc), 500, traceback.format_exc()))


@router.post("/sameas")
async def entity_sameas(
    payload: Dict[str, Any] = Body(...),
    token: str = Query(...),
    kg: Optional[str] = Query("wikidata"),
    lamapi_service: LamAPI = Depends(get_lamapi),
    task_queue: TaskQueue = Depends(get_task_queue),
):
    token_valid, token_error = lamapi_service.validate_token(token)
    if not token_valid:
        return error_response(token_error)

    kg_valid, kg_value = lamapi_service.validate_kg(kg)
    if not kg_valid:
        return error_response(kg_value)

    data, error = extract_json(payload)
    if error:
        return error

    try:
        result = await task_queue.submit(lamapi_service.get_sameas, data, kg=kg_value)
        return result
    except Exception as exc:  # noqa: BLE001
        return error_response(build_error(str(exc), 500, traceback.format_exc()))


@router.post("/literals")
async def entity_literals(
    payload: Dict[str, Any] = Body(...),
    token: str = Query(...),
    kg: Optional[str] = Query(None),
    lamapi_service: LamAPI = Depends(get_lamapi),
    task_queue: TaskQueue = Depends(get_task_queue),
):
    token_valid, token_error = lamapi_service.validate_token(token)
    if not token_valid:
        return error_response(token_error)

    kg_valid, kg_value = lamapi_service.validate_kg(kg)
    if not kg_valid:
        return error_response(kg_value)

    data, error = extract_json(payload)
    if error:
        return error

    try:
        result = await task_queue.submit(lamapi_service.get_literals, data, kg=kg_value)
        return result
    except Exception as exc:  # noqa: BLE001
        return error_response(build_error(str(exc), 500, traceback.format_exc()))
