import traceback
from typing import Any, Dict

from fastapi import APIRouter, Body, Depends, Query

from app.api.utils import error_response, extract_json
from app.core.task_queue import TaskQueue
from app.dependencies import get_lamapi, get_task_queue
from lamapi import LamAPI
from lamapi.utils import build_error

router = APIRouter(prefix="/classify", tags=["classify"])


@router.post("/literal-recognizer")
async def literal_recognizer(
    payload: Dict[str, Any] = Body(...),
    token: str = Query(...),
    lamapi_service: LamAPI = Depends(get_lamapi),
    task_queue: TaskQueue = Depends(get_task_queue),
):
    token_valid, token_error = lamapi_service.validate_token(token)
    if not token_valid:
        return error_response(token_error)

    data, error = extract_json(payload, error_message="Invalid Data")
    if error:
        return error

    try:
        result = await task_queue.submit(lamapi_service.classify_literals, data)
        return result
    except Exception as exc:  # noqa: BLE001
        return error_response(build_error(str(exc), 500, traceback.format_exc()))


@router.post("/name-entity-recognition")
async def name_entity_recognition(
    payload: Dict[str, Any] = Body(...),
    token: str = Query(...),
    lamapi_service: LamAPI = Depends(get_lamapi),
    task_queue: TaskQueue = Depends(get_task_queue),
):
    token_valid, token_error = lamapi_service.validate_token(token)
    if not token_valid:
        return error_response(token_error)

    data, error = extract_json(payload, error_message="Invalid Data")
    if error:
        return error

    try:
        result = await task_queue.submit(lamapi_service.recognize_entities, data)
        return result
    except Exception as exc:  # noqa: BLE001
        return error_response(build_error(str(exc), 500, traceback.format_exc()))
