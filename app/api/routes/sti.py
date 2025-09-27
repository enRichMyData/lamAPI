import traceback
from typing import Any, Dict, Optional

from fastapi import APIRouter, Body, Depends, Query

from app.api.utils import error_response, extract_json
from app.core.task_queue import TaskQueue
from app.dependencies import get_lamapi, get_task_queue
from lamapi import LamAPI
from lamapi.utils import build_error

router = APIRouter(prefix="/sti", tags=["sti"])


@router.post("/column-analysis")
async def column_analysis(
    payload: Dict[str, Any] = Body(...),
    token: str = Query(...),
    model_type: Optional[str] = Query("fast"),
    lamapi_service: LamAPI = Depends(get_lamapi),
    task_queue: TaskQueue = Depends(get_task_queue),
):
    token_valid, token_error = lamapi_service.validate_token(token)
    if not token_valid:
        return error_response(token_error)

    data, error = extract_json(payload, error_message="Invalid Data")
    if error:
        return error

    model = model_type if model_type in {"fast", "accurate"} else "fast"

    try:
        result = await task_queue.submit(lamapi_service.classify_columns, data, model_type=model)
        return result
    except Exception as exc:  # noqa: BLE001
        return error_response(build_error(str(exc), 500, traceback.format_exc()))
