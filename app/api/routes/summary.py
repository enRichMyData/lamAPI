import traceback
from typing import List, Optional

from fastapi import APIRouter, Depends, Query

from app.api.utils import error_response
from app.core.task_queue import TaskQueue
from app.dependencies import get_lamapi, get_task_queue
from lamapi import LamAPI
from lamapi.utils import build_error

router = APIRouter(prefix="/summary", tags=["summary"])


@router.get("/")
async def summary(
    token: str = Query(...),
    kg: Optional[str] = Query("wikidata"),
    data_type: str = Query("objects"),
    rank_order: Optional[str] = Query(None),
    k: Optional[int] = Query(10),
    entities: Optional[List[str]] = Query(None),
    lamapi_service: LamAPI = Depends(get_lamapi),
    task_queue: TaskQueue = Depends(get_task_queue),
):
    token_valid, token_error = lamapi_service.validate_token(token)
    if not token_valid:
        return error_response(token_error)

    kg_valid, kg_value = lamapi_service.validate_kg(kg)
    if not kg_valid:
        return error_response(kg_value)

    if rank_order and rank_order not in {"asc", "desc"}:
        return error_response(build_error("Invalid rank order. Use 'asc' or 'desc'.", 400))

    entities_values = lamapi_service.parse_multi_values(entities)

    try:
        if data_type == "objects":
            result = await task_queue.submit(
                lamapi_service.get_objects_summary,
                entities_values or None,
                kg=kg_value,
                rank_order=rank_order,
                k=k or 10,
            )
        elif data_type == "literals":
            result = await task_queue.submit(
                lamapi_service.get_literals_summary,
                entities_values or None,
                kg=kg_value,
                rank_order=rank_order,
                k=k or 10,
            )
        else:
            return error_response(
                build_error("Invalid data type. Use 'objects' or 'literals'.", 400)
            )
        return result
    except Exception as exc:  # noqa: BLE001
        return error_response(build_error(str(exc), 500, traceback.format_exc()))
