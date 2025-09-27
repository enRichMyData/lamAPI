import asyncio
import logging
import os
import traceback
from dataclasses import dataclass
from functools import partial
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Sequence

from fastapi import APIRouter, Body, FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from lamapi import LamAPI
from lamapi.model.utils import build_error

logger = logging.getLogger("lamapi.server")

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
MAX_QUEUE_SIZE = int(os.getenv("LAMAPI_QUEUE_SIZE", "256"))
MAX_WORKERS = int(os.getenv("LAMAPI_WORKERS", "4"))
JOB_RETRIES = int(os.getenv("LAMAPI_JOB_RETRIES", "3"))
RETRY_BACKOFF = float(os.getenv("LAMAPI_RETRY_BACKOFF", "0.5"))


@dataclass
class Job:
    func: Callable[..., Any]
    args: Sequence[Any]
    kwargs: Dict[str, Any]
    future: asyncio.Future


lamapi_service = LamAPI()
database = lamapi_service.database

DESCRIPTION = (Path(__file__).resolve().parent / "data.txt").read_text()

app = FastAPI(title="LamAPI", description=DESCRIPTION, version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

job_queue: Optional[asyncio.Queue[Job]] = None
workers: List[asyncio.Task] = []


def error_response(error_tuple):
    payload, status = error_tuple
    return JSONResponse(payload, status_code=status)


def extract_json(payload: Dict[str, Any], error_message: str = "Invalid json format"):
    if not isinstance(payload, dict) or "json" not in payload:
        return None, error_response(build_error(error_message, 400))
    return payload["json"], None


async def submit_job(func: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
    loop = asyncio.get_running_loop()
    future: asyncio.Future = loop.create_future()
    if job_queue is None:
        raise RuntimeError("Job queue not initialised")
    job = Job(func=func, args=args, kwargs=kwargs, future=future)
    await job_queue.put(job)
    return await future


async def worker_loop(worker_id: int) -> None:
    loop = asyncio.get_running_loop()
    while True:
        try:
            job = await job_queue.get()
        except asyncio.CancelledError:
            break

        try:
            last_exc: Optional[Exception] = None
            for attempt in range(max(JOB_RETRIES, 1)):
                try:
                    call = partial(job.func, *job.args, **job.kwargs)
                    result = await loop.run_in_executor(None, call)
                    if not job.future.done():
                        job.future.set_result(result)
                    break
                except Exception as exc:  # noqa: BLE001
                    last_exc = exc
                    if attempt < JOB_RETRIES - 1:
                        await asyncio.sleep(RETRY_BACKOFF * (attempt + 1))
                    else:
                        if not job.future.done():
                            job.future.set_exception(exc)
            else:
                if last_exc and not job.future.done():
                    job.future.set_exception(last_exc)
        except Exception as exc:  # noqa: BLE001
            logger.exception("Worker %s execution failure", worker_id)
            if not job.future.done():
                job.future.set_exception(exc)
        finally:
            job_queue.task_done()


@app.on_event("startup")
async def on_startup() -> None:
    global job_queue, workers
    job_queue = asyncio.Queue(maxsize=MAX_QUEUE_SIZE)
    workers = [
        asyncio.create_task(worker_loop(i), name=f"lamapi-worker-{i}") for i in range(MAX_WORKERS)
    ]
    logger.info("LamAPI FastAPI server started with %s workers", MAX_WORKERS)


@app.on_event("shutdown")
async def on_shutdown() -> None:
    for worker in workers:
        worker.cancel()
    await asyncio.gather(*workers, return_exceptions=True)
    if job_queue is not None:
        while not job_queue.empty():
            job = job_queue.get_nowait()
            if not job.future.done():
                job.future.set_exception(RuntimeError("Server shutting down"))
            job_queue.task_done()


info_router = APIRouter(prefix="/info", tags=["info"])
lookup_router = APIRouter(prefix="/lookup", tags=["lookup"])
entity_router = APIRouter(prefix="/entity", tags=["entity"])
classify_router = APIRouter(prefix="/classify", tags=["classify"])
sti_router = APIRouter(prefix="/sti", tags=["sti"])
summary_router = APIRouter(prefix="/summary", tags=["summary"])


@info_router.get("/")
async def info() -> Dict[str, Any]:
    return {
        "title": "LamAPI",
        "description": "This is an API which retrieves data about entities in different Knowledge Graphs and performs entity linking task.",
        "license": {
            "name": "Apache 2.0",
            "url": "https://www.apache.org/licenses/LICENSE-2.0.html",
        },
        "version": "1.0.0",
    }


@lookup_router.get("/entity-retrieval")
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
    ids: Optional[List[str]] = Query(None, description="Specific IDs to include."),
    language: Optional[str] = Query(None, description="Language filter."),
    query: Optional[str] = Query(None, description="Raw Elasticsearch query."),
    cache: Optional[str] = Query(None, description="Use cached result."),
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

    types_values = lamapi_service.parse_multi_values(types)
    extended_types_values = lamapi_service.parse_multi_values(extended_types)
    ids_values = lamapi_service.parse_multi_values(ids)
    ids_value = " ".join(ids_values) if ids_values else None
    cache_value = cache in (None, "true", "True")

    lookup_kwargs = {
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
    }

    try:
        result = await submit_job(lamapi_service.lookup, name, **lookup_kwargs)
        return result
    except Exception as exc:  # noqa: BLE001
        logger.exception("Lookup failed")
        return error_response(build_error(str(exc), 500, traceback.format_exc()))


@entity_router.post("/types")
async def entity_types(
    payload: Dict[str, Any] = Body(...),
    token: str = Query(...),
    kg: Optional[str] = Query(None),
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
        result = await submit_job(lamapi_service.get_types, data, kg=kg_value)
        return result
    except Exception as exc:  # noqa: BLE001
        return error_response(build_error(str(exc), 500, traceback.format_exc()))


@entity_router.post("/objects")
async def entity_objects(
    payload: Dict[str, Any] = Body(...),
    token: str = Query(...),
    kg: Optional[str] = Query(None),
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
        result = await submit_job(lamapi_service.get_objects, data, kg=kg_value)
        return result
    except Exception as exc:  # noqa: BLE001
        return error_response(build_error(str(exc), 500, traceback.format_exc()))


@entity_router.post("/bow")
async def entity_bow(
    payload: Dict[str, Any] = Body(...),
    token: str = Query(...),
    kg: Optional[str] = Query(None),
):
    token_valid, token_error = lamapi_service.validate_token(token)
    if not token_valid:
        return error_response(token_error)

    kg_valid, kg_value = lamapi_service.validate_kg(kg)
    if not kg_valid:
        return error_response(kg_value)

    wrapper, error = extract_json(payload)
    if error:
        return error

    if not isinstance(wrapper, dict):
        return error_response(build_error("Invalid Data", 400))

    row_text = wrapper.get("text")
    qids = wrapper.get("qids", [])
    if row_text is None or not isinstance(qids, list):
        return error_response(build_error("Invalid Data", 400))

    try:
        result = await submit_job(lamapi_service.get_bow, row_text, qids, kg=kg_value)
        return result
    except Exception as exc:  # noqa: BLE001
        return error_response(build_error(str(exc), 500, traceback.format_exc()))


@entity_router.post("/predicates")
async def entity_predicates(
    payload: Dict[str, Any] = Body(...),
    token: str = Query(...),
    kg: Optional[str] = Query(None),
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
        result = await submit_job(lamapi_service.get_predicates, data, kg=kg_value)
        return result
    except Exception as exc:  # noqa: BLE001
        return error_response(build_error(str(exc), 500, traceback.format_exc()))


@entity_router.post("/labels")
async def entity_labels(
    payload: Dict[str, Any] = Body(...),
    token: str = Query(...),
    kg: Optional[str] = Query(None),
    lang: Optional[str] = Query(None),
    category: Optional[str] = Query(None),
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
        result = await submit_job(
            lamapi_service.get_labels, data, kg=kg_value, lang=lang, category=category
        )
        return result
    except Exception as exc:  # noqa: BLE001
        return error_response(build_error(str(exc), 500, traceback.format_exc()))


@entity_router.post("/sameas")
async def entity_sameas(
    payload: Dict[str, Any] = Body(...),
    token: str = Query(...),
    kg: Optional[str] = Query("wikidata"),
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
        result = await submit_job(lamapi_service.get_sameas, data, kg=kg_value)
        return result
    except Exception as exc:  # noqa: BLE001
        return error_response(build_error(str(exc), 500, traceback.format_exc()))


@entity_router.post("/literals")
async def entity_literals(
    payload: Dict[str, Any] = Body(...),
    token: str = Query(...),
    kg: Optional[str] = Query(None),
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
        result = await submit_job(lamapi_service.get_literals, data, kg=kg_value)
        return result
    except Exception as exc:  # noqa: BLE001
        return error_response(build_error(str(exc), 500, traceback.format_exc()))


@classify_router.post("/literal-recognizer")
async def literal_recognizer(payload: Dict[str, Any] = Body(...), token: str = Query(...)):
    token_valid, token_error = lamapi_service.validate_token(token)
    if not token_valid:
        return error_response(token_error)

    data, error = extract_json(payload, error_message="Invalid Data")
    if error:
        return error

    try:
        result = await submit_job(lamapi_service.classify_literals, data)
        return result
    except Exception as exc:  # noqa: BLE001
        return error_response(build_error(str(exc), 500, traceback.format_exc()))


@classify_router.post("/name-entity-recognition")
async def name_entity_recognition(payload: Dict[str, Any] = Body(...), token: str = Query(...)):
    token_valid, token_error = lamapi_service.validate_token(token)
    if not token_valid:
        return error_response(token_error)

    data, error = extract_json(payload, error_message="Invalid Data")
    if error:
        return error

    try:
        result = await submit_job(lamapi_service.recognize_entities, data)
        return result
    except Exception as exc:  # noqa: BLE001
        return error_response(build_error(str(exc), 500, traceback.format_exc()))


@sti_router.post("/column-analysis")
async def column_analysis(
    payload: Dict[str, Any] = Body(...),
    token: str = Query(...),
    model_type: Optional[str] = Query("fast"),
):
    token_valid, token_error = lamapi_service.validate_token(token)
    if not token_valid:
        return error_response(token_error)

    data, error = extract_json(payload, error_message="Invalid Data")
    if error:
        return error

    model = model_type if model_type in {"fast", "accurate"} else "fast"

    try:
        result = await submit_job(lamapi_service.classify_columns, data, model_type=model)
        return result
    except Exception as exc:  # noqa: BLE001
        return error_response(build_error(str(exc), 500, traceback.format_exc()))


@summary_router.get("/")
async def summary(
    token: str = Query(...),
    kg: Optional[str] = Query("wikidata"),
    data_type: str = Query("objects"),
    rank_order: Optional[str] = Query(None),
    k: Optional[int] = Query(10),
    entities: Optional[List[str]] = Query(None),
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
            result = await submit_job(
                lamapi_service.get_objects_summary,
                entities_values or None,
                kg=kg_value,
                rank_order=rank_order,
                k=k or 10,
            )
        elif data_type == "literals":
            result = await submit_job(
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


app.include_router(info_router)
app.include_router(lookup_router)
app.include_router(entity_router)
app.include_router(classify_router)
app.include_router(sti_router)
app.include_router(summary_router)
