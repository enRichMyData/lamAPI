from typing import Any, Dict, Tuple

from fastapi.responses import JSONResponse

from lamapi.model.utils import build_error


def error_response(error_tuple: Tuple[Dict[str, Any], int]) -> JSONResponse:
    payload, status = error_tuple
    return JSONResponse(payload, status_code=status)


def extract_json(payload: Dict[str, Any], error_message: str = "Invalid json format"):
    if not isinstance(payload, dict) or "json" not in payload:
        return None, error_response(build_error(error_message, 400))
    return payload["json"], None
