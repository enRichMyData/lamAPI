"""Public entrypoints for the LamAPI core package."""

from dotenv import load_dotenv

load_dotenv(override=True)

from .core import LamAPI
from .database import Database
from .elastic import Elastic
from .utils import (
    build_error,
    clean_str,
    compute_similarity_between_string,
    editdistance,
    recognize_entity,
)
from .validator import ParamsValidator

__all__ = [
    "LamAPI",
    "Database",
    "Elastic",
    "ParamsValidator",
    "build_error",
    "clean_str",
    "compute_similarity_between_string",
    "editdistance",
    "recognize_entity",
]
