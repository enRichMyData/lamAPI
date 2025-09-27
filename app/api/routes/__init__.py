from fastapi import APIRouter

from . import classify, entity, info, lookup, sti, summary

api_router = APIRouter()
api_router.include_router(info.router)
api_router.include_router(lookup.router)
api_router.include_router(entity.router)
api_router.include_router(classify.router)
api_router.include_router(sti.router)
api_router.include_router(summary.router)


__all__ = ["api_router"]
