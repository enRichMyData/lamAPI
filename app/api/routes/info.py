from fastapi import APIRouter

from app.core.config import get_settings

router = APIRouter(prefix="/info", tags=["info"])

settings = get_settings()


@router.get("/")
async def info():
    return {
        "title": settings.app_name,
        "description": settings.description,
        "license": {
            "name": "Apache 2.0",
            "url": "https://www.apache.org/licenses/LICENSE-2.0.html",
        },
        "version": settings.app_version,
    }
