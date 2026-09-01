"""Analytics read endpoint."""
from fastapi import APIRouter

from app.services import analytics_service

router = APIRouter(prefix="/analytics", tags=["analytics"])


@router.get("")
def get_analytics() -> dict:
    return analytics_service.get_analytics()
