# routes/example_route.py
from fastapi import APIRouter
from ..services.dynamodb_service import get_item

router = APIRouter()


@router.get("/")
async def root():
    return {"message": "Hello, World!"}
