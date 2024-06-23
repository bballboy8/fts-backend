# routes/example_route.py
from fastapi import APIRouter

router = APIRouter()


@router.get("/")
async def root():
    return {"message": "Hello, World!"}
