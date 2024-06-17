from fastapi import APIRouter, HTTPException
from fastapi.responses import JSONResponse
from app.schemas.nasdaq import Nasdaq
from app.models.nasdaq import get_nasdaq_data
import json

import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/nasdaq",
    tags=["nasdaq"]
)

@router.post("/get_data")
async def get_nasdaq_data_by_date(request: Nasdaq):
    nasdaq_table_data = get_nasdaq_data(request.target_date)
    try:
        logging.info(f"Fetched table successfully")
        return JSONResponse(content=nasdaq_table_data, status_code=201)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
    