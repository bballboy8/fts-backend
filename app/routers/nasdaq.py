import gzip
import io
import json
import os
import time
import asyncio
from typing import Optional, List, Dict, Any
from contextlib import asynccontextmanager

import asyncpg
from bs4 import BeautifulSoup
from fastapi import APIRouter, Response, HTTPException
from fastapi.responses import StreamingResponse
import requests
from pydantic import BaseModel, Field
from datetime import datetime, date
import pytz

from app.models.nasdaq import fetch_all_data, fetch_all_tickers, is_ticker_valid
from app.application_logger import get_logger

logger = get_logger(__name__)

router = APIRouter(prefix="/nasdaq", tags=["nasdaq"])

# Constants
CHUNK_SIZE = 65536
COMPRESSION_LEVEL = 1
MAX_DB_RETRIES = 3
RETRY_DELAY = 1  # seconds
HOLIDAY_URL = "https://www.nyse.com/markets/hours-calendars"


# Pydantic models for request validation
class NasdaqDataRequest(BaseModel):
    symbol: str = Field(..., min_length=1, max_length=10)
    start_datetime: str


class DatabaseConfig:
    def __init__(self):
        self.config = {
            "database": os.getenv("dbname"),
            "user": os.getenv("user"),
            "password": os.getenv("password"),
            "host": os.getenv("host"),
            "port": int(os.getenv("port", "5432")),
            "min_size": 100,
            "max_size": 500,
        }


class DatabaseManager:
    def __init__(self):
        self.config = DatabaseConfig()
        self.pool: Optional[asyncpg.Pool] = None

    async def init_pool(self) -> None:
        """Initialize the database connection pool with retry logic"""
        if self.pool is not None:
            return

        for attempt in range(MAX_DB_RETRIES):
            try:
                self.pool = await asyncpg.create_pool(**self.config.config)
                logger.info("Database pool initialized successfully")
                return
            except Exception as e:
                if attempt == MAX_DB_RETRIES - 1:
                    logger.error(
                        f"Failed to initialize database pool after {MAX_DB_RETRIES} attempts: {e}"
                    )
                    raise
                logger.warning(
                    f"Database pool initialization attempt {attempt + 1} failed: {e}"
                )
                await asyncio.sleep(RETRY_DELAY)

    async def close_pool(self) -> None:
        """Close the database connection pool"""
        if self.pool:
            await self.pool.close()
            self.pool = None
            logger.info("Database pool closed")

    @asynccontextmanager
    async def get_connection(self):
        """Get a database connection from the pool with proper error handling"""
        if not self.pool:
            await self.init_pool()

        try:
            async with self.pool.acquire() as connection:
                yield connection
        except asyncpg.PostgresError as e:
            logger.error(f"Database error: {e}")
            raise HTTPException(status_code=503, detail="Database error occurred")
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            raise HTTPException(status_code=500, detail="Internal server error")


db_manager = DatabaseManager()


# Startup and shutdown events
@router.on_event("startup")
async def startup_event():
    await db_manager.init_pool()


@router.on_event("shutdown")
async def shutdown_event():
    await db_manager.close_pool()


# Utility functions
def get_ny_datetime() -> datetime:
    """Get current datetime in NY timezone"""
    utc_datetime = datetime.utcnow()
    ny_tz = pytz.timezone("America/New_York")
    return utc_datetime.replace(tzinfo=pytz.utc).astimezone(ny_tz)


async def stream_records(records: List[Dict[str, Any]]):
    """Stream records with efficient JSON serialization"""
    for record in records:
        record_dict = {
            k: v.isoformat() if isinstance(v, (datetime, date)) else v
            for k, v in record.items()
        }
        yield json.dumps(record_dict) + "\n"


async def compress_stream(
    stream, chunk_size=CHUNK_SIZE, compression_level=COMPRESSION_LEVEL
):
    """Compress the data stream efficiently"""
    buffer = io.BytesIO()
    with gzip.GzipFile(fileobj=buffer, mode="w", compresslevel=compression_level) as gz:
        async for chunk in stream:
            gz.write(chunk.encode("utf-8"))
    buffer.seek(0)
    while chunk := buffer.read(chunk_size):
        yield chunk


def parse_date(date_string):
    """Parse date with multiple format options."""
    date_formats = ["%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M"]

    for fmt in date_formats:
        try:
            return datetime.strptime(date_string, fmt)
        except ValueError:
            continue  # Try the next format

    logger.error("Date parsing error: invalid date format")
    return None  # Return None if no format matched


# Enhanced route handlers
@router.post("/get_data")
async def get_nasdaq_data_by_date(request: NasdaqDataRequest):
    start_time = time.time()
    logger.info(f"Starting data fetch for symbol: {request.symbol}")

    try:
        # Parse date with improved exception handling
        start_datetime = parse_date(request.start_datetime)
        if start_datetime is None:
            return Response(status_code=400, content="Invalid date format")

        request.start_datetime = start_datetime

        async with db_manager.get_connection() as conn:
            records = await fetch_all_data(conn, request.symbol, request.start_datetime)

            if not records:
                return Response(status_code=204)

            response = StreamingResponse(
                compress_stream(stream_records(records)),
                media_type="application/json",
                headers={"Content-Encoding": "gzip", "Transfer-Encoding": "chunked"},
            )

            logger.info(
                f"Data fetch completed in {time.time() - start_time:.2f}s "
                f"for symbol: {request.symbol}"
            )
            return response

    except Exception as e:
        logger.error(f"Error processing request: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/get_tickers")
async def get_tickers():
    async with db_manager.get_connection() as conn:
        return await fetch_all_tickers(conn)


@router.get("/is_ticker_valid/{ticker}")
async def ticker_valid(ticker: str):
    async with db_manager.get_connection() as conn:
        return await is_ticker_valid(conn, ticker)


@router.get("/holidays")
async def get_holidays():
    """Fetch NYSE holidays with caching"""
    try:
        response = requests.get(HOLIDAY_URL, timeout=10)
        response.raise_for_status()

        soup = BeautifulSoup(response.content, "html.parser")
        holidays = parse_holiday_table(soup)

        return holidays
    except requests.RequestException as e:
        logger.error(f"Error fetching holidays: {e}")
        raise HTTPException(status_code=503, detail="Unable to fetch holiday data")


def parse_holiday_table(soup: BeautifulSoup) -> List[Dict[str, str]]:
    """Parse holiday table with better error handling"""
    holidays = []
    table = soup.find(
        "table", {"class": "table-data w-full table-fixed table-border-rows"}
    )

    if not table:
        raise HTTPException(status_code=500, detail="Holiday table not found")

    try:
        headers = [
            h.get_text(strip=True) for h in table.find("thead").find_all("td")[1:]
        ]
        rows = table.find("tbody").find_all("tr")

        for row in rows:
            cols = row.find_all("td")
            holiday_name = cols[0].get_text(strip=True)
            dates = [col.get_text(strip=True) for col in cols[1:]]

            for year, date_str in zip(headers, dates):
                try:
                    parsed_date = parse_holiday_date(date_str, year)
                    holidays.append(
                        {
                            "year": year,
                            "holiday_name": holiday_name,
                            "date": date_str,
                            "date_time": parsed_date.strftime("%Y-%m-%d")
                            if parsed_date
                            else None,
                        }
                    )
                except ValueError as e:
                    logger.warning(f"Error parsing date {date_str}: {e}")
                    continue

        return holidays
    except Exception as e:
        logger.error(f"Error parsing holiday table: {e}")
        raise HTTPException(status_code=500, detail="Error parsing holiday data")


def parse_holiday_date(date_str: str, year: str) -> Optional[datetime]:
    """Parse holiday date with better error handling"""
    try:
        date_parts = date_str.split(",")[1].strip().split("*")[0].split("(")[0]
        month, day = date_parts.strip().split(" ")
        return datetime.strptime(f"{year} {month} {day.strip()}", "%Y %B %d")
    except (ValueError, IndexError):
        return None
