import gzip
import io
import json
import os
import time
import asyncpg
from bs4 import BeautifulSoup
from fastapi import APIRouter, Request, Response
from fastapi.responses import StreamingResponse
import requests
from app.models.nasdaq import fetch_all_data, fetch_all_tickers, is_ticker_valid
from app.application_logger import get_logger
import pytz
from datetime import datetime
from fastapi import HTTPException

logger = get_logger(__name__)


router = APIRouter(prefix="/nasdaq", tags=["nasdaq"])

utc_datetime = datetime.utcnow()
# Set the desired timezone
desired_timezone = pytz.timezone("America/New_York")
# Convert the UTC time to the desired timezone
localized_datetime = utc_datetime.replace(tzinfo=pytz.utc).astimezone(desired_timezone)
midnight_time = datetime.combine(localized_datetime, datetime.min.time())


db_params = {
    "dbname": os.getenv("dbname"),
    "user": os.getenv("user"),
    "password": os.getenv("password"),
    "host": os.getenv("host"),
    "port": "5432",
}

db_pool = None


async def init_db_pool():
    global db_pool
    db_pool = await asyncpg.create_pool(
        database=db_params["dbname"],
        user=db_params["user"],
        password=db_params["password"],
        host=db_params["host"],
        port=db_params["port"],
        min_size=500,  # Number of connection the pool will be initialized with.
        max_size=1000,  # Max number of connections in the pool.
    )


@router.on_event("startup")
async def startup_event():
    await init_db_pool()


@router.on_event("shutdown")
async def shutdown_event():
    global db_pool
    await db_pool.close()


HOLIDAY_URL = "https://www.nyse.com/markets/hours-calendars"


def fetch_holidays():
    try:
        response = requests.get(HOLIDAY_URL)
        response.raise_for_status()
    except requests.RequestException as e:
        raise HTTPException(status_code=500, detail=f"Error fetching data: {e}")
    # Parse the content using BeautifulSoup
    soup = BeautifulSoup(response.content, "html.parser")
    holidays = []
    try:
        # Locate the holiday table using the provided class name
        holiday_table = soup.find(
            "table", {"class": "table-data w-full table-fixed table-border-rows"}
        )
        if not holiday_table:
            raise HTTPException(
                status_code=500, detail="Holiday table not found on the page."
            )
        # Extract header and rows
        headers = holiday_table.find("thead").find_all("td")
        years = [
            header.get_text(strip=True) for header in headers[1:]
        ]  # Skip the first header, which is "Holiday"
        rows = holiday_table.find("tbody").find_all("tr")
        for row in rows:
            columns = row.find_all("td")
            holiday_name = columns[0].get_text(strip=True)
            dates = [col.get_text(strip=True) for col in columns[1:]]
            # Create a dictionary for each year with its corresponding holiday name and date
            for year, date_str in zip(years, dates):
                try:
                    # Parse the date into a datetime object
                    date_parts = date_str.split(",")
                    month_day = (
                        date_parts[1].strip().split("*")[0].split("(")[0]
                    )  # Clean extra symbols like *, ()
                    month, day = month_day.split(" ")
                    # Construct the full date string
                    full_date_str = f"{year} {month} {day.strip()}"
                    date_time = datetime.strptime(full_date_str, "%Y %B %d")
                    # Format the datetime object
                    formatted_date_time = date_time.strftime("%Y-%m-%d")
                except Exception:
                    formatted_date_time = "Invalid Date Format"
                holidays.append(
                    {
                        "year": year,
                        "holiday_name": holiday_name,
                        "date": date_str,
                        "date_time": formatted_date_time,
                    }
                )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error parsing data: {e}")
    return holidays


@router.get("/holidays", response_model=list)
def get_holidays():
    holidays = fetch_holidays()
    return holidays


async def async_record_generator(records):
    for record in records:
        record_dict = dict(record)
        for key, value in record_dict.items():
            if isinstance(value, datetime):
                record_dict[key] = value.isoformat()
        yield json.dumps(record_dict) + "\n"


async def async_compressed_record_generator(
    records, chunk_size=65536, compression_level=1
):
    buffer = io.BytesIO()
    with gzip.GzipFile(fileobj=buffer, mode="w", compresslevel=compression_level) as f:
        async for record in async_record_generator(records):
            f.write(record.encode("utf-8"))
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


@router.post("/get_data")
async def get_nasdaq_data_by_date(request: Request):
    start_time = time.time()

    body = await request.json()
    symbol = body.get("symbol")
    start_datetime = body.get("start_datetime")

    logger.info(
        f"Starting get_nasdaq_data_by_date... - symbol {symbol} - start_datetime {start_datetime} "
    )
    # Parse date with improved exception handling
    start_datetime = parse_date(start_datetime)
    if start_datetime is None:
        return Response(status_code=400, content="Invalid date format")

    async with db_pool.acquire() as connection:
        try:
            fetch_start_time = time.time()
            logger.info("Fetching data")
            records = await fetch_all_data(connection, symbol, start_datetime)
            fetch_end_time = time.time()
            logger.info(
                f"Data fetched in {fetch_end_time - fetch_start_time:.2f} seconds"
            )

            if not records:
                logger.info("No records found")
                return Response(status_code=204)  # Return 204 No Content

            serialize_start_time = time.time()
            logger.info(
                f"Returning records - symbol {symbol} - start_datetime {start_datetime}"
            )
            chunk_size = 65536  # Adjust chunk size if necessary
            compression_level = 1  # Lower compression level for faster compression
            response = StreamingResponse(
                async_compressed_record_generator(
                    records, chunk_size=chunk_size, compression_level=compression_level
                ),
                media_type="application/json",
                headers={"Content-Encoding": "gzip", "Transfer-Encoding": "chunked"},
            )
            serialize_end_time = time.time()
            logger.info(
                f"Data serialized and compressed in {serialize_end_time - serialize_start_time:.2f} seconds - symbol {symbol} - start_datetime {start_datetime}"
            )

            return response
        except Exception as e:
            logger.error(
                f"Error during data fetch or response preparation - symbol {symbol} - start_datetime {start_datetime}: {e}"
            )
            return Response(status_code=500, content="Server error")
        finally:
            end_time = time.time()
            logger.info(
                f"Total time taken: {end_time - start_time:.2f} seconds - symbol {symbol} - start_datetime {start_datetime}"
            )


@router.get("/get_tickers")
async def get_tickers():
    async with db_pool.acquire() as connection:
        records = await fetch_all_tickers(connection)
        return records


@router.get("/is_ticker_valid")
async def ticker_valid(ticker: str):
    async with db_pool.acquire() as connection:
        valid = await is_ticker_valid(ticker, connection)
        return valid
