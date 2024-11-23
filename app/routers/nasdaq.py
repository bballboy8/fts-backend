from typing import Optional
from app.schemas.nasdaq import Nasdaq
from app.models.nasdaq import fetch_all_data, fetch_all_tickers
from fastapi import APIRouter, WebSocket, WebSocketDisconnect
from threading import Thread
from app.application_logger import get_logger
from ncdssdk import NCDSClient
import pytz
import os
import dotenv
from datetime import timedelta, datetime
import pandas as pd
import time
import random
import asyncio
from fastapi import HTTPException
from bs4 import BeautifulSoup
import requests

dotenv.load_dotenv()
logger = get_logger(__name__)

router = APIRouter(prefix="/nasdaq", tags=["nasdaq"])

utc_datetime = datetime.utcnow()
# Set the desired timezone
desired_timezone = pytz.timezone("America/New_York")
# Convert the UTC time to the desired timezone
localized_datetime = utc_datetime.replace(tzinfo=pytz.utc).astimezone(desired_timezone)
midnight_time = datetime.combine(localized_datetime, datetime.min.time())
dummy_symbols_price_range = pd.read_csv("app/routers/dummy_data.csv")
send_dummy_data = os.getenv("SEND_DUMMY_DATA", "true") == "true"

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
                # Parse the date into a datetime object
                date_parts = date_str.split(",")
                month_day = (
                    date_parts[1].strip().split("*")[0].split("(")[0]
                ).strip()  # Clean extra symbols like *, ()
                month, day = month_day.split(" ")

                # Construct the full date string
                full_date_str = f"{year} {month} {day.strip()}"
                # Parse the full date string
                date_time = datetime.strptime(full_date_str, "%Y %B %d")

                # Format the datetime object
                formatted_date_time = date_time.strftime("%Y-%m-%d")

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


class WebSocketManager:
    """Class defining socket events"""

    def __init__(self):
        """init method, keeping track of connections"""
        self.active_connections = []
        self.isRunning = False

    async def connect(self, websocket: WebSocket):
        """connect event"""
        await websocket.accept()
        self.active_connections.append({"isRunning": False, "socket": websocket})

    async def send_personal_message(self, message: str, websocket: WebSocket):
        """Direct Message"""
        await websocket.send_text(message)

    def startStream(self, websocket: WebSocket):
        for idx, connection in enumerate(self.active_connections):
            if connection["socket"] == websocket:
                self.active_connections[idx]["isRunning"] = True
                break

    def stopStream(self, websocket: WebSocket):
        for idx, connection in enumerate(self.active_connections):
            if connection["socket"] == websocket:
                self.active_connections[idx]["isRunning"] = False
                break

    def disconnect(self, websocket: WebSocket):
        """disconnect event"""
        for connection in self.active_connections:
            if connection["socket"] == websocket:
                print("found")
                self.active_connections.remove(connection)
                break


# Existing manager for NLSUTP
manager_utp = WebSocketManager()

# New manager for NLSCTA
manager_cta = WebSocketManager()


@router.websocket("/get_real_data_utp")
async def websocket_endpoint_utp(websocket: WebSocket):
    await manager_utp.connect(websocket)
    try:
        while True:
            data = await websocket.receive_text()
            if data == "start":
                manager_utp.startStream(websocket)
            elif data == "stop":
                manager_utp.stopStream(websocket)
            await manager_utp.send_personal_message(f"Received:{data}", websocket)
    except WebSocketDisconnect:
        print("disconnected")
        manager_utp.disconnect(websocket)
        # await manager_utp.send_personal_message("Bye!!!", websocket)


@router.websocket("/get_real_data_cta")
async def websocket_endpoint_cta(websocket: WebSocket):
    await manager_cta.connect(websocket)
    try:
        while True:
            data = await websocket.receive_text()
            if data == "start":
                manager_cta.startStream(websocket)
            elif data == "stop":
                manager_cta.stopStream(websocket)
            await manager_cta.send_personal_message(f"Received:{data}", websocket)
    except WebSocketDisconnect:
        print("disconnected")
        manager_cta.disconnect(websocket)
        # await manager_cta.send_personal_message("Bye!!!", websocket)


@router.post("/get_data")
async def get_nasdaq_data_by_date(request: Optional[Nasdaq]):
    records = await fetch_all_data(request.symbol, request.start_datetime)
    return records


@router.get("/get_tickers")
async def get_tickers():
    records = await fetch_all_tickers()
    return records


@router.get("/get_connections_utp")
async def get_connections_utp():
    connections = []
    for idx, connection in enumerate(manager_utp.active_connections):
        connections.append(connection["socket"]["client"])
    return connections


@router.get("/get_connections_cta")
async def get_connections_cta():
    connections = []
    for idx, connection in enumerate(manager_cta.active_connections):
        connections.append(connection["socket"]["client"])
    return connections


def makeRespFromKafkaMessages(messages):
    resp = {
        "headers": [
            "trackingID",
            "date",
            "msgType",
            "symbol",
            "price",
            "soup_partition",
            "soup_sequence",
            "market_center",
            "security_class",
            "control_number",
            "size",
            "sale_condition",
            "consolidated_volume",
            "color",  # Adding the new field
        ],
        "data": [],
    }

    # Dictionary to track the latest price for each symbol
    latest_prices = {}

    for message in messages:
        msg = message.value()
        symbol = msg["symbol"] if "symbol" in msg else ""
        price = int(msg["price"]) if "price" in msg else -1
        msg_type = msg["msgType"]

        # Determine the color based on the msgType and price
        if msg_type == "H":
            color = "yellow"
            if symbol in latest_prices:
                price = latest_prices[symbol]
            else:
                color = "black"
        else:
            if symbol in latest_prices:
                if price > latest_prices[symbol]:
                    color = "green"
                elif price < latest_prices[symbol]:
                    color = "red"
                else:
                    color = "green"  # Default for unchanged price
            else:
                color = "black"  # Default for first occurrence of symbol

        # Update the latest price for the symbol
        if price != -1:
            latest_prices[symbol] = price

        # Add the record to the response
        resp["data"].append(
            [
                int(msg["trackingID"]),
                str(convert_tracking_id_to_timestamp(str(msg["trackingID"]))),
                msg_type,
                symbol,
                price,
                msg.get("SoupPartition"),
                msg.get("SoupSequence"),
                msg.get("marketCenter"),
                msg.get("securityClass"),
                msg.get("controlNumber"),
                msg.get("size"),
                msg.get("saleCondition"),
                msg.get("cosolidatedVolume"),
                color,
            ]
        )

    return resp


def convert_tracking_id_to_timestamp(tracking_id: str) -> datetime:
    # Ensure the tracking ID is a string of digits
    if not tracking_id.isdigit() or len(tracking_id) != 14:
        raise ValueError("Invalid tracking ID format")

    # Extract bytes 2-7, which represent the timestamp (6 bytes in this case)
    timestamp_bytes = tracking_id[:]

    # Convert the extracted bytes to an integer representing nanoseconds from midnight
    nanoseconds_from_midnight = int(timestamp_bytes)

    # Calculate the time of day from the nanoseconds
    seconds_from_midnight = nanoseconds_from_midnight / 1e9
    time_of_day = timedelta(seconds=seconds_from_midnight)

    # Assume the date is today for simplicity, adjust as needed
    today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)

    # Add the time of day to the current date
    timestamp = today + time_of_day

    return timestamp


def init_nasdaq_kafka_connection(topic):
    print(os.getenv("NASDAQ_KAFKA_ENDPOINT"))
    security_cfg = {
        "oauth.token.endpoint.uri": os.getenv("NASDAQ_KAFKA_ENDPOINT"),
        "oauth.client.id": os.getenv("NASDAQ_KAFKA_CLIENT_ID"),
        "oauth.client.secret": os.getenv("NASDAQ_KAFKA_CLIENT_SECRET"),
    }
    kafka_cfg = {
        "bootstrap.servers": os.getenv("NASDAQ_KAFKA_BOOTSTRAP_URL"),
        "auto.offset.reset": "latest",
        "socket.keepalive.enable": True,
    }

    ncds_client = NCDSClient(security_cfg, kafka_cfg)
    consumer = ncds_client.ncds_kafka_consumer(topic)
    logger.info(f"Success to connect NASDAQ Kafka server for topic {topic}.")
    return consumer


def is_market_open():
    """Check if the market is open based on EST time."""
    est = pytz.timezone("America/New_York")
    now_est = datetime.now(est)

    # Check if today is a weekend
    if now_est.weekday() >= 5:  # 5 = Saturday, 6 = Sunday
        return False

    # Market hours are from 4 AM to 8 PM EST
    market_open_time = now_est.replace(hour=4, minute=0, second=0, microsecond=0)
    market_close_time = now_est.replace(hour=20, minute=0, second=0, microsecond=0)
    return True
    return market_open_time <= now_est < market_close_time


def generate_dummy_data():
    """Generate dummy data that simulates the market being open."""
    # Get current datetime
    est = pytz.timezone("America/New_York")
    current_datetime = datetime.now(est)

    # Extract the hour from the current datetime
    hour = current_datetime.hour

    # Adjust the datetime based on the provided conditions
    if 20 <= hour <= 23:
        # If between 8 PM and midnight, adjust the hour and change date to next day
        adjusted_hour = hour - 16  # 20 -> 4, 21 -> 5, 22 -> 6, 23 -> 7
        # Handle day rollover
        current_datetime = current_datetime.replace(hour=adjusted_hour) + timedelta(
            days=1
        )
    elif 0 <= hour < 4:
        # If between midnight and 4 AM, adjust the hour only
        adjusted_hour = hour + 8  # 0 -> 8, 1 -> 9, 2 -> 10, 3 -> 11
        current_datetime = current_datetime.replace(hour=adjusted_hour)

    # Format the adjusted datetime to the desired format
    current_timestamp = current_datetime.strftime("%Y-%m-%d %H:%M:%S.%f")

    return {
        "headers": [
            "trackingID",
            "date",
            "msgType",
            "symbol",
            "price",
            "soup_partition",
            "soup_sequence",
            "market_center",
            "security_class",
            "control_number",
            "size",
            "sale_condition",
            "consolidated_volume",
        ],
        "data": [
            [
                random.randint(
                    10000000000000, 99999999999999
                ),  # dummy trackingID (14-digit)
                current_timestamp,
                "T",  # example message type
                row["symbol"],  # symbol from DataFrame
                random.randint(
                    int(row["lower_price"]), int(row["higher_price"])
                ),  # price in dollars
                "0",  # dummy soup_partition
                "0",  # dummy soup_sequence
                "Q",  # dummy market_center
                "Q",  # dummy security_class
                "001",  # dummy control_number
                random.randint(int(row["lower_size"]), int(row["higher_size"])),  # size
                "@",  # dummy sale_condition
                random.randint(1000, 1000000),  # dummy consolidated_volume
            ]
            for index, row in dummy_symbols_price_range.iterrows()
        ],
    }


async def listen_message_from_nasdaq_kafka(manager, topic):
    consumer = None
    logger.info(f"Starting listening messages from nasdaq kafka for topic {topic}!")
    while True:
        try:
            if send_dummy_data and not is_market_open():
                if not any(
                    connection["isRunning"] for connection in manager.active_connections
                ):
                    time.sleep(0.5)
                    continue
                time.sleep(0.5)
                # Market is closed; send dummy data
                response = generate_dummy_data()
                logger.info("Market closed. Sending dummy data.")
            else:
                # Market is open; consume real data
                if not consumer:
                    consumer = init_nasdaq_kafka_connection(topic)
                    logger.info("Market open. Sending real data.")
                messages = consumer.consume(num_messages=2000, timeout=0.1)
                response = makeRespFromKafkaMessages(messages)
            for idx, connection in enumerate(manager.active_connections):
                if connection["isRunning"]:
                    webSocket = connection["socket"]
                    try:
                        await webSocket.send_json(response)
                    except Exception as e:
                        logger.error(
                            f"Total Connections: {len(manager.active_connections)}\nError occurred while sending data to client: {e}",
                            exc_info=True,
                        )
                        logger.info(f"In except, this is the response: {response}")
        except Exception as e:
            logger.error(f"Error in consuming: {e}", exc_info=True)
            consumer = None


@router.on_event("startup")
async def startup_event():
    # Start thread for NLSUTP
    nasdaq_kafka_thread_utp = Thread(
        target=between_callback, args=(manager_utp, "NLSUTP")
    )
    nasdaq_kafka_thread_utp.start()

    # Start thread for NLSCTA
    nasdaq_kafka_thread_cta = Thread(
        target=between_callback, args=(manager_cta, "NLSCTA")
    )
    nasdaq_kafka_thread_cta.start()


def between_callback(manager, topic):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(listen_message_from_nasdaq_kafka(manager, topic))
    finally:
        loop.close()


if __name__ == "__main__":
    output = fetch_holidays()
    print(output)
