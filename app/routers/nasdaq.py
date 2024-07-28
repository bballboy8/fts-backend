import json
import os
from typing import Optional
import asyncpg
from fastapi import APIRouter, Request
from fastapi.responses import StreamingResponse
from app.schemas.nasdaq import Nasdaq
from app.models.nasdaq import fetch_all_data, fetch_all_tickers
from fastapi import WebSocket
from concurrent.futures import Future
from threading import Thread
from app.application_logger import get_logger
import pytz
from datetime import timedelta, datetime

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


def call_with_future(fn, future, args, kwargs):
    try:
        result = fn(*args, **kwargs)
        future.set_result(result)
    except Exception as exc:
        future.set_exception(exc)


def threaded(fn):
    def wrapper(*args, **kwargs):
        future = Future()
        Thread(target=call_with_future, args=(fn, future, args, kwargs)).start()
        return future

    return wrapper


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


# manager = WebSocketManager()


# @router.websocket("/get_real_data")
# async def websocket_endpoint(websocket: WebSocket):
#     await manager.connect(websocket)
#     try:
#         while True:
#             data = await websocket.receive_text()
#             if data == "start":
#                 manager.startStream(websocket)
#             elif data == "stop":
#                 manager.stopStream(websocket)
#             await manager.send_personal_message(f"Received:{data}", websocket)
#     except WebSocketDisconnect:
#         print("disconnected")
#         manager.disconnect(websocket)
#         # await manager.send_personal_message("Bye!!!", websocket)


def record_generator(records):
    for record in records:
        record_dict = dict(record)
        # Convert datetime objects to strings
        for key, value in record_dict.items():
            if isinstance(value, datetime):
                record_dict[key] = value.isoformat()
        yield json.dumps(record_dict) + "\n"


@router.post("/get_data")
async def get_nasdaq_data_by_date(request: Request):
    body = await request.json()
    symbol = body.get("symbol")
    start_datetime = body.get("start_datetime")

    logger.info("Starting get_nasdaq_data_by_date...")
    start_datetime = (
        datetime.strptime(start_datetime, "%Y-%m-%dT%H:%M") if start_datetime else None
    )

    pool = await asyncpg.create_pool(
        database=db_params["dbname"],
        user=db_params["user"],
        password=db_params["password"],
        host=db_params["host"],
        port=db_params["port"],
    )

    try:
        logger.info("Fetching data")
        records = await fetch_all_data(pool, symbol, start_datetime)
        logger.info("Returning records")
        return StreamingResponse(
            record_generator(records), media_type="application/json"
        )
    finally:
        await pool.close()


@router.get("/get_tickers")
async def get_tickers():
    records = await fetch_all_tickers()
    return records


# def makeRespFromKafkaMessages(messages):
#     resp = {"headers": ["trackingID", "date", "msgType", "symbol", "price"], "data": []}
#     for message in messages:
#         msg = message.value()
#         resp["data"].append(
#             (
#                 [
#                     int(msg["trackingID"]),
#                     str(convert_tracking_id_to_timestamp(str(msg["trackingID"]))),
#                     msg["msgType"],
#                     msg["symbol"] if "symbol" in msg else "",
#                     int(msg["price"]) if "price" in msg else -1,
#                 ]
#             )
#         )
#     return resp


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


# def init_nasdaq_kafka_connection():
#     print(os.getenv("NASDAQ_KAFKA_ENDPOINT"))
#     security_cfg = {
#         "oauth.token.endpoint.uri": os.getenv("NASDAQ_KAFKA_ENDPOINT"),
#         "oauth.client.id": os.getenv("NASDAQ_KAFKA_CLIENT_ID"),
#         "oauth.client.secret": os.getenv("NASDAQ_KAFKA_CLIENT_SECRET"),
#     }
#     kafka_cfg = {
#         "bootstrap.servers": os.getenv("NASDAQ_KAFKA_BOOTSTRAP_URL"),
#         # "bootstrap.servers": "{streams_endpoint_url}:9094",
#         "auto.offset.reset": "latest",
#     }

#     ncds_client = NCDSClient(security_cfg, kafka_cfg)
#     topic = "NLSUTP"
#     consumer = ncds_client.ncds_kafka_consumer(topic)
#     logger.info(f"Success to connect NASDAQ Kafka server.")
#     return consumer
#     # print(messages)


# async def listen_message_from_nasdaq_kafka(consumer):
#     while True:
#         messages = consumer.consume(num_messages=2000, timeout=10)
#         response = makeRespFromKafkaMessages(messages)
#         # print(len(manager.active_connections))
#         for idx, connection in enumerate(manager.active_connections):
#             if connection["isRunning"]:
#                 webSocket = connection["socket"]
#                 try:
#                     await webSocket.send_json(response)
#                 except Exception as e:
#                     logger.error(
#                         f"Error occured while sending data to client: {e}",
#                         exc_info=True,
#                     )


# consumer = init_nasdaq_kafka_connection()


# def between_callback():
#     loop = asyncio.new_event_loop()
#     asyncio.set_event_loop(loop)

#     loop.run_until_complete(listen_message_from_nasdaq_kafka(consumer))
#     loop.close()


# nasdaq_kafka_thread = Thread(target=between_callback)
# nasdaq_kafka_thread.start()
