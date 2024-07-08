from typing import Optional
from app.schemas.nasdaq import Nasdaq
from app.models.nasdaq import fetch_all_data, fetch_all_tickers
from fastapi import APIRouter, WebSocket, WebSocketDisconnect
from concurrent.futures import Future
from threading import Thread
from app.application_logger import get_logger
from ncdssdk import NCDSClient
import pytz, asyncio, os, logging
from datetime import timedelta, datetime

logger = get_logger(__name__)

router = APIRouter(prefix="/nasdaq", tags=["nasdaq"])

utc_datetime = datetime.utcnow()
# Set the desired timezone
desired_timezone = pytz.timezone("America/New_York")
# Convert the UTC time to the desired timezone
localized_datetime = utc_datetime.replace(tzinfo=pytz.utc).astimezone(desired_timezone)
midnight_time = datetime.combine(localized_datetime, datetime.min.time())


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


manager = WebSocketManager()


@router.websocket("/get_real_data")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        while True:
            data = await websocket.receive_text()
            if data == "start":
                manager.startStream(websocket)
            elif data == "stop":
                manager.stopStream(websocket)
            await manager.send_personal_message(f"Received:{data}", websocket)
    except WebSocketDisconnect:
        print("disconnected")
        manager.disconnect(websocket)
        # await manager.send_personal_message("Bye!!!", websocket)


@router.post("/get_data")
async def get_nasdaq_data_by_date(request: Optional[Nasdaq]):
    records = await fetch_all_data(request.symbol, request.start_datetime)
    return records


@router.get("/get_tickers")
async def get_tickers():
    records = await fetch_all_tickers()
    return records

@router.get("/get_connections")
async def get_connections():
    connections = []
    for idx, connection in enumerate(manager.active_connections):
        connections.append(connection["socket"]["client"])
    return connections

def makeRespFromKafkaMessages(messages):
    resp = {"headers": ["trackingID", "date", "msgType", "symbol", "price"], "data": []}
    for message in messages:
        msg = message.value()
        resp["data"].append(
            (
                [
                    int(msg["trackingID"]),
                    str(convert_tracking_id_to_timestamp(str(msg["trackingID"]))),
                    msg["msgType"],
                    msg["symbol"] if "symbol" in msg else "",
                    int(msg["price"]) if "price" in msg else -1,
                ]
            )
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


def init_nasdaq_kafka_connection():
    print(os.getenv("NASDAQ_KAFKA_ENDPOINT"))
    security_cfg = {
        "oauth.token.endpoint.uri": os.getenv("NASDAQ_KAFKA_ENDPOINT"),
        "oauth.client.id": os.getenv("NASDAQ_KAFKA_CLIENT_ID"),
        "oauth.client.secret": os.getenv("NASDAQ_KAFKA_CLIENT_SECRET"),
    }
    kafka_cfg = {
        "bootstrap.servers": os.getenv("NASDAQ_KAFKA_BOOTSTRAP_URL"),
        "auto.offset.reset": "latest",
    }

    ncds_client = NCDSClient(security_cfg, kafka_cfg)
    topic = "NLSUTP"
    consumer = ncds_client.ncds_kafka_consumer(topic)
    logger.info(f"Success to connect NASDAQ Kafka server.")
    return consumer


async def listen_message_from_nasdaq_kafka(consumer):
    logger.info("Starting listening messages from nasdaq kafka!")
    while True:
        messages = consumer.consume(num_messages=2000, timeout=10)
        response = makeRespFromKafkaMessages(messages)
        for idx, connection in enumerate(manager.active_connections):
            if connection["isRunning"]:
                webSocket = connection["socket"]
                try:
                    await webSocket.send_json(response)
                except Exception as e:
                    logger.error(
                        f"Error occurred while sending data to client: {e}",
                        exc_info=True,
                    )
                    logger.error(f"Total Connections: {len(manager.active_connections)}")


consumer = init_nasdaq_kafka_connection()


@router.on_event("startup")
async def startup_event():
    nasdaq_kafka_thread = Thread(target=between_callback)
    nasdaq_kafka_thread.start()


def between_callback():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(listen_message_from_nasdaq_kafka(consumer))
    finally:
        loop.close()
