from typing import Optional
from app.schemas.nasdaq import Nasdaq
from app.models.nasdaq import fetch_all_data, fetch_all_tickers
from fastapi import APIRouter, WebSocket, WebSocketDisconnect
from starlette.websockets import WebSocketState
from threading import Thread
from app.application_logger import get_logger
from ncdssdk import NCDSClient
import pytz
import asyncio
import os
from datetime import timedelta, datetime
import random
import time

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


# New manager for Sample Data Symbol AAPL
manager_dummy = WebSocketManager()


# Existing manager for NLSUTP
manager_utp = manager_dummy

# New manager for NLSCTA
manager_cta = manager_dummy


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


@router.websocket("/get_real_data_dummy")
async def websocket_endpoint_dummy(websocket: WebSocket):
    await manager_dummy.connect(websocket)
    try:
        while True:
            data = await websocket.receive_text()
            if data == "start":
                manager_dummy.startStream(websocket)
            elif data == "stop":
                manager_dummy.stopStream(websocket)
            await manager_dummy.send_personal_message(f"Received:{data}", websocket)
    except WebSocketDisconnect:
        print("disconnected")
        manager_dummy.disconnect(websocket)
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


def makeRespFromKafkaMessages(messages, is_dummy=False):
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
        ],
        "data": [],
    }

    # Get current date and time if is_dummy is True
    current_timestamp = (
        datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f") if is_dummy else None
    )

    for message in messages:
        msg = message.value()
        resp["data"].append(
            (
                [
                    int(msg["trackingID"]),
                    current_timestamp
                    if is_dummy
                    else str(convert_tracking_id_to_timestamp(str(msg["trackingID"]))),
                    msg["msgType"],
                    msg["symbol"] if "symbol" in msg else "",
                    int(msg["price"]) if "price" in msg else -1,
                    msg.get("SoupPartition"),
                    msg.get("SoupSequence"),
                    msg.get("marketCenter"),
                    msg.get("securityClass"),
                    msg.get("controlNumber"),
                    msg.get("size"),
                    msg.get("saleCondition"),
                    msg.get("cosolidatedVolume"),
                ]
            )
        )
    return resp


# Initialize a price dictionary to maintain the state of the prices for each symbol
price_state = {}


# Convert tracking ID to timestamp
def generate_dummy_message(symbol):
    # Define the price range for the dummy messages
    min_price = 2257000
    max_price = 2357000

    # Cycle price within the range for the symbol
    if symbol not in price_state:
        price_state[symbol] = random.randint(min_price, max_price)
    else:
        # Increment or reset the price within the range
        if price_state[symbol] >= max_price:
            price_state[symbol] = min_price
        else:
            price_state[symbol] += random.randint(1, 5)

    # Generate a valid 14-digit tracking ID
    current_time_millis = int(time.time() * 1000)
    tracking_id = str(current_time_millis).ljust(
        14, "0"
    )  # Pad with zeros to ensure 14 digits

    # Generate a dummy message
    message = {
        "trackingID": tracking_id,
        "msgType": "T",
        "symbol": symbol,
        "price": price_state[symbol],
        "SoupPartition": random.randint(1, 10),
        "SoupSequence": random.randint(1, 100),
        "marketCenter": "Q",
        "securityClass": "Q",
        "controlNumber": random.randint(1, 100000),
        "size": random.randint(1, 1000),
        "saleCondition": "@FTo",
        "cosolidatedVolume": random.randint(1, 10000),
    }

    return DummyMessage(message)


class DummyMessage:
    def __init__(self, message):
        self.message = message

    def value(self):
        return self.message


async def listen_message_from_nasdaq_kafka(manager, topic):
    if topic == "DUMMY":
        records = await fetch_all_tickers()
        symbols = [rec["symbol"] for rec in records]
        while True:
            try:
                # Add a delay of 0.1 seconds before the next loop iteration
                time.sleep(0.1)
                # Generate Dummy messages for all symbols
                dummy_messages = []
                for symbol in symbols:
                    dummy_messages.extend(
                        [
                            generate_dummy_message(symbol)
                            for _ in range(random.randint(1, 3))
                        ]
                    )

                response = makeRespFromKafkaMessages(dummy_messages, is_dummy=True)
                for idx, connection in enumerate(manager.active_connections):
                    if connection["isRunning"]:
                        webSocket = connection["socket"]
                        # Check if the WebSocket is still connected
                        if webSocket.application_state == WebSocketState.CONNECTED:
                            try:
                                await webSocket.send_json(response)
                                # logger.info(f"Sent data to client {webSocket}.")
                            except Exception as e:
                                logger.error(
                                    f"Error occurred while sending data to client: {e}",
                                    exc_info=True,
                                )

            except Exception as e:
                logger.error(f"Error in dummy data websocket: {e}", exc_info=True)

        return

    consumer = None
    logger.info(f"Starting listening messages from nasdaq kafka for topic {topic}!")
    while True:
        try:
            if not consumer:
                consumer = init_nasdaq_kafka_connection(topic)
            messages = consumer.consume(num_messages=2000, timeout=0.1)
            response = makeRespFromKafkaMessages(messages)
            for idx, connection in enumerate(manager.active_connections):
                if connection["isRunning"]:
                    webSocket = connection["socket"]
                    # Check if the WebSocket is still connected
                    if webSocket.application_state == WebSocketState.CONNECTED:
                        try:
                            await webSocket.send_json(response)
                            # logger.info(f"Sent data to client {webSocket}.")
                        except Exception as e:
                            logger.error(
                                f"Error occurred while sending data to client: {e}",
                                exc_info=True,
                            )
                            logger.error(
                                f"Total Connections: {len(manager.active_connections)}"
                            )
                            logger.info(f"In except, this is the response: {response}")
                            consumer = None
        except Exception as e:
            logger.error(f"Error in consuming: {e}", exc_info=True)
            consumer = None


@router.on_event("startup")
async def startup_event():
    # Start thread for NLSUTP
    # nasdaq_kafka_thread_utp = Thread(
    #     target=between_callback, args=(manager_utp, "NLSUTP")
    # )
    # nasdaq_kafka_thread_utp.start()

    # Start thread for dummy
    nasdaq_kafka_thread_dummy = Thread(
        target=between_callback, args=(manager_dummy, "DUMMY")
    )
    nasdaq_kafka_thread_dummy.start()


def between_callback(manager, topic):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(listen_message_from_nasdaq_kafka(manager, topic))
    finally:
        loop.close()
