from fastapi import APIRouter, HTTPException
from fastapi.responses import JSONResponse
from app.schemas.nasdaq import Nasdaq
from app.models.nasdaq import get_nasdaq_data
from fastapi import WebSocket, WebSocketDisconnect
from concurrent.futures import Future
from threading import Thread
from ncdssdk import NCDSClient
import pytz, asyncio, os, logging, json
from datetime import timedelta, datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/nasdaq",
    tags=["nasdaq"]
)

utc_datetime = datetime.utcnow()
# Set the desired timezone
desired_timezone = pytz.timezone('America/New_York')
# Convert the UTC time to the desired timezone
localized_datetime = utc_datetime.replace(
    tzinfo=pytz.utc).astimezone(desired_timezone)
midnight_time = datetime.combine(localized_datetime, datetime.min.time())

def call_with_future(fn, future, args, kwargs):
    try:
        result = fn(*args, **kwargs)
        future.set_result(result)
    except Exception as exc:
        future.set_exception(exc)


def threaded(fn):
    def wrapper(*args, **kwargs):
        future = Future()
        Thread(target=call_with_future, args=(
            fn, future, args, kwargs)).start()
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
        self.active_connections.append({
            "isRunning": False,
            "socket": websocket
        })

    async def send_personal_message(self, message: str, websocket: WebSocket):
        """Direct Message"""
        await websocket.send_text(message)

    def startStream(self, websocket: WebSocket):
        for idx, connection in enumerate(self.active_connections):
            if connection['socket'] == websocket:
                self.active_connections[idx]['isRunning'] = True
                break

    def stopStream(self, websocket: WebSocket):
        for idx, connection in enumerate(self.active_connections):
            if connection['socket'] == websocket:
                self.active_connections[idx]['isRunning'] = False
                break

    def disconnect(self, websocket: WebSocket):
        """disconnect event"""
        for connection in self.active_connections:
            if connection['socket'] == websocket:
                print('found')
                self.active_connections.remove(connection)
                break


manager = WebSocketManager()


@router.websocket("/get_real_data")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        while True:
            data = await websocket.receive_text()
            if data == 'start':
                manager.startStream(websocket)
            elif data == 'stop':
                manager.stopStream(websocket)
            await manager.send_personal_message(f"Received:{data}", websocket)
    except WebSocketDisconnect:
        await manager.send_personal_message("Bye!!!", websocket)
        manager.disconnect(websocket)


@router.post("/get_data")
async def get_nasdaq_data_by_date(request: Nasdaq):
    nasdaq_table_data = get_nasdaq_data(request.target_date, request.symbol)
    try:
        logging.info(f"Fetched table successfully")
        return JSONResponse(content=nasdaq_table_data, status_code=201)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


def makeRespFromKafkaMessages(messages):
    resp = {
        "headers": ["trackingID", "date", "msgType", "symbol", "price"],
        "data": []
    }
    for message in messages:
        msg = message.value()
        trackingID = int(msg['trackingID'])
        message_time = midnight_time + timedelta(milliseconds=trackingID / 1000000)
        resp['data'].append(([int(msg['trackingID']), message_time.strftime('%Y-%m-%d'), msg['msgType'], msg['symbol'], int(
                msg['price']) if 'price' in msg else -1]))
    return resp


def init_nasdaq_kafka_connection():
    print(os.getenv('NASDAQ_KAFKA_ENDPOINT'))
    security_cfg = {
        "oauth.token.endpoint.uri": os.getenv('NASDAQ_KAFKA_ENDPOINT'),
        "oauth.client.id": os.getenv('NASDAQ_KAFKA_CLIENT_ID'),
        "oauth.client.secret": os.getenv('NASDAQ_KAFKA_CLIENT_SECRET')
    }
    kafka_cfg = {
        "bootstrap.servers": os.getenv('NASDAQ_KAFKA_BOOTSTRAP_URL'),
        # "bootstrap.servers": "{streams_endpoint_url}:9094",
        "auto.offset.reset": "latest"
    }

    ncds_client = NCDSClient(security_cfg, kafka_cfg)
    topic = "NLSUTP"
    consumer = ncds_client.ncds_kafka_consumer(topic)
    return consumer
    # print(messages)


async def listen_message_from_nasdaq_kafka(consumer):
    while True:
        messages = consumer.consume(num_messages=2000, timeout=10)
        response = makeRespFromKafkaMessages(messages)
        for idx, connection in enumerate(manager.active_connections):
            if connection['isRunning']:
                webSocket = connection['socket']
                await webSocket.send_json(response)
        print(len(messages))

consumer = init_nasdaq_kafka_connection()


def between_callback():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    loop.run_until_complete(listen_message_from_nasdaq_kafka(consumer))
    loop.close()


nasdaq_kafka_thread = Thread(target=between_callback)
nasdaq_kafka_thread.start()