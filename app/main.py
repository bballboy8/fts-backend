from fastapi import FastAPI
from app.routers import user
from app.services.dynamodb_service import get_dynamodb_resource
import app.globals as global_vars

app = FastAPI()

app.include_router(user.router)


@app.on_event("startup")
async def startup_event():
    try:
        global_vars.dynamodb = get_dynamodb_resource()
        print("DynamoDB resource initialized:", global_vars.dynamodb)
    except Exception as e:
        print("Error during DynamoDB initialization:", str(e))


@app.get("/")
async def read_root():
    return {"message": "Welcome to FastAPI-DynamoDB Application!"}
