from fastapi import FastAPI
from app.routers import user
from app.routers import nasdaq
from app.services.dynamodb_service import DynamoDBService
from app.services.dynamodb_service import create_dynamodb_tables

import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

app = FastAPI()

app.include_router(user.router)
app.include_router(nasdaq.router)

@app.on_event("startup")
async def startup_event():
    create_dynamodb_tables()
    logging.info("DynamoDB tables checked/created on startup")

@app.get("/")
async def read_root():
    return {"message": "Welcome to FastAPI-DynamoDB Application!"}
