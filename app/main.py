from fastapi import FastAPI
from app.routers import user
from app.routers import nasdaq
from app.models.user import create_users_table, create_user_settings_table

import logging

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

app = FastAPI()

app.include_router(user.router)
app.include_router(nasdaq.router)


@app.on_event("startup")
async def startup_event():
    await create_users_table()
    logging.info("users tables checked/created on startup")

    await create_user_settings_table()
    logging.info("users tables checked/created on startup")


@app.get("/")
async def read_root():
    return {"message": "Welcome to FastAPI-DynamoDB Application!"}
