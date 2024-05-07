from fastapi import FastAPI
from app.routers import user

app = FastAPI()

app.include_router(user.router)

@app.get("/")
async def read_root():
    return {"message": "Welcome to FastAPI-DynamoDB Application!"}
