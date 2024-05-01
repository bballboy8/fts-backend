# main.py
from fastapi import FastAPI
from .routes.example_route import router as example_router

app = FastAPI()

app.include_router(example_router)
