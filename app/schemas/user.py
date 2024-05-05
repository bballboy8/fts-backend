from pydantic import BaseModel, EmailStr
from typing import Optional, Dict

class User(BaseModel):
    username: str
    email: EmailStr

class UserIn(BaseModel):
    username: str
    email: EmailStr
    password: str

class UserSettings(BaseModel):
    username: str
    settings: Dict
