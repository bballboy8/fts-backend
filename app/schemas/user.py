from pydantic import BaseModel, EmailStr
from typing import Optional, Dict

class User(BaseModel):
    username: str
    email: EmailStr

class UserSignUp(BaseModel):
    username: str
    email: EmailStr
    password: str

class UserSettings(BaseModel):
    username: str
    settings: Dict

class UserLogin(BaseModel):
    username: str
    password: str
