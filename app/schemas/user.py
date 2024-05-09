from pydantic import BaseModel, EmailStr, Field
from typing import Optional, Dict

class User(BaseModel):
    username: str
    email: EmailStr

class UserSignUp(BaseModel):
    username: str
    email: EmailStr
    password: str

class UserSettings(BaseModel):
    theme: Optional[str] = Field("light", description="Theme preference: light or dark")
    notifications: bool = Field(True, description="Enable or disable notifications")
    language: Optional[str] = Field("en", description="Preferred language for UI")

class UpdateUserSettingsRequest(BaseModel):
    username: str
    settings: UserSettings

class UserLogin(BaseModel):
    username: str
    password: str
    

class UserLogout(BaseModel):
    username: str
