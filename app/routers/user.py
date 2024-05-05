from fastapi import APIRouter, HTTPException, status
from app.auth.hashing import get_password_hash, verify_password
from app.auth.authentication import create_access_token
from app.models.user import save_user, get_user, update_user_settings
from app.schemas.user import UserIn, UserSettings

router = APIRouter(
    prefix="/user",
    tags=["user"]
)

@router.post("/signup/")
async def signup(user_in: UserIn):
    hashed_password = get_password_hash(user_in.password)
    user_data = {
        "username": user_in.username,
        "email": user_in.email,
        "hashed_password": hashed_password
    }
    save_user(user_data)
    return {"username": user_in.username, "email": user_in.email}

@router.post("/login/")
async def login(username: str, password: str):
    user = get_user(username)
    if not user or not verify_password(password, user['hashed_password']):
        raise HTTPException(status_code=400, detail="Incorrect username or password")
    access_token = create_access_token(data={"sub": username})
    return {"access_token": access_token, "token_type": "bearer"}

@router.post("/settings/")
async def update_settings(settings: UserSettings):
    update_user_settings(settings.username, settings.settings)
    return {"message": "Settings updated"}
