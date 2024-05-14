from fastapi import APIRouter, HTTPException, status
from app.auth.hashing import get_password_hash, verify_password
from app.auth.authentication import create_access_token
from app.models.user import save_user, get_user, update_user_settings, get_user_settings
from app.schemas.user import UserSignUp, UserSettings, UserLogin, UpdateUserSettingsRequest, UserLogout

import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/user",
    tags=["user"]
)

@router.post("/signup/")
async def signup(user_in: UserSignUp):
    hashed_password = get_password_hash(user_in.password)
    user_data = {
        "user_id": user_in.user_id,
        "email": user_in.email,
        "hashed_password": hashed_password,
        "first_name": user_in.first_name,
        "last_name": user_in.last_name,
        "company_name": user_in.company_name,
        "phone": user_in.phone,
        "address_1": user_in.address_1,
        "address_2": user_in.address_2,
        "city": user_in.city,
        "state": user_in.state,
        "region": user_in.region,
        "postal_code": user_in.postal_code,
        "country": user_in.country,
        "trading_experience": user_in.trading_experience.model_dump()
    }
    
    try:
        save_user(user_data)
        logging.info(f"User {user_in.user_id} signed up successfully")
        return {"user_id": user_in.user_id, "email": user_in.email}
        
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
    

@router.post("/login/")
async def login(user_login: UserLogin):
    
    # check if user is in database (simple for now)
    user = get_user(user_login.username)
    
    # check if password is correct (done on frontend for now)
    if not user:
        logging.error(f"User {user_login.username} not found")
        raise HTTPException(status_code=400, detail="User not found")
    
    access_token = create_access_token(data={"sub": user_login.username})
    message = f"User {user_login.username} logged in successfully"
    return {"access_token": access_token, "token_type": "bearer", "message": message}

@router.post("/settings/")
async def update_settings(request: UpdateUserSettingsRequest):
    
    try:
        update_user_settings(request.username, request.settings)
        logging.info(f"User {request.username} settings updated")
    except Exception as e:
        logging.error(f"Error updating user {request.username} settings: {str(e)}")
        raise HTTPException(status_code=400, detail=str(e))
    
    return {"message": "User settings updated"}

@router.post("/logout/")
async def logout(request: UserLogout):
    
    # check if user is in database (simple for now)
    user = get_user(request.username)
    
    if not user:
        logging.error(f"User {request.username} not found to logout")
        raise HTTPException(status_code=400, detail="User not found")
        
    return {"message": f"{request.username} logged out successfully"}
