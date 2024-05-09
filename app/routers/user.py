from fastapi import APIRouter, HTTPException, status
from app.auth.hashing import get_password_hash, verify_password
from app.auth.authentication import create_access_token
from app.models.user import save_user, get_user, update_user_settings, get_user_settings
from app.schemas.user import UserSignUp, UserSettings, UserLogin, UpdateUserSettingsRequest, UserLogout

router = APIRouter(
    prefix="/user",
    tags=["user"]
)

@router.post("/signup/")
async def signup(user_in: UserSignUp):
    hashed_password = get_password_hash(user_in.password)
    user_data = {
        "username": user_in.username,
        "email": user_in.email,
        "hashed_password": hashed_password
    }
    
    try:
        save_user(user_data)
        # return a success message with the user's username and email if the user is successfully saved
        message = f"User {user_in.username} created successfully"
        return {"message": message, "username": user_in.username, "email": user_in.email}
        
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
    
@router.post("/login/")
async def login(user_login: UserLogin):
    user = get_user(user_login.username)
    if not user or not verify_password(user_login.password, user['hashed_password']):
        raise HTTPException(status_code=400, detail="Incorrect username or password")
    access_token = create_access_token(data={"sub": user_login.username})
    return {"access_token": access_token, "token_type": "bearer"}

@router.post("/settings/")
async def update_settings(request: UpdateUserSettingsRequest):
    
    try:
        update_user_settings(request.username, request.settings)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
    
    return {"message": "User settings updated"}

@router.post("/logout/")
async def logout(request: UserLogout):
    
    # check if user is in database (simple for now)
    user = get_user(request.username)
    if not user:
        raise HTTPException(status_code=400, detail="User not found")
        
    return {"message": f"{request.username} logged out successfully"}
