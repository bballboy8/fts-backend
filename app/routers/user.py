from fastapi import APIRouter, HTTPException
from fastapi.responses import JSONResponse
from app.auth.hashing import get_password_hash, verify_password
from app.auth.authentication import create_access_token
from app.models.user import (
    save_user,
    get_user,
    update_user_settings,
)  # , get_all_user_settings, get_all_users
from app.schemas.user import (
    UserSignUp,
    UserLogin,
    UpdateUserSettingsRequest,
    UserLogout,
    # BulkUser
)
import json

from app.application_logger import get_logger

logger = get_logger(__name__)

router = APIRouter(prefix="/user", tags=["user"])


@router.post("/exist_email")
async def email_existence(request: UserLogout):
    user = await get_user(request.email)

    if not user:
        logger.error(f"User {request.email} not exist on the database", exc_info=True)
        return {"exist": False}

    return {"exist": True}


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
        "trading_experience": json.dumps(user_in.trading_experience.model_dump()),
    }

    try:
        await save_user(user_data)
        logger.info(f"User {user_in.user_id} signed up successfully")
        return JSONResponse(
            content={"message": "User signed up successfully"}, status_code=201
        )

    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/login/")
async def login(user_login: UserLogin):
    # check if user is in database (simple for now)
    user = await get_user(user_login.email)

    # check if password is correct (done on frontend for now)
    if not user:
        logger.error(f"User {user_login.email} not found", exc_info=True)
        raise HTTPException(status_code=400, detail="User not found")

    if not verify_password(user_login.password, user["hashed_password"]):
        logger.error(f"User {user_login.email} incorrect password", exc_info=True)
        raise HTTPException(status_code=400, detail="Incorrect Password")

    access_token = create_access_token(data={"sub": user_login.email})
    message = f"User {user_login.email} logged in successfully"
    return {"access_token": access_token, "token_type": "bearer", "message": message}


@router.post("/settings/")
async def update_settings(request: UpdateUserSettingsRequest):
    try:
        await update_user_settings(
            request.email, json.dumps(request.settings.model_dump())
        )
        logger.info(f"User {request.email} settings updated")
    except Exception as e:
        logger.error(
            f"Error updating user {request.email} settings: {str(e)}", exc_info=True
        )
        raise HTTPException(status_code=400, detail=str(e))

    return {"message": "User settings updated"}


@router.post("/logout/")
async def logout(request: UserLogout):
    # check if user is in database (simple for now)
    user = await get_user(request.email)

    if not user:
        logger.error(f"User {request.email} not found to logout", exc_info=True)
        raise HTTPException(status_code=400, detail="User not found")

    return {"message": f"{request.email} logged out successfully"}


# @router.get("/get_users/")
# async def get_users():
#     # check if user is in database (simple for now)
#     users = await get_all_users()
#
#     if not users:
#         logger.error(f"Users not found", exc_info=True)
#         raise HTTPException(status_code=400, detail="Users not found")
#
#     return {"message": f"Users found", "data": users}


# @router.get("/get_users_settings/")
# async def get_users_settings():
#     # check if user is in database (simple for now)
#     users_settings = await get_all_user_settings()
#
#     if not users_settings:
#         logger.error(f"users_settings not found", exc_info=True)
#         raise HTTPException(status_code=400, detail="users_settings not found")
#
#     return {"message": f"users_settings found", "data": users_settings}
#
#
# @router.post("/bulk_insert/")
# async def bulk_user_insert(users: list[BulkUser]):
#     for user_in in users:
#         user_data = {
#             "user_id": user_in.user_id,
#             "email": user_in.email,
#             "hashed_password": user_in.hashed_password,  # as password is already hashed
#             "first_name": user_in.first_name,
#             "last_name": user_in.last_name,
#             "company_name": user_in.company_name,
#             "phone": user_in.phone,
#             "address_1": user_in.address_1,
#             "address_2": user_in.address_2,
#             "city": user_in.city,
#             "state": user_in.state,
#             "region": user_in.region,
#             "postal_code": user_in.postal_code,
#             "country": user_in.country,
#             "trading_experience": json.dumps(user_in.trading_experience.model_dump()),
#         }
#
#         try:
#             await save_user(user_data)
#             logger.info(f"User {user_in.user_id} signed up successfully")
#
#
#         except Exception as e:
#             raise HTTPException(status_code=400, detail=str(e))
#
#     return JSONResponse(
#         content={"message": "All user signed up successfully"}, status_code=201
#     )
