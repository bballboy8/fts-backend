import asyncpg
from fastapi import HTTPException
from app.main import db_params
from application_logger import init_logger

logger = init_logger(__name__)


async def save_user(user_data):
    conn = await asyncpg.connect(**db_params)

    query = """
        INSERT INTO users (
            user_id, email, hashed_password, first_name, last_name,
            company_name, phone, address_1, address_2, city, state,
            region, postal_code, country, trading_experience
        ) VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15
        )
    """

    try:
        await conn.execute(
            query,
            user_data["user_id"],
            user_data["email"],
            user_data["hashed_password"],
            user_data["first_name"],
            user_data["last_name"],
            user_data["company_name"],
            user_data["phone"],
            user_data["address_1"],
            user_data["address_2"],
            user_data["city"],
            user_data["state"],
            user_data["region"],
            user_data["postal_code"],
            user_data["country"],
            user_data["trading_experience"],
        )
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
    finally:
        await conn.close()


async def check_user_exists(email):
    conn = await asyncpg.connect(**db_params)

    query = """
        SELECT 1
        FROM users
        WHERE email = $1
    """

    try:
        result = await conn.fetch(query, email)
        logger.info("Checked if user exists")
        return len(result) > 0
    except Exception as e:
        logger.error(f"Error checking if user exists: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        await conn.close()


async def get_user(email):
    conn = await asyncpg.connect(**db_params)

    query = """
        SELECT user_id, email, hashed_password, first_name, last_name,
               company_name, phone, address_1, address_2, city, state,
               region, postal_code, country, trading_experience
        FROM users
        WHERE email = $1
    """

    try:
        result = await conn.fetchrow(query, email)
        if result:
            logger.info("User retrieved successfully")
            return dict(result)
        else:
            raise HTTPException(status_code=404, detail="User not found")
    except Exception as e:
        logger.error(f"Error retrieving user: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        await conn.close()


async def update_user_settings(email, settings):
    conn = await asyncpg.connect(**db_params)

    # Check if the user exists
    if not await check_user_exists(email):
        raise HTTPException(status_code=404, detail="User does not exist")

    query = """
        UPDATE user_settings
        SET theme = $1, notifications = $2, language = $3
        WHERE email = $4
    """

    try:
        await conn.execute(
            query, settings.theme, settings.notifications, settings.language, email
        )
        logger.info("User settings updated successfully")
    except Exception as e:
        logger.error(f"Error updating user settings: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        await conn.close()


async def get_user_settings(email):
    conn = await asyncpg.connect(**db_params)

    query = """
        SELECT theme, notifications, language
        FROM user_settings
        WHERE email = $1
    """

    try:
        result = await conn.fetchrow(query, email)
        if result:
            logger.info("User settings retrieved successfully")
            return dict(result)
        else:
            raise HTTPException(status_code=404, detail="User settings not found")
    except Exception as e:
        logger.error(f"Error retrieving user settings: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        await conn.close()
