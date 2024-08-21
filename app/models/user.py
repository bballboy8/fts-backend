import os
import dotenv
import asyncpg
from fastapi import HTTPException
from app.application_logger import get_logger

dotenv.load_dotenv()

logger = get_logger(__name__)

db_params = {
    "database": os.getenv("dbname"),
    "user": os.getenv("user"),
    "password": os.getenv("password"),
    "host": os.getenv("host"),
    "port": "5432",
}


async def create_user_settings_table():
    conn = await asyncpg.connect(**db_params)

    try:
        create_table_query = """
        CREATE TABLE IF NOT EXISTS user_settings (
            email TEXT PRIMARY KEY,
            settings JSONB NOT NULL DEFAULT '{}'
        );
        """
        await conn.execute(create_table_query)
        logger.info("user_settings table created successfully")
    except Exception as e:
        logger.error(f"Error creating user_settings table: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        await conn.close()


async def create_users_table():
    conn = await asyncpg.connect(**db_params)

    try:
        create_table_query = """
        CREATE TABLE IF NOT EXISTS users (
            email TEXT PRIMARY KEY,
            user_id TEXT NOT NULL,
            first_name TEXT,
            last_name TEXT,
            company_name TEXT,
            address_1 TEXT,
            address_2 TEXT,
            city TEXT,
            state TEXT,
            postal_code TEXT,
            country TEXT,
            region TEXT,
            phone TEXT,
            hashed_password TEXT NOT NULL,
            trading_experience JSONB
        );
        """
        await conn.execute(create_table_query)
        logger.info("Users table created successfully")
    except Exception as e:
        logger.error(f"Error creating users table: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        await conn.close()


async def save_user(user_data):
    conn = await asyncpg.connect(**db_params)

    try:
        # Check if the user already exists
        exists = await check_user_exists(user_data["email"], conn)
        if exists:
            raise Exception("User already exists")

        # Save the user
        query = """
            INSERT INTO users (
                email,
                user_id,
                first_name,
                last_name,
                company_name,
                address_1,
                address_2,
                city,
                state,
                postal_code,
                country,
                region,
                phone,
                hashed_password,
                trading_experience
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15
            )
        """
        await conn.execute(
            query,
            user_data["email"],
            user_data["user_id"],
            user_data.get("first_name"),
            user_data.get("last_name"),
            user_data.get("company_name"),
            user_data.get("address_1"),
            user_data.get("address_2"),
            user_data.get("city"),
            user_data.get("state"),
            user_data.get("postal_code"),
            user_data.get("country"),
            user_data.get("region"),
            user_data.get("phone"),
            user_data["hashed_password"],
            user_data.get("trading_experience"),
        )
        logger.info("User saved successfully")
    except Exception as e:
        logger.error(f"Error saving user: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        await conn.close()


async def check_user_exists(email, conn=None):
    if conn is None:
        conn = await asyncpg.connect(**db_params)

    try:
        query = "SELECT 1 FROM users WHERE email = $1"
        result = await conn.fetchval(query, email)
        logger.info("Checked if user exists")
        return result is not None
    except Exception as e:
        logger.error(f"Error checking if user exists: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


async def get_user(email):
    conn = await asyncpg.connect(**db_params)

    try:
        query = "SELECT * FROM users WHERE email = $1"
        result = await conn.fetchrow(query, email)
        logger.info("User retrieved successfully")
        return dict(result) if result else None
    except Exception as e:
        logger.error(f"Error retrieving user: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        await conn.close()


async def update_user_settings(email, settings):
    conn = await asyncpg.connect(**db_params)

    try:
        # Check if the email exists to update the settings
        exists = await check_user_exists(email, conn)
        if not exists:
            raise Exception("User does not exist")

        # Upsert query to insert or update the user settings
        query = """
                INSERT INTO user_settings (email, settings)
                VALUES ($1, $2)
                ON CONFLICT (email)
                DO UPDATE SET
                    settings = EXCLUDED.settings
            """
        await conn.execute(query, email, settings)
        logger.info("User settings updated successfully")
    except Exception as e:
        logger.error(f"Error updating user settings: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        await conn.close()


async def get_user_settings(email):
    conn = await asyncpg.connect(**db_params)

    try:
        query = "SELECT * FROM user_settings WHERE email = $1"
        result = await conn.fetchrow(query, email)
        logger.info("User settings retrieved successfully")
        return dict(result) if result else None
    except Exception as e:
        logger.error(f"Error retrieving user settings: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        await conn.close()


async def get_all_users():
    conn = await asyncpg.connect(**db_params)

    try:
        query = "SELECT * FROM users"
        results = await conn.fetch(query)
        logger.info("All users retrieved successfully")
        return [dict(result) for result in results]
    except Exception as e:
        logger.error(f"Error retrieving all users: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        await conn.close()


async def get_all_user_settings():
    conn = await asyncpg.connect(**db_params)

    try:
        query = "SELECT * FROM user_settings"
        results = await conn.fetch(query)
        logger.info("All user settings retrieved successfully")
        return [dict(result) for result in results]
    except Exception as e:
        logger.error(f"Error retrieving all user settings: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        await conn.close()
