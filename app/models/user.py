from fastapi import HTTPException
from app.services.dynamodb_service import DynamoDBService
from application_logger import get_logger


logger = get_logger(__name__)


dynamodb = DynamoDBService.get_resource()

user_table = dynamodb.Table("Users")
user_settings_table = dynamodb.Table("UserSettings")


def save_user(user_data):
    # check if the user already exists
    if check_user_exists(user_data["email"]):
        raise Exception("User already exists")

    try:
        user_table.put_item(Item=user_data)
        logger.info("User saved successfully")
    except Exception as e:
        logger.error(f"Error saving user: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


def check_user_exists(email):
    try:
        response = user_table.get_item(Key={"email": email})
        logger.info("Checked if user exists")
    except Exception as e:
        logger.error(f"Error checking if user exists: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))
    return "Item" in response


def get_user(email):
    try:
        response = user_table.get_item(Key={"email": email})
        logger.info("User retrieved successfully")
    except Exception as e:
        logger.error(f"Error retrieving user: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))
    return response.get("Item")


def update_user_settings(email, settings):
    # check if the email exists to update the settings
    if not check_user_exists(email):
        raise Exception("User does not exist")

    try:
        user_settings_table.update_item(
            Key={"email": email},
            UpdateExpression="SET #t = :t, notifications = :n, #lang = :l",
            ExpressionAttributeNames={"#t": "theme", "#lang": "language"},
            ExpressionAttributeValues={
                ":t": settings.theme,
                ":n": settings.notifications,
                ":l": settings.language,
            },
        )
        logger.info("User settings updated successfully")
    except Exception as e:
        logger.error(f"Error updating user settings: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


def get_user_settings(email):
    try:
        response = user_settings_table.get_item(Key={"email": email})
        logger.info("User settings retrieved successfully")
    except Exception as e:
        logger.error(f"Error retrieving user settings: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

    return response.get("Item")
