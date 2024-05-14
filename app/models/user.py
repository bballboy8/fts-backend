from http.client import HTTPException
from app.services.dynamodb_service import DynamoDBService

import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

dynamodb = DynamoDBService.get_resource()

user_table = dynamodb.Table('Users')
user_settings_table = dynamodb.Table('UserSettings')

def save_user(user_data):
    
    # check if the user already exists
    if check_user_exists(user_data['username']):
        raise Exception("User already exists")
    
    user_table.put_item(Item=user_data)
    
    
def check_user_exists(username):
    try:
        response = user_table.get_item(Key={'username': username})
        logger.info('User exists and found in the database')
    except Exception as e:
        logger.error(f"Error checking if user exists: {e}")
        return HTTPException(status_code=500, detail=str(e))
    return response.get('Item') is not None


def get_user(username):
    try:
        response = user_table.get_item(Key={'username': username})
        logger.info('User exists and found in the database')
    except Exception as e:
        logger.error(f"Error checking if user exists: {e}")
        return HTTPException(status_code=500, detail=str(e))
    return response.get('Item')


def update_user_settings(username, settings):
    
    # check if the username exists to update the settings
    if not check_user_exists(username):
        raise Exception("User does not exist")
    
    user_settings_table.update_item(
        Key={'username': username},
        UpdateExpression="SET #t = :t, notifications = :n, #lang = :l",
        ExpressionAttributeNames={
            '#t': 'theme',
            '#lang': 'language'
        },
        ExpressionAttributeValues={
            ':t': settings.theme,
            ':n': settings.notifications,
            ':l': settings.language
        }
    )

def get_user_settings(username):
        
    try:
        response = user_settings_table.get_item(Key={'username': username})
        logger.info('User settings found in the database')
    except Exception as e:
        logger.error(f"Error getting user settings: {e}")
        return HTTPException(status_code=500, detail=str(e))
    
    return response.get('Item')
