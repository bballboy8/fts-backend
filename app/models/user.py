from http.client import HTTPException
from app.services.dynamodb_service import DynamoDBService
# from app.schemas.user import UserSettingsFields

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
    except Exception as e:
        return HTTPException(status_code=500, detail=str(e))
    return response.get('Item') is not None


def get_user(username):
    try:
        response = user_table.get_item(Key={'username': username})
    except Exception as e:
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
    response = user_settings_table.get_item(Key={'username': username})
    return response.get('Item')
