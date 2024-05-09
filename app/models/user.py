from app.services.dynamodb_service import DynamoDBService
# from app.schemas.user import UserSettingsFields

dynamodb = DynamoDBService.get_resource()

user_table = dynamodb.Table('Users')
user_settings_table = dynamodb.Table('UserSettings')


def save_user(user_data):
    user_table.put_item(Item=user_data)


def get_user(username):
    response = user_table.get_item(Key={'username': username})
    return response.get('Item')


def update_user_settings(username, settings):
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
