import app.globals as global_vars


def save_user(user_data):
    if global_vars.dynamodb is None:
        raise RuntimeError("DynamoDB resource is not initialized.")
    
    user_table = global_vars.dynamodb.Table('Users')
    user_table.put_item(Item=user_data)


def get_user(username):
    if global_vars.dynamodb is None:
        raise RuntimeError("DynamoDB resource is not initialized.")
    
    user_table = global_vars.dynamodb.Table('Users')
    response = user_table.get_item(Key={'username': username})
    return response.get('Item')


def update_user_settings(username, settings):
    if global_vars.dynamodb is None:
        raise RuntimeError("DynamoDB resource is not initialized.")
    
    user_settings_table = global_vars.dynamodb.Table('UserSettings')
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
    if global_vars.dynamodb is None:
        raise RuntimeError("DynamoDB resource is not initialized.")
    
    user_settings_table = global_vars.dynamodb.Table('UserSettings')
    response = user_settings_table.get_item(Key={'username': username})
    return response.get('Item')
