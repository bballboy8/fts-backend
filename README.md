
# Setting up a pre-commit hook to run Ruff linter

## Requirement



# FastAPI Project with DynamoDB

This project demonstrates a FastAPI application with authentication and user management using DynamoDB as the backend database.

## Setup

1. Install dependencies using `pip install -r requirements.txt`.
2. Run `app/services/dynamodb_services.py` to create DynamoDB tables.
4. Start the application with `uvicorn app.main:app --reload`.

## Endpoints

### `/user/signup/`
**Method:** `POST`  
**Description:** Register a new user.

**Request Body:**
```json
{
  "username": "new_user",
  "email": "new_user@example.com",
  "password": "secure_password"
}
```

### `/user/login/`
**Method:** `POST`  
**Description:** Log in with username and password, and receive a JWT token.

**Request Body:**
```json
{
  "username": "new_user",
  "password": "secure_password"
}
```

### `/user/settings/`
**Method:** `POST`  
**Description:** Update user settings.

**Request Body:**
```json
{
  "username": "new_user",
  "settings": {
    "name": "new_user",
    "company": "xyz"
  }
}
```
