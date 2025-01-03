
# Setting up a pre-commit hook to run Ruff linter

## Requirements

- Python - latest version
- pip installed

# FastAPI Project with DynamoDB

This project demonstrates a FastAPI application with authentication and user management using DynamoDB as the backend database.

## Setup

1. Install dependencies using `pip install -r requirements.txt`.
2. Run `python3 app/services/dynamodb_services.py` to create DynamoDB tables.
4. Start the application with `uvicorn app.main:app --reload`.

```

## Instructions

To set up a pre-commit hook to run the Ruff linter, ensure you have Python and Pip installed on your system.

1. Install pre-commit by executing the following command in your terminal:

    ```pip install pre-commit```


2. After installing pre-commit, activate the pre-commit hook for your repository by running:

    ```pre-commit install```


This will set up the pre-commit hook to run the Ruff linter before each commit, helping maintain code quality standards in your project.

.
