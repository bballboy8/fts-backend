import os
db_params = {
    "dbname": os.getenv("dbname"),
    "database": os.getenv("dbname"),
    "user": os.getenv("user"),
    "password": os.getenv("password"),
    "host": os.getenv("host"),
    "port": "5432",
}