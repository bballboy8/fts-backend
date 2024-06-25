import logging
import logging.handlers
import os
import asyncpg
from datetime import datetime


db_params = {
    "dbname": os.getenv("dbname"),
    "user": os.getenv("user"),
    "password": os.getenv("password"),
    "host": os.getenv("host"),
    "port": "5432",
}


# Custom logging handler to log to PostgreSQL
class PostgresHandler(logging.Handler):
    def __init__(self, db_params):
        logging.Handler.__init__(self)
        self.db_params = db_params

    async def emit(self, record):
        log_entry = self.format(record)
        conn = await asyncpg.connect(**self.db_params)
        query = """
            INSERT INTO logs (log_level, log_message, log_time)
            VALUES ($1, $2, $3)
        """
        await conn.execute(query, record.levelname, log_entry, datetime.utcnow())
        await conn.close()


# Initialize logger
def get_logger(logger_name="my_app_logger"):
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.INFO)

    # File handler
    file_handler = logging.FileHandler("app.log")
    file_handler.setLevel(logging.INFO)
    file_formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    file_handler.setFormatter(file_formatter)
    logger.addHandler(file_handler)

    # PostgreSQL handler
    pg_handler = PostgresHandler(db_params)
    pg_handler.setLevel(logging.INFO)
    pg_formatter = logging.Formatter("%(message)s")
    pg_handler.setFormatter(pg_formatter)
    logger.addHandler(pg_handler)

    return logger
