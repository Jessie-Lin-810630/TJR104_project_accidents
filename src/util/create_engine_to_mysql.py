from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
import os
from dotenv import load_dotenv
from urllib.parse import quote_plus

load_dotenv()
host = os.getenv("MYSQL_HOST")
port = os.getenv("MYSQL_PORT")
username = quote_plus(os.getenv("MYSQL_USER"))
password = quote_plus(os.getenv("MYSQL_PASSWORD"))


def create_engine_to_mysql(database: str | None = None) -> Engine:
    """
    Create a SQLAlchemy engine to connect to a MySQL database.

    Parameters:
        database (str | None): The name of the database to connect to.

    Returns:
        Engine: A SQLAlchemy Engine instance connected to the specified MySQL database.
    """

    # Create the connection string
    if database:
        connection_url = f"mysql+pymysql://{username}:{password}@{host}:{port}/{database}?charset=utf8mb4"
    else:
        connection_url = f"mysql+pymysql://{username}:{password}@{host}:{port}/?charset=utf8mb4"
    # Create and return the SQLAlchemy engine
    engine = create_engine(connection_url, echo=False,
                           pool_size=5, pool_recycle=3600, pool_pre_ping=True,
                           connect_args={"connect_timeout": 120})
    return engine
