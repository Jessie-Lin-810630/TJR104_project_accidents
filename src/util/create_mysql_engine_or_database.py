from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
import pymysql
from pymysql import Connection
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


def get_pymysql_conn_to_mysql(database: str | None) -> Connection:
    """Create a pymysql Connection to connect to a MySQL database. More suitable for Upserting
    than using Pandas.to_sql().
    Parameters:
        database (str | None): The name of the database to connect to.

    Returns:
        Connection: A pymysql Connection instance connected to the specified MySQL database."""
    conn = pymysql.connect(host=host,
                           port=int(port),
                           user=username,
                           password=password,
                           database=database,
                           charset="utf8mb4",
                           autocommit=False,
                           connect_timeout=60,      # 連線建立超時
                           read_timeout=600,        # 讀取超時（適合大查詢）
                           write_timeout=600,       # 寫入超時
                           )
    return conn


def create_database(engine: Engine, database_name: str) -> None:
    """Inspect if the designed database exists and create it if not exists."""
    try:
        with engine.connect() as conn:
            conn.execute(text(
                f"CREATE DATABASE IF NOT EXISTS {database_name} CHARACTER SET utf8mb4;"))
            print(f"Database '{database_name}' created successfully.")
    except Exception as e:
        print(f"An error occurred while creating the database: {e}")
    finally:
        engine.dispose()
    return None
