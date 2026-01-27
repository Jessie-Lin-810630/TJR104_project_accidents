# 取得Schema的欄位資訊
from sqlalchemy import create_engine, inspect
import os
from dotenv import load_dotenv

load_dotenv()
username = os.getenv("mysqllocal_username")
password = os.getenv("mysqllocal_password")
server = "127.0.0.1:3306"
DB = "TESTDB"
engine = create_engine(f"mysql+pymysql://{username}:{password}@{server}/{DB}",)


inspector = inspect(engine)
table_columns = [col['name']
                 for col in inspector.get_columns('weather_historical_data')]
print(table_columns)
