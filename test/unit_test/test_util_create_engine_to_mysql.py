"""Unit tests for src.util.create_engine_to_mysql module."""

import pytest  # 測試框架本體
from unittest.mock import patch, MagicMock  # patch 用來替換函數，MagicMock 用來偽造物件
from sqlalchemy.engine import Engine
import pymysql
from util.create_db_engine_or_database import (create_engine_to_mysql,
                                               get_pymysql_conn_to_mysql
                                               )


class TestCreateEngineToMysql:
    """Test suite for create_engine_to_mysql function."""

    # @patch(...)告訴Python"當執行到 src.util.create_db_engine_or_database"檔案裡的"create_engine()"時，
    # 請給我一個假的create_engine(）。不要真的呼叫 sqlalchemy 的 create_engine
    @patch("src.util.create_db_engine_or_database.create_engine")
    def test_create_engine_with_database(self, mock_create_engine):
        """Test engine creation with specified database."""
        # 1. Arrange: 建立一個假的Engine物件，並宣告它具備Engine的特徵
        mock_engine = MagicMock(spec=Engine)

        # 設定攔截點：當程式呼叫 create_engine 時，請回傳mock_engine
        mock_create_engine.return_value = mock_engine

        # 2. Act: 實際執行函數，但因為有@patch()，create_engine_to_mysql()中的create_engine會被攔截
        result = create_engine_to_mysql(database="traffic_accidents")

        # 3. Assert: 比對1&2的結果是否相等
        assert result == mock_engine  # 驗證函數回傳的是不是我們給的假對象。
        assert mock_create_engine.called  # 驗證：那個被攔截的函數是否真的有被呼叫到

    @patch("src.util.create_db_engine_or_database.create_engine")
    def test_create_engine_without_database(self, mock_create_engine):
        """Test engine creation without database."""
        mock_engine = MagicMock(spec=Engine)
        mock_create_engine.return_value = mock_engine

        result = create_engine_to_mysql(database=None)

        assert result == mock_engine


class TestGetPymysqlConnToMysql:
    """Test suite for get_pymysql_conn_to_mysql function."""

    @patch("src.util.create_db_engine_or_database.pymysql.connect")
    def test_get_pymysql_conn_with_database(self, mock_pymysql_connect):
        """Test pymysql connection with database."""
        mock_conn = MagicMock()
        mock_pymysql_connect.return_value = mock_conn

        result = get_pymysql_conn_to_mysql(database="traffic_accidents")

        assert result == mock_conn

    @patch("src.util.create_db_engine_or_database.pymysql.connect")
    def test_get_pymysql_conn_without_database(self, mock_pymysql_connect):
        """Test pymysql connection without database."""
        mock_conn = MagicMock()
        mock_pymysql_connect.return_value = mock_conn

        result = get_pymysql_conn_to_mysql(database=None)

        assert result == mock_conn

    @patch("src.util.create_db_engine_or_database.pymysql.connect")
    def test_get_pymysql_conn_charset_utf8mb4(self, mock_pymysql_connect):
        """Test utf8mb4 charset is configured."""
        mock_conn = MagicMock()
        mock_pymysql_connect.return_value = mock_conn

        get_pymysql_conn_to_mysql(database="traffic_accidents")

        # 取得當初呼叫 pymysql.connect 時傳進去的所有「關鍵字參數 (**kwargs)」
        kwargs = mock_pymysql_connect.call_args[1]

        assert kwargs["charset"] == "utf8mb4"  # 驗證編碼是否為 utf8mb4（防止中文亂碼）
        assert kwargs["autocommit"] is False   # 驗證是否關閉自動提交

    @patch("src.util.create_db_engine_or_database.pymysql.connect")
    def test_get_pymysql_conn_timeout_config(self, mock_pymysql_connect):
        """Test timeout configuration."""
        mock_conn = MagicMock()
        mock_pymysql_connect.return_value = mock_conn

        get_pymysql_conn_to_mysql(database="traffic_accidents")

        kwargs = mock_pymysql_connect.call_args[1]
        assert kwargs["connect_timeout"] == 60   # 驗證連線逾時
        assert kwargs["read_timeout"] == 600    # 驗證讀取逾時
        assert kwargs["write_timeout"] == 600   # 驗證寫入逾時

    @patch("src.util.create_db_engine_or_database.pymysql.connect")
    def test_get_pymysql_conn_connection_error(self, mock_pymysql_connect):
        """Test connection error can be correctly raised."""
        # 設定 side_effect：當呼叫這個函數時，不要回傳值，而是直接出一個錯誤
        mock_pymysql_connect.side_effect = pymysql.err.OperationalError(
            2003, "Connection failed")

        # 驗證：執行該函數時，是否真的有拋出預期的OperationalError
        with pytest.raises(pymysql.err.OperationalError):
            get_pymysql_conn_to_mysql(database="not_exist_db")
