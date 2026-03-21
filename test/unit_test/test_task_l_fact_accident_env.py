"""
Unit tests for src.task.l_fact_accident_env module.
"""

import pytest
import pandas as pd
from unittest.mock import patch, MagicMock
from src.task.l_fact_accident_env import l_fact_accident_env


class TestLFactAccidentEnv:
    """Test suite for l_fact_accident_env function."""

    def test_successful_insert(self):
        """Test successful data insertion."""
        test_df = pd.DataFrame({
            "accident_id": [1, 2],
            "weather_condition": ["晴", "雨"],
            "visibility": [1000, 500]
        })

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        with patch("src.task.l_fact_accident_env.get_pymysql_conn_to_mysql", return_value=mock_conn):
            l_fact_accident_env(test_df, "test_db")

            mock_conn.cursor.assert_called_once()
            mock_cursor.executemany.assert_called_once()
            mock_conn.commit.assert_called_once()
            mock_cursor.close.assert_called_once()
            mock_conn.close.assert_called_once()

    def test_database_error_handling(self):
        """Test handling of database errors."""
        test_df = pd.DataFrame({
            "accident_id": [1],
            "weather_condition": ["晴"],
            "visibility": [1000]
        })

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.executemany.side_effect = Exception("Database error")

        with patch("src.task.l_fact_accident_env.get_pymysql_conn_to_mysql", return_value=mock_conn):
            l_fact_accident_env(test_df, "test_db")

            mock_conn.rollback.assert_called_once()
            mock_cursor.close.assert_called_once()
            mock_conn.close.assert_called_once()

    def test_empty_dataframe(self):
        """Test handling of empty DataFrame."""
        test_df = pd.DataFrame()

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        with patch("src.task.l_fact_accident_env.get_pymysql_conn_to_mysql", return_value=mock_conn):
            l_fact_accident_env(test_df, "test_db")

            mock_cursor.executemany.assert_called_once()
            args = mock_cursor.executemany.call_args[0]
            assert args[1] == []

    def test_sql_statement_generation(self):
        """Test SQL statement generation."""
        test_df = pd.DataFrame({
            "col1": ["val1"],
            "col2": ["val2"]
        })

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        with patch("src.task.l_fact_accident_env.get_pymysql_conn_to_mysql", return_value=mock_conn):
            l_fact_accident_env(test_df, "test_db")

            args = mock_cursor.executemany.call_args[0]
            sql_statement = args[0]

            assert "INSERT INTO fact_accident_env (col1, col2)" in sql_statement
            assert "VALUES (%s, %s)" in sql_statement
            assert "ON DUPLICATE KEY UPDATE weather_condition=VALUES(weather_condition)" in sql_statement

    def test_connection_failure_handling(self):
        """Test handling when connection cannot be established."""
        test_df = pd.DataFrame({
            "accident_id": [1],
            "weather_condition": ["晴"],
            "visibility": [1000]
        })

        with patch("src.task.l_fact_accident_env.get_pymysql_conn_to_mysql", side_effect=Exception("Connection failed")):
            l_fact_accident_env(test_df, "test_db")
