"""
Unit tests for src.task.l_fact_accident_main module.
"""

import pytest
import pandas as pd
from unittest.mock import patch, MagicMock
from src.task.l_fact_accident_main import l_fact_accident_main


class TestLFactAccidentMain:
    """Test suite for l_fact_accident_main function."""

    def test_successful_insert(self):
        """Test successful data insertion."""
        test_df = pd.DataFrame({
            "accident_id": [1, 2],
            "accident_time": ["14:05:30", "09:45:00"],
            "death_count": [1, 0]
        })

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        with patch("src.task.l_fact_accident_main.get_pymysql_conn_to_mysql", return_value=mock_conn):
            l_fact_accident_main(test_df, "test_db")

            mock_conn.cursor.assert_called_once()
            mock_cursor.executemany.assert_called_once()
            mock_conn.commit.assert_called_once()
            mock_cursor.close.assert_called_once()
            mock_conn.close.assert_called_once()

    def test_database_error_handling(self):
        """Test handling of database errors."""
        test_df = pd.DataFrame({
            "accident_id": [1],
            "accident_time": ["14:05:30"],
            "death_count": [1]
        })

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.executemany.side_effect = Exception("Database error")

        with patch("src.task.l_fact_accident_main.get_pymysql_conn_to_mysql", return_value=mock_conn):
            l_fact_accident_main(test_df, "test_db")

            mock_conn.rollback.assert_called_once()
            mock_cursor.close.assert_called_once()
            mock_conn.close.assert_called_once()

    def test_empty_dataframe(self):
        """Test handling of empty DataFrame."""
        test_df = pd.DataFrame()

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        with patch("src.task.l_fact_accident_main.get_pymysql_conn_to_mysql", return_value=mock_conn):
            l_fact_accident_main(test_df, "test_db")

            mock_cursor.executemany.assert_called_once()
            args = mock_cursor.executemany.call_args[0]
            assert args[1] == []

    def test_sql_statement_generation(self):
        """Test SQL statement generation."""
        test_df = pd.DataFrame({
            "col1": ["val1"],
            "col2": ["val2"],
            "col3": ["val3"]
        })

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        with patch("src.task.l_fact_accident_main.get_pymysql_conn_to_mysql", return_value=mock_conn):
            l_fact_accident_main(test_df, "test_db")

            args = mock_cursor.executemany.call_args[0]
            sql_statement = args[0]

            assert "INSERT INTO fact_accident_main (col1, col2, col3)" in sql_statement
            assert "VALUES (%s, %s, %s)" in sql_statement
            assert "ON DUPLICATE KEY UPDATE accident_time=VALUES(accident_time)" in sql_statement

    def test_connection_failure_handling(self):
        """Test handling when connection cannot be established."""
        test_df = pd.DataFrame({
            "accident_id": [1],
            "accident_time": ["14:05:30"],
            "death_count": [1]
        })

        with patch("src.task.l_fact_accident_main.get_pymysql_conn_to_mysql", side_effect=Exception("Connection failed")):
            l_fact_accident_main(test_df, "test_db")