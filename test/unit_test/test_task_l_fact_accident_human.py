"""
Unit tests for src.task.l_fact_accident_human module.
"""

import pytest
import pandas as pd
from unittest.mock import patch, MagicMock
from src.task.l_fact_accident_human import l_fact_accident_human


class TestLFactAccidentHuman:
    """Test suite for l_fact_accident_human function."""

    def test_successful_insert(self):
        """Test successful data insertion."""
        test_df = pd.DataFrame({
            "human_id": [1, 2],
            "hit_and_run": [1, 0],
            "age": [30, 25]
        })

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        with patch("src.task.l_fact_accident_human.get_pymysql_conn_to_mysql", return_value=mock_conn):
            l_fact_accident_human(test_df, "test_db")

            mock_conn.cursor.assert_called_once()
            mock_cursor.executemany.assert_called_once()
            mock_conn.commit.assert_called_once()
            mock_cursor.close.assert_called_once()
            mock_conn.close.assert_called_once()

    def test_database_error_handling(self):
        """Test handling of database errors."""
        test_df = pd.DataFrame({
            "human_id": [1],
            "hit_and_run": [1],
            "age": [30]
        })

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.executemany.side_effect = Exception("Database error")

        with patch("src.task.l_fact_accident_human.get_pymysql_conn_to_mysql", return_value=mock_conn):
            l_fact_accident_human(test_df, "test_db")

            mock_conn.rollback.assert_called_once()
            mock_cursor.close.assert_called_once()
            mock_conn.close.assert_called_once()

    def test_empty_dataframe(self):
        """Test handling of empty DataFrame."""
        test_df = pd.DataFrame()

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        with patch("src.task.l_fact_accident_human.get_pymysql_conn_to_mysql", return_value=mock_conn):
            l_fact_accident_human(test_df, "test_db")

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

        with patch("src.task.l_fact_accident_human.get_pymysql_conn_to_mysql", return_value=mock_conn):
            l_fact_accident_human(test_df, "test_db")

            args = mock_cursor.executemany.call_args[0]
            sql_statement = args[0]

            assert "INSERT INTO fact_accident_human (col1, col2)" in sql_statement
            assert "VALUES (%s, %s)" in sql_statement
            assert "ON DUPLICATE KEY UPDATE hit_and_run=VALUES(hit_and_run)" in sql_statement

    def test_connection_failure_handling(self):
        """Test handling when connection cannot be established."""
        test_df = pd.DataFrame({
            "human_id": [1],
            "hit_and_run": [1],
            "age": [30]
        })

        with patch("src.task.l_fact_accident_human.get_pymysql_conn_to_mysql", side_effect=Exception("Connection failed")):
            l_fact_accident_human(test_df, "test_db")