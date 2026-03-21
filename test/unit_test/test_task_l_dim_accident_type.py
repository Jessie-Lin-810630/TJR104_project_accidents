"""
Unit tests for src.task.l_dim_accident_type module.
"""

import pytest
import pandas as pd
from unittest.mock import patch, MagicMock
from src.task.l_dim_accident_type import l_dim_accident_type


class TestLDimAccidentType:
    """Test suite for l_dim_accident_type function."""

    def test_successful_insert(self):
        """Test successful data insertion."""
        test_df = pd.DataFrame({
            "accident_category": ["A", "B"],
            "accident_position_major": ["位置1", "位置3"],
            "accident_position_minor": ["位置2", "位置4"],
            "accident_type_major": ["主類", "主類2"],
            "accident_type_minor": ["次類", "次類2"]
        })

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        with patch("src.task.l_dim_accident_type.get_pymysql_conn_to_mysql", return_value=mock_conn):
            l_dim_accident_type(test_df, "test_db")

            mock_conn.cursor.assert_called_once()
            mock_cursor.executemany.assert_called_once()
            mock_conn.commit.assert_called_once()
            mock_cursor.close.assert_called_once()
            mock_conn.close.assert_called_once()

    def test_database_error_handling(self):
        """Test handling of database errors."""
        test_df = pd.DataFrame({
            "accident_category": ["A"],
            "accident_position_major": ["位置1"],
            "accident_position_minor": ["位置2"],
            "accident_type_major": ["主類"],
            "accident_type_minor": ["次類"]
        })

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.executemany.side_effect = Exception("Database error")

        with patch("src.task.l_dim_accident_type.get_pymysql_conn_to_mysql", return_value=mock_conn):
            l_dim_accident_type(test_df, "test_db")

            mock_conn.rollback.assert_called_once()
            mock_cursor.close.assert_called_once()
            mock_conn.close.assert_called_once()

    def test_empty_dataframe(self):
        """Test handling of empty DataFrame."""
        test_df = pd.DataFrame()

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        with patch("src.task.l_dim_accident_type.get_pymysql_conn_to_mysql", return_value=mock_conn):
            l_dim_accident_type(test_df, "test_db")

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

        with patch("src.task.l_dim_accident_type.get_pymysql_conn_to_mysql", return_value=mock_conn):
            l_dim_accident_type(test_df, "test_db")

            args = mock_cursor.executemany.call_args[0]
            sql_statement = args[0]

            assert "INSERT INTO dim_accident_type (col1, col2)" in sql_statement
            assert "VALUES (%s, %s)" in sql_statement
            assert "ON DUPLICATE KEY UPDATE accident_category=VALUES(accident_category)" in sql_statement

    def test_connection_failure_handling(self):
        """Test handling when connection cannot be established."""
        test_df = pd.DataFrame({
            "accident_category": ["A"],
            "accident_position_major": ["位置1"],
            "accident_position_minor": ["位置2"],
            "accident_type_major": ["主類"],
            "accident_type_minor": ["次類"]
        })

        with patch("src.task.l_dim_accident_type.get_pymysql_conn_to_mysql", side_effect=Exception("Connection failed")):
            l_dim_accident_type(test_df, "test_db")