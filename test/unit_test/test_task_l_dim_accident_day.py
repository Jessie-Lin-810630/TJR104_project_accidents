"""
Unit tests for src.task.l_dim_accident_day module.
"""

import pytest
import pandas as pd
from unittest.mock import patch, MagicMock
from src.task.l_dim_accident_day import l_dim_accident_day


class TestLDimAccidentDay:
    """Test suite for l_dim_accident_day function."""

    def test_l_dim_accident_day_successful_insert(self):
        """Test successful data insertion."""
        # Create test DataFrame
        test_df = pd.DataFrame({
            "accident_date": ["2024-01-01", "2024-01-02"],
            "accident_weekday": ["星期一", "星期二"],
            "national_activity": ["中華民國開國紀念日", "無特殊活動"],
            "is_holiday": [1, 0]
        })

        # Mock database connection and cursor
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        with patch("src.task.l_dim_accident_day.get_pymysql_conn_to_mysql", return_value=mock_conn):
            l_dim_accident_day(test_df, "test_db")

            # Verify connection was established
            mock_conn.cursor.assert_called_once()
            # Verify executemany was called
            mock_cursor.executemany.assert_called_once()
            # Verify commit was called
            mock_conn.commit.assert_called_once()
            # Verify connections were closed
            mock_cursor.close.assert_called_once()
            mock_conn.close.assert_called_once()

    def test_l_dim_accident_day_database_error(self):
        """Test handling of database errors."""
        test_df = pd.DataFrame({
            "accident_date": ["2024-01-01"],
            "accident_weekday": ["星期一"],
            "national_activity": ["中華民國開國紀念日"],
            "is_holiday": [1]
        })

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.executemany.side_effect = Exception("Database error")

        with patch("src.task.l_dim_accident_day.get_pymysql_conn_to_mysql", return_value=mock_conn):
            l_dim_accident_day(test_df, "test_db")

            # Verify rollback was called on error
            mock_conn.rollback.assert_called_once()
            # Verify connections were still closed
            mock_cursor.close.assert_called_once()
            mock_conn.close.assert_called_once()

    def test_l_dim_accident_day_empty_dataframe(self):
        """Test handling of empty DataFrame."""
        test_df = pd.DataFrame()

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        with patch("src.task.l_dim_accident_day.get_pymysql_conn_to_mysql", return_value=mock_conn):
            l_dim_accident_day(test_df, "test_db")

            # Verify executemany was called with empty list
            mock_cursor.executemany.assert_called_once()
            args, kwargs = mock_cursor.executemany.call_args
            assert args[1] == []  # Empty data list

    def test_l_dim_accident_day_sql_generation(self):
        """Test SQL statement generation."""
        test_df = pd.DataFrame({
            "col1": ["val1"],
            "col2": ["val2"],
            "col3": ["val3"],
            "col4": ["val4"]
        })

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        with patch("src.task.l_dim_accident_day.get_pymysql_conn_to_mysql", return_value=mock_conn):
            l_dim_accident_day(test_df, "test_db")

            # Verify the SQL statement structure
            args, kwargs = mock_cursor.executemany.call_args
            sql_statement = args[0]

            # Check INSERT part
            assert "INSERT INTO dim_accident_day (col1, col2, col3, col4)" in sql_statement
            assert "VALUES (%s, %s, %s, %s)" in sql_statement
            # Check ON DUPLICATE KEY UPDATE part
            assert "ON DUPLICATE KEY UPDATE accident_weekday=VALUES(accident_weekday)" in sql_statement

    def test_l_dim_accident_day_without_database(self):
        """Test function call without database parameter."""
        test_df = pd.DataFrame({
            "accident_date": ["2024-01-01"],
            "accident_weekday": ["星期一"],
            "national_activity": ["中華民國開國紀念日"],
            "is_holiday": [1]
        })

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        with patch("src.task.l_dim_accident_day.get_pymysql_conn_to_mysql", return_value=mock_conn) as mock_get_conn:
            l_dim_accident_day(test_df)

            # Verify get_pymysql_conn_to_mysql was called with None
            mock_get_conn.assert_called_once_with(None)

    def test_l_dim_accident_day_connection_failure(self):
        """Test handling when connection cannot be established."""
        test_df = pd.DataFrame({
            "accident_date": ["2024-01-01"],
            "accident_weekday": ["星期一"],
            "national_activity": ["中華民國開國紀念日"],
            "is_holiday": [1]
        })

        with patch("src.task.l_dim_accident_day.get_pymysql_conn_to_mysql", side_effect=Exception("Connection failed")):
            # Function should handle the exception gracefully
            l_dim_accident_day(test_df, "test_db")

            # No assertion needed - just verify no unhandled exception