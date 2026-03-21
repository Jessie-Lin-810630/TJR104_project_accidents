"""
Unit tests for src.task.l_dim_road_design module.
"""

import pytest
import pandas as pd
from unittest.mock import patch, MagicMock
from src.task.l_dim_road_design import l_dim_road_design


class TestLDimRoadDesign:
    """Test suite for l_dim_road_design function."""

    def test_l_dim_road_design_successful_insert(self):
        """Test successful data insertion."""
        # Create test DataFrame
        test_df = pd.DataFrame({
            "road_type_primary_party": ["國道", "省道"],
            "road_form_major": ["國道", "省道"],
            "road_form_minor": ["國道", "省道"]
        })

        # Mock database connection and cursor
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        with patch("src.task.l_dim_road_design.get_pymysql_conn_to_mysql", return_value=mock_conn):
            l_dim_road_design(test_df, "test_db")

            # Verify connection was established
            mock_conn.cursor.assert_called_once()
            # Verify executemany was called
            mock_cursor.executemany.assert_called_once()
            # Verify commit was called
            mock_conn.commit.assert_called_once()
            # Verify connections were closed
            mock_cursor.close.assert_called_once()
            mock_conn.close.assert_called_once()

    def test_l_dim_road_design_database_error(self):
        """Test handling of database errors."""
        test_df = pd.DataFrame({
            "road_type_primary_party": ["國道"],
            "road_form_major": ["國道"],
            "road_form_minor": ["國道"]
        })

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.executemany.side_effect = Exception("Database error")

        with patch("src.task.l_dim_road_design.get_pymysql_conn_to_mysql", return_value=mock_conn):
            l_dim_road_design(test_df, "test_db")

            # Verify rollback was called on error
            mock_conn.rollback.assert_called_once()
            # Verify connections were still closed
            mock_cursor.close.assert_called_once()
            mock_conn.close.assert_called_once()

    def test_l_dim_road_design_empty_dataframe(self):
        """Test handling of empty DataFrame."""
        test_df = pd.DataFrame()

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        with patch("src.task.l_dim_road_design.get_pymysql_conn_to_mysql", return_value=mock_conn):
            l_dim_road_design(test_df, "test_db")

            # Verify executemany was called with empty list
            mock_cursor.executemany.assert_called_once()
            args, kwargs = mock_cursor.executemany.call_args
            assert args[1] == []  # Empty data list

    def test_l_dim_road_design_sql_generation(self):
        """Test SQL statement generation."""
        test_df = pd.DataFrame({
            "col1": ["val1"],
            "col2": ["val2"],
            "col3": ["val3"]
        })

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        with patch("src.task.l_dim_road_design.get_pymysql_conn_to_mysql", return_value=mock_conn):
            l_dim_road_design(test_df, "test_db")

            # Verify the SQL statement structure
            args, kwargs = mock_cursor.executemany.call_args
            sql_statement = args[0]

            # Check INSERT part
            assert "INSERT INTO dim_road_design (col1, col2, col3)" in sql_statement
            assert "VALUES (%s, %s, %s)" in sql_statement
            # Check ON DUPLICATE KEY UPDATE part
            assert "ON DUPLICATE KEY UPDATE road_form_minor=VALUES(road_form_minor)" in sql_statement

    def test_l_dim_road_design_without_database(self):
        """Test function call without database parameter."""
        test_df = pd.DataFrame({
            "road_type_primary_party": ["國道"],
            "road_form_major": ["國道"],
            "road_form_minor": ["國道"]
        })

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        with patch("src.task.l_dim_road_design.get_pymysql_conn_to_mysql", return_value=mock_conn) as mock_get_conn:
            l_dim_road_design(test_df)

            # Verify get_pymysql_conn_to_mysql was called with None
            mock_get_conn.assert_called_once_with(None)

    def test_l_dim_road_design_connection_failure(self):
        """Test handling when connection cannot be established."""
        test_df = pd.DataFrame({
            "road_type_primary_party": ["國道"],
            "road_form_major": ["國道"],
            "road_form_minor": ["國道"]
        })

        with patch("src.task.l_dim_road_design.get_pymysql_conn_to_mysql", side_effect=Exception("Connection failed")):
            # Function should handle the exception gracefully
            l_dim_road_design(test_df, "test_db")

            # No assertion needed - just verify no unhandled exception