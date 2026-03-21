"""
Unit tests for src.task.l_dim_lane_design module.
"""

import pytest
import pandas as pd
from unittest.mock import patch, MagicMock
from src.task.l_dim_lane_design import l_dim_lane_design


class TestLDimLaneDesign:
    """Test suite for l_dim_lane_design function."""

    def test_successful_insert(self):
        """Test successful data insertion."""
        test_df = pd.DataFrame({
            "lane_divider_direction_major": ["主1", "主2"],
            "lane_divider_direction_minor": ["次1", "次2"],
            "lane_divider_main_general": ["幹1", "幹2"],
            "lane_divider_fast_slow": ["快", "慢"],
            "lane_edge_marking": ["無", "有"]
        })

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        with patch("src.task.l_dim_lane_design.get_pymysql_conn_to_mysql", return_value=mock_conn):
            l_dim_lane_design(test_df, "test_db")

            mock_conn.cursor.assert_called_once()
            mock_cursor.executemany.assert_called_once()
            mock_conn.commit.assert_called_once()
            mock_cursor.close.assert_called_once()
            mock_conn.close.assert_called_once()

    def test_database_error_handling(self):
        """Test handling of database errors."""
        test_df = pd.DataFrame({
            "lane_divider_direction_major": ["主1"],
            "lane_divider_direction_minor": ["次1"],
            "lane_divider_main_general": ["幹1"],
            "lane_divider_fast_slow": ["快"],
            "lane_edge_marking": ["無"]
        })

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.executemany.side_effect = Exception("Database error")

        with patch("src.task.l_dim_lane_design.get_pymysql_conn_to_mysql", return_value=mock_conn):
            l_dim_lane_design(test_df, "test_db")

            mock_conn.rollback.assert_called_once()
            mock_cursor.close.assert_called_once()
            mock_conn.close.assert_called_once()

    def test_empty_dataframe(self):
        """Test handling of empty DataFrame."""
        test_df = pd.DataFrame()

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        with patch("src.task.l_dim_lane_design.get_pymysql_conn_to_mysql", return_value=mock_conn):
            l_dim_lane_design(test_df, "test_db")

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

        with patch("src.task.l_dim_lane_design.get_pymysql_conn_to_mysql", return_value=mock_conn):
            l_dim_lane_design(test_df, "test_db")

            args = mock_cursor.executemany.call_args[0]
            sql_statement = args[0]

            assert "INSERT INTO dim_lane_design (col1, col2, col3)" in sql_statement
            assert "VALUES (%s, %s, %s)" in sql_statement
            assert "ON DUPLICATE KEY UPDATE lane_edge_marking=VALUES(lane_edge_marking)" in sql_statement

    def test_connection_failure_handling(self):
        """Test handling when connection cannot be established."""
        test_df = pd.DataFrame({
            "lane_divider_direction_major": ["主1"],
            "lane_divider_direction_minor": ["次1"],
            "lane_divider_main_general": ["幹1"],
            "lane_divider_fast_slow": ["快"],
            "lane_edge_marking": ["無"]
        })

        with patch("src.task.l_dim_lane_design.get_pymysql_conn_to_mysql", side_effect=Exception("Connection failed")):
            l_dim_lane_design(test_df, "test_db")