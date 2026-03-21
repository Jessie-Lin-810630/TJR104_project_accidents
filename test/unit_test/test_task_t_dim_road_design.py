"""
Unit tests for src.task.t_dim_road_design module.
"""

import pytest
import pandas as pd
from unittest.mock import patch, MagicMock
from pathlib import Path
import tempfile
from src.task.t_dim_road_design import t_dim_road_design


class TestTDimRoadDesign:
    """Test suite for t_dim_road_design function."""

    def test_t_dim_road_design_with_valid_csv(self):
        """Test successful processing of valid CSV files."""
        # Create temporary CSV files
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir_path = Path(tmpdir)

            # Create test CSV content (need at least 3 rows because of skipfooter=2)
            csv_content = """道路類別名稱,道路型態子類別名稱,道路型態子類別名稱
國道,國道,國道
省道,省道,省道
縣道,縣道,縣道
總計,總計,總計
備註,備註,備註"""

            csv_file1 = tmpdir_path / "test1.csv"
            csv_file2 = tmpdir_path / "test2.csv"
            csv_file1.write_text(csv_content, encoding="utf-8")
            csv_file2.write_text(csv_content, encoding="utf-8")

            # Mock the column mapping
            with patch("src.task.t_dim_road_design.dim_road_design_col_map", {
                "道路類別名稱": "road_type_primary_party",
                "道路型態子類別名稱": "road_form_major",
                "道路型態子類別名稱": "road_form_minor"
            }):
                result = t_dim_road_design([str(csv_file1), str(csv_file2)])

                # Verify result is DataFrame
                assert isinstance(result, pd.DataFrame)
                # Should have deduplicated rows
                assert len(result) == 3  # 3 unique combinations
                # Should have correct columns
                expected_columns = ["road_type_primary_party", "road_form_major", "road_form_minor"]
                assert list(result.columns) == expected_columns

    def test_t_dim_road_design_empty_file_list(self):
        """Test behavior with empty file list."""
        result = t_dim_road_design([])

        # Should return empty DataFrame
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 0

    def test_t_dim_road_design_file_not_found(self):
        """Test handling of non-existent files."""
        with pytest.raises(FileNotFoundError):
            t_dim_road_design(["/non/existent/file.csv"])

    def test_t_dim_road_design_invalid_csv_format(self):
        """Test handling of malformed CSV files."""
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir_path = Path(tmpdir)

            # Create invalid CSV (missing columns)
            csv_content = """invalid_column1,invalid_column2
value1,value2"""

            csv_file = tmpdir_path / "invalid.csv"
            csv_file.write_text(csv_content, encoding="utf-8")

            with patch("src.task.t_dim_road_design.dim_road_design_col_map", {
                "道路類別名稱": "road_type_primary_party",
                "道路型態子類別名稱": "road_form_major",
                "道路型態子類別名稱": "road_form_minor"
            }):
                with pytest.raises(KeyError):
                    t_dim_road_design([str(csv_file)])

    def test_t_dim_road_design_deduplication(self):
        """Test that duplicate rows are properly removed."""
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir_path = Path(tmpdir)

            # Create CSV with duplicate rows
            csv_content = """道路類別名稱,道路型態子類別名稱,道路型態子類別名稱
國道,國道,國道
國道,國道,國道
省道,省道,省道"""

            csv_file = tmpdir_path / "duplicates.csv"
            csv_file.write_text(csv_content, encoding="utf-8")

            with patch("src.task.t_dim_road_design.dim_road_design_col_map", {
                "道路類別名稱": "road_type_primary_party",
                "道路型態子類別名稱": "road_form_major",
                "道路型態子類別名稱": "road_form_minor"
            }):
                result = t_dim_road_design([str(csv_file)])

                # Should have only 2 unique rows
                assert len(result) == 2
                # Verify deduplication worked
                assert result["road_type_primary_party"].tolist() == ["國道", "省道"]

    def test_t_dim_road_design_string_stripping(self):
        """Test that string values are properly stripped of whitespace."""
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir_path = Path(tmpdir)

            # Create CSV with whitespace
            csv_content = """道路類別名稱,道路型態子類別名稱,道路型態子類別名稱
 國道 , 國道 , 國道
省道,省道,省道"""

            csv_file = tmpdir_path / "whitespace.csv"
            csv_file.write_text(csv_content, encoding="utf-8")

            with patch("src.task.t_dim_road_design.dim_road_design_col_map", {
                "道路類別名稱": "road_type_primary_party",
                "道路型態子類別名稱": "road_form_major",
                "道路型態子類別名稱": "road_form_minor"
            }):
                result = t_dim_road_design([str(csv_file)])

                # Verify whitespace is stripped
                assert result["road_type_primary_party"].iloc[0] == "國道"
                assert result["road_form_major"].iloc[0] == "國道"
