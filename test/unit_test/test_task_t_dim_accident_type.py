"""
Unit tests for src.task.t_dim_accident_type module.
"""

import pytest
import pandas as pd
import tempfile
from pathlib import Path
from unittest.mock import patch, MagicMock
from src.task.t_dim_accident_type import t_dim_accident_type


class TestTDimAccidentType:
    """Test suite for t_dim_accident_type function."""

    def test_empty_csv_file(self):
        """Test handling of empty CSV file."""
        csv_content = """交通事故分類,事故發生位置(路面位置),事故發生位置(車道位置),事故類型(主分類),事故類型(次分類)
"""

        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False, encoding='utf-8') as f:
            f.write(csv_content)
            temp_file = f.name

        try:
            mock_col_map = {
                '交通事故分類': 'accident_category',
                '事故發生位置(路面位置)': 'accident_position_major',
                '事故發生位置(車道位置)': 'accident_position_minor',
                '事故類型(主分類)': 'accident_type_major',
                '事故類型(次分類)': 'accident_type_minor',
            }

            with patch('src.task.t_dim_accident_type.dim_accident_type_col_map', mock_col_map):
                result = t_dim_accident_type([temp_file])

                # Check result is empty DataFrame
                assert isinstance(result, pd.DataFrame)
                assert len(result) == 0
        finally:
            Path(temp_file).unlink()

    def test_file_not_found(self):
        """Test handling when file is not found."""
        nonexistent_file = "/nonexistent/path/file.csv"

        with pytest.raises(FileNotFoundError):
            t_dim_accident_type([nonexistent_file])

    def test_deduplication_logic(self):
        """Test deduplication removes exact duplicates."""
        csv_content = """交通事故分類,事故發生位置(路面位置),事故發生位置(車道位置),事故類型(主分類),事故類型(次分類)
A,位置1,位置2,主類,次類
A,位置1,位置2,主類,次類
A,位置1,位置2,主類,次類"""

        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False, encoding='utf-8') as f:
            f.write(csv_content)
            temp_file = f.name

        try:
            mock_col_map = {
                '交通事故分類': 'accident_category',
                '事故發生位置(路面位置)': 'accident_position_major',
                '事故發生位置(車道位置)': 'accident_position_minor',
                '事故類型(主分類)': 'accident_type_major',
                '事故類型(次分類)': 'accident_type_minor',
            }

            with patch('src.task.t_dim_accident_type.dim_accident_type_col_map', mock_col_map):
                result = t_dim_accident_type([temp_file])

                # 3 identical rows should be deduplicated to 1
                assert len(result) == 1
        finally:
            Path(temp_file).unlink()

    def test_multiple_files_concat(self):
        """Test processing multiple CSV files."""
        csv_content1 = """交通事故分類,事故發生位置(路面位置),事故發生位置(車道位置),事故類型(主分類),事故類型(次分類)
A,位置1,位置2,主類,次類
總計,總計,總計,總計,總計
備註,備註,備註,備註,備註"""

        csv_content2 = """交通事故分類,事故發生位置(路面位置),事故發生位置(車道位置),事故類型(主分類),事故類型(次分類)
B,位置2,位置3,主類1,次類1
總計,總計,總計,總計,總計
備註,備註,備註,備註,備註"""

        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False, encoding='utf-8') as f1:
            f1.write(csv_content1)
            temp_file1 = f1.name

        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False, encoding='utf-8') as f2:
            f2.write(csv_content2)
            temp_file2 = f2.name

        try:
            mock_col_map = {
                '交通事故分類': 'accident_category',
                '事故發生位置(路面位置)': 'accident_position_major',
                '事故發生位置(車道位置)': 'accident_position_minor',
                '事故類型(主分類)': 'accident_type_major',
                '事故類型(次分類)': 'accident_type_minor',
            }

            with patch('src.task.t_dim_accident_type.dim_accident_type_col_map', mock_col_map):
                result = t_dim_accident_type([temp_file1, temp_file2])

                # Should have 2 rows (one from each file)
                assert len(result) == 2
        finally:
            Path(temp_file1).unlink()
            Path(temp_file2).unlink()
