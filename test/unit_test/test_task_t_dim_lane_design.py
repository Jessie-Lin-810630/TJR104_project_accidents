"""
Unit tests for src.task.t_dim_lane_design module.
"""

import pytest
import pandas as pd
import tempfile
from pathlib import Path
from unittest.mock import patch
from src.task.t_dim_lane_design import t_dim_lane_design


class TestTDimLaneDesign:
    """Test suite for t_dim_lane_design function."""

    def test_normal_csv_processing(self):
        """Test normal CSV processing for lane design data."""
        csv_content = """車道分隔線方向(主),車道分隔線方向(次),車道分隔線(主幹線),快慢分隔,無邊線
主方向1,次方向1,主幹1,快,無
主方向1,次方向1,主幹1,快,無
主方向2,次方向2,主幹2,慢,有
總計,總計,總計,總計,總計
備註,備註,備註,備註,備註"""
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False, encoding='utf-8') as f:
            f.write(csv_content)
            temp_file = f.name

        try:
            mock_col_map = {
                '車道分隔線方向(主)': 'lane_divider_direction_major',
                '車道分隔線方向(次)': 'lane_divider_direction_minor',
                '車道分隔線(主幹線)': 'lane_divider_main_general',
                '快慢分隔': 'lane_divider_fast_slow',
                '無邊線': 'lane_edge_marking',
            }
            
            with patch('src.task.t_dim_lane_design.dim_lane_design_col_map', mock_col_map):
                result = t_dim_lane_design([temp_file])
                
                assert isinstance(result, pd.DataFrame)
                assert len(result.columns) == 5
                assert len(result) == 2  # Deduplication: 3 rows → 2 unique
        finally:
            Path(temp_file).unlink()

    def test_empty_csv_file(self):
        """Test handling of empty CSV file."""
        csv_content = """車道分隔線方向(主),車道分隔線方向(次),車道分隔線(主幹線),快慢分隔,無邊線
總計,總計,總計,總計,總計
備註,備註,備註,備註,備註"""
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False, encoding='utf-8') as f:
            f.write(csv_content)
            temp_file = f.name

        try:
            mock_col_map = {
                '車道分隔線方向(主)': 'lane_divider_direction_major',
                '車道分隔線方向(次)': 'lane_divider_direction_minor',
                '車道分隔線(主幹線)': 'lane_divider_main_general',
                '快慢分隔': 'lane_divider_fast_slow',
                '無邊線': 'lane_edge_marking',
            }
            
            with patch('src.task.t_dim_lane_design.dim_lane_design_col_map', mock_col_map):
                result = t_dim_lane_design([temp_file])
                
                assert isinstance(result, pd.DataFrame)
                assert len(result) == 0
        finally:
            Path(temp_file).unlink()

    def test_file_not_found(self):
        """Test handling when file is not found."""
        with pytest.raises(FileNotFoundError):
            t_dim_lane_design(["/nonexistent/path/file.csv"])

    def test_deduplication_logic(self):
        """Test deduplication removes exact duplicates."""
        csv_content = """車道分隔線方向(主),車道分隔線方向(次),車道分隔線(主幹線),快慢分隔,無邊線
A,B,C,D,E
A,B,C,D,E
A,B,C,D,E
總計,總計,總計,總計,總計
備註,備註,備註,備註,備註"""
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False, encoding='utf-8') as f:
            f.write(csv_content)
            temp_file = f.name

        try:
            mock_col_map = {
                '車道分隔線方向(主)': 'lane_divider_direction_major',
                '車道分隔線方向(次)': 'lane_divider_direction_minor',
                '車道分隔線(主幹線)': 'lane_divider_main_general',
                '快慢分隔': 'lane_divider_fast_slow',
                '無邊線': 'lane_edge_marking',
            }
            
            with patch('src.task.t_dim_lane_design.dim_lane_design_col_map', mock_col_map):
                result = t_dim_lane_design([temp_file])
                
                assert len(result) == 1
        finally:
            Path(temp_file).unlink()

    def test_multiple_files_concat(self):
        """Test processing multiple CSV files."""
        csv1 = """車道分隔線方向(主),車道分隔線方向(次),車道分隔線(主幹線),快慢分隔,無邊線
A,B,C,D,E
總計,總計,總計,總計,總計
備註,備註,備註,備註,備註"""
        
        csv2 = """車道分隔線方向(主),車道分隔線方向(次),車道分隔線(主幹線),快慢分隔,無邊線
F,G,H,I,J
總計,總計,總計,總計,總計
備註,備註,備註,備註,備註"""
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False, encoding='utf-8') as f1:
            f1.write(csv1)
            temp_file1 = f1.name
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False, encoding='utf-8') as f2:
            f2.write(csv2)
            temp_file2 = f2.name

        try:
            mock_col_map = {
                '車道分隔線方向(主)': 'lane_divider_direction_major',
                '車道分隔線方向(次)': 'lane_divider_direction_minor',
                '車道分隔線(主幹線)': 'lane_divider_main_general',
                '快慢分隔': 'lane_divider_fast_slow',
                '無邊線': 'lane_edge_marking',
            }
            
            with patch('src.task.t_dim_lane_design.dim_lane_design_col_map', mock_col_map):
                result = t_dim_lane_design([temp_file1, temp_file2])
                
                assert len(result) == 2
        finally:
            Path(temp_file1).unlink()
            Path(temp_file2).unlink()

    def test_string_stripping(self):
        """Test string values are stripped of whitespace."""
        csv_content = """車道分隔線方向(主),車道分隔線方向(次),車道分隔線(主幹線),快慢分隔,無邊線
  A  , B , C , D , E 
總計,總計,總計,總計,總計
備註,備註,備註,備註,備註"""
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False, encoding='utf-8') as f:
            f.write(csv_content)
            temp_file = f.name

        try:
            mock_col_map = {
                '車道分隔線方向(主)': 'lane_divider_direction_major',
                '車道分隔線方向(次)': 'lane_divider_direction_minor',
                '車道分隔線(主幹線)': 'lane_divider_main_general',
                '快慢分隔': 'lane_divider_fast_slow',
                '無邊線': 'lane_edge_marking',
            }
            
            with patch('src.task.t_dim_lane_design.dim_lane_design_col_map', mock_col_map):
                result = t_dim_lane_design([temp_file])
                
                assert result.iloc[0, 0] == "A"
                assert result.iloc[0, 1] == "B"
        finally:
            Path(temp_file).unlink()