"""
Unit tests for src.task.t_dim_accident_day module.
"""

import pytest
import pandas as pd
from datetime import datetime
from src.task.t_dim_accident_day import t_data_for_dim_accident_day, taiwan_national_activities


class TestTDataForDimAccidentDay:
    """Test suite for t_data_for_dim_accident_day function."""

    def test_normal_date_range_generation(self):
        """Test normal date range generation with holidays."""
        start_date = "2024-01-01"
        end_date = "2024-01-05"

        result = t_data_for_dim_accident_day(start_date, end_date, taiwan_national_activities)

        # Check DataFrame structure
        assert isinstance(result, pd.DataFrame)
        assert list(result.columns) == ["accident_date", "accident_weekday", "national_activity", "is_holiday"]

        # Check date range
        assert len(result) == 5
        assert result["accident_date"].tolist() == ["2024-01-01", "2024-01-02",
                                                    "2024-01-03", "2024-01-04", "2024-01-05"]

    def test_weekday_mapping_zh_tw(self):
        """Test weekday mapping in Chinese."""
        start_date = "2024-01-01"  # Monday
        end_date = "2024-01-07"   # Sunday

        result = t_data_for_dim_accident_day(start_date, end_date, taiwan_national_activities)

        expected_weekdays = ["星期一", "星期二", "星期三", "星期四", "星期五", "星期六", "星期日"]
        assert result["accident_weekday"].tolist() == expected_weekdays

    def test_weekday_mapping_en(self):
        """Test weekday mapping in English."""
        start_date = "2024-01-01"  # Monday
        end_date = "2024-01-07"   # Sunday

        result = t_data_for_dim_accident_day(start_date, end_date, taiwan_national_activities, weekday_language="en")

        expected_weekdays = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
        assert result["accident_weekday"].tolist() == expected_weekdays

    def test_holiday_detection(self):
        """Test holiday detection logic."""
        start_date = "2024-01-01"  # Monday, national holiday
        end_date = "2024-01-06"   # Saturday

        result = t_data_for_dim_accident_day(start_date, end_date, taiwan_national_activities)

        # 2024-01-01 is a national holiday, 2024-01-06 is Saturday (weekend)
        assert result.loc[result["accident_date"] == "2024-01-01", "is_holiday"].iloc[0] == 1
        assert result.loc[result["accident_date"] == "2024-01-06", "is_holiday"].iloc[0] == 1
        # 2024-01-02 is Tuesday, not holiday
        assert result.loc[result["accident_date"] == "2024-01-02", "is_holiday"].iloc[0] == 0

    def test_national_activity_mapping(self):
        """Test national activity mapping."""
        start_date = "2024-01-01"
        end_date = "2024-01-02"

        result = t_data_for_dim_accident_day(start_date, end_date, taiwan_national_activities)

        # 2024-01-01 is a national holiday
        assert result.loc[result["accident_date"] == "2024-01-01", "national_activity"].iloc[0] == "中華民國開國紀念日"
        # 2024-01-02 has no special activity
        assert result.loc[result["accident_date"] == "2024-01-02", "national_activity"].iloc[0] == "無特殊活動"

    def test_single_date_range(self):
        """Test single date range."""
        start_date = "2024-01-01"
        end_date = "2024-01-01"

        result = t_data_for_dim_accident_day(start_date, end_date, taiwan_national_activities)

        assert len(result) == 1
        assert result["accident_date"].iloc[0] == "2024-01-01"

    def test_empty_national_activities_dict(self):
        """Test with empty national activities dictionary."""
        start_date = "2024-01-01"
        end_date = "2024-01-03"
        empty_activities = {}

        result = t_data_for_dim_accident_day(start_date, end_date, empty_activities)

        # All dates should have "無特殊活動"
        assert all(result["national_activity"] == "無特殊活動")

    def test_invalid_weekday_language(self):
        """Test invalid weekday language defaults to zh_tw."""
        start_date = "2024-01-01"
        end_date = "2024-01-03"

        result = t_data_for_dim_accident_day(
            start_date, end_date, taiwan_national_activities, weekday_language="invalid")

        # Should default to zh_tw weekdays
        assert result["accident_weekday"].iloc[0] == "星期一"

    def test_date_ordering(self):
        """Test that dates are in correct order."""
        start_date = "2024-01-01"
        end_date = "2024-01-05"

        result = t_data_for_dim_accident_day(start_date, end_date, taiwan_national_activities)

        dates = result["accident_date"].tolist()
        assert dates == sorted(dates)  # Should be in ascending order
