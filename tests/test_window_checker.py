import pytest
from datetime import datetime, time
from src.data_validator.config import RunWindow
from src.data_validator.utils.window_checker import WindowChecker


def test_no_window_configured():
    checker = WindowChecker(None)
    assert checker.is_within_window() is True
    assert checker.seconds_until_window_opens() == 0


def test_within_window():
    # Window from 9 AM to 5 PM
    window = RunWindow(
        start_time=time(9, 0),
        end_time=time(17, 0),
        days_of_week=[0, 1, 2, 3, 4]  # Monday to Friday
    )
    checker = WindowChecker(window)
    
    # Test at 2 PM on a Monday
    test_time = datetime(2024, 1, 15, 14, 0)  # A Monday
    assert checker.is_within_window(test_time) is True


def test_outside_window_time():
    # Window from 9 AM to 5 PM
    window = RunWindow(
        start_time=time(9, 0),
        end_time=time(17, 0),
        days_of_week=[0, 1, 2, 3, 4]
    )
    checker = WindowChecker(window)
    
    # Test at 8 PM on a Monday
    test_time = datetime(2024, 1, 15, 20, 0)
    assert checker.is_within_window(test_time) is False


def test_outside_window_day():
    # Window only on weekdays
    window = RunWindow(
        start_time=time(9, 0),
        end_time=time(17, 0),
        days_of_week=[0, 1, 2, 3, 4]
    )
    checker = WindowChecker(window)
    
    # Test at 2 PM on a Saturday
    test_time = datetime(2024, 1, 20, 14, 0)  # A Saturday
    assert checker.is_within_window(test_time) is False


def test_window_crosses_midnight():
    # Window from 10 PM to 2 AM
    window = RunWindow(
        start_time=time(22, 0),
        end_time=time(2, 0),
        days_of_week=list(range(7))  # All days
    )
    checker = WindowChecker(window)
    
    # Test at 11 PM
    test_time_before = datetime(2024, 1, 15, 23, 0)
    assert checker.is_within_window(test_time_before) is True
    
    # Test at 1 AM
    test_time_after = datetime(2024, 1, 16, 1, 0)
    assert checker.is_within_window(test_time_after) is True
    
    # Test at 3 AM (outside window)
    test_time_outside = datetime(2024, 1, 16, 3, 0)
    assert checker.is_within_window(test_time_outside) is False