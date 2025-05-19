import logging
from datetime import datetime, time
from typing import Optional

from ..config import RunWindow

logger = logging.getLogger(__name__)


class WindowChecker:
    def __init__(self, run_window: Optional[RunWindow]):
        self.run_window = run_window
    
    def is_within_window(self, check_time: Optional[datetime] = None) -> bool:
        """Check if current time is within the configured run window."""
        if not self.run_window:
            return True  # No window configured, always allowed
        
        check_time = check_time or datetime.now()
        current_time = check_time.time()
        current_day = check_time.weekday()  # 0=Monday, 6=Sunday
        
        # Check if current day is allowed
        if current_day not in self.run_window.days_of_week:
            logger.info(f"Current day {current_day} not in allowed days {self.run_window.days_of_week}")
            return False
        
        # Check if current time is within window
        if self.run_window.start_time <= self.run_window.end_time:
            # Normal case: window doesn't cross midnight
            in_window = self.run_window.start_time <= current_time <= self.run_window.end_time
        else:
            # Window crosses midnight
            in_window = current_time >= self.run_window.start_time or current_time <= self.run_window.end_time
        
        if not in_window:
            logger.info(f"Current time {current_time} not within window {self.run_window.start_time}-{self.run_window.end_time}")
            
        return in_window
    
    def seconds_until_window_opens(self) -> Optional[int]:
        """Calculate seconds until the next window opens."""
        if not self.run_window:
            return 0  # No window, always open
        
        now = datetime.now()
        current_time = now.time()
        current_day = now.weekday()
        
        # If we're already in the window, return 0
        if self.is_within_window(now):
            return 0
        
        # Find the next allowed day
        days_to_check = 7  # Maximum days to check
        for days_ahead in range(days_to_check):
            check_day = (current_day + days_ahead) % 7
            
            if check_day in self.run_window.days_of_week:
                # Calculate the target datetime
                target_date = now.date()
                if days_ahead > 0:
                    from datetime import timedelta
                    target_date = target_date + timedelta(days=days_ahead)
                
                target_datetime = datetime.combine(target_date, self.run_window.start_time)
                
                # If it's today and the start time has passed, try tomorrow
                if days_ahead == 0 and current_time > self.run_window.start_time:
                    continue
                
                # Calculate seconds until target
                seconds_until = (target_datetime - now).total_seconds()
                return max(0, int(seconds_until))
        
        # Shouldn't reach here if days_of_week is not empty
        return None