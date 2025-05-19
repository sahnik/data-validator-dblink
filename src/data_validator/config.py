from typing import Optional, List
from pydantic import BaseModel, Field, validator
from datetime import datetime, time
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()


class DatabaseConfig(BaseModel):
    username: str
    password: str
    host: str
    port: int = 1521
    service_name: str
    
    @validator('*', pre=True)
    def expand_env_vars(cls, v):
        """Expand environment variables in string values."""
        if isinstance(v, str) and v.startswith('${') and v.endswith('}'):
            env_var = v[2:-1]
            return os.getenv(env_var, v)
        return v
    
    @property
    def connection_string(self) -> str:
        return f"{self.username}/{self.password}@{self.host}:{self.port}/{self.service_name}"


class EmailConfig(BaseModel):
    enabled: bool = False
    smtp_host: Optional[str] = None
    smtp_port: int = 587
    smtp_username: Optional[str] = None
    smtp_password: Optional[str] = None
    from_address: Optional[str] = None
    to_addresses: List[str] = Field(default_factory=list)
    use_tls: bool = True
    
    @validator('*', pre=True)
    def expand_env_vars(cls, v):
        """Expand environment variables in string values."""
        if isinstance(v, str) and v.startswith('${') and v.endswith('}'):
            env_var = v[2:-1]
            return os.getenv(env_var, v)
        # Handle lists of strings
        if isinstance(v, list):
            return [
                os.getenv(item[2:-1], item) if isinstance(item, str) and item.startswith('${') and item.endswith('}') else item
                for item in v
            ]
        return v


class TableMapping(BaseModel):
    source_table: str
    target_table: str
    natural_keys: List[str]
    exclude_columns: List[str] = Field(default_factory=list)
    chunk_size: int = 10000
    where_clause: Optional[str] = None


class RunWindow(BaseModel):
    start_time: time
    end_time: time
    days_of_week: List[int] = Field(default_factory=lambda: list(range(7)))  # 0=Monday, 6=Sunday


class ValidationConfig(BaseModel):
    source_db: DatabaseConfig
    target_db: DatabaseConfig
    db_link_name: str
    table_mappings: List[TableMapping]
    
    max_concurrent_validations: int = 5
    progress_tracking_enabled: bool = True
    progress_table_name: str = "DATA_VALIDATION_PROGRESS"
    results_table_name: str = "DATA_VALIDATION_RESULTS"
    
    email_config: EmailConfig = Field(default_factory=EmailConfig)
    run_window: Optional[RunWindow] = None
    
    incremental_mode: bool = False
    incremental_column: Optional[str] = None  # e.g., "LAST_MODIFIED_DATE"
    
    class Config:
        use_enum_values = True


class ValidationProgress(BaseModel):
    id: int
    table_name: str
    status: str  # 'IN_PROGRESS', 'COMPLETED', 'FAILED', 'PAUSED'
    total_rows: int
    processed_rows: int
    last_processed_key: Optional[str]
    started_at: datetime
    updated_at: datetime
    completed_at: Optional[datetime]
    error_message: Optional[str]


class ValidationResult(BaseModel):
    id: int
    table_name: str
    total_rows: int
    matched_rows: int
    mismatched_rows: int
    missing_in_target: int
    extra_in_target: int
    validation_duration_seconds: float
    started_at: datetime
    completed_at: datetime
    status: str  # 'SUCCESS', 'FAILED', 'PARTIAL'
    error_message: Optional[str]