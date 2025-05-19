import pytest
from datetime import time
from src.data_validator.config import (
    DatabaseConfig, EmailConfig, TableMapping, 
    RunWindow, ValidationConfig
)


def test_database_config():
    config = DatabaseConfig(
        username="test_user",
        password="test_pass",
        host="localhost",
        port=1521,
        service_name="test_service"
    )
    
    assert config.username == "test_user"
    assert config.connection_string == "test_user/test_pass@localhost:1521/test_service"


def test_table_mapping():
    mapping = TableMapping(
        source_table="TEST_TABLE",
        target_table="TEST_TABLE",
        natural_keys=["ID"],
        exclude_columns=["TIMESTAMP"],
        chunk_size=1000
    )
    
    assert mapping.source_table == "TEST_TABLE"
    assert mapping.natural_keys == ["ID"]
    assert mapping.chunk_size == 1000


def test_run_window():
    window = RunWindow(
        start_time=time(22, 0),
        end_time=time(6, 0),
        days_of_week=[0, 1, 2, 3, 4]  # Monday to Friday
    )
    
    assert window.start_time == time(22, 0)
    assert window.end_time == time(6, 0)
    assert 5 not in window.days_of_week  # Saturday not included


def test_validation_config():
    config_data = {
        "source_db": {
            "username": "source_user",
            "password": "source_pass",
            "host": "source_host",
            "service_name": "source_service"
        },
        "target_db": {
            "username": "target_user",
            "password": "target_pass",
            "host": "target_host",
            "service_name": "target_service"
        },
        "db_link_name": "TEST_LINK",
        "table_mappings": [
            {
                "source_table": "TEST_TABLE",
                "target_table": "TEST_TABLE",
                "natural_keys": ["ID"]
            }
        ]
    }
    
    config = ValidationConfig(**config_data)
    
    assert config.db_link_name == "TEST_LINK"
    assert len(config.table_mappings) == 1
    assert config.max_concurrent_validations == 5  # default value