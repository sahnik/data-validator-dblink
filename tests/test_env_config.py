import os
import pytest
from src.data_validator.config import DatabaseConfig, EmailConfig, ValidationConfig


def test_database_config_env_expansion(monkeypatch):
    """Test that environment variables are expanded in database config."""
    # Set environment variables
    monkeypatch.setenv("TEST_DB_USER", "test_user")
    monkeypatch.setenv("TEST_DB_PASS", "test_password")
    monkeypatch.setenv("TEST_DB_HOST", "test_host")
    monkeypatch.setenv("TEST_DB_SERVICE", "test_service")
    
    # Create config with environment variable references
    config = DatabaseConfig(
        username="${TEST_DB_USER}",
        password="${TEST_DB_PASS}",
        host="${TEST_DB_HOST}",
        service_name="${TEST_DB_SERVICE}"
    )
    
    # Verify values were expanded
    assert config.username == "test_user"
    assert config.password == "test_password"
    assert config.host == "test_host"
    assert config.service_name == "test_service"


def test_email_config_env_expansion(monkeypatch):
    """Test that environment variables are expanded in email config."""
    # Set environment variables
    monkeypatch.setenv("TEST_SMTP_USER", "test@example.com")
    monkeypatch.setenv("TEST_SMTP_PASS", "test_smtp_password")
    
    # Create config with environment variable references
    config = EmailConfig(
        enabled=True,
        smtp_username="${TEST_SMTP_USER}",
        smtp_password="${TEST_SMTP_PASS}",
        from_address="${TEST_SMTP_USER}",
        to_addresses=["${TEST_SMTP_USER}", "admin@example.com"]
    )
    
    # Verify values were expanded
    assert config.smtp_username == "test@example.com"
    assert config.smtp_password == "test_smtp_password"
    assert config.from_address == "test@example.com"
    assert config.to_addresses == ["test@example.com", "admin@example.com"]


def test_non_env_values_unchanged():
    """Test that non-environment variable values are not changed."""
    config = DatabaseConfig(
        username="regular_user",
        password="regular_password",
        host="regular_host",
        service_name="regular_service"
    )
    
    assert config.username == "regular_user"
    assert config.password == "regular_password"
    assert config.host == "regular_host"
    assert config.service_name == "regular_service"


def test_missing_env_var_returns_original():
    """Test that missing environment variables return the original value."""
    # Ensure the env var doesn't exist
    if "MISSING_VAR" in os.environ:
        del os.environ["MISSING_VAR"]
    
    config = DatabaseConfig(
        username="${MISSING_VAR}",
        password="regular_password",
        host="regular_host",
        service_name="regular_service"
    )
    
    # Should return the original string if env var not found
    assert config.username == "${MISSING_VAR}"