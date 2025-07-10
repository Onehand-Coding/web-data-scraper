import pytest
import yaml
from web_data_scraper.scraper.utils.config_loader import ConfigLoader, ValidationError
from pathlib import Path
from unittest.mock import MagicMock, patch
import tempfile
import os


@pytest.fixture
def sample_web_config(tmp_path):
    config = {
        "name": "Test Web Job",
        "job_type": "web",
        "urls": ["http://example.com"],
        "selectors": {
            "type": "css",
            "item": "div.item",
            "fields": {
                "title": "h1.title",
                "price": {"selector": "span.price", "attr": "data-value"},
            },
        },
        "output_format": "csv",
        "output_dir": str(tmp_path / "outputs"),
        "request_delay": 0.5,
        "max_retries": 2,
    }
    config_path = tmp_path / "web_config.yaml"
    with open(config_path, "w", encoding="utf-8") as f:
        yaml.dump(config, f)
    return config_path


@pytest.fixture
def sample_api_config(tmp_path):
    config = {
        "name": "Test API Job",
        "job_type": "api",
        "api_config": {
            "base_url": "https://api.example.com",
            "endpoints": ["/data"],
            "field_mappings": {"name": "user.name", "id": "user.id"},
        },
        "output_format": "json",
        "output_dir": str(tmp_path / "outputs"),
    }
    config_path = tmp_path / "api_config.yaml"
    with open(config_path, "w", encoding="utf-8") as f:
        yaml.dump(config, f)
    return config_path


def test_load_valid_web_config(sample_web_config):
    loader = ConfigLoader()
    config = loader.load_config(str(sample_web_config))
    assert config["name"] == "Test Web Job"
    assert config["job_type"] == "web"
    assert config["urls"] == ["http://example.com"]
    assert config["selectors"]["type"] == "css"


def test_load_valid_api_config(sample_api_config):
    loader = ConfigLoader()
    config = loader.load_config(str(sample_api_config))
    assert config["name"] == "Test API Job"
    assert config["job_type"] == "api"
    assert config["api_config"]["base_url"] == "https://api.example.com"


def test_invalid_config_missing_required(tmp_path):
    invalid_config = {
        "job_type": "web",
        "urls": ["http://example.com"],
        "selectors": {
            "type": "css",
            "item": "div.item",
            "fields": {"title": "h1.title"},
        },
    }  # Missing 'name'
    config_path = tmp_path / "invalid_config.yaml"
    with open(config_path, "w") as f:
        yaml.dump(invalid_config, f)
    loader = ConfigLoader()
    with pytest.raises(ValidationError, match="name.*required"):
        loader.validate_config(invalid_config)


def test_generate_sample_config(tmp_path, mocker):
    loader = ConfigLoader()

    # Mock the Path class to redirect to our tmp_path
    def mock_path_constructor(path_str):
        if path_str == "configs":
            return tmp_path / "configs"
        return Path(path_str)

    # Patch the Path constructor in the config_loader module
    with patch(
        "web_data_scraper.scraper.utils.config_loader.Path",
        side_effect=mock_path_constructor,
    ):
        # Run the method
        files = loader.generate_sample_config(output_filename_base="sample")

    # Verify the method returns paths
    assert len(files) == 2
    assert all(isinstance(f, str) for f in files)

    # Expected file paths in our tmp directory
    expected_web_path = tmp_path / "configs" / "generated_samples" / "sample_web.yaml"
    expected_api_path = tmp_path / "configs" / "generated_samples" / "sample_api.yaml"

    # Verify files were created
    assert expected_web_path.exists()
    assert expected_api_path.exists()

    # Verify content is valid YAML
    with open(expected_web_path, "r", encoding="utf-8") as f:
        web_config = yaml.safe_load(f)
        assert web_config["name"] == "Sample Dynamic Web Job"
        assert web_config["job_type"] == "web"
        assert web_config["dynamic"] is True

    with open(expected_api_path, "r", encoding="utf-8") as f:
        api_config = yaml.safe_load(f)
        assert api_config["name"] == "Sample API Job - JSONPlaceholder"
        assert api_config["job_type"] == "api"
        assert "api_config" in api_config


def test_generate_sample_config_with_default_names(tmp_path, mocker):
    loader = ConfigLoader()

    # Mock the Path class to redirect to our tmp_path
    def mock_path_constructor(path_str):
        if path_str == "configs":
            return tmp_path / "configs"
        return Path(path_str)

    # Patch the Path constructor in the config_loader module
    with patch(
        "web_data_scraper.scraper.utils.config_loader.Path",
        side_effect=mock_path_constructor,
    ):
        # Run the method without filename base
        files = loader.generate_sample_config()

    # Verify the method returns paths
    assert len(files) == 2

    # Expected file paths with default names
    expected_web_path = (
        tmp_path / "configs" / "generated_samples" / "sample_web_config.yaml"
    )
    expected_api_path = (
        tmp_path / "configs" / "generated_samples" / "sample_api_config.yaml"
    )

    # Verify files were created
    assert expected_web_path.exists()
    assert expected_api_path.exists()


def test_generate_sample_config_handles_file_write_error(tmp_path, mocker):
    loader = ConfigLoader()

    # Mock the Path class to redirect to our tmp_path
    def mock_path_constructor(path_str):
        if path_str == "configs":
            return tmp_path / "configs"
        return Path(path_str)

    # Mock logger to capture error messages
    mock_logger = mocker.patch.object(loader, "logger")

    # Create the directory structure but make it read-only to cause write errors
    config_dir = tmp_path / "configs" / "generated_samples"
    config_dir.mkdir(parents=True, exist_ok=True)

    # Make the directory read-only
    os.chmod(config_dir, 0o444)

    try:
        with patch(
            "web_data_scraper.scraper.utils.config_loader.Path",
            side_effect=mock_path_constructor,
        ):
            files = loader.generate_sample_config()

        # Should return empty list or partial results when writes fail
        assert isinstance(files, list)
        # Should have logged errors
        mock_logger.error.assert_called()
    finally:
        # Restore write permissions for cleanup
        os.chmod(config_dir, 0o755)
