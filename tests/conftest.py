import pytest
import yaml
from pathlib import Path
from web_data_scraper.scraper.utils.config_loader import ConfigLoader


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


@pytest.fixture
def mock_requests_get(mocker):
    return mocker.patch("requests.Session.get")


@pytest.fixture
def mock_webdriver(mocker):
    return mocker.patch("selenium.webdriver.Chrome")
