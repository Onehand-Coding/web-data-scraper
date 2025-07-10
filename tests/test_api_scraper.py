import pytest
from unittest.mock import MagicMock
from web_data_scraper.scraper.api_scraper import APIScraper


@pytest.fixture
def api_scraper_config():
    return {
        "name": "Test API Scraper",
        "job_type": "api",
        "api_config": {
            "base_url": "https://api.example.com",
            "endpoints": ["/users"],
            "field_mappings": {"id": "user.id", "name": "user.name"},
        },
    }


def test_api_scraper_fetch(api_scraper_config, mocker):
    # Mock the fetch_page method from BaseScraper since that's what APIScraper uses
    mock_fetch_page = mocker.patch(
        "web_data_scraper.scraper.base_scraper.BaseScraper.fetch_page"
    )
    mock_fetch_page.return_value = '[{"user": {"id": 1, "name": "Test User"}}]'

    scraper = APIScraper(api_scraper_config)
    data = scraper.run()

    assert len(data["data"]) == 1
    assert data["data"][0]["id"] == 1
    assert data["data"][0]["name"] == "Test User"

    # Verify that fetch_page was called with the correct URL
    mock_fetch_page.assert_called_with("https://api.example.com/users")
