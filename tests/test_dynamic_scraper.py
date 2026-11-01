from unittest.mock import MagicMock

import pytest
from selenium.webdriver.remote.webdriver import WebDriver

from web_data_scraper.scraper.dynamic_scraper import DynamicScraper


@pytest.fixture
def dynamic_scraper_config():
    return {
        "name": "Test Dynamic Scraper",
        "urls": ["http://example.com"],
        "dynamic": True,
        "selectors": {"type": "css", "item": "div.item", "fields": {"title": "h1"}},
        "login_config": {
            "login_url": "http://example.com/login",
            "username_selector": "input#user",
            "password_selector": "input#pass",
            "submit_selector": "button#submit",
            "username": "test",
            "password": "pass",
            "success_url_contains": "dashboard",
        },
    }


def test_dynamic_scraper_login(dynamic_scraper_config):
    # Create a mock WebDriver
    mock_driver = MagicMock(spec=WebDriver)
    # Instantiate the scraper
    scraper = DynamicScraper(dynamic_scraper_config)
    # Set the mock driver directly
    scraper.driver = mock_driver
    # Call the login method
    scraper._perform_login()
    # Assert that .get() was called with the login URL
    mock_driver.get.assert_called_with(
        dynamic_scraper_config["login_config"]["login_url"]
    )
