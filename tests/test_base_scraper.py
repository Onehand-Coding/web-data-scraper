from unittest.mock import MagicMock

import pytest

from web_data_scraper.scraper.base_scraper import BaseScraper


class ConcreteScraper(BaseScraper):
    """Concrete implementation of BaseScraper for testing purposes."""

    def extract_data(self, content, url):
        return []

    def run(self):
        return {"data": [], "stats": self.get_stats(), "config": self.config}


@pytest.fixture
def mock_scraper():
    config = {
        "name": "Test Scraper",
        "job_type": "web",
        "urls": ["http://example.com"],
        "request_delay": 1.0,
    }
    return ConcreteScraper(config)


def test_base_scraper_throttle(mock_scraper, mocker):
    # Mock time.sleep and time.time
    mock_sleep = mocker.patch("time.sleep")
    mock_time = mocker.patch("time.time")
    # Provide values: first for elapsed calculation, second for updating last_request_time
    mock_time.side_effect = [0.5, 1.0]  # 0.5s elapsed since last request (0.0)

    mock_scraper.last_request_time = 0.0
    mock_scraper.throttle_requests()

    # Should sleep for (1.0 - (0.5 - 0.0)) = 0.5 seconds
    mock_sleep.assert_called_once_with(0.5)


def test_base_scraper_no_throttle_when_enough_time_passed(mock_scraper, mocker):
    # Mock time.sleep and time.time
    mock_sleep = mocker.patch("time.sleep")
    mock_time = mocker.patch("time.time")
    # Provide values: first for elapsed calculation, second for updating last_request_time
    mock_time.side_effect = [2.0, 2.0]  # 2.0s elapsed since last request (0.0)

    mock_scraper.last_request_time = 0.0
    mock_scraper.throttle_requests()

    # Should not sleep since 2.0 > 1.0 (request_delay)
    mock_sleep.assert_not_called()


def test_base_scraper_no_throttle_when_delay_is_zero(mock_scraper, mocker):
    # Mock time.sleep
    mock_sleep = mocker.patch("time.sleep")

    mock_scraper.request_delay = 0.0
    mock_scraper.throttle_requests()

    # Should not sleep when delay is 0
    mock_sleep.assert_not_called()


def test_base_scraper_robots_permission_disabled(mock_scraper):
    # Test robots.txt checking when disabled
    mock_scraper.robot_parser = None  # Disable robots checking

    result = mock_scraper.check_robots_permission("http://example.com/test")

    # Should return True when robots checking is disabled
    assert result is True


def test_base_scraper_robots_permission_enabled(mock_scraper, mocker):
    # Test robots.txt checking when enabled
    mock_robot_parser = MagicMock()
    mock_robot_parser.can_fetch.return_value = True
    mock_scraper.robot_parser = mock_robot_parser
    mock_scraper._robots_txt_domain = "http://example.com"

    result = mock_scraper.check_robots_permission("http://example.com/test")

    assert result is True
    mock_robot_parser.can_fetch.assert_called_once()
