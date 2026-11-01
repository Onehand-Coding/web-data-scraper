from unittest.mock import MagicMock

import pytest

from web_data_scraper.scraper.html_scraper import HTMLScraper


@pytest.fixture
def html_scraper_config():
    return {
        "name": "Test HTML Scraper",
        "urls": ["http://example.com"],
        "selectors": {
            "type": "css",
            "item": "div.item",
            "fields": {"title": "h1", "url": {"selector": "a", "attr": "href"}},
        },
    }


def test_html_scraper_extract(html_scraper_config, mock_requests_get):
    html_content = """
    <div class="item"><h1>Test Title</h1><a href="/page">Link</a></div>
    <div class="item"><h1>Another</h1><a href="/page2">Link2</a></div>
    """
    mock_requests_get.return_value = MagicMock(status_code=200, text=html_content)
    scraper = HTMLScraper(html_scraper_config)
    data = scraper.extract_data(html_content, "http://example.com")
    assert len(data) == 2
    assert data[0] == {"title": "Test Title", "url": "http://example.com/page"}
