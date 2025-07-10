import pytest
from web_data_scraper.scraper.utils.proxy_rotator import ProxyRotator


def test_proxy_rotator_get_proxy():
    proxies = [{"http": "http://proxy1:8080"}, {"http": "http://proxy2:8080"}]
    rotator = ProxyRotator(proxies)
    proxy = rotator.get_proxy()
    assert proxy in proxies


def test_proxy_rotator_no_proxies():
    rotator = ProxyRotator([])
    assert rotator.get_proxy() is None


def test_proxy_rotator_mark_bad(mocker):
    proxies = [{"http": "http://proxy1:8080"}]
    rotator = ProxyRotator(proxies)
    mocker.patch.object(rotator.logger, "warning")
    rotator.mark_bad(proxies[0])
    assert rotator.get_proxy() is None
