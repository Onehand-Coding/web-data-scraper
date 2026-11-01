from datetime import date  # <-- fix here

import pytest

from web_data_scraper.scraper.data_processor import DataProcessor


@pytest.fixture
def processor_config():
    return {
        "processing_rules": {
            "field_types": {
                "price": {"type": "float"},
                "date": {"type": "date", "format": "%Y-%m-%d"},
            },
            "text_cleaning": {"name": {"trim": True, "uppercase": True}},
            "transformations": {"price": "value * 1.1"},
            "validations": {"name": {"required": True, "min_length": 2}},
            "drop_fields": ["temp"],
        }
    }


def test_data_processor(processor_config):
    processor = DataProcessor(processor_config)
    data = [
        {"name": " test ", "price": "100", "date": "2023-01-01", "temp": "x"},
        {"name": "", "price": "invalid", "date": "bad_date"},
    ]
    processed = processor.process(data)
    assert len(processed) == 2  # Both items remain
    assert processed[0]["name"] == "TEST"
    assert processed[0]["price"] == pytest.approx(110.0)
    assert isinstance(processed[0]["date"], date)  # <-- fix here
    assert "temp" not in processed[0]

    # Second item: all fields should be None except for missing/dropped fields
    assert processed[1]["name"] is None
    assert processed[1]["price"] is None
    assert processed[1]["date"] is None
    assert "temp" not in processed[1]
