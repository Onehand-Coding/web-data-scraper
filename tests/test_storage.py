import json
import sqlite3
from pathlib import Path

import pandas as pd
import pytest

from web_data_scraper.scraper.storage.csv_handler import CSVStorage
from web_data_scraper.scraper.storage.json_handler import JSONStorage
from web_data_scraper.scraper.storage.sqlite_handler import SQLiteStorage


@pytest.fixture
def storage_config(tmp_path):
    return {"output_dir": str(tmp_path / "outputs"), "name": "test_job"}


def test_csv_storage_save(storage_config, tmp_path):
    data = [{"id": 1, "name": "Test"}, {"id": 2, "name": "Test2"}]
    storage = CSVStorage(storage_config)
    filepath = storage.save(data, "test.csv")
    assert Path(filepath).exists()
    df = pd.read_csv(filepath)
    assert len(df) == 2
    assert list(df.columns) == ["id", "name"]


def test_csv_storage_empty_data(storage_config, tmp_path):
    storage = CSVStorage(storage_config)
    filepath = storage.save([], "empty.csv")
    assert Path(filepath).exists()
    expected_columns = list(
        storage_config.get("selectors", {}).get("fields", {}).keys()
    )
    if expected_columns:
        df = pd.read_csv(filepath)
        assert df.empty
        assert list(df.columns) == expected_columns
    else:
        with pytest.raises(pd.errors.EmptyDataError):
            pd.read_csv(filepath)


def test_json_storage_save(storage_config, tmp_path):
    data = [{"id": 1, "name": "Test"}]
    storage = JSONStorage(storage_config)
    filepath = storage.save(data, "test.json")
    assert Path(filepath).exists()
    with open(filepath) as f:
        saved_data = json.load(f)
    assert saved_data == data


def test_sqlite_storage_save(storage_config, tmp_path):
    data = [{"id": 1, "name": "Test", "price": 10.5}]
    storage = SQLiteStorage(storage_config)
    db_path = storage.save(data)
    assert Path(db_path).exists()
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute(f"SELECT * FROM {storage.table_name}")
    rows = cursor.fetchall()
    assert len(rows) == 1
    conn.close()
