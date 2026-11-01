# Project Context

## Purpose

A configurable web data scraper for static HTML pages, JavaScript-rendered pages,
and JSON APIs. It ships a Streamlit UI, a Typer CLI, and a legacy Flask UI.

## Architecture

- `scraper/base_scraper.py` owns sessions, robots.txt handling, throttling,
  retries, proxy rotation, and the generic `fetch_page` request.
- `scraper/html_scraper.py`, `dynamic_scraper.py`, and `api_scraper.py` implement
  the source strategies.
- `scraper/data_processor.py` applies type conversion, cleaning, validation, and
  transformations.
- `scraper/storage/` writes CSV, JSON, and SQLite.
- `interfaces/` contains Streamlit, Typer, and Flask entry points.
- `configs/` contains example YAML jobs.

## Commands

```bash
uv sync --extra dev
uv run web-scraper-cli run config.yaml
uv run web-scraper-web
uv run ruff check .
uv run ruff format --check .
uv run pytest
```

## Decisions

- Keep YAML jobs as the source of truth.
- Use one `BaseScraper.fetch_page` for GET and non-GET requests.
- API configurations may set `method`, `params`, `headers`, and `data`; dict
  bodies are sent as JSON and string bodies as raw data.
- Respect robots.txt by default.
- Keep the legacy Flask UI for reference.

## Gotchas

- `dynamic_scraper` requires Chrome/Chromium.
- The legacy Flask UI and Streamlit UI are not the primary test targets.
- Proxies and login credentials are supplied through YAML. Do not commit real
  credentials.
- `fetch_page` returns text; API scraping parses JSON afterwards.

## Known Limitations

- No authentication layer for the local web UIs.
- Dynamic scraping is not exercised against a real browser in CI.
- Pagination strategies are limited to the documented selectors/URL patterns.
