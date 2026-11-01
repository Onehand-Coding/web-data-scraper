# Web Data Scraper Hardening Design

Date: 2026-11-01
Status: Approved for implementation

## Context

The framework has 23 tests but no CI or Ruff configuration. `APIScraper`
declares `method`, `params`, `headers`, and `data` in its schema but ignores them,
so only GET requests work. `BaseScraper.fetch_page` is GET-only. Dynamic scraping
also references an undefined `os` module, which breaks Chrome driver path checks.

## Goals

- Support GET, POST, PUT, PATCH, and DELETE in API jobs.
- Send query params, headers, JSON bodies (dict), and raw bodies (string).
- Fix the undefined `os` import and the remaining lint issues.
- Add Ruff, pytest config, CI, README updates, and CONTEXT.

## Design

- Extend `BaseScraper.fetch_page` with keyword-only `method`, `params`,
  `headers`, `json_body`, and `data_body`; use `session.request`.
- `APIScraper` merges config params with call params, passes headers, and sends
  dict data as JSON and string data as a raw body.
- Keep the existing GET path unchanged when no params/headers are configured.
- Add a test that asserts the POST call shape.
- Add CI with `uv sync --extra dev`, Ruff, format check, pytest, and wheel build.

## Acceptance

- POST API configs reach `session.request` with the configured body/headers.
- The undefined `os` bug is fixed.
- `ruff`, format, and `pytest` pass locally and in CI.
