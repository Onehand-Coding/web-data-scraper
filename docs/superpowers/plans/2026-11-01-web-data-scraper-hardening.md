# Web Data Scraper Hardening Plan

1. Extend `BaseScraper.fetch_page` with method/params/headers/body support.
2. Update `APIScraper.fetch_data` to use the new kwargs and remove the TODO.
3. Fix `dynamic_scraper` missing `os` import and unused variables.
4. Fix remaining Ruff issues in CLI/Flask/Streamlit.
5. Add API POST regression test, Ruff config, CI, README, and CONTEXT.
6. Run ruff, format, pytest, and build.

No commit or push unless the user explicitly asks.
