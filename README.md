# Web Data Scraper Framework

A flexible and configurable framework designed for efficient and automated web data extraction. Built with Python, it supports scraping static HTML, dynamic JavaScript-rendered websites, and interacting with web APIs, making it suitable for various data gathering tasks.

## Features

* **Multiple Scraping Strategies:**
    * **HTML Scraper:** Uses `requests` and `BeautifulSoup4` (with `lxml` parser if available) for fast scraping of static HTML content. Supports CSS and XPath selectors.
    * **Dynamic Scraper:** Uses `selenium` (with `chromedriver` by default) to control a browser for scraping dynamic sites that rely heavily on JavaScript. Supports login automation and dynamic pagination clicks. Supports CSS and XPath selectors.
    * **API Scraper:** Interacts directly with JSON-based web APIs. Handles different HTTP methods (GET primarily supported via base fetch), parameters, headers, data paths, and field mappings.
* **Configuration-Driven:** Define scraping jobs using simple YAML files. No coding required for most standard scraping tasks.
* **Data Processing Engine:** Clean, validate, transform (using Python expressions), convert types, and restructure extracted data using configurable rules defined in YAML.
* **Pagination:**
    * HTML Scraper: Automatically finds and follows "next page" links based on CSS selectors.
    * Dynamic Scraper: Clicks "next page" elements, handles disabled states, and waits for navigation.
    * Configurable `max_pages` limit for both.
* **Multiple Output Formats:** Save scraped data as CSV, JSON, or into an SQLite database. Filenames include job name and timestamp.
* **User Interfaces:**
    * **Web UI:** A user-friendly Flask application to create, view, edit, delete, and run scraping jobs via your browser. Includes dynamic form generation for processing rules.
    * **Command-Line Interface (CLI):** Run jobs and generate sample configurations directly from your terminal using `typer`.
* **Configurable Behavior:** Control request delays, retries, user agents, respect for `robots.txt`, proxy usage, Selenium options (headless, waits, timeouts).
* **Proxy Rotation:** Basic proxy rotation support using `requests` sessions (for HTML/API) and WebDriver options (for Dynamic). Marks failing proxies.
* **Logging:** Detailed logging for monitoring and debugging, stored in the `logs/` directory (`cli_scraper.log` and `web_app.log`).

## Project Structure

```web-data-scraper/
├── configs/               # Default location for YAML configuration files
│   ├── scraping_jobs/     # Subdir for web UI generated/managed configs
│   ├── api_test_jsonplaceholder.yaml # Example API config
│   ├── dynamic_login_test.yaml     # Example Dynamic+Login config
│   ├── dynamic_proxy_test.yaml     # Example Dynamic+Proxy config
│   ├── example_config.yaml         # Example complex web scrape
│   ├── quotes_paged_config.yaml    # Example HTML pagination
│   └── quotes_xpath_test.yaml      # Example HTML XPath
├── interfaces/            # User interfaces
│   ├── cli.py             # Command Line Interface (Typer)
│   └── web_app/           # Web Application Interface (Flask)
│       ├── app.py
│       └── templates/
├── logs/                  # Log files (cli_scraper.log, web_app.log)
├── outputs/               # Default location for saved output data
│   └── [Job_Name]/        # Subdirectories automatically created per job
├── scraper/               # Core scraping logic
│   ├── storage/           # Data storage handlers (base, csv, json, sqlite)
│   ├── utils/             # Utility modules (config_loader, logger, etc.)
│   ├── base_scraper.py    # Abstract base class for scrapers
│   ├── html_scraper.py    # Static HTML scraper (requests + bs4/lxml)
│   ├── dynamic_scraper.py # Dynamic JS scraper (selenium)
│   ├── api_scraper.py     # API interaction scraper (requests)
│   └── data_processor.py  # Handles cleaning, validation, transformation
├── .gitignore
├── README.md              # This file
└── requirements.txt       # Python dependencies
```

## Setup

**Prerequisites:**

* Python 3.9+
* pip (Python package installer)
* Git (optional, for cloning)
* **For Dynamic Scraping:** Google Chrome browser and the matching `chromedriver` executable. Ensure `chromedriver` is either in your system's PATH or its path is specified via the `webdriver_path` key in your dynamic scraping configuration (this is not currently exposed in the UI).

**Installation:**

1.  **Clone the repository (if applicable):**
    ```bash
    git clone https://github.com/Phoenix1025/web-data-scraper.git
    cd web-data-scraper
    ```
2.  **Create and Activate a Virtual Environment (Recommended):**
    ```bash
    python -m venv .venv
    # Linux/macOS:
    source .venv/bin/activate
    # Windows (cmd/powershell):
    # .venv\Scripts\activate
    ```
3.  **Install Dependencies:**
    ```bash
    pip install -r requirements.txt
    ```
    *Note: `lxml` is included in `requirements.txt` for XPath support.*

## Configuration (`config.yaml`)

Define scraping jobs in YAML files (e.g., in `configs/` or `configs/scraping_jobs/`).

**Core Keys:**

* `name`: (Required) Descriptive name for the job (used for output filenames/dirs).
* `description`: Optional description of the job's purpose.
* `job_type`: (Required) `web` (for HTML/Dynamic) or `api`.
* `output_dir`: Base directory for saving results (defaults to `outputs/`). Job-specific subdirs are created automatically.
* `request_delay`: Delay (in seconds) between requests (default: 1).
* `max_retries`: Max retries for failed requests (default: 3).
* `user_agent`: Custom User-Agent string (defaults to a Googlebot UA).
* `proxies`: List of proxy dicts (e.g., `[{http: "...", https: "..."}, ...]`). Empty by default.
* `processing_rules`: (Optional) See "Data Processing" section below.

**`job_type: web` Specific Keys:**

* `urls`: (Required) List of starting URLs.
* `dynamic`: Set to `true` to use the Selenium-based DynamicScraper (requires WebDriver). Default is `false` (uses HTMLScraper).
* `respect_robots`: `true` (default) or `false`. Whether to check `robots.txt`.
* `selectors`: (Required) Defines how to find data:
    * `type`: `css` (default) or `xpath`.
    * `item`: Selector for each individual item element.
    * `fields`: Dictionary mapping output field names to selectors:
        * `field_name: "selector_string"` (extracts text)
        * `field_name: {selector: "...", attr: "attribute_name"}` (extracts attribute like `href`, `src`)
* `pagination` (Optional):
    * `next_page_selector`: Selector for the "Next" page link/button.
    * `max_pages`: Maximum number of pages to scrape.
* **If `dynamic: true`:**
    * `headless`: `true` (default) or `false` (run browser visibly).
    * `wait_for_selector` (Optional): Wait for this element before extracting data.
    * `wait_time` (Optional): General wait time after page load/actions (default: 5s).
    * `login_config` (Optional): Dictionary with login details (see `configs/dynamic_login_test.yaml`):
        * `login_url`, `username_selector`, `password_selector`, `submit_selector`, `username`, `password` (required within block).
        * `success_selector` OR `success_url_contains` (required for verification).
        * `wait_after_login` (optional).

**`job_type: api` Specific Keys:**

* `api_config`: (Required) Dictionary defining the API interaction:
    * `base_url`: (Required) Base URL of the API (e.g., `https://api.example.com/v1`).
    * `endpoints`: (Required) List of endpoint paths to query (e.g., `["/users", "/posts?category=news"]`).
    * `method`: HTTP method (default: `GET`). (`POST`, `PUT`, etc. require code modification currently).
    * `params` (Optional): Dictionary of URL parameters for GET requests.
    * `headers` (Optional): Dictionary of request headers.
    * `data` (Optional): Dictionary or string for request body (for `POST`/`PUT`).
    * `data_path` (Optional): Dot-notation path to the list of items within the JSON response (e.g., `results.items`). If empty, assumes the root response is the list/item.
    * `field_mappings` (Optional): Dictionary mapping `output_field_name: "sourceFieldName"` (use dot notation for nested source fields, e.g., `address.city`). If omitted, assumes API items are already the desired dictionaries.

**Data Processing Rules (`processing_rules`):**

(Optional section in config YAML)

* `field_types`: Convert fields to specific types (e.g., `int`, `float`, `datetime`). Specify `format` for dates.
    ```yaml
    field_types:
      price: { type: float }
      publish_date: { type: date, format: "%Y-%m-%d" }
    ```
* `text_cleaning`: Apply text cleaning operations.
    ```yaml
    text_cleaning:
      title: { trim: true, remove_extra_spaces: true }
      author: { trim: true, uppercase: true }
      description: { remove_newlines: true }
    ```
* `validations`: Define rules to validate fields (sets field to `None` if invalid).
    ```yaml
    validations:
      url: { required: true, pattern: "^https?://" }
      stock_count: { required: false, min_length: 1 } # Example: optional but must have content if present
    ```
* `transformations`: Create new fields or modify existing ones using Python expressions. Use `item['field_name']` to access other fields in the current item.
    ```yaml
    transformations:
      full_name: "f\"{item.get('first_name', '')} {item.get('last_name', '')}\".strip()"
      price_eur: "item.get('price_usd', 0) * 0.95"
    ```
* `drop_fields`: List of field names to remove from the final output.
    ```yaml
    drop_fields:
      - raw_html_snippet
      - temporary_id
    ```

*Refer to the sample config files in `configs/` for more examples.*

## Usage

**1. Web Interface:**

* Start the Flask app:
    ```bash
    python -m interfaces.web_app.app
    ```
* Open your web browser to `http://127.0.0.1:5001` (or your machine's IP address on port 5001 if running remotely).
* **Create:** Click "Create New Job".
    * Select "Job Type" (Web Scrape or API Request).
    * Fill out the relevant sections (URLs/Selectors for Web, API Config for API).
    * Define any desired "Processing Rules".
    * Save the configuration.
* **Manage:** View, Edit, Delete, or Run existing jobs listed on the home page.
* **Run:** Click the "Run" button next to a job. Results are saved in the `outputs/` directory (within a subfolder named after the job/config file). The results page shows statistics and a preview.

**2. Command Line Interface (CLI):**

* Navigate to the project root directory in your terminal (ensure virtual environment is active).
* Use `python -m interfaces.cli --help` to see available commands.

* **Run a Scraper Job:**
    ```bash
    python -m interfaces.cli run path/to/your_config.yaml --format [csv|json|sqlite]
    ```
    *Example (Web):*
    ```bash
    python -m interfaces.cli run configs/quotes_paged_config.yaml --format csv
    ```
    *Example (API):*
    ```bash
    python -m interfaces.cli run configs/api_test_jsonplaceholder.yaml --format json
    ```
    *Add `--no-headless` for dynamic web scraping if you want to see the browser.*

* **Generate Sample Configs:** (Creates web and API examples)
    ```bash
    python -m interfaces.cli generate-config my_web_config.yaml
    ```
    *(This will also create `my_web_config_api_example.yaml`)*

## Output

Scraped data is saved under the `outputs/` directory.
* A subdirectory is automatically created based on the job's name (derived from the config file).
* Output filenames include the job name and a timestamp.
* Supported formats: CSV, JSON, SQLite.

## Future Enhancements / Ideas

* More robust dynamic pagination (infinite scroll, "load more").
* UI improvements for proxy list management and processing rules.
* Support for POST/PUT/DELETE methods in API Scraper via `Workspace_page` or direct calls.
* API pagination handling.
* Job Scheduling (using APScheduler, Celery, etc.).
* More output options (e.g., direct database insertion beyond SQLite).
* More sophisticated error handling and reporting.
* Plugin system for custom processing steps.
