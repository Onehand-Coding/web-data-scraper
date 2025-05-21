# Web Data Scraper Framework

A powerful, flexible Python-based framework for automated web data extraction that supports scraping static HTML pages, dynamic JavaScript-rendered websites, and direct API integration.

## Key Features

* **Multiple Scraping Strategies:**
  * **HTML Scraper:** For static websites using requests + BeautifulSoup4
  * **Dynamic Scraper:** For JavaScript-heavy sites using Selenium WebDriver
  * **API Scraper:** For direct interaction with JSON-based web APIs

* **Highly Configurable:**
  * Define complete scraping jobs in simple YAML files
  * Supports both CSS and XPath selectors
  * Advanced pagination handling
  * Proxy rotation and management
  * Authentication/login automation for protected content

* **Comprehensive Data Processing Engine:**
  * Field type conversion (int, float, string, boolean, datetime, date)
  * Text cleaning (whitespace, case, special characters, regex replacements)
  * Field validation (required fields, length constraints, regex patterns)
  * Data transformations using Python expressions
  * Field filtering

* **Multiple User Interfaces:**
  * **Streamlit Web UI (Primary):** User-friendly interface for job management
  * **Command Line Interface:** For automation and scripting
  * **Legacy Flask UI:** Available as reference

* **Flexible Output Options:**
  * CSV, JSON, or SQLite database storage
  * Configurable output paths and naming

* **Robustness Features:**
  * Configurable request delays and rate limiting
  * Automatic retries for failed requests
  * Custom User-Agent support
  * Robots.txt compliance

## Project Structure

```
web-data-scraper/
├── configs/
│   └── scraping_jobs/      # User-defined job YAML configurations
├── interfaces/
│   ├── cli.py              # Command Line Interface
│   ├── flask_ui/           # Legacy Flask Web UI
│   │   ├── app.py
│   │   └── templates/
│   └── streamlit_ui/       # Primary Streamlit Web UI
│       └── app.py
├── logs/                   # Log files directory
├── outputs/                # Scraped data output directory
├── scraper/
│   ├── storage/            # Data storage handlers
│   │   ├── base_storage.py
│   │   ├── csv_handler.py
│   │   ├── json_handler.py
│   │   └── sqlite_handler.py
│   ├── utils/              # Utility modules
│   │   ├── config_loader.py
│   │   ├── logger.py
│   │   └── proxy_rotator.py
│   ├── api_scraper.py
│   ├── base_scraper.py
│   ├── data_processor.py
│   ├── dynamic_scraper.py
│   └── html_scraper.py
├── tests/                  # Test directory
├── .gitignore
├── README.md
└── requirements.txt
```

## Setup and Installation

### Prerequisites

* Python 3.9+
* pip (Python package installer)
* Git (optional, for cloning)
* **For Dynamic Scraping:** Google Chrome browser and matching ChromeDriver

### Installation Steps

1. **Clone the repository:**
   ```bash
   git clone https://github.com/Phoenix1025/web-data-scraper.git
   cd web-data-scraper
   ```

2. **Create and activate a virtual environment (recommended):**
   ```bash
   python -m venv .venv
   # On Linux/macOS:
   source .venv/bin/activate
   # On Windows:
   .venv\Scripts\activate
   ```

3. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

4. **WebDriver Setup (for Dynamic Scraping):**
   * Download ChromeDriver matching your Chrome version: [https://chromedriver.chromium.org/downloads](https://chromedriver.chromium.org/downloads)
   * Either add it to your system PATH or specify its location in job configurations

## Using the Application

### Streamlit Web UI (Recommended)

This is the primary interface for managing and running scraping jobs.

1. **Start the Streamlit app:**
   ```bash
   streamlit run interfaces/streamlit_ui/app.py
   ```

2. **Access the UI** in your browser (typically at http://localhost:8501)

3. **Features:**
   * Create, view, edit, run, and delete scraping jobs
   * Interactive form for configuring all job aspects
   * Real-time feedback on field availability in processing rules
   * View job execution statistics and sample data
   * Download results in various formats

### Command Line Interface (CLI)

Useful for automated workflows and scripting.

1. **Run a scraping job:**
   ```bash
   python -m interfaces.cli run configs/scraping_jobs/your_config.yaml --format [csv|json|sqlite]
   ```

2. **Generate sample configurations:**
   ```bash
   python -m interfaces.cli generate-config my_sample_config.yaml
   ```

## Configuration File Structure

Job configurations are defined in YAML files stored in the `configs/scraping_jobs/` directory.

### Core Configuration Elements

* **Basic Information:**
  ```yaml
  name: "Example Job"
  description: "Scrapes example data from a website"
  job_type: "web"  # or "api"
  output_format: "csv"  # or "json" or "sqlite"
  ```

* **Common Options:**
  ```yaml
  request_delay: 2  # seconds between requests
  max_retries: 3
  user_agent: "Custom User Agent String"
  respect_robots: true
  proxies:
    - http: "http://proxy1.example.com:8080"
      https: "https://proxy1.example.com:8080"
  ```

### Web Scraping Configuration

```yaml
urls:
  - "https://example.com/page1"
  - "https://example.com/page2"
dynamic: true  # use Selenium WebDriver (false for static HTML)
selectors:
  type: "css"  # or "xpath"
  item: ".product-item"
  fields:
    title: "h2.product-title"
    price:
      selector: ".price"
      attr: "data-price"  # Extract attribute instead of text
    url:
      selector: "a.product-link"
      attr: "href"
pagination:
  next_page_selector: "a.next-page"
  max_pages: 5
```

### Dynamic Scraper Options

```yaml
# Only applicable when dynamic: true
headless: true
disable_images: true
page_load_timeout: 30
webdriver_path: "/path/to/chromedriver"
wait_for_selector: ".content-loaded"
wait_time: 5

# Login configuration (if needed)
login_config:
  login_url: "https://example.com/login"
  username_selector: "#username"
  password_selector: "#password"
  submit_selector: "button[type='submit']"
  username: "your_username"
  password: "your_password"
  success_selector: ".welcome-message"  # OR
  success_url_contains: "dashboard"
  wait_after_login: 3
```

### API Scraper Configuration

```yaml
api_config:
  base_url: "https://api.example.com/v1"
  endpoints:
    - "/users"
    - "/posts?category=news"
  method: "GET"  # Default
  params:
    api_key: "your_api_key"
    limit: 100
  headers:
    Authorization: "Bearer YOUR_TOKEN"
  data_path: "results.items"  # Path to array in response
  field_mappings:
    user_id: "id"
    full_name: "name"
    user_email: "contact.email"  # Dot notation for nested fields
```

### Data Processing Rules

```yaml
processing_rules:
  # Type conversion
  field_types:
    price:
      type: "float"
    published_on:
      type: "date"
      format: "%Y-%m-%d"

  # Text cleaning
  text_cleaning:
    title:
      trim: true
      remove_extra_spaces: true
    description:
      trim: true
      lowercase: true
      regex_replace:
        "\\[.*?\\]": ""  # Remove content in square brackets

  # Field validation
  validations:
    url:
      required: true
      pattern: "^https?://"
    price:
      required: true
      min_value: 0

  # Transformations using Python expressions
  transformations:
    full_name: "f\"{item.get('first_name', '')} {item.get('last_name', '')}\".strip()"
    price_with_tax: "item.get('price', 0) * 1.2"

  # Drop unnecessary fields
  drop_fields:
    - "temp_id"
    - "internal_code"
```

## Output

Scraped data is saved in the `outputs/` directory, organized in subdirectories by job name:

* `outputs/JobName/JobName_YYYY-MM-DD_HH-MM-SS.csv`
* `outputs/JobName/JobName_YYYY-MM-DD_HH-MM-SS.json`
* `outputs/JobName/JobName_YYYY-MM-DD_HH-MM-SS.db` (SQLite)

## Best Practices

* **Respect Websites' Terms of Service**: Always check if scraping is allowed
* **Use Request Delays**: Add appropriate delays between requests (2-5 seconds recommended)
* **Set User-Agent**: Identify your scraper properly with a descriptive User-Agent
* **Limit Requests**: Use pagination's `max_pages` to control volume
* **Test on Small Samples**: Validate your configuration on a small scale before full scraping

## Future Enhancements

* Enhanced API pagination support
* Improved scheduling capabilities
* Direct database integrations beyond SQLite
* More sophisticated proxy management
* Support for more complex authentication flows
* Extended test coverage
* Enhanced error reporting and monitoring

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

---

*For examples and more detailed explanations, see the sample configurations in the `configs/scraping_jobs/` directory.*
