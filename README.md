# Web Data Scraper Framework

A powerful, flexible Python-based framework for automated web data extraction that supports scraping static HTML pages, dynamic JavaScript-rendered websites, and direct API integration.

## 🚀 Key Features

### Multiple Scraping Strategies
- **HTML Scraper:** For static websites using `requests` + `BeautifulSoup4`
- **Dynamic Scraper:** For JavaScript-heavy sites using Selenium WebDriver
- **API Scraper:** For direct interaction with JSON-based web APIs

### Highly Configurable via YAML
- Define complete scraping jobs in simple, human-readable YAML files
- Support for both CSS and XPath selectors
- Advanced pagination handling (next page links, max pages)
- Proxy rotation and management
- Authentication/login automation for protected content

### Comprehensive Data Processing Engine
- **Type Conversion:** int, float, string, boolean, datetime, date
- **Text Cleaning:** whitespace, case conversion, special character removal, regex replacements
- **Field Validation:** required fields, length constraints, regex patterns
- **Data Transformations:** using Python expressions to create or modify fields
- **Field Filtering:** dropping unnecessary fields

### Multiple User Interfaces
- **🎯 Streamlit Web UI (Primary):** User-friendly interface for creating, managing, and running jobs with real-time feedback
- **⚡ Command Line Interface:** For automation, scripting, and generating sample configurations
- **📚 Legacy Flask UI:** Available as reference in `src/web_data_scraper/interfaces/flask_ui/`

### Flexible Output Options
- Save scraped data as CSV, JSON, or SQLite database
- Configurable output paths with timestamped filenames
- Download results directly from the web interface

### Robustness Features
- Configurable request delays and rate limiting
- Automatic retries for failed network requests
- Custom User-Agent support
- Robots.txt compliance
- Comprehensive logging and error handling

## 📁 Project Structure

```
web-data-scraper/
├── configs/
│   ├── example_templates/      # Curated, comprehensive example YAML files
│   │   ├── comprehensive_api_test.yaml
│   │   ├── comprehensive_dynamic_css_proxies_test.yaml
│   │   ├── comprehensive_static_css_test.yaml
│   │   └── comprehensive_web_xpath_test.yaml
│   ├── generated_samples/      # Default output for CLI's 'generate-config'
│   │   ├── sample_api_config.yaml
│   │   └── sample_web_config.yaml
│   └── scraping_jobs/          # User-defined job configurations (from UI)
│       └── .gitkeep
│
├── src/
│   └── web_data_scraper/       # Main package source code
│       ├── __init__.py
│       ├── config.py           # Package configuration
│       ├── interfaces/         # User interfaces
│       │   ├── cli.py          # Command Line Interface (using Typer)
│       │   ├── streamlit_ui/   # Primary Streamlit Web UI
│       │   │   ├── app.py
│       │   │   └── launcher.py
│       │   └── flask_ui/       # Legacy Flask Web UI (reference)
│       │       └── ...
│       └── scraper/            # Core scraping logic
│           ├── storage/        # Data storage handlers
│           │   ├── base_storage.py
│           │   ├── csv_handler.py
│           │   ├── json_handler.py
│           │   └── sqlite_handler.py
│           ├── utils/          # Utility modules
│           │   ├── config_loader.py
│           │   ├── logger.py
│           │   ├── proxy_rotator.py
│           │   └── user_agents.py
│           ├── api_scraper.py
│           ├── base_scraper.py
│           ├── data_processor.py
│           ├── dynamic_scraper.py
│           └── html_scraper.py
│
├── logs/                       # Log files (Git ignored)
├── outputs/                    # Scraped data output (Git ignored)
├── tests/
│   └── ...                     # Tests directory
├── .gitignore
├── packages.txt                # System dependencies for Streamlit Cloud
├── pyproject.toml              # Modern Python project configuration
├── README.md
└── uv.lock                     # UV dependency lock file
```


---

## 🛠️ Setup, Development & Testing

### Prerequisites

- Python 3.9+
- `uv` (recommended) or `pip` (Python package installer)
- `git` (for cloning the repository)
- **For Dynamic Scraping:** Google Chrome/Chromium browser

### Installation Steps

1. **Clone the repository:**
   ```bash
   git clone https://github.com/yourusername/web-data-scraper.git
   cd web-data-scraper
   ```

2. **Install dependencies using UV (recommended):**
   ```bash
   uv sync
   ```

   **Or using pip:**
   ```bash
   pip install -r requirements.txt
   ```

3. **WebDriver Setup (for Dynamic Scraping):**

   **Option A - Automatic (Recommended):**
   - Leave `webdriver_path` blank in your job configurations
   - Selenium Manager (part of recent Selenium versions) will attempt to automatically download and manage a compatible ChromeDriver. This is often the easiest approach.

   **Option B - Manual:**
   - Download ChromeDriver matching your Chrome version: [ChromeDriver Downloads](https://chromedriver.chromium.org/downloads)
   - Add ChromeDriver to your system PATH or specify the absolute path in `webdriver_path` field

### Development & Testing

- **Run tests:**
  ```bash
  uv run pytest
  ```

- **Lint your code:**
  ```bash
  ruff check src/
  ```

- **Modern packaging:**
  This project uses a `src/` layout and is configured with `pyproject.toml` using [hatchling](https://hatch.pypa.io/latest/).

---

### 📝 src/ Layout

All source code is under `src/web_data_scraper/` for best practices in modern Python packaging.
This prevents import issues and accidental shadowing during development and testing.

---

## 🖥️ Using the Application

### Streamlit Web UI (Primary Interface)

The Streamlit Web UI provides the most comprehensive and user-friendly experience.

1. **Start the application:**
   ```bash
   # Using UV (recommended)
   uv run web-scraper-web

   # Or using the module directly
   uv run streamlit run src/web_data_scraper/interfaces/streamlit_ui/app.py
   ```

2. **Access the UI** at `http://localhost:8501`

3. **Navigation:**
   - **📋 Manage Jobs:** View, run, edit, or delete saved scraping configurations
   - **➕ Create/Edit Job:** Interactive form for building job configurations
   - **🚀 Example Jobs:** Explore pre-configured examples and templates

4. **Key Features:**
   - Intuitive forms for all configuration options
   - Real-time field availability feedback for processing rules
   - Direct job execution with statistics and data previews
   - Download results in multiple formats
   - YAML view for any job configuration

### Command Line Interface (CLI)

Perfect for automation and scripting workflows.

1. **Run a scraping job:**
   ```bash
   # Using UV (recommended)
   uv run web-scraper-cli run path/to/config.yaml

   # Or using the module directly
   uv run python -m web_data_scraper.interfaces.cli run path/to/config.yaml
   ```

   **Example:**
   ```bash
   uv run web-scraper-cli run configs/example_templates/comprehensive_static_css_test.yaml
   ```

2. **Generate sample configurations:**
   ```bash
   # Creates basic templates in configs/generated_samples/
   uv run web-scraper-cli generate-config

   # Create with custom base name
   uv run web-scraper-cli generate-config my_custom_samples
   ```

3. **Available CLI commands:**
   - `run` - Execute a scraping job from a YAML configuration
   - `generate-config` - Generate sample configuration files

## ⚙️ Configuration Guide

Job configurations are defined in YAML files with the following structure:

### Basic Configuration

```yaml
name: "Example Scraping Job"
description: "Scrapes product data from an e-commerce site"
job_type: "web"  # or "api"
output_format: "csv"  # csv, json, or sqlite

# Common options
request_delay: 2          # Seconds between requests
max_retries: 3           # Retry failed requests
user_agent: "MyBot 1.0"  # Custom User-Agent
respect_robots: true     # Follow robots.txt
```

### Web Scraping Configuration

```yaml
urls:
  - "https://example.com/products"
  - "https://example.com/categories"

dynamic: false  # Set to true for JavaScript-heavy sites

selectors:
  type: "css"  # or "xpath"
  item: ".product-item"  # Container for each item
  fields:
    title: "h2.product-title"
    price:
      selector: ".price"
      attr: "data-price"  # Extract attribute instead of text
    url:
      selector: "a.product-link"
      attr: "href"

# Pagination support
pagination:
  next_page_selector: "a.next-page"
  max_pages: 10
```

### Dynamic Scraper (Selenium) Options

```yaml
dynamic: true
headless: true           # Run browser in headless mode
disable_images: true     # Speed up by not loading images
page_load_timeout: 30    # Seconds to wait for page load
webdriver_path: ""       # Leave blank for auto-management
wait_for_selector: ".content-loaded"  # Wait for specific element
wait_time: 5            # Additional wait time

# Login automation
login_config:
  login_url: "https://example.com/login"
  username_selector: "#username"
  password_selector: "#password"
  submit_selector: "button[type='submit']"
  username: "your_username"
  password: "your_password"
  success_selector: ".welcome-message"
  wait_after_login: 3
```

### API Scraper Configuration

```yaml
job_type: "api"
api_config:
  base_url: "https://api.example.com/v1"
  endpoints:
    - "/users"
    - "/posts?category=tech"
  method: "GET"
  params:
    api_key: "your_api_key"
    limit: 100
  headers:
    Authorization: "Bearer YOUR_TOKEN"
    Content-Type: "application/json"
  data_path: "results.data"  # Path to array in JSON response

  # Map API fields to output fields
  field_mappings:
    user_id: "id"
    full_name: "name"
    email: "contact.email"  # Dot notation for nested fields
```

### Proxy Configuration

```yaml
proxies:
  - http: "http://proxy1.example.com:8080"
    https: "https://proxy1.example.com:8080"
  - http: "http://proxy2.example.com:8080"
    https: "https://proxy2.example.com:8080"
```

### Data Processing Rules

```yaml
processing_rules:
  # Type conversion
  field_types:
    price:
      type: "float"
    published_date:
      type: "date"
      format: "%Y-%m-%d"
    is_featured:
      type: "boolean"

  # Text cleaning
  text_cleaning:
    title:
      trim: true
      remove_extra_spaces: true
    description:
      trim: true
      lowercase: true
      regex_replace:
        "\\[.*?\\]": ""  # Remove square brackets content

  # Field validation
  validations:
    url:
      required: true
      pattern: "^https?://"
    price:
      required: true
      min_value: 0
      max_value: 10000

  # Custom transformations using Python expressions
  transformations:
    full_name: "f\"{item.get('first_name', '')} {item.get('last_name', '')}\".strip()"
    price_with_tax: "item.get('price', 0) * 1.2"
    slug: "item.get('title', '').lower().replace(' ', '-')"

  # Remove unnecessary fields
  drop_fields:
    - "temp_id"
    - "internal_code"
```

## 📤 Output

Scraped data is organized in the `outputs/` directory:

```
outputs/
├── JobName/
│   ├── JobName_2024-01-15_14-30-25.csv
│   ├── JobName_2024-01-15_16-45-10.json
│   └── JobName_2024-01-15_18-20-35.db
└── EXAMPLE_JobName/  # For example jobs run from UI
    └── ...
```

Each run creates a timestamped file in the job's subdirectory.

## 🌐 Deploying to Streamlit Cloud

For deployment on Streamlit Community Cloud:

### Dynamic Scraping Support

1. **Create `packages.txt` in your repository root:**
   ```txt
    chromium
    libglib2.0-0
    libnss3
    libgconf-2-4
    libfontconfig1
    libx11-xcb1
    libxcomposite1
    libxdamage1
    libxrandr2
    libgbm1
    libasound2
    libatk1.0-0
    libatk-bridge2.0-0
    libcups2
    libgtk-3-0
    libxss1
    libxshmfence1
   ```

2. **Configure dynamic jobs:**
   - Leave `webdriver_path` blank in job configurations
   - Set `headless: true` for all dynamic jobs
   - Use appropriate timeouts for cloud environment

### Important Deployment Notes

⚠️ **Data Persistence Limitations:**
- **Job Configurations:** New jobs created via the deployed UI are stored temporarily and **will be lost** when the app restarts
- **Output Data:** Scraped data is also temporary - **always download results** immediately after job completion
- **Solution:** Use "View YAML" to copy configurations locally, or fork the repository to save your jobs permanently

## 🎯 Best Practices

### Ethical Scraping
- **Respect Terms of Service:** Always check if scraping is permitted
- **Follow robots.txt:** Keep `respect_robots: true` (default)
- **Use appropriate delays:** 2-5 seconds between requests for web scraping
- **Set descriptive User-Agent:** Identify your scraper clearly

### Performance Optimization
- **Test incrementally:** Validate configurations on small samples first
- **Use `max_pages`:** Limit pagination to control volume
- **Enable `disable_images`:** For faster dynamic scraping
- **Choose appropriate selectors:** CSS is generally faster than XPath

### Security Considerations
- **Avoid hardcoding credentials:** Use environment variables for sensitive data
- **Secure proxy usage:** Ensure proxy servers are trustworthy
- **Monitor rate limits:** Respect API rate limits and quotas

## 🔮 Future Enhancements

- **Enhanced API Support:** Token-based pagination, GraphQL support
- **Scheduling System:** Built-in cron-like job scheduling
- **Advanced Storage:** PostgreSQL, MongoDB, cloud storage integrations
- **Monitoring Dashboard:** Real-time job monitoring and analytics
- **Authentication Flows:** OAuth, multi-factor authentication support
- **Distributed Scraping:** Multi-worker, cloud-native scaling
- **Automated Testing:** Comprehensive test suite with CI/CD integration

## 🤝 Contributing

Contributions are welcome! Please:

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

For major changes, please open an issue first to discuss your proposed changes.

## 📚 Examples and Templates

Explore the comprehensive example configurations:
- **Static Web Scraping:** `configs/example_templates/comprehensive_static_css_test.yaml`
- **Dynamic Web Scraping:** `configs/example_templates/comprehensive_dynamic_css_proxies_test.yaml`
- **API Integration:** `configs/example_templates/comprehensive_api_test.yaml`
- **XPath Selectors:** `configs/example_templates/comprehensive_web_xpath_test.yaml`

Access these examples through the "🚀 Example Jobs" section in the Streamlit UI to see them in action!

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.

---

**Ready to start scraping?** Launch the Streamlit UI with `uv run web-scraper-web` and explore the example jobs to see the framework in action!
