# File: web-data-scraper/interfaces/cli.py

"""
Command Line Interface for the web scraper.
"""

import typer
from typing import Optional, Dict
import yaml
from pathlib import Path
from jsonschema import ValidationError # <--- Added import
# Scraper imports
from scraper.html_scraper import HTMLScraper
from scraper.dynamic_scraper import DynamicScraper
# Storage imports
from scraper.storage.csv_handler import CSVStorage
from scraper.storage.json_handler import JSONStorage
from scraper.storage.sqlite_handler import SQLiteStorage
# Utility imports
from scraper.utils.logger import setup_logging
from scraper.utils.config_loader import ConfigLoader

app = typer.Typer(help="Web Scraper Framework")

@app.command("run") # Explicitly name command to avoid confusion with filename
def run_scraper(
    config_file: Path = typer.Argument(..., help="Path to config YAML file", exists=True, readable=True, show_default=False),
    output_format: str = typer.Option("csv", "--format", "-f", help="Output format (csv, json, sqlite)", case_sensitive=False),
    headless: bool = typer.Option(True, "--headless/--no-headless", help="Run browser in headless mode (for dynamic scraping)")
):
    """Run scraping job based on configuration file."""
    # Basic log setup, consider making level configurable via CLI option
    # --- Update setup_logging call ---
    # CLI can log to the default 'scraper.log' or a specific file if needed
    # Set console level potentially based on a --verbose flag later?
    setup_logging(log_filename='cli_scraper.log', level=logging.INFO, console_level=logging.INFO)
    # --- End update ---
    config_loader = ConfigLoader()
    logger = logging.getLogger(__name__) # Get logger for CLI specific messages

    try:
        # Load and validate config
        config = config_loader.load_config(str(config_file)) # Use loader
        config['output_format'] = output_format.lower()
        config['headless'] = headless # Add headless option to config for dynamic scraper

        typer.echo(f"Running scraper job: {config.get('name', 'Unnamed Job')}")
        typer.echo(f"Output format: {output_format}")
        logger.info(f"Loaded configuration from: {config_file}")

        # Select appropriate scraper
        scraper_instance = None # Initialize
        if config.get('dynamic', False):
            typer.echo("Using Dynamic Scraper (Selenium)")
            scraper_instance = DynamicScraper(config)
        else:
            typer.echo("Using HTML Scraper (BeautifulSoup)")
            scraper_instance = HTMLScraper(config)

        # Execute scraping
        result = scraper_instance.run()

        # Select storage handler based on output format
        storage = None # Initialize
        output_format_lower = output_format.lower() # Ensure lowercase comparison

        # --- Determine Storage Handler ---
        # Use output dir from config for storage handler initialization
        output_base_dir = Path(config.get('output_dir', 'outputs'))
        # Create job-specific subfolder if needed (similar to web app)
        job_output_dir = output_base_dir / Path(config_file).stem # Use config filename stem
        job_output_dir.mkdir(parents=True, exist_ok=True)
        # Create a temporary config copy for the storage handler to ensure it gets the job-specific path
        storage_config = config.copy()
        storage_config['output_dir'] = str(job_output_dir)


        if output_format_lower == 'csv':
            storage = CSVStorage(storage_config)
        elif output_format_lower == 'json':
            storage = JSONStorage(storage_config)
        elif output_format_lower == 'sqlite':
            storage = SQLiteStorage(storage_config)
        else:
            # This should ideally be caught by typer's choice validation if used,
            # but keep manual check as fallback.
            typer.echo(f"Error: Unsupported output format '{output_format}'. Supported formats: csv, json, sqlite.", err=True)
            raise typer.Exit(1)

        # Save results
        if result.get('data'):
            output_path = storage.save(result['data'])
            typer.echo(f"\nScraping completed successfully!")
            typer.echo(f"Results saved to: {output_path}") # Show actual save path
            typer.echo(f"Statistics: {result.get('stats', {})}")
        else:
            typer.echo("\nScraping completed, but no data was extracted.")
            typer.echo(f"Statistics: {result.get('stats', {})}")


    except (ValidationError, yaml.YAMLError) as e: # Catch specific config errors
        # Use logger for detailed error, typer for user message
        logger.error(f"Configuration Error in {config_file}: {e}", exc_info=True) # Log traceback
        typer.echo(f"Configuration Error in {config_file}: {e}", err=True)
        raise typer.Exit(1)
    except FileNotFoundError as e: # Catch file not found for config specifically
         logger.error(f"Configuration file not found: {e}")
         typer.echo(f"Error: Configuration file not found at '{config_file}'", err=True)
         raise typer.Exit(1)
    except Exception as e:
        # Log the full traceback for unexpected errors
        logger.exception(f"An unexpected error occurred during scraping: {e}")
        typer.echo(f"\nScraping failed with an unexpected error: {e}", err=True)
        raise typer.Exit(1)

@app.command()
def generate_config(
    output_file: Path = typer.Argument("scraping_config.yaml", help="Output config file path", writable=True, resolve_path=True)
):
    """Generate a sample configuration file."""
    setup_logging(log_filename='cli_utils.log', level=logging.INFO, console_level=logging.INFO)
    config_loader = ConfigLoader() # Use ConfigLoader
    logger = logging.getLogger(__name__)
    try:
        # Check if file exists and ask before overwriting?
        if output_file.exists():
             overwrite = typer.confirm(f"File '{output_file}' already exists. Overwrite?", abort=True)

        config_loader.generate_sample_config(str(output_file)) # Use loader method
        typer.echo(f"Sample configuration generated at: {output_file}")
    except Exception as e:
        logger.exception(f"Failed to generate config file: {e}")
        typer.echo(f"Failed to generate config: {e}", err=True)
        raise typer.Exit(1)

# --- Add logging import ---
import logging

if __name__ == "__main__":
    app()
