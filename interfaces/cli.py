# File: web-data-scraper/interfaces/cli.py

import typer
from typing import Optional, Dict
import yaml
from pathlib import Path
from jsonschema import ValidationError
import logging # Import logging

# --- Import APIScraper ---
from scraper.api_scraper import APIScraper
# --- Keep other scraper imports ---
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

@app.command("run")
def run_scraper(
    config_file: Path = typer.Argument(..., help="Path to config YAML file", exists=True, readable=True, show_default=False),
    output_format: str = typer.Option("csv", "--format", "-f", help="Output format (csv, json, sqlite)", case_sensitive=False),
    headless: bool = typer.Option(True, "--headless/--no-headless", help="Run browser in headless mode (for dynamic web scraping)")
):
    """Run scraping job based on configuration file."""
    setup_logging(log_filename='cli_scraper.log', level=logging.INFO, console_level=logging.INFO)
    config_loader = ConfigLoader()
    logger = logging.getLogger(__name__)

    try:
        config = config_loader.load_config(str(config_file))
        # Store CLI options in config if they need to override file values
        # (currently output_format isn't in config, headless is)
        config['headless'] = headless # For dynamic scraper

        job_type = config.get('job_type', 'web') # Default to web if not specified
        typer.echo(f"Running job: {config.get('name', 'Unnamed Job')} (Type: {job_type.upper()})")
        typer.echo(f"Output format: {output_format}")
        logger.info(f"Loaded configuration from: {config_file}")

        # --- Select appropriate scraper based on job_type ---
        scraper_instance = None
        if job_type == 'api':
             typer.echo("Using API Scraper")
             scraper_instance = APIScraper(config)
        elif job_type == 'web':
            if config.get('dynamic', False):
                typer.echo("Using Dynamic Web Scraper (Selenium)")
                scraper_instance = DynamicScraper(config)
            else:
                typer.echo("Using HTML Web Scraper (BeautifulSoup)")
                scraper_instance = HTMLScraper(config)
        else:
             # Should be caught by config validation if enum is correct
             typer.echo(f"Error: Unknown job_type '{job_type}' in configuration.", err=True)
             raise typer.Exit(1)
        # --- End Scraper Selection ---

        # Execute scraping
        result = scraper_instance.run()

        # --- Storage Handling (remains mostly the same) ---
        storage = None
        output_format_lower = output_format.lower()
        output_base_dir = Path(config.get('output_dir', 'outputs'))
        job_output_dir = output_base_dir / Path(config_file).stem
        job_output_dir.mkdir(parents=True, exist_ok=True)
        storage_config = config.copy()
        storage_config['output_dir'] = str(job_output_dir)

        if output_format_lower == 'csv': storage = CSVStorage(storage_config)
        elif output_format_lower == 'json': storage = JSONStorage(storage_config)
        elif output_format_lower == 'sqlite': storage = SQLiteStorage(storage_config)
        else: typer.echo(f"Error: Unsupported output format '{output_format}'.", err=True); raise typer.Exit(1)

        # Save results
        if result.get('data'):
            output_path = storage.save(result['data'])
            typer.echo(f"\nScraping completed successfully!")
            typer.echo(f"Results saved to: {output_path}")
            typer.echo(f"Statistics: {result.get('stats', {})}")
        else:
            typer.echo("\nScraping completed, but no data was extracted.")
            typer.echo(f"Statistics: {result.get('stats', {})}")

    except (ValidationError, yaml.YAMLError) as e:
        logger.error(f"Configuration Error in {config_file}: {e}", exc_info=False) # Don't need full traceback for validation error
        typer.echo(f"Configuration Error in {config_file}: {e}", err=True)
        raise typer.Exit(1)
    except FileNotFoundError as e:
         logger.error(f"Configuration file not found: {e}")
         typer.echo(f"Error: Config file not found at '{config_file}'", err=True)
         raise typer.Exit(1)
    except Exception as e:
        logger.exception(f"An unexpected error occurred during scraping: {e}") # Log full traceback
        typer.echo(f"\nScraping failed with an unexpected error: {e}", err=True)
        raise typer.Exit(1)


@app.command("generate-config")
def generate_config(
    output_file: Path = typer.Argument("scraping_config.yaml", help="Output config file path for WEB sample", writable=True, resolve_path=True),
    # api_output_file: Optional[Path] = typer.Option(None, "--api-sample", help="Output path for API sample config") # Option for separate API file
):
    """Generate sample WEB and API configuration files."""
    setup_logging(log_filename='cli_utils.log', level=logging.INFO, console_level=logging.INFO)
    config_loader = ConfigLoader()
    logger = logging.getLogger(__name__)
    try:
        web_path = output_file
        api_path = web_path.parent / f"{web_path.stem}_api_example.yaml"

        if web_path.exists(): typer.confirm(f"File '{web_path}' already exists. Overwrite?", abort=True)
        if api_path.exists(): typer.confirm(f"File '{api_path}' also exists. Overwrite?", abort=True)

        # generate_sample_config now creates both files
        config_loader.generate_sample_config(str(web_path))
        # The messages are now logged within generate_sample_config

    except Exception as e:
        logger.exception(f"Failed to generate config file(s): {e}")
        typer.echo(f"Failed to generate config: {e}", err=True)
        raise typer.Exit(1)

if __name__ == "__main__":
    app()
