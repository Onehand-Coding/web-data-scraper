import time
import yaml
import typer
import logging
from pathlib import Path
from typing import Optional, List
from jsonschema import ValidationError

# --- Scraper imports ---
from scraper.api_scraper import APIScraper
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
config_loader_cli_instance = ConfigLoader()
logger_cli = logging.getLogger(__name__)

# --- run_scraper command ---
@app.command("run")
def run_scraper(
    config_file: Path = typer.Argument(..., help="Path to config YAML file", exists=True, readable=True, show_default=False),
    output_format: str = typer.Option("csv", "--format", "-f", help="Output format (csv, json, sqlite)", case_sensitive=False),
    headless: bool = typer.Option(True, "--headless/--no-headless", help="Run browser in headless mode (for dynamic web scraping)")
):
    """Run scraping job based on configuration file."""
    setup_logging(log_filename='cli_scraper.log', level=logging.INFO, console_level=logging.INFO)

    try:
        config = config_loader_cli_instance.load_config(str(config_file))
        if config.get('dynamic'):
            config['headless'] = headless

        job_type = config.get('job_type', 'web')
        typer.echo(f"Running job: {config.get('name', 'Unnamed Job')} (Type: {job_type.upper()})")
        typer.echo(f"Output format: {output_format}")
        logger_cli.info(f"Loaded configuration from: {config_file}")

        scraper_instance = None
        if job_type == 'api':
             typer.echo("Using API Scraper")
             scraper_instance = APIScraper(config)
        elif job_type == 'web':
            if config.get('dynamic', False):
                typer.echo(f"Using Dynamic Web Scraper (Selenium, Headless: {config['headless']})")
                scraper_instance = DynamicScraper(config)
            else:
                typer.echo("Using HTML Web Scraper (BeautifulSoup)")
                scraper_instance = HTMLScraper(config)
        else:
             typer.echo(f"Error: Unknown job_type '{job_type}' in configuration.", err=True)
             raise typer.Exit(1)

        result = scraper_instance.run()

        storage = None
        output_format_lower = output_format.lower()
        output_base_dir = Path(config.get('output_dir', 'outputs'))
        safe_job_name_for_dir = "".join(c if c.isalnum() else '_' for c in config.get('name', 'job'))
        job_output_dir = output_base_dir / safe_job_name_for_dir
        job_output_dir.mkdir(parents=True, exist_ok=True)
        storage_config = config.copy()
        storage_config['output_dir'] = str(job_output_dir)

        if output_format_lower == 'csv': storage = CSVStorage(storage_config)
        elif output_format_lower == 'json': storage = JSONStorage(storage_config)
        elif output_format_lower == 'sqlite': storage = SQLiteStorage(storage_config)
        else: typer.echo(f"Error: Unsupported output format '{output_format}'.", err=True); raise typer.Exit(1)

        if result.get('data'):
            output_path = storage.save(result['data'])
            typer.echo(f"\nScraping completed successfully!")
            typer.echo(f"Results saved to: {output_path}")
            typer.echo(f"Statistics: {result.get('stats', {})}")
        else:
            typer.echo("\nScraping completed, but no data was extracted.")
            typer.echo(f"Statistics: {result.get('stats', {})}")

    except (ValidationError, yaml.YAMLError) as e:
        logger_cli.error(f"Configuration Error in {config_file}: {e}", exc_info=False)
        typer.echo(f"Configuration Error in {config_file}: {e}", err=True)
        raise typer.Exit(1)
    except FileNotFoundError as e:
         logger_cli.error(f"Configuration file not found: {e}")
         typer.echo(f"Error: Config file not found at '{config_file}'", err=True)
         raise typer.Exit(1)
    except Exception as e:
        logger_cli.exception(f"An unexpected error occurred during scraping: {e}")
        typer.echo(f"\nScraping failed with an unexpected error: {e}", err=True)
        raise typer.Exit(1)


@app.command("generate-config")
def generate_config_command( # Renamed from generate_config to avoid conflict if imported elsewhere
    filename_base: Optional[str] = typer.Argument(
        None, # Default to None, so ConfigLoader uses its internal defaults
        help="Optional base name for the generated web config file (e.g., 'my_scraper'). "
             "API config will be named similarly. Files are saved in 'configs/generated_samples/'."
    )
):
    """Generates sample WEB and API configuration files in 'configs/generated_samples/'."""
    setup_logging(log_filename='cli_utils.log', level=logging.INFO, console_level=logging.INFO)
    # Use the module-level instance or create a new one
    # config_loader_instance = ConfigLoader()

    try:
        # The ConfigLoader.generate_sample_config method now handles directory creation
        # and default naming if filename_base is None.
        generated_files: List[str] = config_loader_cli_instance.generate_sample_config(filename_base)

        if generated_files:
            typer.echo("Sample configuration files generated successfully:")
            for file_path in generated_files:
                typer.echo(f"- {file_path}")
        else:
            # This case might occur if ConfigLoader itself logs an error and returns empty
            typer.echo("Failed to generate sample files. Please check 'logs/cli_utils.log' and 'logs/config_loader.log'.")

    except Exception as e:
        logger_cli.exception(f"Failed to generate config file(s): {e}")
        typer.echo(f"Failed to generate sample configs: {e}", err=True)
        raise typer.Exit(1)

if __name__ == "__main__":
    app()
