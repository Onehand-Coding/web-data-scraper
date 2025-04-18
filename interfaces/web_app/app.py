# File: web-data-scraper/interfaces/web_app/app.py

from flask import Flask, render_template, request, jsonify, redirect, url_for, flash, abort
from pathlib import Path
import yaml
import time
import os
import logging
from werkzeug.utils import secure_filename
from datetime import datetime

# (Keep Scraper and utils imports)
from scraper.html_scraper import HTMLScraper
from scraper.dynamic_scraper import DynamicScraper
from scraper.storage.csv_handler import CSVStorage
from scraper.storage.json_handler import JSONStorage
from scraper.storage.sqlite_handler import SQLiteStorage
from scraper.utils.logger import setup_logging
from scraper.utils.config_loader import ConfigLoader, ValidationError
from jsonschema import ValidationError

app = Flask(__name__)
app.secret_key = 'my-very-secret-key'
BASE_DIR = Path(__file__).resolve().parent
app.config['UPLOAD_FOLDER'] = BASE_DIR / '..' / '..' / 'configs' / 'scraping_jobs'
app.config['UPLOAD_FOLDER'].mkdir(parents=True, exist_ok=True)
app.config['OUTPUT_FOLDER'] = BASE_DIR / '..' / '..' / 'outputs'
app.config['OUTPUT_FOLDER'].mkdir(parents=True, exist_ok=True)

# Log web app activity to its own file within the main logs directory
setup_logging(log_filename='web_app.log', level=logging.INFO, console_level=logging.DEBUG if app.debug else logging.INFO)
logger = logging.getLogger(__name__)
config_loader = ConfigLoader()

@app.template_filter('timestamp_to_datetime')
def timestamp_to_datetime_filter(s):
    try: return datetime.fromtimestamp(float(s)).strftime('%Y-%m-%d %H:%M:%S')
    except: return "N/A"

def get_config_files_details():
    configs = []; folder = app.config['UPLOAD_FOLDER']
    try: yaml_files = list(folder.glob('*.yaml'))
    except Exception as e: logger.error(f"Error reading config dir {folder}: {e}"); return []
    for f_path in yaml_files:
        try: stat_result = f_path.stat(); configs.append({'name': f_path.name, 'modified_time': stat_result.st_mtime,})
        except Exception as e: logger.warning(f"No stats for {f_path.name}: {e}"); configs.append({'name': f_path.name, 'modified_time': 0})
    configs.sort(key=lambda x: x.get('modified_time', 0), reverse=True)
    return configs

# --- Routes ---
@app.route('/')
def index():
    config_files_details = get_config_files_details()
    upload_folder_path = str(app.config['UPLOAD_FOLDER'].relative_to(Path.cwd()))
    return render_template('index.html', config_files=config_files_details, upload_folder=upload_folder_path)

@app.route('/view_config/<filename>')
def view_config(filename):
    # (Keep existing view_config code)
    safe_filename = secure_filename(filename)
    if safe_filename != filename or not filename.endswith('.yaml'): abort(404)
    config_path = app.config['UPLOAD_FOLDER'] / safe_filename
    if not config_path.is_file(): abort(404)
    try:
        with open(config_path, 'r', encoding='utf-8') as f: config_content = f.read()
        return render_template('view_config.html', filename=safe_filename, content=config_content)
    except Exception as e:
        logger.error(f"Error reading config {safe_filename}: {e}"); flash(f"Could not read '{safe_filename}'.", "error")
        return redirect(url_for('index'))

@app.route('/delete_config/<filename>', methods=['POST'])
def delete_config(filename):
    # (Keep existing delete_config code)
    safe_filename = secure_filename(filename)
    if safe_filename != filename or not filename.endswith('.yaml'): abort(400)
    config_path = app.config['UPLOAD_FOLDER'] / safe_filename
    if not config_path.is_file(): flash(f"'{safe_filename}' not found.", "warning"); return redirect(url_for('index'))
    try: config_path.unlink(); logger.info(f"Deleted config: {config_path}"); flash(f"Deleted '{safe_filename}'.", "success")
    except Exception as e: logger.error(f"Error deleting {safe_filename}: {e}"); flash(f"Could not delete '{safe_filename}': {e}", "error")
    return redirect(url_for('index'))


# --- NEW: Edit Config Route ---
@app.route('/edit_config/<filename>')
def edit_config(filename):
    """Load an existing config file and render the configuration form for editing."""
    safe_filename = secure_filename(filename)
    if safe_filename != filename or not filename.endswith('.yaml'):
        logger.warning(f"Attempt to edit potentially unsafe file: {filename}")
        abort(404) # Not found or forbidden

    config_path = app.config['UPLOAD_FOLDER'] / safe_filename
    if not config_path.is_file():
         logger.warning(f"Config file not found for editing: {config_path}")
         flash(f"Configuration file '{safe_filename}' not found.", "error")
         return redirect(url_for('index'))

    try:
        # Load the config data using the loader (includes validation)
        config_data = config_loader.load_config(str(config_path))
        logger.info(f"Loading config '{safe_filename}' for editing.")

        # Prepare data for the form template (`configure.html`)
        form_data = {}
        form_data['job_name'] = config_data.get('name', '')
        form_data['description'] = config_data.get('description', '')
        form_data['urls'] = "\n".join(config_data.get('urls', [])) # Join list back to string
        form_data['dynamic'] = config_data.get('dynamic', False)
        form_data['wait_for_selector'] = config_data.get('wait_for_selector', '')
        form_data['wait_time'] = config_data.get('wait_time', 5)

        # Selectors
        selectors = config_data.get('selectors', {})
        form_data['selector_type'] = selectors.get('type', 'css')
        form_data['container_selector'] = selectors.get('container', '')
        form_data['item_selector'] = selectors.get('item', '')

        # Format fields for template rendering
        fields_list = []
        for name, config in selectors.get('fields', {}).items():
            if isinstance(config, str):
                fields_list.append({'name': name, 'selector': config, 'attr': ''})
            elif isinstance(config, dict):
                 fields_list.append({'name': name, 'selector': config.get('selector', ''), 'attr': config.get('attr', '')})
        form_data['fields'] = fields_list # Pass list of dicts

        # Scraping Behavior
        form_data['request_delay'] = config_data.get('request_delay', 2)
        form_data['max_retries'] = config_data.get('max_retries', 3)
        form_data['user_agent'] = config_data.get('user_agent', 'Mozilla/5.0')
        form_data['respect_robots'] = config_data.get('respect_robots', True)

        # Processing Rules - Convert back to YAML string for textarea
        processing_rules = config_data.get('processing_rules')
        if processing_rules:
            try:
                form_data['processing_rules'] = yaml.dump(processing_rules, sort_keys=False, default_flow_style=False, allow_unicode=True)
            except yaml.YAMLError:
                 logger.error(f"Could not dump processing rules back to YAML for editing {safe_filename}")
                 form_data['processing_rules'] = "# Error loading rules" # Show error in textarea
        else:
             form_data['processing_rules'] = ""

        # Add original filename if needed (e.g., to modify POST logic later to overwrite)
        # form_data['original_filename'] = safe_filename

        # Render the existing configure template with the pre-filled data
        return render_template('configure.html', form_data=form_data)

    except (ValidationError, yaml.YAMLError) as e:
        error_path = " -> ".join(map(str, e.path)) or "Config root"
        message = f"Configuration Error loading {safe_filename} at '{error_path}': {e.message}"
        logger.error(message)
        flash(f"Cannot edit '{safe_filename}': {message}", 'error')
        return redirect(url_for('index'))
    except Exception as e:
        logger.error(f"Error loading config file {safe_filename} for editing: {e}", exc_info=True)
        flash(f"Could not load configuration file '{safe_filename}' for editing.", "error")
        return redirect(url_for('index'))


# --- configure_scraper Route (POST remains unchanged - always saves as new) ---
@app.route('/configure', methods=['GET', 'POST'])
def configure_scraper():
    # (Existing configure_scraper code remains the same)
    # GET request part will now be handled by edit_config route when editing
    # POST request part will always save as new config file
    form_data_for_template = {}
    if request.method == 'POST':
        # ... (Keep exact same POST logic as before) ...
        form_data_for_template = request.form.to_dict(flat=False)
        form_data_for_template['dynamic'] = 'dynamic' in request.form
        form_data_for_template['respect_robots'] = 'respect_robots' in request.form
        form_data_for_template['urls'] = request.form.get('urls', '')
        form_data_for_template['processing_rules'] = request.form.get('processing_rules', '')
        try:
            job_name = request.form.get('job_name', f'WebJob_{int(time.time())}').strip()
            if not job_name: flash('Job Name is required.', 'error'); return render_template('configure.html', form_data=form_data_for_template)
            description = request.form.get('description', '').strip()
            urls_input = request.form.get('urls', '').strip()
            if not urls_input: flash('Please provide at least one URL.', 'error'); return render_template('configure.html', form_data=form_data_for_template)
            urls = [url.strip() for url in urls_input.splitlines() if url.strip()]
            is_dynamic = 'dynamic' in request.form
            selector_type = request.form.get('selector_type', 'css')
            container_selector = request.form.get('container_selector', '').strip()
            item_selector = request.form.get('item_selector', '').strip()
            if not item_selector: flash('Item selector is required.', 'error'); return render_template('configure.html', form_data=form_data_for_template)
            field_names = request.form.getlist('field_name[]')
            field_selectors = request.form.getlist('field_selector[]')
            field_attrs = request.form.getlist('field_attr[]')
            fields_dict = {}
            valid_field_found = False
            for name, selector, attr in zip(field_names, field_selectors, field_attrs):
                name = name.strip(); selector = selector.strip(); attr = attr.strip()
                if name and selector:
                    valid_field_found = True
                    if attr: fields_dict[name] = {'selector': selector, 'attr': attr}
                    else: fields_dict[name] = selector
                elif name or selector or attr: flash(f'Incomplete field: "{name or "N/A"}".', 'warning')
            if not valid_field_found: flash('Define at least one valid field.', 'error'); return render_template('configure.html', form_data=form_data_for_template)
            processing_rules_yaml = request.form.get('processing_rules', '').strip()
            processing_rules_dict = None
            if processing_rules_yaml:
                try:
                    parsed_rules = yaml.safe_load(processing_rules_yaml)
                    if isinstance(parsed_rules, dict): processing_rules_dict = parsed_rules
                    else: flash('Processing Rules must be YAML dict.', 'error'); return render_template('configure.html', form_data=form_data_for_template)
                except yaml.YAMLError as e: flash(f'YAML Error: {e}.', 'error'); form_data_for_template['processing_rules'] = processing_rules_yaml; return render_template('configure.html', form_data=form_data_for_template)
            config = {
                'name': job_name,'description': description,'urls': urls,'dynamic': is_dynamic,
                'selectors': {'type': selector_type, **({'container': container_selector} if container_selector else {}), 'item': item_selector,'fields': fields_dict},
                **({'processing_rules': processing_rules_dict} if processing_rules_dict else {}),
                'output_dir': str(app.config['OUTPUT_FOLDER']), 'request_delay': float(request.form.get('request_delay', 2)),
                'max_retries': int(request.form.get('max_retries', 3)), 'user_agent': request.form.get('user_agent', 'Mozilla/5.0'),
                'respect_robots': 'respect_robots' in request.form,
            }
            if is_dynamic:
                 wait_selector = request.form.get('wait_for_selector', '').strip()
                 if wait_selector: config['wait_for_selector'] = wait_selector
                 config['wait_time'] = float(request.form.get('wait_time', 5))
            logger.debug(f"Config dictionary before validation: {config}")
            config_loader.validate_config(config)
            safe_job_name = secure_filename(job_name)
            timestamp = int(time.time())
            config_filename = f"{safe_job_name}-{timestamp}.yaml" # Always save as new file
            config_path = app.config['UPLOAD_FOLDER'] / config_filename
            with open(config_path, 'w', encoding='utf-8') as f: yaml.dump(config, f, sort_keys=False, default_flow_style=False, allow_unicode=True)
            logger.info(f"Configuration saved (potentially as edit): {config_path}")
            flash(f'Configuration "{job_name}" saved successfully as {config_filename}!', 'success')
            return redirect(url_for('index'))
        except ValidationError as e: error_path = " -> ".join(map(str, e.path)) or "Config root"; message = f"Config Error: {e.message} (at {error_path})"; logger.error(f"Validation failed: {message}"); flash(message, 'error'); return render_template('configure.html', form_data=form_data_for_template)
        except Exception as e: logger.exception(f"Error saving config: {e}"); flash(f'Unexpected error saving: {e}', 'error'); return render_template('configure.html', form_data=form_data_for_template)

    # GET request to /configure (for creating NEW job)
    return render_template('configure.html', form_data={}) # Pass empty dict for new form

# (Keep run_scraper route)
@app.route('/run/<config_file>')
def run_scraper(config_file):
    # ... (existing run_scraper code) ...
    config_path = app.config['UPLOAD_FOLDER'] / secure_filename(config_file) # Secure filename here too
    if not config_path.is_file(): flash(f"Config file '{config_file}' not found.", 'error'); return redirect(url_for('index'))
    logger.info(f"Running scraper with config: {config_file}")
    try:
        config = config_loader.load_config(str(config_path)); logger.debug(f"Loaded config: {config}")
        output_format = config.get('output_format', 'csv'); logger.info(f"Output format: {output_format}")
        output_base_dir = Path(config.get('output_dir', str(app.config['OUTPUT_FOLDER'])))
        job_output_dir = output_base_dir / Path(config_file).stem; job_output_dir.mkdir(parents=True, exist_ok=True)
        config['output_dir'] = str(job_output_dir)
        scraper_instance = DynamicScraper(config) if config.get('dynamic', False) else HTMLScraper(config)
        logger.info(f"Using {scraper_instance.__class__.__name__}")
        result = scraper_instance.run(); logger.info(f"Run completed. Stats: {result.get('stats')}")
        output_path_str = ""
        if result.get('data'):
            storage = None; output_format_lower = output_format.lower(); storage_config = config.copy()
            if output_format_lower == 'csv': storage = CSVStorage(storage_config)
            elif output_format_lower == 'json': storage = JSONStorage(storage_config)
            elif output_format_lower == 'sqlite': storage = SQLiteStorage(storage_config)
            else: logger.error(f"Unsupported format '{output_format}'."); flash(f"Unsupported format '{output_format}'.", "error"); return render_template('results.html', job_name=config.get('name', config_file), output_path="Error", stats=result.get('stats',{}), sample_data=result.get('data',[])[:10])
            output_path_str = storage.save(result['data'])
            logger.info(f"Results saved to: {output_path_str}"); flash(f'Success! Results saved ({output_format.upper()}).', 'success')
        else: flash('Scraping finished, no data collected.', 'warning')
        try: relative_path = Path(output_path_str).relative_to(Path.cwd())
        except (ValueError, TypeError): relative_path = Path(output_path_str).name if output_path_str else "N/A"
        return render_template('results.html', job_name=config.get('name', config_file), output_path=str(relative_path), output_format=output_format.upper(), stats=result.get('stats',{}), sample_data=result.get('data',[])[:10])
    except (ValidationError, yaml.YAMLError) as e: error_path = " -> ".join(map(str, e.path)) or "Config root"; message = f"Config Error: {e.message} (at {error_path})"; logger.error(message); flash(message, 'error'); return redirect(url_for('index'))
    except Exception as e: logger.exception(f"Error running job {config_file}: {e}"); flash(f"Scraping failed: {e}", 'error'); return render_template('error.html', error=str(e), config_file=config_file)


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5001)
