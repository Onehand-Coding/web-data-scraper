# File: web-data-scraper/interfaces/web_app/app.py (Verified with Import)

from flask import Flask, render_template, request, jsonify, redirect, url_for, flash, abort
from pathlib import Path
import yaml
import json # Import json for parsing text area inputs
import time
import os
import logging
from werkzeug.utils import secure_filename
from datetime import datetime

# Scraper and utils imports
from scraper.api_scraper import APIScraper
from scraper.html_scraper import HTMLScraper
from scraper.dynamic_scraper import DynamicScraper
from scraper.storage.csv_handler import CSVStorage
from scraper.storage.json_handler import JSONStorage
from scraper.storage.sqlite_handler import SQLiteStorage
# === Ensure this import is present ===
from scraper.utils.logger import setup_logging
# === End Ensure ===
from scraper.utils.config_loader import ConfigLoader, ValidationError
from jsonschema import ValidationError

app = Flask(__name__)
# Remember to set a strong secret key!
app.secret_key = "my-secret-key" # Replace with a real secret key
BASE_DIR = Path(__file__).resolve().parent
app.config['UPLOAD_FOLDER'] = BASE_DIR / '..' / '..' / 'configs' / 'scraping_jobs'
app.config['UPLOAD_FOLDER'].mkdir(parents=True, exist_ok=True)
app.config['OUTPUT_FOLDER'] = BASE_DIR / '..' / '..' / 'outputs'
app.config['OUTPUT_FOLDER'].mkdir(parents=True, exist_ok=True)

# Setup logging (ensure log files go to project's logs/ dir)
# This line should now work as setup_logging is imported above
setup_logging(log_filename='web_app.log', level=logging.INFO, console_level=logging.DEBUG if app.debug else logging.INFO)
logger = logging.getLogger(__name__)
config_loader = ConfigLoader()

# Template Filter for Timestamps
@app.template_filter('timestamp_to_datetime')
def timestamp_to_datetime_filter(s):
    """Jinja filter to convert Unix timestamp to readable datetime string."""
    try:
        return datetime.fromtimestamp(float(s)).strftime('%Y-%m-%d %H:%M:%S')
    except (ValueError, TypeError, OSError):
        return "N/A"

# Helper Function to Get Config Files
def get_config_files_details():
    """Gets a list of config files with details."""
    configs = []
    folder = app.config['UPLOAD_FOLDER']
    try:
        yaml_files = list(folder.glob('*.yaml'))
    except Exception as e:
        logger.error(f"Error reading config directory {folder}: {e}")
        return [] # Return empty list on error

    for f_path in yaml_files:
        try:
            stat_result = f_path.stat()
            configs.append({
                'name': f_path.name,
                'modified_time': stat_result.st_mtime,
            })
        except Exception as e:
             logger.warning(f"Could not get stats for file {f_path.name}: {e}")
             configs.append({'name': f_path.name, 'modified_time': 0}) # Add with default time

    # Sort by modified time, newest first
    configs.sort(key=lambda x: x.get('modified_time', 0), reverse=True)
    return configs

# --- Routes ---

# Index Route (List Configs)
@app.route('/')
def index():
    """Render the main interface page and list existing configs."""
    config_files_details = get_config_files_details()
    upload_folder_path = "N/A"
    try:
         # Show path relative to project root for clarity
        upload_folder_path = str(app.config['UPLOAD_FOLDER'].relative_to(Path.cwd()))
    except ValueError:
         upload_folder_path = str(app.config['UPLOAD_FOLDER']) # Fallback to absolute path

    return render_template('index.html',
                           config_files=config_files_details,
                           upload_folder=upload_folder_path)

# View Config Route
@app.route('/view_config/<filename>')
def view_config(filename):
    """Display the content of a specific configuration file."""
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

# Delete Config Route
@app.route('/delete_config/<filename>', methods=['POST'])
def delete_config(filename):
    """Delete a specific configuration file."""
    safe_filename = secure_filename(filename)
    if safe_filename != filename or not filename.endswith('.yaml'): abort(400)
    config_path = app.config['UPLOAD_FOLDER'] / safe_filename
    if not config_path.is_file(): flash(f"'{safe_filename}' not found.", "warning")
    else:
        try: config_path.unlink(); logger.info(f"Deleted config: {config_path}"); flash(f"Deleted '{safe_filename}'.", "success")
        except Exception as e: logger.error(f"Error deleting {safe_filename}: {e}"); flash(f"Could not delete '{safe_filename}': {e}", "error")
    return redirect(url_for('index'))


# Edit Config Route (Load into Form)
@app.route('/edit_config/<filename>')
def edit_config(filename):
    """Load an existing config file and render the configuration form for editing."""
    safe_filename = secure_filename(filename)
    if safe_filename != filename or not filename.endswith('.yaml'): abort(404)
    config_path = app.config['UPLOAD_FOLDER'] / safe_filename
    if not config_path.is_file(): flash(f"Config '{safe_filename}' not found.", "error"); return redirect(url_for('index'))

    try:
        config_data = config_loader.load_config(str(config_path))
        logger.info(f"Loading config '{safe_filename}' for editing.")
        form_data = {} # Prepare data for template
        job_type = config_data.get('job_type', 'web')
        form_data['job_type'] = job_type
        form_data['job_name'] = config_data.get('name', '')
        form_data['description'] = config_data.get('description', '')
        defined_field_names = []

        if job_type == 'web':
            form_data['urls'] = "\n".join(config_data.get('urls', [])); form_data['dynamic'] = config_data.get('dynamic', False);
            form_data['wait_for_selector'] = config_data.get('wait_for_selector', ''); form_data['wait_time'] = config_data.get('wait_time', 5)
            selectors = config_data.get('selectors', {}); form_data['selector_type'] = selectors.get('type', 'css');
            form_data['container_selector'] = selectors.get('container', ''); form_data['item_selector'] = selectors.get('item', '')
            fields_list = []
            for name, config in selectors.get('fields', {}).items():
                defined_field_names.append(name)
                if isinstance(config, str): fields_list.append({'name': name, 'selector': config, 'attr': ''})
                elif isinstance(config, dict): fields_list.append({'name': name, 'selector': config.get('selector', ''), 'attr': config.get('attr', '')})
            form_data['fields'] = fields_list
            pagination_config = config_data.get('pagination')
            if pagination_config: form_data['next_page_selector'] = pagination_config.get('next_page_selector', ''); form_data['max_pages'] = pagination_config.get('max_pages', '')

        elif job_type == 'api':
            api_config = config_data.get('api_config', {}); form_data['api_base_url'] = api_config.get('base_url', '')
            form_data['api_endpoints'] = "\n".join(api_config.get('endpoints', [])); form_data['api_method'] = api_config.get('method', 'GET')
            form_data['api_params'] = json.dumps(api_config.get('params', {}), indent=2) if api_config.get('params') else ''
            form_data['api_headers'] = json.dumps(api_config.get('headers', {}), indent=2) if api_config.get('headers') else ''
            form_data['api_data'] = json.dumps(api_config.get('data', {}), indent=2) if api_config.get('data') else ''
            form_data['api_data_path'] = api_config.get('data_path', '')
            api_mappings = api_config.get('field_mappings', {}); form_data['api_field_mappings'] = json.dumps(api_mappings, indent=2) if api_mappings else ''
            defined_field_names = list(api_mappings.keys())

        # Common fields population
        form_data['request_delay'] = config_data.get('request_delay', 1); form_data['max_retries'] = config_data.get('max_retries', 3);
        form_data['user_agent'] = config_data.get('user_agent', 'Python Scraper Framework / 1.0'); form_data['respect_robots'] = config_data.get('respect_robots', True)

        # Format Processing Rules for Template
        processing_rules_raw = config_data.get('processing_rules', {})
        # Field Types
        form_data['field_type_rules'] = []
        for field, type_info in processing_rules_raw.get('field_types', {}).items():
            if field not in defined_field_names: defined_field_names.append(field)
            form_data['field_type_rules'].append({'field': field, 'type': type_info.get('type'), 'format': type_info.get('format', '')})
        # Text Cleaning
        form_data['text_cleaning_rules'] = []
        for field, clean_options in processing_rules_raw.get('text_cleaning', {}).items():
             if field not in defined_field_names: defined_field_names.append(field)
             form_data['text_cleaning_rules'].append({'field': field, 'options': clean_options})
        # Validations
        form_data['validation_rules'] = []
        for field, val_options in processing_rules_raw.get('validations', {}).items():
             if field not in defined_field_names: defined_field_names.append(field)
             form_data['validation_rules'].append({'field': field, 'options': val_options})
        # Drop Fields
        form_data['drop_field_rules'] = []
        for field in processing_rules_raw.get('drop_fields', []):
             if field not in defined_field_names: defined_field_names.append(field)
             form_data['drop_field_rules'].append({'field': field})
        # TODO: Add formatting for transformations later

        form_data['defined_field_names'] = sorted(list(set(defined_field_names)))
        return render_template('configure.html', form_data=form_data)

    except (ValidationError, yaml.YAMLError) as e: error_path = " -> ".join(map(str, e.path)) or "Config root"; message = f"Config Error loading {safe_filename}: {e.message} (at {error_path})"; logger.error(message); flash(f"Cannot edit '{safe_filename}': {message}", 'error'); return redirect(url_for('index'))
    except Exception as e: logger.error(f"Error loading {safe_filename} for editing: {e}", exc_info=True); flash(f"Could not load '{safe_filename}' for editing.", "error"); return redirect(url_for('index'))


# Configure Route (Create or Save Edit)
@app.route('/configure', methods=['GET', 'POST'])
def configure_scraper():
    form_data_for_template = {}
    if request.method == 'POST':
        form_data_for_template = request.form.to_dict(flat=False);
        list_fields = ['urls', 'api_endpoints', 'api_params', 'api_headers', 'api_data', 'api_field_mappings']
        for key in list_fields: form_data_for_template[key] = request.form.get(key, '')
        form_data_for_template['dynamic'] = 'dynamic' in request.form; form_data_for_template['respect_robots'] = 'respect_robots' in request.form
        form_data_for_template['job_type'] = request.form.get('job_type', 'web'); form_data_for_template['selector_type'] = request.form.get('selector_type', 'css')
        form_data_for_template['api_method'] = request.form.get('api_method', 'GET')

        try:
            job_type = request.form.get('job_type', 'web')
            config = { 'name': request.form.get('job_name', f'Job_{int(time.time())}').strip(), 'description': request.form.get('description', '').strip(), 'job_type': job_type, 'output_dir': str(app.config['OUTPUT_FOLDER']), 'request_delay': float(request.form.get('request_delay', 1)), 'max_retries': int(request.form.get('max_retries', 3)), 'user_agent': request.form.get('user_agent', 'Python Scraper Framework / 1.0').strip(), 'respect_robots': 'respect_robots' in request.form, }
            if not config['name']: flash('Job Name required.', 'error'); return render_template('configure.html', form_data=form_data_for_template)

            processing_rules = {}

            if job_type == 'web':
                 urls_input = request.form.get('urls', '').strip()
                 if not urls_input: flash('URLs required.', 'error'); return render_template('configure.html', form_data=form_data_for_template)
                 config['urls'] = [url.strip() for url in urls_input.splitlines() if url.strip()]
                 config['dynamic'] = 'dynamic' in request.form
                 if config['dynamic']:
                      ws = request.form.get('wait_for_selector', '').strip(); wt = request.form.get('wait_time', 5)
                      if ws: config['wait_for_selector'] = ws
                      try: config['wait_time'] = float(wt) if float(wt) >= 0 else 5
                      except ValueError: config['wait_time'] = 5
                 item_selector = request.form.get('item_selector', '').strip()
                 if not item_selector: flash('Item selector required.', 'error'); return render_template('configure.html', form_data=form_data_for_template)
                 container_selector = request.form.get('container_selector', '').strip()
                 fields_dict = {}; field_names = request.form.getlist('field_name[]')
                 field_selectors = request.form.getlist('field_selector[]'); field_attrs = request.form.getlist('field_attr[]')
                 valid_field_found = False
                 for n, s, a in zip(field_names, field_selectors, field_attrs):
                      n=n.strip(); s=s.strip(); a=a.strip()
                      if n and s: valid_field_found=True; fields_dict[n]=({'selector':s,'attr':a} if a else s)
                      elif n or s or a: flash(f'Incomplete field: "{n or "N/A"}".','warning')
                 if not valid_field_found: flash('Define one valid field.', 'error'); return render_template('configure.html', form_data=form_data_for_template)
                 config['selectors'] = {'type':request.form.get('selector_type','css'), **({'container':container_selector} if container_selector else {}), 'item':item_selector, 'fields':fields_dict}
                 next_page_sel = request.form.get('next_page_selector','').strip(); max_pg_str = request.form.get('max_pages','').strip()
                 if next_page_sel: config['pagination']={'next_page_selector':next_page_sel}; config['pagination']['max_pages']=int(max_pg_str) if max_pg_str.isdigit() else float('inf')

            elif job_type == 'api':
                 api_conf = {}; base_url = request.form.get('api_base_url','').strip()
                 if not base_url: flash('Base URL required.', 'error'); return render_template('configure.html', form_data=form_data_for_template)
                 eps = request.form.get('api_endpoints','').strip()
                 if not eps: flash('Endpoints required.', 'error'); return render_template('configure.html', form_data=form_data_for_template)
                 api_conf['base_url']=base_url; api_conf['endpoints']=[ep.strip() for ep in eps.splitlines() if ep.strip()]; api_conf['method']=request.form.get('api_method','GET')
                 try:
                      p_str=request.form.get('api_params','').strip(); h_str=request.form.get('api_headers','').strip(); d_str=request.form.get('api_data','').strip(); m_str=request.form.get('api_field_mappings','').strip()
                      if p_str: api_conf['params'] = json.loads(p_str)
                      if h_str: api_conf['headers'] = json.loads(h_str)
                      if d_str: api_conf['data'] = json.loads(d_str)
                      if m_str: api_conf['field_mappings'] = json.loads(m_str)
                 except json.JSONDecodeError as e: flash(f'Invalid JSON: {e}','error'); return render_template('configure.html',form_data=form_data_for_template)
                 dp=request.form.get('api_data_path','').strip()
                 if dp: api_conf['data_path'] = dp
                 config['api_config'] = api_conf

            # Parse Structured Processing Rules
            logger.debug("--- Parsing Processing Rules ---")
            # Field Types
            ft_fields = request.form.getlist('ft_field[]'); ft_types = request.form.getlist('ft_type[]'); ft_formats = request.form.getlist('ft_format[]')
            logger.debug(f"Received ft_field[]: {ft_fields}"); logger.debug(f"Received ft_type[]: {ft_types}"); logger.debug(f"Received ft_format[]: {ft_formats}")
            field_types_dict = {}
            for field, type_val, format_val in zip(ft_fields, ft_types, ft_formats):
                 logger.debug(f"Processing ft zip: field='{field}', type='{type_val}', format='{format_val}'")
                 if field and type_val: field_types_dict[field] = {'type':type_val, **({'format':format_val.strip()} if format_val.strip() and type_val in ['datetime','date'] else {})}
            if field_types_dict: processing_rules['field_types'] = field_types_dict
            # Text Cleaning
            text_cleaning_dict = {}; i = 0
            while True:
                field = request.form.get(f'tc_field_{i}');
                if field is None: break
                if field: options = { 'trim': request.form.get(f'tc_trim_{i}') == 'true', 'lowercase': request.form.get(f'tc_lowercase_{i}') == 'true', 'uppercase': request.form.get(f'tc_uppercase_{i}') == 'true', 'remove_newlines': request.form.get(f'tc_newlines_{i}') == 'true', 'remove_extra_spaces': request.form.get(f'tc_spaces_{i}') == 'true', 'remove_special_chars': request.form.get(f'tc_special_{i}') == 'true' }; text_cleaning_dict[field] = {k: v for k, v in options.items() if v}; logger.debug(f"Processing tc rule {i}: field='{field}', options={text_cleaning_dict[field]}")
                i += 1
            if text_cleaning_dict: processing_rules['text_cleaning'] = text_cleaning_dict
            # Validations
            validations_dict = {}; i = 0
            while True:
                 field = request.form.get(f'val_field_{i}')
                 if field is None: break
                 if field:
                      rules = {}; required = request.form.get(f'val_required_{i}') == 'true'; min_len_str = request.form.get(f'val_min_length_{i}', '').strip(); max_len_str = request.form.get(f'val_max_length_{i}', '').strip(); pattern = request.form.get(f'val_pattern_{i}', '').strip()
                      if required: rules['required'] = True
                      if min_len_str.isdigit(): rules['min_length'] = int(min_len_str)
                      if max_len_str.isdigit(): rules['max_length'] = int(max_len_str)
                      if pattern: rules['pattern'] = pattern
                      if rules: validations_dict[field] = rules; logger.debug(f"Processing val rule {i}: field='{field}', rules={rules}")
                 i += 1
            if validations_dict: processing_rules['validations'] = validations_dict
            # Drop Fields
            df_fields = request.form.getlist('df_field[]')
            logger.debug(f"Received df_field[]: {df_fields}")
            drop_fields_list = [field for field in df_fields if field]
            if drop_fields_list: processing_rules['drop_fields'] = drop_fields_list
            # --- End Rule Parsing ---

            logger.debug(f"Constructed processing_rules dict: {processing_rules}")
            if processing_rules: config['processing_rules'] = processing_rules
            else: logger.debug("Processing_rules dict empty.")

            logger.debug(f"Final config dictionary before validation: {config}")
            config_loader.validate_config(config)

            safe_job_name = secure_filename(config['name']); timestamp = int(time.time()); config_filename = f"{safe_job_name}-{timestamp}.yaml"; config_path = app.config['UPLOAD_FOLDER'] / config_filename
            with open(config_path, 'w', encoding='utf-8') as f: yaml.dump(config, f, sort_keys=False, default_flow_style=False, allow_unicode=True)
            logger.info(f"Config saved: {config_path}"); flash(f'Config "{config["name"]}" saved as {config_filename}!', 'success'); return redirect(url_for('index'))

        except ValidationError as e: error_path = " -> ".join(map(str, e.path)) or "Config root"; message = f"Config Error: {e.message} (at {error_path})"; logger.error(f"Validation failed: {message}"); flash(message, 'error'); return render_template('configure.html', form_data=form_data_for_template)
        except Exception as e: logger.exception(f"Error saving config: {e}"); flash(f'Unexpected error saving: {e}', 'error'); return render_template('configure.html', form_data=form_data_for_template)

    # --- GET Request ---
    return render_template('configure.html', form_data={'job_type': 'web'})


# Run Scraper Route
@app.route('/run/<config_file>', endpoint='run_job')
def run_scraper(config_file):
    safe_filename = secure_filename(config_file)
    config_path = app.config['UPLOAD_FOLDER'] / safe_filename
    if not config_path.is_file(): flash(f"Config file '{safe_filename}' not found.", 'error'); return redirect(url_for('index'))
    logger.info(f"Running job from config: {safe_filename}")
    try:
        config = config_loader.load_config(str(config_path)); logger.debug(f"Loaded config: {config}")
        job_type = config.get('job_type', 'web')
        output_format = config.get('output_format', 'csv')
        output_base_dir = Path(config.get('output_dir', str(app.config['OUTPUT_FOLDER'])))
        job_output_dir = output_base_dir / Path(safe_filename).stem; job_output_dir.mkdir(parents=True, exist_ok=True)
        config['output_dir'] = str(job_output_dir)
        scraper_instance = None
        if job_type == 'api': logger.info("Using APIScraper"); scraper_instance = APIScraper(config)
        elif job_type == 'web':
             if config.get('dynamic', False): scraper_instance = DynamicScraper(config)
             else: scraper_instance = HTMLScraper(config)
             logger.info(f"Using {scraper_instance.__class__.__name__}")
        else: raise ValueError(f"Invalid job_type '{job_type}' found.")
        result = scraper_instance.run(); logger.info(f"Run completed. Stats: {result.get('stats')}")
        output_path_str = ""
        if result.get('data'):
            storage = None; output_format_lower = output_format.lower(); storage_config = config.copy()
            if output_format_lower == 'csv': storage = CSVStorage(storage_config)
            elif output_format_lower == 'json': storage = JSONStorage(storage_config)
            elif output_format_lower == 'sqlite': storage = SQLiteStorage(storage_config)
            else: logger.error(f"Unsupported format '{output_format}'."); flash(f"Unsupported format '{output_format}'.", "error"); return render_template('results.html', job_name=config.get('name', safe_filename), output_path="Error", stats=result.get('stats',{}), sample_data=result.get('data',[])[:10])
            output_path_str = storage.save(result['data'])
            logger.info(f"Results saved to: {output_path_str}"); flash(f'Success! Results saved ({output_format.upper()}).', 'success')
        else: flash('Scraping finished, no data collected.', 'warning')
        try: relative_path = Path(output_path_str).relative_to(Path.cwd())
        except (ValueError, TypeError): relative_path = Path(output_path_str).name if output_path_str else "N/A"
        return render_template('results.html', job_name=config.get('name', safe_filename), output_path=str(relative_path), output_format=output_format.upper(), stats=result.get('stats',{}), sample_data=result.get('data',[])[:10])
    except (ValidationError, yaml.YAMLError) as e: error_path = " -> ".join(map(str, e.path)) or "Config root"; message = f"Config Error: {e.message} (at {error_path})"; logger.error(message); flash(message, 'error'); return redirect(url_for('index'))
    except Exception as e: logger.exception(f"Error running job {safe_filename}: {e}"); flash(f"Scraping failed: {e}", 'error'); return render_template('error.html', error=str(e), config_file=safe_filename)


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5001)
