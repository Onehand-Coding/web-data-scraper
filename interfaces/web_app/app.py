# File: web-data-scraper/interfaces/web_app/app.py (Corrected SyntaxError Line 244)

from flask import Flask, render_template, request, jsonify, redirect, url_for, flash, abort
from pathlib import Path
import yaml
import json
import time
import os
import logging
import re
from werkzeug.utils import secure_filename
from datetime import datetime

# Scraper and utils imports
from scraper.api_scraper import APIScraper
from scraper.html_scraper import HTMLScraper
from scraper.dynamic_scraper import DynamicScraper
from scraper.storage.csv_handler import CSVStorage
from scraper.storage.json_handler import JSONStorage
from scraper.storage.sqlite_handler import SQLiteStorage
from scraper.utils.logger import setup_logging
from scraper.utils.config_loader import ConfigLoader
from jsonschema import ValidationError as JsonSchemaValidationError # Alias to avoid name clash

app = Flask(__name__)
app.secret_key = os.environ.get("FLASK_SECRET_KEY", "default-dev-secret-key-change-me!") # Use env var or default
BASE_DIR = Path(__file__).resolve().parent
CONFIG_DIR = BASE_DIR / '..' / '..' / 'configs' / 'scraping_jobs'
OUTPUT_DIR = BASE_DIR / '..' / '..' / 'outputs'
# Ensure directories exist
CONFIG_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# Setup logging
log_level = logging.DEBUG if app.debug else logging.INFO
setup_logging(log_filename='web_app.log', level=log_level, console_level=log_level)
logger = logging.getLogger(__name__)
config_loader = ConfigLoader()

# Template Filter for Timestamps
@app.template_filter('timestamp_to_datetime')
def timestamp_to_datetime_filter(s):
    """Jinja filter to convert Unix timestamp to readable datetime string."""
    try: return datetime.fromtimestamp(float(s)).strftime('%Y-%m-%d %H:%M:%S')
    except: return "N/A"

# Helper Function to Get Config Files
def get_config_files_details():
    """Gets a list of config files with details."""
    configs = []
    try: yaml_files = list(CONFIG_DIR.glob('*.yaml'))
    except Exception as e: logger.error(f"Error reading config directory {CONFIG_DIR}: {e}"); return []
    for f_path in yaml_files:
        try: stat_result = f_path.stat(); configs.append({'name': f_path.name, 'modified_time': stat_result.st_mtime})
        except Exception as e: logger.warning(f"Could not get stats for {f_path.name}: {e}"); configs.append({'name': f_path.name, 'modified_time': 0})
    configs.sort(key=lambda x: x.get('modified_time', 0), reverse=True)
    return configs

# --- Helper to Parse Form Rules (Corrected) ---
def parse_processing_rules(form):
    """Parses dynamic processing rules from Flask form data."""
    rules = {}
    logger.debug(f"Parsing processing rules from form: {form}")
    # Field Types
    ft_fields=form.getlist('ft_field[]');ft_types=form.getlist('ft_type[]');ft_formats=form.getlist('ft_format[]')
    logger.debug(f"Raw Field Types: fields={ft_fields}, types={ft_types}, formats={ft_formats}")
    ft_dict={f:{'type':t,**({'format':fmt.strip()}if fmt.strip()and t in['datetime','date']else{})} for f,t,fmt in zip(ft_fields,ft_types,ft_formats)if f and t};
    if ft_dict:rules['field_types']=ft_dict
    # Text Cleaning
    tc_dict={};tc_keys=[k for k in form if re.match(r'^tc_field_\d+$',k)]
    logger.debug(f"Found Text Cleaning Keys: {tc_keys}")
    for key in tc_keys:
        match = re.match(r'^tc_field_(\d+)$', key)
        if match:
            index = match.group(1); field = form.get(key)
            if field:
                opts={'trim': form.get(f'tc_trim_{index}')=='true','lowercase': form.get(f'tc_lowercase_{index}')=='true','uppercase': form.get(f'tc_uppercase_{index}')=='true','remove_newlines': form.get(f'tc_newlines_{index}')=='true','remove_extra_spaces': form.get(f'tc_spaces_{index}')=='true','remove_special_chars': form.get(f'tc_special_{index}')=='true'}
                active={k:v for k,v in opts.items() if v}
                if active: tc_dict[field]=active
    if tc_dict:rules['text_cleaning']=tc_dict
    # Validations
    val_dict={};val_keys=[k for k in form if re.match(r'^val_field_\d+$',k)]
    logger.debug(f"Found Validation Keys: {val_keys}")
    for key in val_keys:
        match = re.match(r'^val_field_(\d+)$', key)
        if match:
            index = match.group(1); field = form.get(key)
            if field:
                vr={'required':form.get(f'val_required_{index}')=='true'};min_len=form.get(f'val_min_length_{index}','').strip();max_len=form.get(f'val_max_length_{index}','').strip();pattern=form.get(f'val_pattern_{index}','').strip()
                if min_len.isdigit():vr['min_length']=int(min_len)
                if max_len.isdigit():vr['max_length']=int(max_len)
                if pattern:vr['pattern']=pattern
                if vr.get('required') or len(vr) > 1: val_dict[field]=vr
    if val_dict:rules['validations']=val_dict
    # Transformations
    tr_dict={};tr_keys=sorted([k for k in form if re.match(r'^tr_target_field_\d+$',k)],key=lambda x:int(re.search(r'_(\d+)$',x).group(1)))
    logger.debug(f"Found Transformation Keys: {tr_keys}")
    for key in tr_keys:
        match = re.match(r'^tr_target_field_(\d+)$', key)
        if match:
            index = match.group(1); target=form.get(key,'').strip(); expr=form.get(f'tr_expression_{index}','').strip()
            if target and expr:tr_dict[target]=expr
    if tr_dict:rules['transformations']=tr_dict
    # Drop Fields
    df_fields=[f for f in form.getlist('df_field[]')if f]; logger.debug(f"Found Drop Fields: {df_fields}")
    if df_fields:rules['drop_fields']=df_fields
    logger.info(f"Parsed processing_rules: {rules}")
    return rules
# --- End Helper ---

# --- Routes ---

@app.route('/')
def index():
    config_files_details = get_config_files_details()
    config_dir_display = str(CONFIG_DIR.relative_to(Path.cwd())) if CONFIG_DIR.is_relative_to(Path.cwd()) else str(CONFIG_DIR)
    return render_template('index.html', config_files=config_files_details, upload_folder=config_dir_display)

@app.route('/view_config/<filename>')
def view_config(filename):
    safe_filename = secure_filename(filename); config_path = CONFIG_DIR / safe_filename
    if safe_filename != filename or not filename.endswith('.yaml') or not config_path.is_file(): abort(404)
    try:
        with open(config_path, 'r', encoding='utf-8') as f: config_content = f.read()
        return render_template('view_config.html', filename=safe_filename, content=config_content)
    except Exception as e: logger.error(f"Error reading {safe_filename}: {e}"); flash(f"Cannot read '{safe_filename}'.", "error"); return redirect(url_for('index'))

@app.route('/delete_config/<filename>', methods=['POST'])
def delete_config(filename):
    safe_filename = secure_filename(filename); config_path = CONFIG_DIR / safe_filename
    if safe_filename != filename or not filename.endswith('.yaml'): abort(400)
    if not config_path.is_file(): flash(f"'{safe_filename}' not found.", "warning")
    else:
        try: config_path.unlink(); logger.info(f"Deleted config: {config_path}"); flash(f"Deleted '{safe_filename}'.", "success")
        except Exception as e: logger.error(f"Error deleting {safe_filename}: {e}"); flash(f"Could not delete '{safe_filename}': {e}", "error")
    return redirect(url_for('index'))

@app.route('/edit_config/<filename>')
def edit_config(filename):
    safe_filename = secure_filename(filename); config_path = CONFIG_DIR / safe_filename
    if safe_filename != filename or not filename.endswith('.yaml') or not config_path.is_file(): flash(f"'{safe_filename}' not found.", "error"); return redirect(url_for('index'))
    try:
        config_data = config_loader.load_config(str(config_path))
        logger.info(f"Loading config '{safe_filename}' for editing.")
        form_data = {'job_name': config_data.get('name'), 'description': config_data.get('description'), 'job_type': config_data.get('job_type', 'web'), 'request_delay': config_data.get('request_delay', 1), 'max_retries': config_data.get('max_retries', 3), 'user_agent': config_data.get('user_agent'), 'respect_robots': config_data.get('respect_robots', True)}
        defined_field_names = []
        if form_data['job_type'] == 'web':
            form_data['urls'] = "\n".join(config_data.get('urls', [])); form_data['dynamic'] = config_data.get('dynamic', False)
            form_data['wait_for_selector'] = config_data.get('wait_for_selector'); form_data['wait_time'] = config_data.get('wait_time', 5)
            selectors = config_data.get('selectors', {}); form_data['selector_type'] = selectors.get('type', 'css')
            form_data['container_selector'] = selectors.get('container'); form_data['item_selector'] = selectors.get('item')
            fields_list = []
            for name, cfg in selectors.get('fields', {}).items():
                defined_field_names.append(name); fields_list.append({'name': name, 'selector': cfg.get('selector') if isinstance(cfg, dict) else cfg, 'attr': cfg.get('attr', '') if isinstance(cfg, dict) else ''})
            form_data['fields'] = fields_list
            pagination = config_data.get('pagination'); form_data['next_page_selector'] = pagination.get('next_page_selector') if pagination else None; form_data['max_pages'] = pagination.get('max_pages') if pagination else None
            login_cfg = config_data.get('login_config');
            if login_cfg: form_data['login_url'] = login_cfg.get('login_url'); form_data['username_selector'] = login_cfg.get('username_selector'); form_data['password_selector'] = login_cfg.get('password_selector'); form_data['submit_selector'] = login_cfg.get('submit_selector'); form_data['username_cred'] = login_cfg.get('username'); form_data['password_cred'] = login_cfg.get('password'); form_data['success_selector'] = login_cfg.get('success_selector'); form_data['success_url_contains'] = login_cfg.get('success_url_contains'); form_data['wait_after_login'] = login_cfg.get('wait_after_login', 3)
        elif form_data['job_type'] == 'api':
            api_cfg = config_data.get('api_config', {}); form_data['api_base_url'] = api_cfg.get('base_url'); form_data['api_endpoints'] = "\n".join(api_cfg.get('endpoints', [])); form_data['api_method'] = api_cfg.get('method', 'GET'); form_data['api_params'] = json.dumps(api_cfg.get('params'), indent=2) if api_cfg.get('params') else ''; form_data['api_headers'] = json.dumps(api_cfg.get('headers'), indent=2) if api_cfg.get('headers') else ''; form_data['api_data'] = json.dumps(api_cfg.get('data'), indent=2) if api_cfg.get('data') else ''; form_data['api_data_path'] = api_cfg.get('data_path'); form_data['api_field_mappings'] = json.dumps(api_cfg.get('field_mappings'), indent=2) if api_cfg.get('field_mappings') else ''; defined_field_names = list(api_cfg.get('field_mappings', {}).keys())
        rules_raw = config_data.get('processing_rules', {})
        form_data['field_type_rules'] = [{'field':f,**t} for f,t in rules_raw.get('field_types',{}).items()]; defined_field_names.extend(rules_raw.get('field_types',{}).keys())
        form_data['text_cleaning_rules'] = [{'field':f,'options':o} for f,o in rules_raw.get('text_cleaning',{}).items()]; defined_field_names.extend(rules_raw.get('text_cleaning',{}).keys())
        form_data['validation_rules'] = [{'field':f,'options':o} for f,o in rules_raw.get('validations',{}).items()]; defined_field_names.extend(rules_raw.get('validations',{}).keys())
        form_data['transformation_rules'] = [{'target_field':f,'expression':e} for f,e in rules_raw.get('transformations',{}).items()]; defined_field_names.extend(rules_raw.get('transformations',{}).keys())
        form_data['drop_field_rules'] = [{'field':f} for f in rules_raw.get('drop_fields',[])]; defined_field_names.extend(rules_raw.get('drop_fields',[]))
        form_data['defined_field_names'] = sorted(list(set(defined_field_names)))
        return render_template('configure.html', form_data=form_data)
    except (JsonSchemaValidationError, yaml.YAMLError) as e: error_msg = f"Config Error: {getattr(e, 'message', str(e))}"; logger.error(f"Config load error for {safe_filename}: {error_msg}"); flash(f"Cannot edit '{safe_filename}': {error_msg}", 'error'); return redirect(url_for('index'))
    except Exception as e: logger.error(f"Error loading {safe_filename} for editing: {e}", exc_info=True); flash(f"Could not load '{safe_filename}'.", "error"); return redirect(url_for('index'))

@app.route('/configure', methods=['GET', 'POST'])
def configure_scraper():
    if request.method == 'POST':
        form_data_for_template = request.form.to_dict()
        try:
            job_type = request.form.get('job_type', 'web')
            config = {'name': request.form.get('job_name', f'Job_{int(time.time())}').strip(),'description': request.form.get('description', '').strip(),'job_type': job_type,'output_dir': str(OUTPUT_DIR),'request_delay': float(request.form.get('request_delay', 1)),'max_retries': int(request.form.get('max_retries', 3)),'user_agent': request.form.get('user_agent', '').strip(),'respect_robots': 'respect_robots' in request.form,'proxies': []}
            if not config['name']: flash('Job Name required.', 'error'); return render_template('configure.html', form_data=form_data_for_template)
            if job_type == 'web':
                 urls_input = request.form.get('urls', '').strip()
                 if not urls_input: flash('Target URLs required.', 'error'); return render_template('configure.html', form_data=form_data_for_template)
                 config['urls'] = [url.strip() for url in urls_input.splitlines() if url.strip()]
                 config['dynamic'] = 'dynamic' in request.form
                 if config['dynamic']:
                      ws = request.form.get('wait_for_selector', '').strip(); wt = request.form.get('wait_time', 5)
                      if ws: config['wait_for_selector'] = ws
                      try: config['wait_time'] = float(wt) if float(wt) >= 0 else 5
                      except ValueError: config['wait_time'] = 5
                      login_url = request.form.get('login_url', '').strip()
                      if login_url:
                           login_cfg = {'login_url': login_url, 'username_selector': request.form.get('username_selector'), 'password_selector': request.form.get('password_selector'), 'submit_selector': request.form.get('submit_selector'), 'username': request.form.get('username_cred'), 'password': request.form.get('password_cred'), 'wait_after_login': float(request.form.get('wait_after_login', 3))}
                           ss = request.form.get('success_selector','').strip(); su = request.form.get('success_url_contains','').strip()
                           if ss: login_cfg['success_selector'] = ss
                           if su: login_cfg['success_url_contains'] = su
                           if not ss and not su: flash('Login requires Success Selector or Success URL.', 'error'); return render_template('configure.html', form_data=form_data_for_template)
                           required_login_fields = ['username_selector', 'password_selector', 'submit_selector', 'username', 'password']
                           if not all(login_cfg.get(f) for f in required_login_fields): flash('Missing required login configuration fields.', 'error'); return render_template('configure.html', form_data=form_data_for_template)
                           config['login_config'] = login_cfg
                 item_selector = request.form.get('item_selector', '').strip()
                 if not item_selector: flash('Item selector required.', 'error'); return render_template('configure.html', form_data=form_data_for_template)
                 container_selector = request.form.get('container_selector', '').strip()
                 fields_dict = {}; f_names=request.form.getlist('field_name[]');f_sels=request.form.getlist('field_selector[]');f_attrs=request.form.getlist('field_attr[]')
                 valid_field=False
                 for n,s,a in zip(f_names,f_sels,f_attrs):
                     n=n.strip();s=s.strip();a=a.strip()
                     if n and s: valid_field=True; fields_dict[n]=({'selector':s,'attr':a} if a else s)
                     elif n or s or a: flash(f'Incomplete field: Name="{n}", Selector="{s}", Attr="{a}". Skipped.','warning')
                 if not valid_field: flash('Define at least one valid field.', 'error'); return render_template('configure.html', form_data=form_data_for_template)
                 config['selectors'] = {'type':request.form.get('selector_type','css'), **({'container':container_selector} if container_selector else {}), 'item':item_selector, 'fields':fields_dict}
                 next_page_sel = request.form.get('next_page_selector','').strip(); max_pg = request.form.get('max_pages','').strip()
                 if next_page_sel: config['pagination']={'next_page_selector':next_page_sel}; config['pagination']['max_pages']=int(max_pg) if max_pg.isdigit() else float('inf')
            elif job_type == 'api':
                 api_conf = {}; base_url = request.form.get('api_base_url','').strip()
                 if not base_url: flash('Base URL required.', 'error'); return render_template('configure.html', form_data=form_data_for_template)
                 eps = request.form.get('api_endpoints','').strip()
                 if not eps: flash('Endpoints required.', 'error'); return render_template('configure.html', form_data=form_data_for_template)
                 api_conf['base_url']=base_url; api_conf['endpoints']=[ep.strip() for ep in eps.splitlines() if ep.strip()]; api_conf['method']=request.form.get('api_method','GET')
                 try:
                      p=request.form.get('api_params','').strip(); h=request.form.get('api_headers','').strip(); d=request.form.get('api_data','').strip(); m=request.form.get('api_field_mappings','').strip()
                      if p: api_conf['params'] = json.loads(p)
                      if h: api_conf['headers'] = json.loads(h)
                      if d: api_conf['data'] = json.loads(d)
                      if m: api_conf['field_mappings'] = json.loads(m)
                 except json.JSONDecodeError as e: flash(f'Invalid JSON in API config: {e}','error'); return render_template('configure.html',form_data=form_data_for_template)
                 # --- CORRECTED SYNTAX: Put 'if' on new line ---
                 dp = request.form.get('api_data_path','').strip()
                 if dp:
                     api_conf['data_path'] = dp
                 # --- End Correction ---
                 config['api_config'] = api_conf
            processing_rules = parse_processing_rules(request.form)
            if processing_rules: config['processing_rules'] = processing_rules
            config_loader.validate_config(config)
            safe_job_name = secure_filename(config['name']); timestamp = int(time.time()); config_filename = f"{safe_job_name}-{timestamp}.yaml"; config_path = CONFIG_DIR / config_filename
            with open(config_path, 'w', encoding='utf-8') as f: yaml.dump(config, f, sort_keys=False, default_flow_style=False, allow_unicode=True)
            logger.info(f"Config saved: {config_path}"); flash(f'Config "{config["name"]}" saved as {config_filename}!', 'success'); return redirect(url_for('index'))
        except JsonSchemaValidationError as e: error_path = " -> ".join(map(str, e.path)) or "Config root"; message = f"Config Error: {e.message} (at {error_path})"; logger.error(f"Validation failed: {message}"); flash(message, 'error'); return render_template('configure.html', form_data=form_data_for_template)
        except Exception as e: logger.exception(f"Error saving config: {e}"); flash(f'Error saving config: {e}', 'error'); return render_template('configure.html', form_data=form_data_for_template)
    # GET Request
    return render_template('configure.html', form_data={'job_type': 'web'})

@app.route('/run/<config_file>', endpoint='run_job')
def run_scraper(config_file):
    safe_filename = secure_filename(config_file); config_path = CONFIG_DIR / safe_filename
    if not config_path.is_file(): flash(f"Config '{safe_filename}' not found.", 'error'); return redirect(url_for('index'))
    logger.info(f"Running job from config: {safe_filename}")
    try:
        config = config_loader.load_config(str(config_path)); logger.debug(f"Loaded config: {config}")
        job_type = config.get('job_type', 'web'); output_format = request.args.get('format', 'csv')
        job_output_dir = OUTPUT_DIR / Path(safe_filename).stem; job_output_dir.mkdir(parents=True, exist_ok=True)
        config_for_run = config.copy(); config_for_run['output_dir'] = str(job_output_dir)
        scraper_instance = None
        if job_type == 'api': logger.info("Using APIScraper"); scraper_instance = APIScraper(config_for_run)
        elif job_type == 'web':
             if config_for_run.get('dynamic', False): scraper_instance = DynamicScraper(config_for_run)
             else: scraper_instance = HTMLScraper(config_for_run)
             logger.info(f"Using {scraper_instance.__class__.__name__}")
        else: raise ValueError(f"Invalid job_type '{job_type}'.")
        result = scraper_instance.run(); logger.info(f"Run completed. Stats: {result.get('stats')}")
        output_path_str = ""; data_to_save = result.get('data')
        if data_to_save:
            storage = None; fmt_lower = output_format.lower(); storage_cfg=config_for_run
            if fmt_lower == 'csv': storage = CSVStorage(storage_cfg)
            elif fmt_lower == 'json': storage = JSONStorage(storage_cfg)
            elif fmt_lower == 'sqlite': storage = SQLiteStorage(storage_cfg)
            else: flash(f"Unsupported format '{output_format}'.", "error"); return render_template('results.html', job_name=config.get('name', safe_filename), output_path="Error", stats=result.get('stats',{}), sample_data=data_to_save[:10])
            try:
                 output_path_str = storage.save(data_to_save)
                 logger.info(f"Results saved to: {output_path_str}"); flash(f'Success! Results saved ({output_format.upper()}).', 'success')
            except Exception as e:
                 logger.error(f"Failed to save results: {e}", exc_info=True); flash(f"Failed to save results: {e}", "error"); output_path_str = f"Error saving: {e}"
        else: flash('Scraping finished, but no data was collected or saved.', 'warning'); output_path_str = "N/A"
        relative_path_str = "N/A"
        if output_path_str and output_path_str != "N/A" and "Error" not in output_path_str:
             try: relative_path = Path(output_path_str).relative_to(Path.cwd()); relative_path_str = str(relative_path)
             except (ValueError, TypeError): relative_path_str = output_path_str
        elif "Error" in output_path_str: relative_path_str = output_path_str
        return render_template('results.html', job_name=config.get('name', safe_filename), output_path=relative_path_str, output_format=output_format.upper(), stats=result.get('stats',{}), sample_data=data_to_save[:10] if data_to_save else [])
    except (JsonSchemaValidationError, yaml.YAMLError) as e: error_path = " -> ".join(map(str, getattr(e, 'path', []))) or "Config root"; message = f"Config Error running {safe_filename}: {getattr(e, 'message', str(e))} (at {error_path})"; logger.error(message); flash(message, 'error'); return redirect(url_for('index'))
    except Exception as e: logger.exception(f"Error running job {safe_filename}: {e}"); flash(f"Scraping failed for '{config.get('name', safe_filename)}': {e}", 'error'); return render_template('error.html', error=str(e), config_file=safe_filename)

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5001)
