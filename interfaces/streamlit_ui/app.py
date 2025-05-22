# File: web-data-scraper/interfaces/streamlit_ui/app.py
# - "Example Jobs" page:
#   - Action buttons (View YAML, Run, Use as Template) are now icons in columns.
#   - Viewing an example YAML now has its own "Close View" button within its expander.
# - Maintained other UI enhancements (sidebar icons, Manage Jobs button layout, etc.).

import os
import time
import json
import yaml
import logging
from pathlib import Path
from datetime import datetime

import pandas as pd
import streamlit as st

# --- Project Setup & Imports ---
CURRENT_FILE_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = CURRENT_FILE_DIR.parent.parent
CONFIG_DIR = PROJECT_ROOT / 'configs' / 'scraping_jobs'
EXAMPLE_CONFIG_DIR = PROJECT_ROOT / 'configs' / 'example_templates'
OUTPUT_DIR = PROJECT_ROOT / 'outputs'
LOGS_DIR = PROJECT_ROOT / 'logs'

CONFIG_DIR.mkdir(parents=True, exist_ok=True)
EXAMPLE_CONFIG_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
LOGS_DIR.mkdir(parents=True, exist_ok=True)

import sys
sys.path.append(str(PROJECT_ROOT))

from scraper.utils.logger import setup_logging
from scraper.utils.config_loader import ConfigLoader
from scraper.html_scraper import HTMLScraper
from scraper.dynamic_scraper import DynamicScraper
from scraper.api_scraper import APIScraper
from scraper.storage.csv_handler import CSVStorage
from scraper.storage.json_handler import JSONStorage
from scraper.storage.sqlite_handler import SQLiteStorage
from jsonschema import ValidationError as JsonSchemaValidationError

log_level = logging.INFO
if not logging.getLogger().handlers:
    setup_logging(log_filename='streamlit_app.log', log_dir=LOGS_DIR, level=log_level, console_level=log_level)

logger = logging.getLogger(__name__)
config_loader = ConfigLoader()

# --- Utility Functions ---
def get_config_files_details(directory: Path) -> list:
    configs = []
    if not directory.exists():
        logger.warning(f"Configuration directory {directory} does not exist.")
        return configs
    try:
        yaml_files = list(directory.glob('*.yaml')) + list(directory.glob('*.yml'))
    except Exception as e:
        logger.error(f"Error reading config directory {directory}: {e}")
        return configs

    for f_path in yaml_files:
        try:
            config_data = None
            if directory == EXAMPLE_CONFIG_DIR:
                try:
                    config_data = config_loader.load_config(str(f_path))
                except Exception:
                    pass

            name_to_display = f_path.name
            description_to_display = "N/A"
            if config_data:
                name_to_display = config_data.get('name', f_path.name)
                description_to_display = config_data.get('description', 'N/A')

            stat_result = f_path.stat()
            configs.append({
                'display_name': name_to_display,
                'description': description_to_display,
                'filename': f_path.name,
                'path': f_path,
                'modified_time': stat_result.st_mtime
            })
        except Exception as e:
            logger.warning(f"Could not get stats/details for {f_path.name}: {e}")
            configs.append({'display_name': f_path.name, 'filename': f_path.name, 'description':'Error loading details', 'path': f_path, 'modified_time': 0})

    configs.sort(key=lambda x: x.get('display_name', '').lower())
    return configs

def timestamp_to_datetime_str(timestamp):
    try: return datetime.fromtimestamp(float(timestamp)).strftime('%Y-%m-%d %H:%M:%S')
    except: return "N/A"

def load_config_data_from_path(config_path: Path):
    if config_path.is_file():
        try:
            return config_loader.load_config(str(config_path))
        except yaml.YAMLError as e:
            st.error(f"Invalid YAML format in '{config_path.name}': {e}")
            logger.error(f"YAML error loading {config_path.name}: {e}")
            return None
        except Exception as e:
            st.error(f"Error loading configuration '{config_path.name}': {e}")
            logger.error(f"Unexpected error loading {config_path.name}: {e}")
            return None
    st.error(f"Config file not found at path: {config_path}")
    return None

def generate_unique_id(): return str(time.time_ns())

def get_available_field_names():
    fv_local = st.session_state.get('form_values', get_default_form_values())
    names = set()
    if fv_local.get('form_job_type') == 'web':
        for field_item in fv_local.get('form_fields_list', []):
            name = field_item.get('name', '').strip()
            if name: names.add(name)
    elif fv_local.get('form_job_type') == 'api':
        for map_item in fv_local.get('form_api_field_mappings_list', []):
            name = map_item.get('output_name', '').strip()
            if name: names.add(name)
    for trans_rule in fv_local.get('form_processing_rules_transformations', []):
        name = trans_rule.get('target_field', '').strip()
        if name: names.add(name)
    return sorted(list(names)) if names else ["(No fields available yet)"]

# --- Form Data Initialization and Population ---
def get_default_form_values():
    return {
        'form_job_name': '', 'form_description': '', 'form_job_type': 'web',
        'form_urls': '', 'form_dynamic': False,
        'form_wait_for_selector': '', 'form_wait_time': 5.0,
        'form_headless': True, 'form_disable_images': True,
        'form_page_load_timeout': 30, 'form_webdriver_path': '',
        'form_login_url': '', 'form_username_selector': '', 'form_password_selector': '', 'form_submit_selector': '',
        'form_username_cred': '', 'form_password_cred': '', 'form_success_selector': '', 'form_success_url_contains': '',
        'form_wait_after_login': 3.0,
        'form_selector_type': 'css', 'form_container_selector': '', 'form_item_selector': '',
        'form_fields_list': [{'id': generate_unique_id(), 'name': '', 'selector': '', 'attr': ''}],
        'form_next_page_selector': '', 'form_max_pages': '',
        'form_api_base_url': '', 'form_api_endpoints': '', 'form_api_method': 'GET',
        'form_api_params': '{}', 'form_api_headers': '{}', 'form_api_data': '{}',
        'form_api_data_path': '',
        'form_api_field_mappings_list': [{'id': generate_unique_id(), 'output_name': '', 'source_name': ''}],
        'form_request_delay': 1.0, 'form_max_retries': 3, 'form_user_agent': 'Streamlit Scraper Bot/1.0',
        'form_respect_robots': True,
        'form_proxies_list': [],
        'form_output_format': 'csv',
        'form_processing_rules_field_types': [],
        'form_processing_rules_text_cleaning': [],
        'form_processing_rules_validations': [],
        'form_processing_rules_transformations': [],
        'form_processing_rules_drop_fields': [],
        'existing_config_filename': None
    }

def get_default_text_cleaning_rule():
    return {
        'id': generate_unique_id(), 'field': '',
        'trim': True,
        'case_transform': 'None',
        'remove_newlines': True, 'remove_extra_spaces': True,
        'remove_special_chars': False, 'regex_replace_json': '{}'
    }

def get_default_proxy_item():
    return {'id': generate_unique_id(), 'http': '', 'https': ''}

def populate_form_values_from_config(config_data, existing_filename=None, is_template=False):
    defaults = get_default_form_values()
    st.session_state.form_values = defaults.copy()
    fv = st.session_state.form_values

    fv['existing_config_filename'] = existing_filename if not is_template else None
    if not config_data: return

    fv['form_job_name'] = config_data.get('name', defaults['form_job_name'])
    if is_template and fv['form_job_name']:
        if not fv['form_job_name'].endswith(" (Copy)"):
            fv['form_job_name'] += " (Copy)"

    fv['form_description'] = config_data.get('description', defaults['form_description'])
    fv['form_job_type'] = config_data.get('job_type', defaults['form_job_type'])
    fv['form_request_delay'] = float(config_data.get('request_delay', defaults['form_request_delay']))
    fv['form_max_retries'] = int(config_data.get('max_retries', defaults['form_max_retries']))
    fv['form_user_agent'] = config_data.get('user_agent', defaults['form_user_agent'])
    fv['form_respect_robots'] = config_data.get('respect_robots', defaults['form_respect_robots'])
    fv['form_output_format'] = config_data.get('output_format', defaults['form_output_format'])
    loaded_proxies = []
    for p_item in config_data.get('proxies', []):
        loaded_proxies.append({
            'id': generate_unique_id(),
            'http': p_item.get('http', ''),
            'https': p_item.get('https', '')
        })
    fv['form_proxies_list'] = loaded_proxies
    if fv['form_job_type'] == 'web':
        fv['form_urls'] = "\n".join(config_data.get('urls', []))
        fv['form_dynamic'] = config_data.get('dynamic', defaults['form_dynamic'])
        fv['form_headless'] = config_data.get('headless', defaults['form_headless'])
        fv['form_disable_images'] = config_data.get('disable_images', defaults['form_disable_images'])
        fv['form_page_load_timeout'] = int(config_data.get('page_load_timeout', defaults['form_page_load_timeout']))
        fv['form_webdriver_path'] = config_data.get('webdriver_path', defaults['form_webdriver_path'])
        fv['form_wait_for_selector'] = config_data.get('wait_for_selector', defaults['form_wait_for_selector'])
        fv['form_wait_time'] = float(config_data.get('wait_time', defaults['form_wait_time']))
        login_cfg = config_data.get('login_config', {})
        fv.update({
            'form_login_url': login_cfg.get('login_url', defaults['form_login_url']),
            'form_username_selector': login_cfg.get('username_selector', defaults['form_username_selector']),
            'form_password_selector': login_cfg.get('password_selector', defaults['form_password_selector']),
            'form_submit_selector': login_cfg.get('submit_selector', defaults['form_submit_selector']),
            'form_username_cred': login_cfg.get('username', defaults['form_username_cred']),
            'form_password_cred': login_cfg.get('password', defaults['form_password_cred']),
            'form_success_selector': login_cfg.get('success_selector', defaults['form_success_selector']),
            'form_success_url_contains': login_cfg.get('success_url_contains', defaults['form_success_url_contains']),
            'form_wait_after_login': float(login_cfg.get('wait_after_login', defaults['form_wait_after_login']))
        })
        selectors = config_data.get('selectors', {})
        fv['form_selector_type'] = selectors.get('type', defaults['form_selector_type'])
        fv['form_container_selector'] = selectors.get('container', defaults['form_container_selector'])
        fv['form_item_selector'] = selectors.get('item', defaults['form_item_selector'])
        loaded_fields = [{'id': generate_unique_id(), 'name': name, 'selector': (cfg.get('selector') if isinstance(cfg, dict) else cfg), 'attr': (cfg.get('attr', '') if isinstance(cfg, dict) else '')} for name, cfg in selectors.get('fields', {}).items()]
        fv['form_fields_list'] = loaded_fields if loaded_fields else [{'id': generate_unique_id(), 'name': '', 'selector': '', 'attr': ''}]
        pagination = config_data.get('pagination', {});
        fv['form_next_page_selector'] = pagination.get('next_page_selector', defaults['form_next_page_selector'])
        fv['form_max_pages'] = str(pagination.get('max_pages', ''))
    elif fv['form_job_type'] == 'api':
        api_cfg = config_data.get('api_config', {})
        fv['form_api_base_url'] = api_cfg.get('base_url', defaults['form_api_base_url'])
        fv['form_api_endpoints'] = "\n".join(api_cfg.get('endpoints', []))
        fv['form_api_method'] = api_cfg.get('method', defaults['form_api_method'])
        fv['form_api_data_path'] = api_cfg.get('data_path', defaults['form_api_data_path'])
        for json_key in ['params', 'headers', 'data']:
            fv[f'form_api_{json_key}'] = json.dumps(api_cfg.get(json_key, {}), indent=2) if api_cfg.get(json_key) is not None else '{}'
        loaded_mappings = [{'id': generate_unique_id(), 'output_name': out_name, 'source_name': src_name} for out_name, src_name in api_cfg.get('field_mappings', {}).items()]
        fv['form_api_field_mappings_list'] = loaded_mappings if loaded_mappings else [{'id': generate_unique_id(), 'output_name': '', 'source_name': ''}]
    rules_raw = config_data.get('processing_rules', {})
    fv['form_processing_rules_field_types'] = [{'id': generate_unique_id(), 'field': field_name, **type_info} for field_name, type_info in rules_raw.get('field_types', {}).items()] if rules_raw.get('field_types') else []
    loaded_tc_rules = []
    default_tc_options_instance = get_default_text_cleaning_rule()
    for field_name, options_from_config in rules_raw.get('text_cleaning', {}).items():
        rule_item = {'id': generate_unique_id(), 'field': field_name}
        for opt_key in default_tc_options_instance.keys():
            if opt_key in ['id', 'field']: continue
            if opt_key == 'case_transform':
                if options_from_config.get('lowercase', False): rule_item['case_transform'] = 'To Lowercase'
                elif options_from_config.get('uppercase', False): rule_item['case_transform'] = 'To Uppercase'
                else: rule_item['case_transform'] = options_from_config.get(opt_key, default_tc_options_instance[opt_key])
            elif opt_key == 'regex_replace_json':
                 rule_item[opt_key] = json.dumps(options_from_config.get('regex_replace', {}), indent=2) if options_from_config.get('regex_replace') else '{}'
            else:
                rule_item[opt_key] = options_from_config.get(opt_key, default_tc_options_instance[opt_key])
        loaded_tc_rules.append(rule_item)
    fv['form_processing_rules_text_cleaning'] = loaded_tc_rules
    fv['form_processing_rules_validations'] = [{'id': generate_unique_id(), 'field': field_name, **options} for field_name, options in rules_raw.get('validations', {}).items()] if rules_raw.get('validations') else []
    fv['form_processing_rules_transformations'] = [{'id': generate_unique_id(), 'target_field': target_field, 'expression': expr} for target_field, expr in rules_raw.get('transformations', {}).items()] if rules_raw.get('transformations') else []
    fv['form_processing_rules_drop_fields'] = [{'id': generate_unique_id(), 'field_name': field_name} for field_name in rules_raw.get('drop_fields', [])] if rules_raw.get('drop_fields') else []

# --- Initialize Session State & Page Config ---
if 'form_values' not in st.session_state or not isinstance(st.session_state.form_values, dict):
    st.session_state.form_values = get_default_form_values()
if 'current_page' not in st.session_state:
    st.session_state.current_page = "📋 Manage Jobs"

if 'view_example_yaml_filename' not in st.session_state:
    st.session_state.view_example_yaml_filename = None
if 'running_example_job_path' not in st.session_state:
    st.session_state.running_example_job_path = None
if 'example_job_results' not in st.session_state:
    st.session_state.example_job_results = None

for key in ['view_config_content', 'view_config_filename', 'running_job_name', 'job_results', 'show_confirm_delete', 'config_to_edit', 'flash_message']:
    if key not in st.session_state: st.session_state[key] = None

st.set_page_config(page_title="Web Scraper Framework", layout="wide")
st.sidebar.title("📄 Web Scraper Framework")
page_options = ["📋 Manage Jobs", "➕ Create/Edit Job", "🚀 Example Jobs"]
try: current_page_index = page_options.index(st.session_state.current_page)
except ValueError: current_page_index = page_options.index("📋 Manage Jobs")

def update_current_page_from_sidebar():
    st.session_state.current_page = st.session_state.nav_radio_selector
    st.session_state.running_job_name = None; st.session_state.job_results = None
    st.session_state.running_example_job_path = None; st.session_state.example_job_results = None
    st.session_state.view_config_filename = None; st.session_state.show_confirm_delete = None
    st.session_state.view_example_yaml_filename = None

st.sidebar.radio( "Navigation", page_options, index=current_page_index, key="nav_radio_selector", on_change=update_current_page_from_sidebar )

# --- Helper function for running a job and displaying results ---
def run_and_display_job(config_path_or_name, is_example=False):
    results_key = 'example_job_results' if is_example else 'job_results'
    running_flag_key = 'running_example_job_path' if is_example else 'running_job_name'
    job_display_name = Path(config_path_or_name).name if isinstance(config_path_or_name, Path) else config_path_or_name

    if not st.session_state.get(results_key):
        with st.spinner(f"Executing job: {job_display_name}... This may take a moment."):
            # ... (rest of the job execution logic from your provided code - no changes here) ...
            results_data = None; stats_data = None; output_path_str = "Error: Job did not produce an output path."; error_message = None
            job_output_format = "csv"
            try:
                if isinstance(config_path_or_name, Path):
                    config = load_config_data_from_path(config_path_or_name)
                    config_source_display = str(config_path_or_name)
                else:
                    config_path_user_job = CONFIG_DIR / config_path_or_name
                    config = load_config_data_from_path(config_path_user_job)
                    config_source_display = str(config_path_user_job)
                if not config: raise FileNotFoundError(f"Configuration could not be loaded from {config_source_display}")
                logger.info(f"Loaded config for run: {config.get('name')} from {config_source_display}")
                job_name_for_dir = config.get('name', 'untitled_job')
                if is_example: safe_job_name_for_dir = "EXAMPLE_" + "".join(c if c.isalnum() else '_' for c in job_name_for_dir)
                else: safe_job_name_for_dir = "".join(c if c.isalnum() else '_' for c in job_name_for_dir)
                job_output_dir = OUTPUT_DIR / safe_job_name_for_dir; job_output_dir.mkdir(parents=True, exist_ok=True)
                config_for_run = config.copy(); config_for_run['output_dir'] = str(job_output_dir)
                job_output_format = config_for_run.get('output_format', 'csv').lower()
                scraper_instance = None; job_type = config_for_run.get('job_type', 'web')
                if job_type == 'api': scraper_instance = APIScraper(config_for_run)
                elif job_type == 'web':
                    if config_for_run.get('dynamic', False): scraper_instance = DynamicScraper(config_for_run)
                    else: scraper_instance = HTMLScraper(config_for_run)
                else: raise ValueError(f"Invalid job_type '{job_type}'.")
                result = scraper_instance.run(); results_data = result.get('data'); stats_data = result.get('stats')
                if results_data is not None and len(results_data) > 0 :
                    storage = None
                    if job_output_format == 'json': storage = JSONStorage(config_for_run)
                    elif job_output_format == 'sqlite': storage = SQLiteStorage(config_for_run)
                    else: storage = CSVStorage(config_for_run)
                    output_path_str = storage.save(results_data); logger.info(f"Job '{job_name_for_dir}' results saved to: {output_path_str} as {job_output_format.upper()}")
                elif results_data == []: output_path_str = "No data extracted to save (empty list)."
                else: output_path_str = "No data extracted to save (data is None)."
            except (JsonSchemaValidationError, yaml.YAMLError) as e: error_path = " -> ".join(map(str, getattr(e, 'path', []))) or "Config root"; error_message = f"Config Error: {getattr(e, 'message', str(e))} (at {error_path})"; logger.error(error_message)
            except Exception as e: error_message = f"Scraping failed: {e}"; logger.exception(f"Error running job {job_display_name} via Streamlit")
            st.session_state[results_key] = {"raw_data_for_download": results_data if results_data else [], "output_path_on_disk": output_path_str, "saved_format": job_output_format if (results_data is not None and len(results_data) > 0) else "N/A", "stats": stats_data or {}, "sample_data": results_data[:10] if results_data else [], "error": error_message}
            st.rerun()
    if st.session_state.get(results_key):
        results = st.session_state[results_key]; st.subheader("📊 Job Execution Summary")
        if results["error"]: st.error(f"An error occurred: {results['error']}")
        else:
            output_path_on_disk = results["output_path_on_disk"]; saved_format = results.get("saved_format", "csv"); raw_data_for_download = results.get("raw_data_for_download", [])
            if output_path_on_disk and "Error" not in output_path_on_disk and "No data extracted" not in output_path_on_disk:
                output_filename_on_disk = Path(output_path_on_disk).name
                st.success(f"🎉 Success! Your data has been extracted and saved as **{output_filename_on_disk}** (Format: {saved_format.upper()}).")
                st.markdown(f"Full path on server: `{output_path_on_disk}`")
                if raw_data_for_download:
                    try:
                        download_data_bytes = b""; download_mime = "text/plain"
                        if saved_format == "csv": df_download = pd.DataFrame(raw_data_for_download); download_data_bytes = df_download.to_csv(index=False).encode('utf-8'); download_mime = "text/csv"
                        elif saved_format == "json": download_data_bytes = json.dumps(raw_data_for_download, indent=4).encode('utf-8'); download_mime = "application/json"
                        elif saved_format == "sqlite":
                            with open(output_path_on_disk, "rb") as fp_sqlite: download_data_bytes = fp_sqlite.read()
                            download_mime = "application/x-sqlite3"
                        if download_data_bytes: st.download_button(label=f"📥 Download {output_filename_on_disk}", data=download_data_bytes, file_name=output_filename_on_disk, mime=download_mime, key=f"download_btn_fmt_{job_display_name.replace(' ','_')}_{int(time.time())}")
                    except FileNotFoundError: st.error(f"Output file not found at {output_path_on_disk} for download.")
                    except Exception as e: st.error(f"Error preparing download: {e}")
            elif "No data extracted" in output_path_on_disk: st.info(output_path_on_disk)
            else: st.warning(f"Output information: {output_path_on_disk}")
        st.subheader("📈 Run Statistics"); stats = results.get("stats", {})
        if stats:
            st.markdown(f"""
            - **Total Duration:** {stats.get('total_duration', 0):.2f} seconds
            - **Pages Scraped:** {stats.get('pages_scraped', 0)}
            - **Pages Failed:** {stats.get('pages_failed', 0)}
            - **Items Extracted:** {stats.get('items_extracted', 0)}
            - **Items Processed (after rules):** {stats.get('items_processed', 0)}
            - **Robots.txt Skipped URLs:** {stats.get('robots_skipped', 0)}
            - **Proxy Failures:** {stats.get('proxy_failures', 0)}
            """)
        else: st.write("Statistics not available.")
        st.subheader("📋 Sample Data Preview (First 10 Items)")
        if results["sample_data"]:
            try: df_sample = pd.DataFrame(results["sample_data"]); st.dataframe(df_sample)
            except Exception as e: st.error(f"Could not display sample data: {e}"); st.write(results["sample_data"])
        else: st.write("No sample data available.")
        close_button_key = f"close_results_{'example' if is_example else 'user'}_{job_display_name.replace('.', '_').replace(' ', '_')}"
        if st.button("OK, Close Results", key=close_button_key):
            st.session_state[running_flag_key] = None; st.session_state[results_key] = None; st.rerun()


# --- Manage Jobs Page ---
if st.session_state.current_page == "📋 Manage Jobs":
    # ... (Logic for clearing edit state if navigating here) ...
    if st.session_state.config_to_edit:
        st.session_state.config_to_edit = None
        st.session_state.form_values = get_default_form_values()
    st.header("📋 My Scraping Jobs")
    st.markdown("Manage your saved scraping configurations. Create new jobs or edit, run, view, and delete existing ones.")

    col1_manage_btns, col2_manage_btns, _ = st.columns([0.35, 0.35, 0.3])
    with col1_manage_btns:
        if st.button("➕ Create New Scraping Job", key="create_new_job_top_button_manage_page_v3", use_container_width=True):
            st.session_state.current_page = "➕ Create/Edit Job"
            st.session_state.form_values = get_default_form_values()
            st.session_state.config_to_edit = None
            st.rerun()
    with col2_manage_btns:
        if st.button("🚀 View Example Jobs", key="view_examples_button_manage_page_v2", use_container_width=True):
            st.session_state.current_page = "🚀 Example Jobs"
            st.session_state.running_job_name = None; st.session_state.job_results = None
            st.session_state.view_config_filename = None; st.session_state.show_confirm_delete = None
            st.rerun()

    # ... (Rest of Manage Jobs page, including flash messages and job listing, remains the same) ...
    if st.session_state.flash_message:
        msg_type, msg_text = st.session_state.flash_message
        if msg_type == "success": st.success(msg_text)
        elif msg_type == "error": st.error(msg_text)
        else: st.info(msg_text)
        st.session_state.flash_message = None
    user_job_files = get_config_files_details(CONFIG_DIR)
    if not user_job_files and not st.session_state.get('running_job_name'):
         st.info("You haven't created any scraping jobs yet. Use the '➕ Create New Scraping Job' button or try an example from the '🚀 Example Jobs' page!")
    elif user_job_files:
        st.markdown("---")
        for config_item in user_job_files:
            row_cols = st.columns([0.5, 0.2, 0.3])
            with row_cols[0]: st.markdown(f"**{config_item['filename']}**")
            with row_cols[1]: st.caption(f"Last modified: {timestamp_to_datetime_str(config_item['modified_time'])}")
            with row_cols[2]:
                action_cols = st.columns(4)
                if action_cols[0].button("👁️", key=f"view_user_job_{config_item['filename']}", help="View YAML"):
                    st.session_state.view_config_filename = config_item['filename'] if st.session_state.view_config_filename != config_item['filename'] else None
                    st.session_state.show_confirm_delete = None; st.session_state.running_job_name = None; st.session_state.job_results = None; st.rerun()
                if action_cols[1].button("▶️", key=f"run_user_job_{config_item['filename']}", help="Run Job"):
                    st.session_state.running_job_name = config_item['filename']; st.session_state.job_results = None; st.session_state.view_config_filename = None; st.session_state.show_confirm_delete = None; st.rerun()
                if action_cols[2].button("✏️", key=f"edit_user_job_{config_item['filename']}", help="Edit Config"):
                    st.session_state.config_to_edit = config_item['filename']
                    st.session_state.current_page = "➕ Create/Edit Job"
                    st.session_state.running_job_name = None; st.session_state.job_results = None; st.session_state.view_config_filename = None; st.session_state.show_confirm_delete = None
                    st.rerun()
                if action_cols[3].button("🗑️", key=f"delete_user_job_{config_item['filename']}", help="Delete Config"):
                    st.session_state.show_confirm_delete = config_item['filename'] if st.session_state.show_confirm_delete != config_item['filename'] else None
                    st.session_state.view_config_filename = None; st.session_state.running_job_name = None; st.session_state.job_results = None; st.rerun()
            if st.session_state.view_config_filename == config_item['filename']:
                with st.expander(f"Viewing Configuration: {config_item['filename']}", expanded=True):
                    try:
                        with open(config_item['path'], 'r', encoding='utf-8') as f: st.code(f.read(), language='yaml')
                    except Exception as e: st.error(f"Error reading {config_item['filename']} for view: {e}")
                    if st.button("Close View", key=f"close_user_job_view_{config_item['filename']}"): st.session_state.view_config_filename = None; st.rerun() # This is the existing good one
            if st.session_state.show_confirm_delete == config_item['filename']:
                st.warning(f"Are you sure you want to delete '{config_item['filename']}'? This action cannot be undone.")
                del_cols_job = st.columns(2)
                if del_cols_job[0].button("Yes, Delete Permanently", key=f"confirm_del_user_job_btn_{config_item['filename']}"):
                    try: config_item['path'].unlink(); st.success(f"Deleted '{config_item['filename']}'."); logger.info(f"Deleted config: {config_item['path']}")
                    except Exception as e: st.error(f"Error deleting '{config_item['filename']}': {e}"); logger.error(f"Error deleting {config_item['filename']}: {e}")
                    st.session_state.show_confirm_delete = None; st.rerun()
                if del_cols_job[1].button("Cancel Deletion", key=f"cancel_del_user_job_btn_{config_item['filename']}"): st.session_state.show_confirm_delete = None; st.rerun()
            st.markdown("---")
    if st.session_state.running_job_name and not st.session_state.view_config_filename and not st.session_state.show_confirm_delete:
        run_and_display_job(st.session_state.running_job_name, is_example=False)

# --- "Example Jobs" Page ---
elif st.session_state.current_page == "🚀 Example Jobs":
    st.header("🚀 Example Job Templates")
    st.markdown("Explore these pre-configured examples. You can view their structure, run them directly, or use them as a starting point for your own custom jobs.")

    # Conditional "Back to My Jobs" button
    if not st.session_state.get('running_example_job_path') and not st.session_state.get('view_example_yaml_filename'):
        # This button is for when just Browse the list of examples
        pass # The one at the bottom will handle this general case

    # If viewing YAML for an example or if an example job's results are shown
    if st.session_state.get('view_example_yaml_filename') or st.session_state.get('example_job_results'):
        if st.button("⬅️ Back to Examples List", key="back_to_examples_list_v3_active_view"):
            st.session_state.running_example_job_path = None
            st.session_state.example_job_results = None
            st.session_state.view_example_yaml_filename = None
            st.rerun()
        st.markdown("---")

    # Display list of examples OR view YAML OR run results
    if not st.session_state.get('running_example_job_path') and not st.session_state.get('view_example_yaml_filename'):
        example_files = get_config_files_details(EXAMPLE_CONFIG_DIR)
        if not example_files:
            st.info("No example templates found in the `configs/example_templates/` directory.")
        else:
            for example in example_files:
                st.markdown(f"---")
                # Use columns for example name and actions in one row
                col_ex_info, col_ex_actions_icons_container = st.columns([0.7, 0.3])
                with col_ex_info:
                    st.markdown(f"#### {example['display_name']}")
                    if example['description'] and example['description'] != 'N/A':
                        st.caption(example['description'])

                with col_ex_actions_icons_container:
                    action_icon_cols_ex = st.columns(3)
                    with action_icon_cols_ex[0]:
                        if st.button("👁️", key=f"view_example_icon_btn_{example['filename']}", help="View YAML"):
                            st.session_state.view_example_yaml_filename = example['filename']
                            st.session_state.running_example_job_path = None; st.session_state.example_job_results = None; st.rerun()
                    with action_icon_cols_ex[1]:
                        if st.button("▶️", key=f"run_example_icon_btn_{example['filename']}", help="Run Example"):
                            st.session_state.running_example_job_path = example['path']
                            st.session_state.example_job_results = None
                            st.session_state.view_example_yaml_filename = None
                            st.rerun()
                    with action_icon_cols_ex[2]:
                        if st.button("📝", key=f"use_template_icon_btn_{example['filename']}", help="Use as Template"):
                            example_data = load_config_data_from_path(example['path'])
                            if example_data:
                                populate_form_values_from_config(example_data, existing_filename=None, is_template=True)
                                st.session_state.current_page = "➕ Create/Edit Job"
                                st.session_state.config_to_edit = None
                                st.session_state.running_example_job_path = None; st.session_state.example_job_results = None; st.session_state.view_example_yaml_filename = None
                                st.rerun()
                            else:
                                st.error(f"Could not load template: {example['display_name']}")
        st.markdown("---") # Add separator after the list of examples
        if st.button("⬅️ Back to My Jobs", key="back_to_my_jobs_from_examples_bottom_v2"):
            st.session_state.current_page = "📋 Manage Jobs"
            st.rerun()

    if st.session_state.view_example_yaml_filename:
        example_to_view = next((ex for ex in get_config_files_details(EXAMPLE_CONFIG_DIR) if ex['filename'] == st.session_state.view_example_yaml_filename), None)
        if example_to_view:
            with st.expander(f"Viewing YAML: {example_to_view['display_name']}", expanded=True):
                try:
                    with open(example_to_view['path'], 'r', encoding='utf-8') as f:
                        st.code(f.read(), language='yaml')
                except Exception as e:
                    st.error(f"Error reading {example_to_view['filename']} for view: {e}")
                if st.button("Close View", key=f"close_example_yaml_expander_btn_v2_{example_to_view['filename']}"): # NEW
                    st.session_state.view_example_yaml_filename = None
                    st.rerun()

    if st.session_state.running_example_job_path and not st.session_state.view_example_yaml_filename:
        run_and_display_job(st.session_state.running_example_job_path, is_example=True)


# --- "Create/Edit Job" Page Logic ---
elif st.session_state.current_page == "➕ Create/Edit Job":
    # ... (Rest of your Create/Edit Job page logic - should be largely unchanged from your last version) ...
    st.header("🔧 Create or Edit Scraping Job")
    if 'form_values' not in st.session_state or not isinstance(st.session_state.form_values, dict):
        st.session_state.form_values = get_default_form_values()
    fv = st.session_state.form_values
    if st.session_state.config_to_edit and \
       fv.get('existing_config_filename') != st.session_state.config_to_edit:
        user_job_path = CONFIG_DIR / st.session_state.config_to_edit
        loaded_cfg = load_config_data_from_path(user_job_path)
        if loaded_cfg:
            populate_form_values_from_config(loaded_cfg, st.session_state.config_to_edit, is_template=False)
            st.rerun()
        else:
            st.session_state.config_to_edit = None
            st.warning("Failed to load configuration for editing. Displaying new job form.")
    elif not st.session_state.config_to_edit and fv.get('existing_config_filename') and not "(Copy)" in fv.get('form_job_name', ''):
         st.session_state.form_values = get_default_form_values()
    def on_job_type_change():
        st.session_state.form_values['form_job_type'] = st.session_state.job_type_selector_key
        if st.session_state.form_values['form_job_type'] == 'web':
             if 'form_selector_type' not in st.session_state.form_values or \
                not st.session_state.form_values['form_selector_type']:
                st.session_state.form_values['form_selector_type'] = 'css'
        if st.session_state.form_values['form_job_type'] == 'api':
            st.session_state.form_values['form_dynamic'] = False
    def on_selector_type_change():
        st.session_state.form_values['form_selector_type'] = st.session_state.selector_type_selector_key
    def on_dynamic_toggle():
        if 'form_dynamic_checkbox_key_outside_form' in st.session_state:
            st.session_state.form_values['form_dynamic'] = st.session_state.form_dynamic_checkbox_key_outside_form
    def update_list_item_from_widget(list_name_in_fv, index_in_list, item_dict_key, widget_key_in_session_state):
        if widget_key_in_session_state not in st.session_state: return
        try:
            if list_name_in_fv not in st.session_state.form_values or not isinstance(st.session_state.form_values[list_name_in_fv], list):
                st.session_state.form_values[list_name_in_fv] = []
            current_list = st.session_state.form_values[list_name_in_fv]
            if index_in_list < len(current_list):
                if not isinstance(current_list[index_in_list], dict):
                    current_list[index_in_list] = {'id': generate_unique_id()}
                current_list[index_in_list][item_dict_key] = st.session_state[widget_key_in_session_state]
        except Exception as e: logger.error(f"Error updating list item: {e}")

    if fv.get('existing_config_filename'): st.caption(f"Editing: {fv['existing_config_filename']}")
    elif "(Copy)" in fv.get('form_job_name', ''): st.caption("Customizing from template. Save to create a new job in 'My Scraping Jobs'.")
    else: st.caption("Creating a new job configuration.")

    st.subheader("📝 Basic Information")
    fv['form_job_name'] = st.text_input("Job Name*", value=fv.get('form_job_name',''), key="input_job_name_v7_final", help="A unique and descriptive name for your scraping job (e.g., 'Amazon Laptop Prices').", placeholder="e.g., My Product Scraper")
    fv['form_description'] = st.text_area("Description", value=fv.get('form_description',''), key="input_description_v7_final", help="Optional: A brief summary of what this job does or any important notes.", placeholder="e.g., Scrapes product names and prices from example.com daily.")
    st.markdown("---")
    st.subheader("⚙️ Initial Job Setup")
    job_type_options = ["web", "api"]
    try: job_type_idx = job_type_options.index(fv.get('form_job_type', 'web'))
    except ValueError: job_type_idx = 0
    st.radio( "Job Type*", options=job_type_options, index=job_type_idx, key="job_type_selector_key", on_change=on_job_type_change, help="Select 'web' for scraping websites (HTML/Dynamic) or 'api' for fetching data from APIs.", horizontal=True )
    if fv.get('form_job_type') == 'web':
        selector_type_options = ["css", "xpath"]
        try: selector_type_idx = selector_type_options.index(fv.get('form_selector_type', 'css'))
        except ValueError: selector_type_idx = 0
        st.radio( "Selector Type (for Web Fields)", options=selector_type_options, index=selector_type_idx, key="selector_type_selector_key", on_change=on_selector_type_change, horizontal=True, help="Choose the type of selectors you will use for identifying elements on web pages. CSS is generally simpler, XPath is more powerful for complex structures." )
        st.checkbox("Dynamic Content (Use Selenium)", value=fv.get('form_dynamic', False), key="form_dynamic_checkbox_key_outside_form", on_change=on_dynamic_toggle, help="Check this if the website requires JavaScript to load its content, or if you need to automate interactions like logins, button clicks, or scrolling before extracting data.")
    st.markdown("---")
    st.subheader("🗂️ Data Extraction Fields / API Mappings")
    st.caption("Define fields to extract or map. Changes here are live and will update rule dropdowns after interaction (e.g., clicking out of a text box or pressing Enter).")
    if fv.get('form_job_type') == 'web':
        if st.button("➕ Add Web Field", key="add_web_field_btn_v7_final", help="Add a new field to extract from web pages."):
            if not isinstance(fv['form_fields_list'], list): fv['form_fields_list'] = []
            fv['form_fields_list'].append({'id': generate_unique_id(), 'name': '', 'selector': '', 'attr': ''}); st.rerun()
        for i, field_item in enumerate(fv.get('form_fields_list', [])):
            st.markdown(f"**Web Field #{i+1}**"); cols_fields = st.columns([2, 3, 2, 1]); field_id = field_item['id']
            name_widget_key = f"web_field_name_key_final_{field_id}"; selector_widget_key = f"web_field_selector_key_final_{field_id}"; attr_widget_key = f"web_field_attr_key_final_{field_id}"
            cols_fields[0].text_input(f"Field Name*", value=field_item.get('name',''), key=name_widget_key, placeholder=f"e.g., title_{i+1}", on_change=update_list_item_from_widget, args=('form_fields_list', i, 'name', name_widget_key), help="The name this data will have in your output (e.g., 'price', 'author_name').")
            sel_help_text = "CSS selector (e.g., h2.product-title, span.price)" if fv.get('form_selector_type', 'css') == 'css' else "XPath expression (e.g., //h2[@class='product-title'], //span[contains(@class,'price')])"
            cols_fields[1].text_input(f"Selector*", value=field_item.get('selector',''), key=selector_widget_key, placeholder=sel_help_text, help=f"The {fv.get('form_selector_type','css').upper()} selector to locate this piece of data within each item.", on_change=update_list_item_from_widget, args=('form_fields_list', i, 'selector', selector_widget_key))
            cols_fields[2].text_input(f"Attribute", value=field_item.get('attr',''), key=attr_widget_key, placeholder="e.g., href, src, data-id", on_change=update_list_item_from_widget, args=('form_fields_list', i, 'attr', attr_widget_key), help="Optional: If you want an HTML attribute's value (like 'href' from an <a> tag or 'src' from an <img> tag), enter the attribute name here. Leave blank to get the element's text content.")
            if cols_fields[3].button(f"➖", key=f"remove_web_field_btn_v7_final_{field_id}", help="Remove this field definition"): fv['form_fields_list'].pop(i); st.rerun()
            st.caption("")
    elif fv.get('form_job_type') == 'api':
        if st.button("➕ Add API Mapping", key="add_api_map_btn_v7_final", help="Add a new field mapping for the API response."):
            if not isinstance(fv['form_api_field_mappings_list'], list): fv['form_api_field_mappings_list'] = []
            fv['form_api_field_mappings_list'].append({'id': generate_unique_id(), 'output_name': '', 'source_name': ''}); st.rerun()
        for i, map_item in enumerate(fv.get('form_api_field_mappings_list', [])):
            st.markdown(f"**API Field Mapping #{i+1}**"); cols_map = st.columns([2, 2, 1]); map_id = map_item['id']
            output_widget_key = f"api_map_output_key_final_{map_id}"; source_widget_key = f"api_map_source_key_final_{map_id}"
            cols_map[0].text_input(f"Output Field Name*", value=map_item.get('output_name',''), key=output_widget_key, placeholder=f"e.g., userId, productTitle", on_change=update_list_item_from_widget, args=('form_api_field_mappings_list', i, 'output_name', output_widget_key), help="The name this field will have in your final output data.")
            cols_map[1].text_input(f"API Source Field*", value=map_item.get('source_name',''), key=source_widget_key, placeholder="e.g., id, user.profile.name", on_change=update_list_item_from_widget, args=('form_api_field_mappings_list', i, 'source_name', source_widget_key), help="The key (or dot.notation path for nested objects) of this field in the API's JSON response item.")
            if cols_map[2].button(f"➖", key=f"remove_api_map_btn_v7_final_{map_id}", help="Remove this API field mapping"): fv['form_api_field_mappings_list'].pop(i); st.rerun()
            st.caption("")
    st.markdown("---")
    st.subheader("🌐 Proxy Configuration (Optional)")
    st.caption("Add HTTP/HTTPS proxies. Proxies are rotated during the job. Format: `http://user:pass@host:port` or `https://host:port`")
    if st.button("➕ Add Proxy", key="add_proxy_btn_v7_final"):
        if not isinstance(fv['form_proxies_list'], list): fv['form_proxies_list'] = []
        fv['form_proxies_list'].append(get_default_proxy_item()); st.rerun()
    for i, proxy_item in enumerate(fv.get('form_proxies_list', [])):
        st.markdown(f"**Proxy #{i+1}**"); proxy_id = proxy_item['id']; p_cols = st.columns([5, 5, 1])
        http_proxy_key = f"proxy_http_key_final_{proxy_id}"; https_proxy_key = f"proxy_https_key_final_{proxy_id}"
        p_cols[0].text_input("HTTP Proxy URL", value=proxy_item.get('http', ''), key=http_proxy_key, placeholder="e.g., http://user:pass@proxyserver.com:8080", on_change=update_list_item_from_widget, args=('form_proxies_list', i, 'http', http_proxy_key), help="Full HTTP proxy URL.")
        p_cols[1].text_input("HTTPS Proxy URL", value=proxy_item.get('https', ''), key=https_proxy_key, placeholder="e.g., https://user:pass@proxyserver.com:8080", on_change=update_list_item_from_widget, args=('form_proxies_list', i, 'https', https_proxy_key), help="Full HTTPS proxy URL. Often the same as HTTP for many providers.")
        if p_cols[2].button(f"➖", key=f"remove_proxy_btn_v7_final_{proxy_id}", help="Remove this proxy"): fv['form_proxies_list'].pop(i); st.rerun()
        st.caption("")
    st.markdown("---")
    st.subheader("🛠️ Data Processing Rules")
    st.caption("Define rules to clean, transform, and validate extracted data. Add/Remove rules using buttons below. Configure rule details within the main form when saving.")
    pr_manage_tabs = st.tabs(["Field Types", "Text Cleaning", "Validations", "Transformations", "Drop Fields"])
    with pr_manage_tabs[0]:
        if st.button("➕ Add Field Type Rule", key="add_ft_rule_btn_v7_ui"):
            if not isinstance(fv['form_processing_rules_field_types'], list): fv['form_processing_rules_field_types'] = []
            fv['form_processing_rules_field_types'].append({'id': generate_unique_id(), 'field': '', 'type': 'string', 'format': ''}); st.rerun()
        for i, rule in enumerate(fv.get('form_processing_rules_field_types',[])):
            if st.button(f"➖ Remove FT Rule #{i+1} for '{rule.get('field','unassigned')}'", key=f"rm_ft_btn_v7_ui_outside_{rule['id']}"):
                fv['form_processing_rules_field_types'].pop(i); st.rerun()
    with pr_manage_tabs[1]:
        if st.button("➕ Add Text Cleaning Rule", key="add_tc_rule_btn_v7_ui"):
            if not isinstance(fv['form_processing_rules_text_cleaning'], list): fv['form_processing_rules_text_cleaning'] = []
            fv['form_processing_rules_text_cleaning'].append(get_default_text_cleaning_rule()); st.rerun()
        for i, rule in enumerate(fv.get('form_processing_rules_text_cleaning',[])):
            if st.button(f"➖ Remove TC Rule #{i+1} for '{rule.get('field','unassigned')}'", key=f"rm_tc_btn_v7_ui_outside_{rule['id']}"):
                fv['form_processing_rules_text_cleaning'].pop(i); st.rerun()
    with pr_manage_tabs[2]:
        if st.button("➕ Add Validation Rule", key="add_val_rule_btn_v7_ui"):
            if not isinstance(fv.get('form_processing_rules_validations'), list): fv['form_processing_rules_validations'] = []
            fv['form_processing_rules_validations'].append({'id': generate_unique_id(), 'field': '', 'required': False, 'min_length':'', 'max_length':'', 'pattern':''}); st.rerun()
        for i, rule in enumerate(fv.get('form_processing_rules_validations',[])):
            if st.button(f"➖ Remove Validation Rule #{i+1} for '{rule.get('field','unassigned')}'", key=f"rm_val_btn_v7_ui_outside_{rule['id']}"):
                fv['form_processing_rules_validations'].pop(i); st.rerun()
    with pr_manage_tabs[3]:
        if st.button("➕ Add Transformation Rule", key="add_tf_rule_btn_v7_ui"):
            if not isinstance(fv.get('form_processing_rules_transformations'), list): fv['form_processing_rules_transformations'] = []
            fv['form_processing_rules_transformations'].append({'id': generate_unique_id(), 'target_field': '', 'expression': ''}); st.rerun()
        for i, rule in enumerate(fv.get('form_processing_rules_transformations',[])):
            if st.button(f"➖ Remove Transformation Rule #{i+1} for '{rule.get('target_field','unnamed')}'", key=f"rm_tf_btn_v7_ui_outside_{rule['id']}"):
                fv['form_processing_rules_transformations'].pop(i); st.rerun()
    with pr_manage_tabs[4]:
        if st.button("➕ Add Field to Drop", key="add_df_rule_btn_v7_ui"):
            if not isinstance(fv.get('form_processing_rules_drop_fields'), list): fv['form_processing_rules_drop_fields'] = []
            fv['form_processing_rules_drop_fields'].append({'id': generate_unique_id(), 'field_name': ''}); st.rerun()
        for i, rule in enumerate(fv.get('form_processing_rules_drop_fields',[])):
            if st.button(f"➖ Remove Drop Rule #{i+1} for '{rule.get('field_name','unnamed')}'", key=f"rm_df_btn_v7_ui_outside_{rule['id']}"):
                fv['form_processing_rules_drop_fields'].pop(i); st.rerun()
    st.markdown("---")
    with st.form(key="config_form_main_submit_v7_final"):
        # ... (Web/API specific configs, Shared Options, Output Options, and Rule Detail configurations as per your last version) ...
        if fv.get('form_job_type') == 'web':
            st.subheader("📄 Web Scraping Configuration Details"); st.markdown("---")
            fv['form_urls'] = st.text_area("Target URLs* (one per line)", value=fv.get('form_urls',''), height=100, key="input_urls_form_web_in_form_inside_v7_final", placeholder="https://example.com/products\nhttps://another.com/page", help="Enter one starting URL per line.")
            if fv.get('form_dynamic'):
                with st.container(border=True):
                    st.markdown("**Dynamic Options (Selenium):**")
                    fv['form_headless'] = st.checkbox("Run Headless (no browser UI)", value=fv.get('form_headless', True), key="input_headless_v7_final_form", help="Run the browser in the background without opening a visible window. Recommended for servers, faster execution.")
                    fv['form_disable_images'] = st.checkbox("Disable Images (faster loads)", value=fv.get('form_disable_images', True), key="input_disable_images_v7_final_form", help="Prevents images from loading in the browser, can significantly speed up page loads for dynamic sites.")
                    fv['form_page_load_timeout'] = st.number_input("Page Load Timeout (s)", min_value=5, value=int(fv.get('form_page_load_timeout',30)), step=1, key="input_page_load_timeout_v7_final_form", help="Maximum time (in seconds) to wait for a page to complete loading before considering it a timeout.")
                    fv['form_webdriver_path'] = st.text_input("WebDriver Path (Optional)", value=fv.get('form_webdriver_path',''), key="input_webdriver_path_v7_final_form", placeholder="e.g., /usr/local/bin/chromedriver or C:\\webdrivers\\chromedriver.exe", help="Absolute path to your ChromeDriver executable. Leave blank if it's in your system's PATH.")
                    fv['form_wait_for_selector'] = st.text_input("Wait for Selector", value=fv.get('form_wait_for_selector',''), key="input_wait_sel_form_web_in_form_inside_v7_final", placeholder="e.g., #main-content, //div[@id='data-ready']", help="CSS or XPath selector. Scraper will wait for this element to be present on the page before attempting to extract data. Useful for content loaded via AJAX.")
                    fv['form_wait_time'] = st.number_input("General Wait Time (s)", min_value=0.0, value=float(fv.get('form_wait_time',5.0)), step=0.5, key="input_wait_time_form_web_in_form_inside_v7_final", help="A fixed delay (in seconds) to wait after certain actions (like page load or login submission) to allow dynamically loaded content to render.")
                    with st.expander("Login Configuration (Optional for Dynamic)"):
                        st.info("Fill these if the site requires login before scraping target pages.")
                        fv['form_login_url'] = st.text_input("Login Page URL", value=fv.get('form_login_url',''), key="input_login_url_v7_final_dyn")
                        fv['form_username_selector'] = st.text_input("Username Selector", value=fv.get('form_username_selector',''), key="input_user_sel_v7_final_dyn")
                        fv['form_username_cred'] = st.text_input("Username", value=fv.get('form_username_cred',''), key="input_user_cred_v7_final_dyn")
                        fv['form_password_selector'] = st.text_input("Password Selector", value=fv.get('form_password_selector',''), key="input_pass_sel_v7_final_dyn")
                        fv['form_password_cred'] = st.text_input("Password", value=fv.get('form_password_cred',''), type="password", key="input_pass_cred_v7_final_dyn")
                        fv['form_submit_selector'] = st.text_input("Submit Selector", value=fv.get('form_submit_selector',''), key="input_submit_sel_v7_final_dyn")
                        fv['form_success_selector'] = st.text_input("Success Element Selector (Optional)", value=fv.get('form_success_selector',''), key="input_succ_sel_v7_final_dyn")
                        fv['form_success_url_contains'] = st.text_input("Success URL Contains (Optional)", value=fv.get('form_success_url_contains',''), key="input_succ_url_v7_final_dyn")
                        fv['form_wait_after_login'] = st.number_input("Wait After Login (s)", min_value=0.0, value=float(fv.get('form_wait_after_login',3.0)), step=0.5, key="input_wait_login_v7_final_dyn")
            st.markdown("**Selectors (for Target Page):**")
            fv['form_container_selector'] = st.text_input("Container Selector (Optional)", value=fv.get('form_container_selector',''), key="input_cont_sel_form_web_in_form_inside_v7_corrected", placeholder="e.g., div#results-list > ul", help="A CSS/XPath selector for a larger parent element that encloses all individual items. If provided, 'Item Selector' will be relative to this container.")
            fv['form_item_selector'] = st.text_input("Item Selector*", value=fv.get('form_item_selector',''), key="input_item_sel_form_web_in_form_inside_v7_corrected", placeholder="e.g., article.product-card, //div[@class='item']", help="CSS/XPath selector that identifies each individual item/record to be scraped.")
            with st.expander("Pagination (Optional)"):
                fv['form_next_page_selector'] = st.text_input("Next Page Selector", value=fv.get('form_next_page_selector',''), key="input_next_page_form_web_in_form_inside_v7_corrected", placeholder="e.g., a.pagination-next, //a[@rel='next']", help="CSS/XPath selector for the link or button that navigates to the next page of results. For XPath, if targeting an attribute like @href, ensure your scraper logic handles it.")
                fv['form_max_pages'] = st.text_input("Max Pages (number)", value=str(fv.get('form_max_pages','')), key="input_max_pages_form_web_in_form_inside_v7_corrected", placeholder="e.g., 5", help="Limit the number of pages to scrape. Leave blank to attempt all available pages.")
        elif fv.get('form_job_type') == 'api':
            st.subheader("🔌 API Configuration Details"); st.markdown("---")
            fv['form_api_base_url'] = st.text_input("Base URL*", value=fv.get('form_api_base_url',''), key="input_api_base_form_api_in_form_inside_v7_corrected", placeholder="https://api.example.com/v2", help="The base URL for all API endpoints (e.g., https://api.example.com/v1).")
            fv['form_api_endpoints'] = st.text_area("Endpoints* (one per line)", value=fv.get('form_api_endpoints',''), height=75, key="input_api_eps_form_api_in_form_inside_v7_corrected", placeholder="/users\n/products?category=electronics&page={page_num}", help="Specific API paths to query, relative to the Base URL. You can use placeholders like {page_num} if your API scraper supports dynamic endpoint generation.")
            api_method_options = ["GET", "POST", "PUT", "PATCH", "DELETE"]
            api_method_idx = api_method_options.index(fv.get('form_api_method','GET')) if fv.get('form_api_method','GET') in api_method_options else 0
            fv['form_api_method'] = st.selectbox("HTTP Method", api_method_options, index=api_method_idx, key="input_api_method_form_api_in_form_inside_v7_corrected", help="The HTTP method for the API request.")
            fv['form_api_data_path'] = st.text_input("Data Path (dot.notation)", value=fv.get('form_api_data_path',''), key="input_api_datapath_form_api_in_form_inside_v7_corrected", placeholder="e.g., data.items, results.0.records", help="Dot-notation path to the list of items within the JSON response (e.g., 'results.items'). Leave empty if the root of the JSON response is the list of items.")
            with st.expander("Advanced API Options (Optional)"):
                fv['form_api_params'] = st.text_area("URL Parameters (JSON)", value=fv.get('form_api_params','{}'), height=100, key="input_api_params_form_api_in_form_inside_v7_corrected", placeholder='e.g., {"api_key": "YOUR_KEY", "limit": 100}', help="JSON object for URL query parameters (typically for GET requests).")
                fv['form_api_headers'] = st.text_area("Request Headers (JSON)", value=fv.get('form_api_headers','{}'), height=100, key="input_api_headers_form_api_in_form_inside_v7_corrected", placeholder='e.g., {"Authorization": "Bearer YOUR_TOKEN", "X-Custom-Header": "value"}', help="JSON object for custom HTTP request headers.")
                fv['form_api_data'] = st.text_area("Request Body/Data (JSON for POST/PUT)", value=fv.get('form_api_data','{}'), height=100, key="input_api_data_form_api_in_form_inside_v7_corrected", placeholder='e.g., {"name": "New Item", "value": 42}', help="JSON object for the request body if using POST, PUT, or PATCH methods.")
        st.subheader("🤝 Shared Options")
        fv['form_request_delay'] = st.number_input("Request Delay (s)", min_value=0.0, value=float(fv.get('form_request_delay',1.0)), step=0.1, format="%.1f", key="input_req_delay_form_shared_in_form_inside_v7_corrected", help="Minimum seconds to wait between consecutive requests to be polite to servers. Default: 1.0s.")
        fv['form_max_retries'] = st.number_input("Max Retries", min_value=0, value=int(fv.get('form_max_retries',3)), step=1, key="input_max_retries_form_shared_in_form_inside_v7_corrected", help="Number of times to retry a failed network request before giving up. Default: 3.")
        fv['form_user_agent'] = st.text_input("User Agent", value=fv.get('form_user_agent',get_default_form_values()['form_user_agent']), key="input_ua_form_shared_in_form_inside_v7_corrected", placeholder="Mozilla/5.0 (Windows NT 10.0; Win64; x64)...", help="The User-Agent string your scraper will identify itself with. Some sites block default Python/requests UAs.")
        if fv.get('form_job_type') == 'web':
            fv['form_respect_robots'] = st.checkbox("Respect robots.txt", value=fv.get('form_respect_robots',True), key="input_robots_form_shared_in_form_inside_v7_corrected", help="If checked, the scraper will attempt to fetch and obey the website's robots.txt exclusion rules. Only applies to Web jobs.")
        st.subheader("📤 Output Options")
        output_format_options = ["csv", "json", "sqlite"]
        try: output_format_idx = output_format_options.index(fv.get('form_output_format', 'csv'))
        except ValueError: output_format_idx = 0
        fv['form_output_format'] = st.selectbox("Default Output File Format", options=output_format_options, index=output_format_idx, key="form_output_format_selector_key_v7_corrected", help="Select the default format for the main output file saved by the job.")

        with st.expander("Configure Processing Rule Details (within form)", expanded=True):
            available_fields_for_rules = get_available_field_names()
            pr_config_tabs_display = st.tabs(["Field Types", "Text Cleaning", "Validations", "Transformations", "Drop Fields"])
            with pr_config_tabs_display[0]:
                st.markdown("Define data type conversions for extracted fields.")
                if not fv.get('form_processing_rules_field_types'): st.caption("No Field Type rules added. Use '➕ Add Field Type Rule' button above.")
                for i, rule in enumerate(fv.get('form_processing_rules_field_types',[])):
                    st.markdown(f"**Field Type Rule #{i+1}**")
                    cols = st.columns([3,2,2]);
                    if available_fields_for_rules == ["(No fields available yet)"]: cols[0].caption(available_fields_for_rules[0]); rule['field'] = ''
                    else: default_ft_index = available_fields_for_rules.index(rule.get('field','')) if rule.get('field','') and rule.get('field','') in available_fields_for_rules else 0; rule['field'] = cols[0].selectbox("Field Name", options=available_fields_for_rules, index=default_ft_index, key=f"ft_field_form_in_form_v7_corrected_{rule['id']}", help="Select the field whose data type you want to convert.")
                    type_options = ["string", "int", "float", "boolean", "datetime", "date"]; type_idx = type_options.index(rule.get('type','string')) if rule.get('type','string') in type_options else 0
                    rule['type'] = cols[1].selectbox("Convert to Type", type_options, index=type_idx, key=f"ft_type_form_in_form_v7_corrected_{rule['id']}", help="Target data type.")
                    if rule.get('type') in ['datetime', 'date']: rule['format'] = cols[2].text_input("Date/Datetime Format (Optional)", value=rule.get('format',''), placeholder="%Y-%m-%d or %Y-%m-%d %H:%M:%S", key=f"ft_format_form_in_form_v7_corrected_{rule['id']}", help="Python strptime format string (e.g., '%Y-%m-%d %H:%M:%S', '%d/%m/%Y'). If omitted, common ISO formats are attempted.")
                    else: rule['format'] = ''
                    st.caption("")
            with pr_config_tabs_display[1]:
                st.markdown("Apply cleaning operations to text fields.")
                if not fv.get('form_processing_rules_text_cleaning'): st.caption("No Text Cleaning rules added. Use '➕ Add Text Cleaning Rule' button above.")
                for i, rule in enumerate(fv.get('form_processing_rules_text_cleaning',[])):
                    st.markdown(f"**Text Cleaning Rule #{i+1} for Field:**")
                    if available_fields_for_rules == ["(No fields available yet)"]: st.caption(available_fields_for_rules[0]); rule['field'] = ''
                    else: default_tc_index = available_fields_for_rules.index(rule.get('field','')) if rule.get('field','') and rule.get('field','') in available_fields_for_rules else 0; rule['field'] = st.selectbox("Field Name ", options=available_fields_for_rules, index=default_tc_index, key=f"tc_field_form_in_form_v7_corrected_{rule['id']}", help="Select the text field to apply cleaning operations to.")
                    tc_cols1 = st.columns(3)
                    rule['trim'] = tc_cols1[0].checkbox("Trim Whitespace", value=rule.get('trim', True), key=f"tc_trim_form_in_form_v7_corrected_{rule['id']}", help="Remove leading/trailing whitespace.")
                    rule['remove_newlines'] = tc_cols1[1].checkbox("Remove Newlines", value=rule.get('remove_newlines', True), key=f"tc_nl_form_in_form_v7_corrected_{rule['id']}", help="Replace newline characters (and tabs) with a single space.")
                    rule['remove_extra_spaces'] = tc_cols1[2].checkbox("Remove Extra Spaces", value=rule.get('remove_extra_spaces', True), key=f"tc_space_form_in_form_v7_corrected_{rule['id']}", help="Consolidate multiple spaces into single spaces.")
                    case_options = ["None", "To Uppercase", "To Lowercase"]; current_case_transform = rule.get('case_transform', 'None')
                    try: case_idx = case_options.index(current_case_transform)
                    except ValueError: case_idx = 0
                    rule['case_transform'] = st.radio("Case Transformation", case_options, index=case_idx, key=f"tc_case_transform_v7_corrected_{rule['id']}", horizontal=True, help="Convert text case.")
                    rule['remove_special_chars'] = st.checkbox("Remove Special Chars", value=rule.get('remove_special_chars', False), key=f"tc_special_form_in_form_v7_corrected_{rule['id']}", help="Remove characters that are not alphanumeric, whitespace, hyphen, period, or comma.")
                    rule['regex_replace_json'] = st.text_area("Regex Replace (JSON: {\"pattern\": \"replacement\"})", value=rule.get('regex_replace_json','{}'), height=80, key=f"tc_regex_form_in_form_v7_corrected_{rule['id']}", help="Advanced: Define key-value pairs of regex patterns and their replacements. E.g., {\"Read More\": \"\", \"Advertisement\": \"\"}")
                    st.caption("")
            with pr_config_tabs_display[2]:
                st.markdown("Set validation criteria for fields.")
                if not fv.get('form_processing_rules_validations'): st.caption("No Validation rules added. Use '➕ Add Validation Rule' button above.")
                for i, rule in enumerate(fv.get('form_processing_rules_validations', [])):
                    st.markdown(f"**Validation Rule #{i+1}**")
                    v_cols_field_req = st.columns([3, 1])
                    if available_fields_for_rules == ["(No fields available yet)"]: v_cols_field_req[0].caption(available_fields_for_rules[0]); rule['field'] = ''
                    else: default_val_idx = available_fields_for_rules.index(rule.get('field', '')) if rule.get('field', '') in available_fields_for_rules else 0; rule['field'] = v_cols_field_req[0].selectbox("Field to Validate", options=available_fields_for_rules, index=default_val_idx, key=f"val_field_form_in_form_v7_corrected_{rule['id']}", help="Select field to apply validation to.")
                    rule['required'] = v_cols_field_req[1].checkbox("Required", value=rule.get('required', False), key=f"val_req_form_in_form_v7_corrected_{rule['id']}", help="Field must have a non-empty value.")
                    v_cols_len_pattern = st.columns([1, 1, 2])
                    rule['min_length'] = v_cols_len_pattern[0].text_input("Min Len", value=str(rule.get('min_length', '')), key=f"val_minl_form_in_form_v7_corrected_{rule['id']}", placeholder="e.g., 5", help="Minimum string length.")
                    rule['max_length'] = v_cols_len_pattern[1].text_input("Max Len", value=str(rule.get('max_length', '')), key=f"val_maxl_form_in_form_v7_corrected_{rule['id']}", placeholder="e.g., 100", help="Maximum string length.")
                    rule['pattern'] = v_cols_len_pattern[2].text_input("Matches Regex Pattern", value=rule.get('pattern',''), key=f"val_pattern_form_in_form_v7_corrected_{rule['id']}", placeholder="e.g., ^\\d{5}(-\\d{4})?$", help="Python regex pattern the field's string value must match.")
                    st.caption("")
            with pr_config_tabs_display[3]:
                st.markdown("Create or modify fields using Python expressions. Use `item.get('field_name')` to access other field values.")
                if not fv.get('form_processing_rules_transformations'): st.caption("No Transformation rules added. Use '➕ Add Transformation Rule' button above.")
                for i, rule in enumerate(fv.get('form_processing_rules_transformations',[])):
                    st.markdown(f"**Transformation Rule #{i+1}**")
                    tf_cols = st.columns([2,3])
                    target_field_help = "Name of the new field to create or existing field to overwrite (e.g., 'full_address', 'price_eur')."
                    expression_help = "Python expression. Access current item's fields with item.get('original_field_name'). Example: `f\"{item.get('street', '')}, {item.get('city', '')}\"` or `item.get('price_usd', 0) * 0.93`"
                    rule['target_field'] = tf_cols[0].text_input("Target Field Name*", value=rule.get('target_field',''), key=f"tf_target_form_in_form_v7_corrected_{rule['id']}", placeholder="e.g., full_name", help=target_field_help)
                    rule['expression'] = tf_cols[1].text_area("Python Expression*", value=rule.get('expression',''), height=80, key=f"tf_expr_form_in_form_v7_corrected_{rule['id']}", placeholder="e.g., item.get('price', 0) * 0.9", help=expression_help)
                    st.caption("")
            with pr_config_tabs_display[4]:
                st.markdown("Specify fields to remove from the final output.")
                if not fv.get('form_processing_rules_drop_fields'): st.caption("No Drop Field rules added. Use '➕ Add Field to Drop' button above.")
                for i, rule in enumerate(fv.get('form_processing_rules_drop_fields',[])):
                    st.markdown(f"**Drop Field Rule #{i+1}**")
                    if available_fields_for_rules == ["(No fields available yet)"]: st.caption(available_fields_for_rules[0]); rule['field_name'] = ''
                    else: default_df_index = available_fields_for_rules.index(rule.get('field_name','')) if rule.get('field_name','') and rule.get('field_name','') in available_fields_for_rules else 0; rule['field_name'] = st.selectbox("Field Name to Drop*", options=available_fields_for_rules, index=default_df_index, key=f"df_field_form_in_form_v7_corrected_{rule['id']}", help="Select a field to remove from the final output results.")
                    st.caption("")
        st.markdown("---")
        submitted = st.form_submit_button("💾 Save Configuration")

    # --- Save Logic ---
    if submitted:
        cfg_name = fv['form_job_name'].strip()
        if not cfg_name: st.error("Job Name is required.")
        else:
            temp_config = {'name': cfg_name, 'description': fv['form_description'].strip(), 'job_type': fv['form_job_type'], 'output_dir': str(OUTPUT_DIR), 'output_format': fv.get('form_output_format', 'csv'), 'request_delay': float(fv['form_request_delay']), 'max_retries': int(fv['form_max_retries']), 'user_agent': fv['form_user_agent'].strip() or None, 'respect_robots': fv.get('form_respect_robots', True)}
            proxies_to_save = []; proxy_warning_issued_this_save = False
            for i_proxy, p_item_proxy in enumerate(fv.get('form_proxies_list', [])):
                http_val_proxy = p_item_proxy.get('http', '').strip(); https_val_proxy = p_item_proxy.get('https', '').strip(); proxy_entry_save = {}
                if http_val_proxy: proxy_entry_save['http'] = http_val_proxy
                if https_val_proxy: proxy_entry_save['https'] = https_val_proxy
                if proxy_entry_save:
                    if not (http_val_proxy and https_val_proxy) and not proxy_warning_issued_this_save: proxy_warning_issued_this_save = True
                    proxies_to_save.append(proxy_entry_save)
            temp_config['proxies'] = proxies_to_save if proxies_to_save else []
            validation_passed = True
            if temp_config['job_type'] == 'web':
                field_names_seen_save = set()
                for item_field_save in fv.get('form_fields_list', []):
                    name_field_save = item_field_save.get('name','').strip()
                    if name_field_save:
                        if name_field_save in field_names_seen_save: st.error(f"Duplicate field name '{name_field_save}' in Web Fields. Field names must be unique."); validation_passed = False; break
                        field_names_seen_save.add(name_field_save)
                if not validation_passed: st.stop()
                temp_config['dynamic'] = fv.get('form_dynamic', False)
                if temp_config['dynamic']:
                    temp_config['headless'] = fv.get('form_headless', True); temp_config['disable_images'] = fv.get('form_disable_images', True); temp_config['page_load_timeout'] = int(fv.get('form_page_load_timeout', 30))
                    if fv.get('form_webdriver_path','').strip(): temp_config['webdriver_path'] = fv['form_webdriver_path'].strip()
                    if fv.get('form_wait_for_selector','').strip(): temp_config['wait_for_selector'] = fv['form_wait_for_selector'].strip()
                    temp_config['wait_time'] = float(fv.get('form_wait_time',5.0))
                    login_url_val = fv['form_login_url'].strip()
                    if login_url_val:
                        login_cfg_data = {'login_url': login_url_val, 'username_selector': fv['form_username_selector'].strip(), 'password_selector': fv['form_password_selector'].strip(), 'submit_selector': fv['form_submit_selector'].strip(), 'username': fv['form_username_cred'].strip(), 'password': fv['form_password_cred'], 'wait_after_login': float(fv['form_wait_after_login'])}
                        success_sel = fv['form_success_selector'].strip(); success_url = fv['form_success_url_contains'].strip()
                        if success_sel: login_cfg_data['success_selector'] = success_sel
                        if success_url: login_cfg_data['success_url_contains'] = success_url
                        if not success_sel and not success_url: st.error("Login requires Success Selector or URL."); validation_passed = False
                        required_login_fields = ['username_selector', 'password_selector', 'submit_selector', 'username']
                        if not all(login_cfg_data.get(f,'') for f in required_login_fields): st.error("Missing required login configuration fields (URL, Selectors for User/Pass/Submit, Username)."); validation_passed = False
                        if validation_passed: temp_config['login_config'] = login_cfg_data
                else:
                    for dynamic_key in ['headless', 'disable_images', 'page_load_timeout', 'webdriver_path', 'wait_for_selector', 'wait_time', 'login_config']:
                        if dynamic_key in temp_config: del temp_config[dynamic_key]
                form_urls_val = fv['form_urls'].strip(); form_item_selector_val = fv['form_item_selector'].strip()
                if not form_urls_val: st.error("Target URLs are required for Web Job."); validation_passed = False
                if not form_item_selector_val: st.error("Item Selector is required for Web Job."); validation_passed = False
                if validation_passed:
                    temp_config['urls'] = [u.strip() for u in form_urls_val.splitlines() if u.strip()]
                    web_fields = {item['name'].strip(): ({'selector': item['selector'].strip(), 'attr': item['attr'].strip()} if item['attr'].strip() else item['selector'].strip()) for item in fv.get('form_fields_list', []) if item.get('name','').strip() and item.get('selector','').strip()}
                    if not web_fields and validation_passed: st.error("Define at least one valid Web Field (Name and Selector are required)."); validation_passed = False
                    if validation_passed and web_fields :
                        temp_config['selectors'] = {'type': fv.get('form_selector_type', 'css'), 'item': form_item_selector_val, 'fields': web_fields}
                        if fv['form_container_selector'].strip(): temp_config['selectors']['container'] = fv['form_container_selector'].strip()
                    next_pg_sel_val = fv['form_next_page_selector'].strip(); max_pg_val = str(fv.get('form_max_pages','')).strip()
                    if validation_passed and (next_pg_sel_val or (max_pg_val and max_pg_val.isdigit())):
                        if 'pagination' not in temp_config: temp_config['pagination'] = {}
                        if next_pg_sel_val: temp_config['pagination']['next_page_selector'] = next_pg_sel_val
                        if max_pg_val.isdigit(): temp_config['pagination']['max_pages'] = int(max_pg_val)
            elif temp_config['job_type'] == 'api':
                api_output_names_seen = set()
                for item_map_save in fv.get('form_api_field_mappings_list', []):
                    name = item_map_save.get('output_name','').strip()
                    if name:
                        if name in api_output_names_seen: st.error(f"Duplicate output field name '{name}' in API Mappings. Output names must be unique."); validation_passed = False; break
                        api_output_names_seen.add(name)
                if not validation_passed: st.stop()
                form_api_base_url_val = fv['form_api_base_url'].strip(); form_api_endpoints_val = fv['form_api_endpoints'].strip()
                if not form_api_base_url_val: st.error("Base URL required for API job."); validation_passed = False
                if not form_api_endpoints_val: st.error("At least one API Endpoint required."); validation_passed = False
                if validation_passed:
                    api_conf_data = {'base_url': form_api_base_url_val, 'endpoints': [ep.strip() for ep in form_api_endpoints_val.splitlines() if ep.strip()], 'method': fv['form_api_method']}
                    try:
                        if fv['form_api_params'].strip() not in ['{}', '']: api_conf_data['params'] = json.loads(fv['form_api_params'])
                        if fv['form_api_headers'].strip() not in ['{}', '']: api_conf_data['headers'] = json.loads(fv['form_api_headers'])
                        if fv['form_api_data'].strip() not in ['{}', '']: api_conf_data['data'] = json.loads(fv['form_api_data'])
                    except json.JSONDecodeError as e: st.error(f"Invalid JSON in API options: {e}"); validation_passed = False
                    if fv['form_api_data_path'].strip(): api_conf_data['data_path'] = fv['form_api_data_path'].strip()
                    api_mappings = {item['output_name'].strip(): item['source_name'].strip() for item in fv.get('form_api_field_mappings_list', []) if item.get('output_name','').strip() and item.get('source_name','').strip()}
                    if api_mappings: api_conf_data['field_mappings'] = api_mappings
                    if validation_passed: temp_config['api_config'] = api_conf_data
            if validation_passed:
                processing_rules_to_save = {}
                ft_rules = {rule['field']: {k:v for k,v in rule.items() if k not in ['id','field'] and v is not None and (v or isinstance(v,bool) or (k=='format' and v!=''))} for rule in fv.get('form_processing_rules_field_types', []) if rule.get('field')}
                if ft_rules: processing_rules_to_save['field_types'] = ft_rules
                tc_rules_final = {}
                default_tc_rule_for_saving = get_default_text_cleaning_rule()
                for rule_idx_tc, rule_tc in enumerate(fv.get('form_processing_rules_text_cleaning', [])):
                    if not rule_tc.get('field'): continue
                    options_to_save = {}
                    for bool_key in ['trim', 'remove_newlines', 'remove_extra_spaces', 'remove_special_chars']:
                        if rule_tc.get(bool_key) is not default_tc_rule_for_saving.get(bool_key): options_to_save[bool_key] = rule_tc.get(bool_key)
                    case_transform_val = rule_tc.get('case_transform', 'None')
                    if case_transform_val == "To Lowercase": options_to_save['lowercase'] = True
                    elif case_transform_val == "To Uppercase": options_to_save['uppercase'] = True
                    try:
                        regex_json_str = rule_tc.get('regex_replace_json','{}').strip()
                        if regex_json_str and regex_json_str != '{}':
                            parsed_regex = json.loads(regex_json_str)
                            if parsed_regex: options_to_save['regex_replace'] = parsed_regex
                    except json.JSONDecodeError:
                        st.error(f"Text Cleaning Rule #{rule_idx_tc+1} for field '{rule_tc['field']}': Invalid JSON in Regex Replace. Fix the JSON format."); validation_passed = False; break
                    if options_to_save or (rule_tc.get('case_transform', 'None') != 'None') :
                       tc_rules_final[rule_tc['field']] = options_to_save
                if not validation_passed: st.stop()
                if tc_rules_final: processing_rules_to_save['text_cleaning'] = tc_rules_final
                val_rules = {}
                for rule_idx_val, rule_val in enumerate(fv.get('form_processing_rules_validations', [])):
                    if not rule_val.get('field'): continue
                    val_detail = {}
                    if rule_val.get('required'): val_detail['required'] = True
                    min_l_str = str(rule_val.get('min_length','')).strip(); max_l_str = str(rule_val.get('max_length','')).strip(); pattern_str = rule_val.get('pattern','').strip()
                    if min_l_str:
                        if not min_l_str.isdigit(): st.error(f"Validation rule #{rule_idx_val+1} for field '{rule_val['field']}': Min Length must be a positive integer."); validation_passed = False; break
                        else: val_detail['min_length'] = int(min_l_str)
                    if max_l_str:
                        if not max_l_str.isdigit(): st.error(f"Validation rule #{rule_idx_val+1} for field '{rule_val['field']}': Max Length must be a positive integer."); validation_passed = False; break
                        else: val_detail['max_length'] = int(max_l_str)
                    if pattern_str: val_detail['pattern'] = pattern_str
                    if val_detail: val_rules[rule_val['field']] = val_detail
                if not validation_passed: st.stop()
                if val_rules : processing_rules_to_save['validations'] = val_rules
                if validation_passed:
                    tf_rules = {}; target_fields_seen_in_transforms_save = set()
                    for rule_idx_tf, rule_tf in enumerate(fv.get('form_processing_rules_transformations', [])):
                        target_field = rule_tf.get('target_field','').strip(); expression = rule_tf.get('expression','').strip()
                        if not target_field or not expression:
                            if target_field or expression: st.error(f"Transformation rule #{rule_idx_tf+1}: Both Target Field and Expression are required."); validation_passed = False; break
                            continue
                        if target_field in target_fields_seen_in_transforms_save: st.error(f"Duplicate target field name '{target_field}' in Transformations. Target field names must be unique within transformations."); validation_passed = False; break
                        target_fields_seen_in_transforms_save.add(target_field)
                        if 'import ' in expression or 'os.' in expression or 'sys.' in expression or '__' in expression: st.warning(f"Transformation expression for '{target_field}' ('{expression}') may contain potentially unsafe code. Please review carefully.")
                        tf_rules[target_field] = expression
                    if not validation_passed: st.stop()
                    if tf_rules: processing_rules_to_save['transformations'] = tf_rules
                if validation_passed:
                    df_rules = [rule_df['field_name'].strip() for rule_df in fv.get('form_processing_rules_drop_fields', []) if rule_df.get('field_name','').strip()]
                    if df_rules: processing_rules_to_save['drop_fields'] = df_rules
                if processing_rules_to_save: temp_config['processing_rules'] = processing_rules_to_save

            if validation_passed:
                try:
                    config_loader.validate_config(temp_config)
                    existing_file = fv.get('existing_config_filename'); original_name_stem = None
                    if existing_file: base_name = Path(existing_file).stem; parts = base_name.rsplit('-', 1); original_name_stem = parts[0] if len(parts) == 2 and parts[1].isdigit() else base_name
                    final_config_filename = existing_file if existing_file and temp_config['name'] == original_name_stem else f"{''.join(c if c.isalnum() else '_' for c in temp_config['name'])}-{int(time.time())}.yaml"
                    config_path = CONFIG_DIR / final_config_filename
                    with open(config_path, 'w', encoding='utf-8') as f: yaml.dump(temp_config, f, sort_keys=False, default_flow_style=False, allow_unicode=True)
                    logger.info(f"Config saved: {config_path}"); st.session_state.flash_message = ("success", f'Config "{temp_config["name"]}" saved as {final_config_filename}!')
                    st.session_state.config_to_edit = None; st.session_state.form_values = get_default_form_values(); st.session_state.current_page = "📋 Manage Jobs"; st.rerun()
                except JsonSchemaValidationError as e: error_path = " -> ".join(map(str, getattr(e, 'path', []))) or "Config root"; message = f"Validation Error: {e.message} (at {error_path})"; st.error(message); logger.error(f"Config validation failed: {message}")
                except Exception as e: st.error(f"Error saving configuration: {e}"); logger.exception("Error saving config from UI")

    if st.button("⬅️ Back to My Jobs", key="cancel_edit_form_button_bottom_v7_final"):
        st.session_state.current_page = "📋 Manage Jobs"
        st.session_state.config_to_edit = None
        st.session_state.form_values = get_default_form_values()
        st.rerun()
