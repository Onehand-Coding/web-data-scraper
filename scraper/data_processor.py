# File: web-data-scraper/scraper/data_processor.py (Corrected SyntaxError on line 125)

from typing import Dict, List, Any
import re
from datetime import datetime, date
import logging
import json

# Helper function for nested access
def get_nested_value(data_dict: Dict, key_path: str, default: Any = None) -> Any:
    """Access nested dictionary value using dot notation."""
    keys = key_path.split('.')
    current_value = data_dict
    try:
        for key in keys:
            if isinstance(current_value, dict):
                current_value = current_value.get(key)
            elif isinstance(current_value, list):
                 try:
                     key_index = int(key)
                     if 0 <= key_index < len(current_value):
                         current_value = current_value[key_index]
                     else: return default # Index out of bounds
                 except (ValueError, IndexError):
                     return default # Key is not a valid index or index out of bounds
            else:
                return default # Cannot traverse further
            if current_value is None:
                 return default # Path doesn't exist fully
        return current_value
    except Exception:
        return default


class DataProcessor:
    """Handles data cleaning and transformation."""

    def __init__(self, config: Dict = None):
        self.config = config or {}
        self.logger = logging.getLogger(__name__)

    def process(self, data: List[Dict], rules: Dict = None) -> List[Dict]:
        """Apply processing rules to dataset."""
        rules = rules or self.config.get('processing_rules', {})
        if not rules:
            return data # No rules, return original

        processed_data = []
        fields_to_drop = set(rules.get('drop_fields', []))

        self.logger.info(f"Processing {len(data)} items with rules: {list(rules.keys())}")

        for i, item in enumerate(data):
            if not isinstance(item, dict):
                 self.logger.warning(f"Skipping item {i+1} as it is not a dictionary (type: {type(item)}).")
                 continue

            try:
                processed_item = self._process_item(item.copy(), rules)
                if fields_to_drop:
                    for field in fields_to_drop:
                        processed_item.pop(field, None)
                processed_data.append(processed_item)
            except Exception as e:
                self.logger.error(f"Failed to process item {i+1} (Original: {item}): {e}", exc_info=True)

        self.logger.info(f"Finished processing. {len(processed_data)} items processed.")
        return processed_data

    def _process_item(self, item: Dict, rules: Dict) -> Dict:
        """Process individual data item according to rules."""
        # 1. Field type conversions
        for field, type_info in rules.get('field_types', {}).items():
            if field in item and item[field] is not None:
                item[field] = self._convert_type(item[field], type_info)

        # 2. Text cleaning
        for field, clean_rules in rules.get('text_cleaning', {}).items():
            if field in item and isinstance(item[field], str):
                 item[field] = self._clean_text(item[field], clean_rules)

        # 3. Field transformations
        transformed_values = {}
        safe_builtins = { "len": len, "str": str, "int": int, "float": float, "list": list, "dict": dict, "set": set, "tuple": tuple, "abs": abs, "round": round, "max": max, "min": min, "sum": sum, "true": True, "false": False, "none": None }
        context = {'value': None, 'item': item, 're': re, 'datetime': datetime, 'date': date}

        for target_field, transform_expression in rules.get('transformations', {}).items():
             context['value'] = item.get(target_field) # Update context
             try:
                  result = eval(transform_expression, {"__builtins__": safe_builtins}, context)
                  transformed_values[target_field] = result
                  # self.logger.debug(f"Field '{target_field}': Transformed. Result: {result}")
             except Exception as e:
                  self.logger.warning(f"Transformation failed for field '{target_field}' with expr '{transform_expression}': {e}")

        item.update(transformed_values) # Apply transformations

        # 4. Field validation
        for field, validation_rules in rules.get('validations', {}).items():
             if field in item:
                 is_valid = self._validate_field(item[field], validation_rules)
                 if not is_valid: self.logger.warning(f"Field '{field}' ('{item[field]}') failed validation: {validation_rules}. Setting to None."); item[field] = None;
             elif validation_rules.get('required'): self.logger.warning(f"Required field '{field}' missing. Setting to None."); item[field] = None;

        return item


    def _convert_type(self, value: Any, type_info: Dict) -> Any:
        """Convert field to specified type. Return None on failure."""
        target_type = type_info.get('type'); format_str = type_info.get('format')
        try:
            if value is None: return None
            if target_type == 'string': return str(value).strip()
            elif target_type == 'int':
                if isinstance(value, str):
                    cleaned_value = re.sub(r'[^\d-]', '', value)
                    return int(cleaned_value) if cleaned_value else None
                elif isinstance(value, (int, float)):
                    return int(value) # Correctly handles float -> int truncation
                else:
                    return None # Cannot convert other types to int directly
            elif target_type == 'float':
                 if isinstance(value, str):
                      cleaned_value = re.sub(r'[^\d.-]', '', value)
                      if cleaned_value.count('.') > 1: cleaned_value = cleaned_value.replace('.', '', cleaned_value.count('.') - 1)
                      return float(cleaned_value) if cleaned_value and cleaned_value != '.' else None
                 elif isinstance(value, (int, float)):
                     return float(value)
                 else:
                      return None
            elif target_type == 'boolean':
                if isinstance(value, (int, float)): return bool(value)
                return str(value).lower().strip() in ('true', '1', 'yes', 'y', 'on')
            elif target_type == 'datetime':
                 if isinstance(value, datetime): return value
                 return datetime.strptime(str(value).strip(), format_str) if format_str else datetime.fromisoformat(str(value).strip())
            elif target_type == 'date':
                 if isinstance(value, date): return value
                 if isinstance(value, datetime): return value.date()
                 return datetime.strptime(str(value).strip(), format_str).date() if format_str else date.fromisoformat(str(value).strip())
            else:
                 self.logger.warning(f"Unknown target type '{target_type}'.")
                 return value
        except (ValueError, TypeError) as e:
            self.logger.warning(f"Type conversion failed for '{value}' to '{target_type}' (Format: {format_str}): {e}")
            return None


    def _clean_text(self, text: str, rules: Dict) -> str:
        """Clean text according to rules."""
        if not isinstance(text, str): return text
        cleaned_text = text;
        if rules.get('trim', True): cleaned_text = cleaned_text.strip()
        if rules.get('lowercase'): cleaned_text = cleaned_text.lower()
        if rules.get('uppercase'): cleaned_text = cleaned_text.upper()
        if rules.get('remove_newlines', True): cleaned_text = re.sub(r'[\r\n]+', ' ', cleaned_text)
        if rules.get('remove_extra_spaces', True): cleaned_text = ' '.join(cleaned_text.split())
        if rules.get('remove_special_chars'): cleaned_text = re.sub(r'[^\w\s-]', '', cleaned_text, flags=re.UNICODE)
        if rules.get('regex_replace'):
            if isinstance(rules['regex_replace'], dict):
                 for pattern, replacement in rules['regex_replace'].items():
                      try: cleaned_text = re.sub(pattern, replacement, cleaned_text)
                      except re.error as e: self.logger.error(f"Invalid regex pattern '{pattern}': {e}")
            else: self.logger.error("'regex_replace' must be a dict.")
        return cleaned_text

    def _validate_field(self, value: Any, validation: Dict) -> bool:
        """Validate field against rules."""
        if validation.get('required') and value in (None, '', [], {}): return False
        if value is not None:
            str_value = str(value)
            if 'min_length' in validation and len(str_value) < validation['min_length']: return False
            if 'max_length' in validation and len(str_value) > validation['max_length']: return False
            if 'pattern' in validation:
                 pattern = validation['pattern']
                 try:
                      if not re.match(pattern, str_value): return False
                 except re.error as e: self.logger.error(f"Invalid regex '{pattern}': {e}"); return False
        return True
