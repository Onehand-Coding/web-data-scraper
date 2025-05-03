# File: web-data-scraper/scraper/data_processor.py (Corrected)

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
        # --- ADD isinstance AND len TO SAFE BUILTINS ---
        safe_builtins = {
            "len": len, "str": str, "int": int, "float": float,
            "list": list, "dict": dict, "set": set, "tuple": tuple,
            "abs": abs, "round": round, "max": max, "min": min, "sum": sum,
            "true": True, "false": False, "none": None,
            "isinstance": isinstance, # <-- ADDED
            "len": len # <-- ADDED
        }
        # Also add utility modules if needed, provide item and current value
        context = {'value': None, 'item': item, 're': re, 'datetime': datetime, 'date': date}

        for target_field, transform_expression in rules.get('transformations', {}).items():
             context['value'] = item.get(target_field) # Update context with the field's current value
             try:
                  # Evaluate the expression within the restricted environment
                  result = eval(transform_expression, {"__builtins__": safe_builtins}, context)
                  transformed_values[target_field] = result
                  # self.logger.debug(f"Field '{target_field}': Transformed. Result: {result}")
             except Exception as e:
                  self.logger.warning(f"Transformation failed for field '{target_field}' with expr '{transform_expression}': {e}")

        item.update(transformed_values) # Apply transformations

        # 4. Field validation
        for field, validation_rules in rules.get('validations', {}).items():
             # Check if field exists or if it's required
             if field in item or validation_rules.get('required'):
                 # Pass the value or None if required but missing
                 value_to_validate = item.get(field)
                 is_valid = self._validate_field(value_to_validate, validation_rules)
                 if not is_valid:
                      self.logger.warning(f"Field '{field}' ('{str(value_to_validate)[:50]}...') failed validation: {validation_rules}. Setting to None.")
                      item[field] = None # Set invalid field to None

        return item


    def _convert_type(self, value: Any, type_info: Dict) -> Any:
        """Convert field to specified type. Return None on failure."""
        target_type = type_info.get('type'); format_str = type_info.get('format')
        try:
            if value is None: return None
            if target_type == 'string': return str(value).strip()
            elif target_type == 'int':
                if isinstance(value, str):
                    # Remove non-digit characters except leading hyphen
                    cleaned_value = re.sub(r'[^\d-]', '', value)
                    # Handle potential empty string after cleaning
                    return int(cleaned_value) if cleaned_value and cleaned_value != '-' else None
                elif isinstance(value, (int, float)):
                    return int(value) # Correctly handles float -> int truncation
                else:
                    self.logger.debug(f"Cannot convert type {type(value)} to int for value '{value}'")
                    return None # Cannot convert other types to int directly
            elif target_type == 'float':
                 if isinstance(value, str):
                      # Remove non-digit characters except decimal point and leading hyphen
                      cleaned_value = re.sub(r'[^\d.-]', '', value)
                      # Ensure only one decimal point remains if multiple are present
                      if cleaned_value.count('.') > 1:
                          parts = cleaned_value.split('.')
                          cleaned_value = parts[0] + '.' + "".join(parts[1:])
                      # Handle potential empty string or just '.' or '-' after cleaning
                      return float(cleaned_value) if cleaned_value and cleaned_value not in ['.', '-'] else None
                 elif isinstance(value, (int, float)):
                     return float(value)
                 else:
                      self.logger.debug(f"Cannot convert type {type(value)} to float for value '{value}'")
                      return None
            elif target_type == 'boolean':
                # Handle boolean conversion more robustly
                if isinstance(value, bool): return value
                if isinstance(value, (int, float)): return bool(value)
                if isinstance(value, str):
                    return value.lower().strip() in ('true', '1', 'yes', 'y', 'on')
                return False # Default to False for other types or None
            elif target_type == 'datetime':
                 if isinstance(value, datetime): return value
                 # Try parsing with format string first, then ISO format
                 try:
                     return datetime.strptime(str(value).strip(), format_str) if format_str else datetime.fromisoformat(str(value).strip())
                 except ValueError:
                     self.logger.warning(f"Failed datetime conversion for '{value}' with format '{format_str}'. Trying ISO format or returning None.")
                     try:
                          # Attempt ISO format as a fallback if no format or format failed
                          return datetime.fromisoformat(str(value).strip())
                     except ValueError:
                          return None # Give up if both fail
            elif target_type == 'date':
                 if isinstance(value, date): return value
                 if isinstance(value, datetime): return value.date()
                 # Try parsing with format string first, then ISO format
                 try:
                     return datetime.strptime(str(value).strip(), format_str).date() if format_str else date.fromisoformat(str(value).strip())
                 except ValueError:
                      self.logger.warning(f"Failed date conversion for '{value}' with format '{format_str}'. Trying ISO format or returning None.")
                      try:
                          # Attempt ISO format as a fallback if no format or format failed
                           return date.fromisoformat(str(value).strip())
                      except ValueError:
                           return None # Give up if both fail
            else:
                 self.logger.warning(f"Unknown target type '{target_type}'. Returning original value.")
                 return value
        except (ValueError, TypeError) as e:
            self.logger.warning(f"Type conversion failed for '{value}' to '{target_type}' (Format: {format_str}): {e}")
            return None


    def _clean_text(self, text: str, rules: Dict) -> str:
        """Clean text according to rules."""
        if not isinstance(text, str): return text # Return non-strings as is
        cleaned_text = text;
        # Apply rules only if they are True in the config
        if rules.get('trim', True): cleaned_text = cleaned_text.strip() # Default True
        if rules.get('lowercase', False): cleaned_text = cleaned_text.lower() # Default False
        if rules.get('uppercase', False): cleaned_text = cleaned_text.upper() # Default False
        if rules.get('remove_newlines', True): cleaned_text = re.sub(r'[\r\n]+', ' ', cleaned_text) # Default True
        if rules.get('remove_extra_spaces', True): cleaned_text = ' '.join(cleaned_text.split()) # Default True
        if rules.get('remove_special_chars', False): # Default False
             # Keep letters, numbers, whitespace, and hyphen (common in many contexts)
             cleaned_text = re.sub(r'[^\w\s-]', '', cleaned_text, flags=re.UNICODE)
        # Regex Replace (Ensure it's a dictionary)
        regex_replace_rules = rules.get('regex_replace', {}) # Default to empty dict
        if isinstance(regex_replace_rules, dict):
            for pattern, replacement in regex_replace_rules.items():
                 try:
                     # Make sure replacement is a string
                     cleaned_text = re.sub(pattern, str(replacement), cleaned_text)
                 except re.error as e:
                      self.logger.error(f"Invalid regex pattern '{pattern}': {e}")
                 except TypeError:
                     self.logger.error(f"Replacement for pattern '{pattern}' must be a string, not {type(replacement)}.")
        elif regex_replace_rules: # If it exists but isn't a dict
             self.logger.error(f"'regex_replace' rule must be a dictionary, but got {type(regex_replace_rules)}.")

        return cleaned_text


    def _validate_field(self, value: Any, validation: Dict) -> bool:
        """Validate field against rules."""
        # Check required first
        is_empty = value in (None, '', [], {})
        if validation.get('required') and is_empty:
            self.logger.debug(f"Validation failed: Field is required but empty/None.")
            return False

        # Skip other checks if the value is None/empty and not required
        if is_empty:
             return True

        # Perform other checks only if value is not empty
        try:
            str_value = str(value) # Convert to string for length/pattern checks
            if 'min_length' in validation:
                min_len = int(validation['min_length'])
                if len(str_value) < min_len:
                     self.logger.debug(f"Validation failed: Length {len(str_value)} < min_length {min_len}.")
                     return False
            if 'max_length' in validation:
                max_len = int(validation['max_length'])
                if len(str_value) > max_len:
                     self.logger.debug(f"Validation failed: Length {len(str_value)} > max_length {max_len}.")
                     return False
            if 'pattern' in validation:
                 pattern = validation['pattern']
                 if not re.match(pattern, str_value):
                      self.logger.debug(f"Validation failed: Value '{str_value[:50]}...' does not match pattern '{pattern}'.")
                      return False
        except (ValueError, TypeError) as e:
             # Handle cases where min/max_length is not a valid number in config
             self.logger.error(f"Invalid validation rule config: {e}. Rule: {validation}")
             return False # Fail validation if rules are bad
        except re.error as e:
             self.logger.error(f"Invalid regex pattern '{validation.get('pattern')}' in validation rules: {e}")
             return False # Fail validation if pattern is bad

        return True # Passed all checks
