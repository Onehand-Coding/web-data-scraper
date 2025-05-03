from typing import Dict, List, Any, Optional
import re
from datetime import datetime, date
import logging
import json # Keep import although only used in _convert_type currently

# Helper function for nested access (if not imported from a shared utils module)
# Consider moving this to a utils module if used elsewhere
def get_nested_value(data_dict: Optional[Dict | List], key_path: str, default: Any = None) -> Any:
    """
    Accesses a value in a nested structure (dict or list) using a dot-separated key path.

    Handles dictionary keys and integer list indices.

    Args:
        data_dict: The dictionary or list to access.
        key_path: A string representing the path (e.g., 'user.address.city', 'results.0.name').
                  If empty, returns the original data_dict.
        default: The value to return if the path is invalid or not found.

    Returns:
        The value found at the specified path, or the default value.
    """
    if not key_path: # If path is empty, return the whole structure
        return data_dict

    keys = key_path.split('.')
    current_value = data_dict

    try:
        for key in keys:
            if isinstance(current_value, dict):
                current_value = current_value.get(key)
            elif isinstance(current_value, list):
                 # Allow accessing list elements by index if key is a digit
                 try:
                     key_index = int(key)
                     # Check bounds to prevent IndexError
                     if 0 <= key_index < len(current_value):
                         current_value = current_value[key_index]
                     else:
                         # Index out of bounds
                         logging.getLogger(__name__).debug(f"Index '{key}' out of bounds for list in path '{key_path}'.")
                         return default
                 except (ValueError, IndexError):
                     # Key is not a valid integer index or other list access error
                     logging.getLogger(__name__).debug(f"Invalid index '{key}' for list in path '{key_path}'.")
                     return default
            else:
                # Cannot traverse further if not a dict or list
                logging.getLogger(__name__).debug(f"Cannot traverse non-dict/list element with key '{key}' in path '{key_path}'.")
                return default

            if current_value is None:
                 # Path segment doesn't exist
                 logging.getLogger(__name__).debug(f"Path segment '{key}' resulted in None for path '{key_path}'.")
                 return default
        # Successfully navigated the full path
        return current_value
    except Exception as e:
        # Catch unexpected errors during traversal
        logging.getLogger(__name__).error(f"Error accessing nested value for key path '{key_path}': {e}")
        return default
# --- End Helper ---


class DataProcessor:
    """
    Handles post-extraction processing of scraped data items.

    Applies rules defined in the configuration for:
    - Field type conversion
    - Text cleaning
    - Field transformations (using Python expressions)
    - Field validation
    - Dropping specified fields
    """

    def __init__(self, config: Dict = None):
        """
        Initializes the DataProcessor.

        Args:
            config: The scraper configuration dictionary, used primarily
                    to potentially fetch default `processing_rules` if not
                    provided directly to the `process` method.
        """
        self.config = config or {}
        self.logger = logging.getLogger(__name__) # Get logger specific to this module

    def process(self, data: List[Dict], rules: Optional[Dict] = None) -> List[Dict]:
        """
        Applies all configured processing rules to a list of scraped items.

        Args:
            data: A list of dictionaries, where each dictionary is a scraped item.
            rules: A dictionary containing the processing rules, typically fetched
                   from the 'processing_rules' section of the config. If None,
                   it attempts to get rules from the config passed during init.

        Returns:
            A list of processed dictionaries. Items that fail critical steps
            or are invalid might be modified (e.g., fields set to None) or
            potentially skipped if they are not dictionaries.
        """
        # Use provided rules or fallback to config
        active_rules = rules or self.config.get('processing_rules', {})
        if not active_rules:
            self.logger.info("No processing rules provided or found in config. Returning original data.")
            return data # No rules, return original data

        processed_data = []
        # Compile the set of fields to drop at the end for efficiency
        fields_to_drop = set(active_rules.get('drop_fields', []))

        self.logger.info(f"Processing {len(data)} items with rule categories: {list(active_rules.keys())}")

        for i, item in enumerate(data):
            # Ensure we are working with a dictionary
            if not isinstance(item, dict):
                 self.logger.warning(f"Skipping item {i+1} as it is not a dictionary (type: {type(item)}).")
                 continue

            try:
                # Process a copy to avoid modifying the original item if errors occur mid-processing
                processed_item = self._process_item(item.copy(), active_rules)

                # Drop specified fields *after* all other processing
                if fields_to_drop:
                    for field in fields_to_drop:
                        # Use pop with default None to avoid KeyError if field was already removed/missing
                        processed_item.pop(field, None)

                processed_data.append(processed_item)
            except Exception as e:
                # Catch unexpected errors during individual item processing
                self.logger.error(f"Failed to process item {i+1} (Original: {item}): {e}", exc_info=True)
                # Optionally: Add the original item to processed_data to avoid losing it,
                # or skip it depending on desired behavior for errors. Currently skips.

        self.logger.info(f"Finished processing. {len(processed_data)} items resulted.")
        return processed_data

    def _process_item(self, item: Dict, rules: Dict) -> Dict:
        """
        Applies the sequence of processing rules to a single item dictionary.

        Order of operations:
        1. Field type conversion
        2. Text cleaning
        3. Transformations
        4. Validations

        Args:
            item: The dictionary representing a single scraped item (should be a copy).
            rules: The dictionary containing all processing rules.

        Returns:
            The processed item dictionary.
        """
        # 1. Field type conversions (applied first)
        for field, type_info in rules.get('field_types', {}).items():
            if field in item and item[field] is not None:
                item[field] = self._convert_type(item[field], type_info)

        # 2. Text cleaning (applied to potentially type-converted strings)
        for field, clean_rules in rules.get('text_cleaning', {}).items():
            if field in item and isinstance(item[field], str):
                 item[field] = self._clean_text(item[field], clean_rules)

        # 3. Field transformations (can use results of previous steps)
        transformed_values = {}
        # Define a safe environment for eval()
        safe_builtins = {
            "len": len, "str": str, "int": int, "float": float,
            "list": list, "dict": dict, "set": set, "tuple": tuple,
            "abs": abs, "round": round, "max": max, "min": min, "sum": sum,
            "true": True, "false": False, "none": None,
            "isinstance": isinstance, # Allow type checking
            # Add other safe builtins if needed
        }
        # Provide access to the current item and potentially other utilities
        # Note: 'value' represents the current value of the *target_field* being transformed
        context = {'value': None, 'item': item, 're': re, 'datetime': datetime, 'date': date}

        for target_field, transform_expression in rules.get('transformations', {}).items():
             context['value'] = item.get(target_field) # Make current value available to expression
             try:
                  # Evaluate the expression within the restricted environment
                  result = eval(transform_expression, {"__builtins__": safe_builtins}, context)
                  transformed_values[target_field] = result
                  # Log transformation success/result if needed (can be verbose)
                  # self.logger.debug(f"Field '{target_field}': Transformed. Result: {result}")
             except Exception as e:
                  # Log transformation errors clearly
                  self.logger.warning(f"Transformation failed for field '{target_field}' with expr '{transform_expression}': {e}")
                  # Decide error handling: keep original value, set None, or raise? Currently keeps original/None.

        item.update(transformed_values) # Apply successful transformations

        # 4. Field validation (applied last, after all transformations)
        for field, validation_rules in rules.get('validations', {}).items():
             # Check if field exists OR if it's required (must validate if required, even if missing)
             if field in item or validation_rules.get('required'):
                 value_to_validate = item.get(field)
                 is_valid = self._validate_field(value_to_validate, validation_rules, field) # Pass field name for logging
                 if not is_valid:
                      # Set invalid field to None - other options: remove item, raise error
                      item[field] = None

        return item


    def _convert_type(self, value: Any, type_info: Dict) -> Any:
        """
        Attempts to convert a value to the specified target type.

        Args:
            value: The value to convert.
            type_info: A dictionary containing 'type' (e.g., 'int', 'float', 'datetime')
                       and optionally 'format' for date/datetime types.

        Returns:
            The converted value, or None if conversion fails or input is None.
        """
        target_type = type_info.get('type')
        format_str = type_info.get('format')
        original_value_repr = repr(value)[:50] # For logging

        if value is None:
            return None # Cannot convert None

        try:
            if target_type == 'string':
                # Convert to string and strip whitespace
                return str(value).strip()
            elif target_type == 'int':
                # Handle string-to-int conversion robustly
                if isinstance(value, str):
                    cleaned_value = re.sub(r'[^\d-]', '', value) # Keep digits and hyphen
                    # Avoid ValueError for empty string or just hyphen
                    return int(cleaned_value) if cleaned_value and cleaned_value != '-' else None
                elif isinstance(value, (int, float)):
                    return int(value) # Standard conversion (float truncates)
                else:
                     # Cannot convert other types (like lists, dicts) directly
                    self.logger.debug(f"Cannot convert type {type(value)} to int for value {original_value_repr}")
                    return None
            elif target_type == 'float':
                 # Handle string-to-float conversion robustly
                 if isinstance(value, str):
                      cleaned_value = re.sub(r'[^\d.-]', '', value) # Keep digits, dot, hyphen
                      # Handle multiple decimal points gracefully (keep first)
                      if cleaned_value.count('.') > 1:
                          parts = cleaned_value.split('.')
                          cleaned_value = parts[0] + '.' + "".join(parts[1:])
                      # Avoid ValueError for empty string or non-numeric remnants
                      return float(cleaned_value) if cleaned_value and cleaned_value not in ['.', '-'] else None
                 elif isinstance(value, (int, float)):
                     return float(value) # Standard conversion
                 else:
                      self.logger.debug(f"Cannot convert type {type(value)} to float for value {original_value_repr}")
                      return None
            elif target_type == 'boolean':
                # Handle common truthy/falsy interpretations
                if isinstance(value, bool): return value
                if isinstance(value, (int, float)): return bool(value)
                if isinstance(value, str):
                    return value.lower().strip() in ('true', '1', 'yes', 'y', 'on')
                return False # Default other types to False
            elif target_type == 'datetime':
                 if isinstance(value, datetime): return value # Already correct type
                 str_value = str(value).strip()
                 # Try with format string if provided
                 if format_str:
                     try: return datetime.strptime(str_value, format_str)
                     except ValueError: pass # Ignore format error and try ISO next
                 # Try ISO format (handles many common variations)
                 try: return datetime.fromisoformat(str_value)
                 except ValueError: pass # Ignore ISO format error
                 self.logger.warning(f"Failed datetime conversion for '{str_value}' (Format: {format_str})")
                 return None # Give up if all attempts fail
            elif target_type == 'date':
                 if isinstance(value, date): return value # Already correct type
                 if isinstance(value, datetime): return value.date() # Extract date part
                 str_value = str(value).strip()
                 # Try with format string if provided
                 if format_str:
                     try: return datetime.strptime(str_value, format_str).date()
                     except ValueError: pass # Ignore format error and try ISO next
                 # Try ISO format
                 try: return date.fromisoformat(str_value)
                 except ValueError: pass # Ignore ISO format error
                 self.logger.warning(f"Failed date conversion for '{str_value}' (Format: {format_str})")
                 return None # Give up if all attempts fail
            else:
                 # Log if an unknown type is specified in the config
                 self.logger.warning(f"Unknown target type '{target_type}' specified. Returning original value.")
                 return value
        except (ValueError, TypeError) as e:
            # Catch errors during conversion (e.g., invalid format, non-numeric string)
            self.logger.warning(f"Type conversion failed for {original_value_repr} to '{target_type}' (Format: {format_str}): {e}")
            return None


    def _clean_text(self, text: str, rules: Dict) -> str:
        """
        Applies various text cleaning operations based on the provided rules.

        Args:
            text: The input string to clean.
            rules: A dictionary of cleaning rules (e.g., {'trim': True, 'lowercase': False}).

        Returns:
            The cleaned string.
        """
        if not isinstance(text, str):
            self.logger.warning(f"Attempted to clean non-string value: {type(text)}. Returning as is.")
            return text # Should not happen if called correctly, but safe check

        cleaned_text = text

        # --- Apply specific cleaning rules ---
        if rules.get('trim', True): # Default: Trim whitespace
             cleaned_text = cleaned_text.strip()
        if rules.get('lowercase', False): # Default: No lowercase
             cleaned_text = cleaned_text.lower()
        if rules.get('uppercase', False): # Default: No uppercase
             cleaned_text = cleaned_text.upper()
        if rules.get('remove_newlines', True): # Default: Replace newlines with space
             cleaned_text = re.sub(r'[\r\n\t]+', ' ', cleaned_text)
        if rules.get('remove_extra_spaces', True): # Default: Consolidate multiple spaces
             cleaned_text = ' '.join(cleaned_text.split())
        if rules.get('remove_special_chars', False): # Default: Keep special chars
             # Example: Remove chars except letters, numbers, whitespace, hyphen, period, comma
             cleaned_text = re.sub(r'[^\w\s\-.,]', '', cleaned_text, flags=re.UNICODE)
             # Note: This regex might need adjustment based on specific needs

        # Handle regex replacements
        regex_replace_rules = rules.get('regex_replace', {})
        if isinstance(regex_replace_rules, dict):
            for pattern, replacement in regex_replace_rules.items():
                 try:
                     # Ensure replacement is a string
                     cleaned_text = re.sub(pattern, str(replacement), cleaned_text)
                 except re.error as e:
                      self.logger.error(f"Invalid regex pattern '{pattern}' in text_cleaning: {e}")
                 except TypeError:
                     self.logger.error(f"Replacement for pattern '{pattern}' must be a string, not {type(replacement)}.")
        elif regex_replace_rules: # Check if it's not a dict but still present
             self.logger.error(f"'regex_replace' rule must be a dictionary, but got {type(regex_replace_rules)}.")

        # Trim again at the end if extra spaces were potentially introduced
        if rules.get('trim', True):
             cleaned_text = cleaned_text.strip()

        return cleaned_text


    def _validate_field(self, value: Any, validation: Dict, field_name_for_log: str = "?") -> bool:
        """
        Validates a single field's value against specified rules.

        Args:
            value: The value to validate.
            validation: Dictionary of validation rules for this field
                        (e.g., {'required': True, 'min_length': 5}).
            field_name_for_log: The name of the field being validated (for logging).

        Returns:
            True if the value is valid according to the rules, False otherwise.
        """
        # Check for requirement first
        is_empty = value in (None, '', [], {}) # Consider various empty types
        if validation.get('required') and is_empty:
            self.logger.debug(f"Validation failed for field '{field_name_for_log}': Required but is empty/None.")
            return False

        # If the field is not required and is empty, it's considered valid (passes non-requirement)
        if not validation.get('required') and is_empty:
             return True

        # --- Perform other checks only if value is NOT considered empty ---
        try:
            # Use string representation for length/pattern checks, handle non-strings gracefully
            str_value = str(value)

            # Check Minimum Length
            if 'min_length' in validation:
                min_len = int(validation['min_length']) # Will raise ValueError if not int
                if len(str_value) < min_len:
                     self.logger.debug(f"Validation failed for field '{field_name_for_log}': Length {len(str_value)} < min_length {min_len}.")
                     return False

            # Check Maximum Length
            if 'max_length' in validation:
                max_len = int(validation['max_length']) # Will raise ValueError if not int
                if len(str_value) > max_len:
                     self.logger.debug(f"Validation failed for field '{field_name_for_log}': Length {len(str_value)} > max_length {max_len}.")
                     return False

            # Check Regex Pattern
            if 'pattern' in validation:
                 pattern = validation['pattern']
                 # Ensure pattern is a valid regex string
                 if not isinstance(pattern, str):
                      self.logger.error(f"Invalid 'pattern' rule for field '{field_name_for_log}': Must be a string.")
                      return False
                 if not re.match(pattern, str_value):
                      # Log only the start of the value if it's long
                      value_repr = repr(str_value[:50]) + ('...' if len(str_value) > 50 else '')
                      self.logger.debug(f"Validation failed for field '{field_name_for_log}': Value {value_repr} does not match pattern '{pattern}'.")
                      return False

        except (ValueError, TypeError) as e:
             # Handle errors if min/max_length rules are not valid numbers in the config
             self.logger.error(f"Invalid validation rule configuration for field '{field_name_for_log}': {e}. Rule: {validation}")
             return False # Fail validation if rules themselves are invalid

        except re.error as e:
             # Handle invalid regex pattern specified in the config
             self.logger.error(f"Invalid regex pattern '{validation.get('pattern')}' in validation rules for field '{field_name_for_log}': {e}")
             return False # Fail validation if pattern is invalid

        # If all checks passed (or were skipped appropriately)
        return True
