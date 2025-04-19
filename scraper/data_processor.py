# File: web-data-scraper/scraper/data_processor.py

"""
Data transformation and cleaning utilities.
"""

from typing import Dict, List, Any
import re
from datetime import datetime, date # Added date back
import logging
import json # Added json for potential complex type storage/retrieval

class DataProcessor:
    """Handles data cleaning and transformation."""

    def __init__(self, config: Dict = None):
        self.config = config or {}
        # Get logger using the standard pattern
        self.logger = logging.getLogger(__name__)

    def process(self, data: List[Dict], rules: Dict = None) -> List[Dict]:
        """Apply processing rules to dataset."""
        # Get rules from config if not explicitly passed
        rules = rules or self.config.get('processing_rules', {})
        if not rules:
            self.logger.info("No processing rules found or provided. Returning original data.")
            return data

        processed_data = []
        fields_to_drop = set(rules.get('drop_fields', [])) # Get fields to drop early

        self.logger.info(f"Processing {len(data)} items with rules: {list(rules.keys())}")

        for i, item in enumerate(data):
            if not isinstance(item, dict):
                 self.logger.warning(f"Skipping item {i+1} as it is not a dictionary (type: {type(item)}).")
                 continue

            try:
                processed_item = self._process_item(item.copy(), rules)
                # Drop specified fields AFTER processing other rules
                if fields_to_drop:
                    for field in fields_to_drop:
                        processed_item.pop(field, None) # Remove field, ignore if not present

                processed_data.append(processed_item)
            except Exception as e:
                # Log the item index along with the error
                self.logger.error(f"Failed to process item {i+1} (Original: {item}): {e}", exc_info=True) # Add traceback

        self.logger.info(f"Finished processing. {len(processed_data)} items processed.")
        return processed_data

    def _process_item(self, item: Dict, rules: Dict) -> Dict:
        """Process individual data item according to rules."""
        # Process in a specific order: Type Conversions -> Cleaning -> Transformations -> Validations
        # Note: drop_fields is handled after this method returns

        # 1. Field type conversions
        for field, type_info in rules.get('field_types', {}).items():
            if field in item and item[field] is not None:
                original_value = item[field]
                item[field] = self._convert_type(original_value, type_info)
                # Log if value changed (and wasn't just whitespace change for string)
                if item[field] != original_value and not (isinstance(original_value, str) and isinstance(item[field], str) and item[field] == original_value.strip()):
                     self.logger.debug(f"Field '{field}': Type converted '{original_value}' ({type(original_value).__name__}) -> '{item[field]}' ({type(item[field]).__name__})")


        # 2. Text cleaning
        for field, clean_rules in rules.get('text_cleaning', {}).items():
            if field in item and isinstance(item[field], str): # Only clean strings
                 original_value = item[field]
                 item[field] = self._clean_text(original_value, clean_rules)
                 if item[field] != original_value:
                      self.logger.debug(f"Field '{field}': Cleaned '{original_value}' -> '{item[field]}'")


        # 3. Field transformations (evaluates Python code - use with caution!)
        # Ensure transformations run after types/cleaning so they operate on processed data
        # Store results temporarily to avoid modifying 'item' during iteration if transformation uses other item fields
        transformed_values = {}
        for target_field, transform_expression in rules.get('transformations', {}).items():
             # Allow transformations to create new fields or overwrite existing ones
             # The expression can access 'value' (field being transformed if exists) or 'item' (whole item dict)
             context = {'value': item.get(target_field), 'item': item, 're': re, 'datetime': datetime, 'date': date}
             try:
                  # Using target_field in context allows transformation to modify existing field: e.g., "value + 1"
                  # Creating a new field: e.g., "item.get('field_a', 0) + item.get('field_b', 0)"
                  transformed_values[target_field] = eval(transform_expression, {"__builtins__": {}}, context) # Limit builtins for safety
                  self.logger.debug(f"Field '{target_field}': Transformed using expression. Result: {transformed_values[target_field]}")
             except Exception as e:
                  self.logger.warning(f"Transformation failed for field '{target_field}' with expression '{transform_expression}': {e}")
                  # Decide how to handle failed transformations, e.g., set to None or keep original
                  # For now, we just don't add it to transformed_values if it fails
                  # If overwriting, original value remains. If creating new field, it won't be created.

        item.update(transformed_values) # Apply successful transformations


        # 4. Field validation
        # Run validation last, after all transformations
        validation_passed = True
        for field, validation_rules in rules.get('validations', {}).items():
             if field in item: # Only validate fields that exist after potential transformations
                 is_valid = self._validate_field(item[field], validation_rules)
                 if not is_valid:
                      self.logger.warning(f"Field '{field}' with value '{item[field]}' failed validation rules: {validation_rules}. Setting to None.")
                      item[field] = None # Set invalid fields to None (or could drop item)
                      validation_passed = False # Track if any validation failed for the item
             elif validation_rules.get('required'):
                  # Field is required but doesn't exist in the item
                  self.logger.warning(f"Required field '{field}' is missing after processing. Setting to None.")
                  item[field] = None # Add field as None if required but missing
                  validation_passed = False


        # Optional: Could add logic here to drop the entire 'item' if validation_passed is False

        return item


    def _convert_type(self, value: Any, type_info: Dict) -> Any:
        """Convert field to specified type. Return None on failure."""
        target_type = type_info.get('type')
        format_str = type_info.get('format')

        try:
            if value is None: return None # Keep None as None

            if target_type == 'string':
                return str(value).strip() # Apply basic strip for string conversion
            elif target_type == 'int':
                # More robust int conversion: remove common currency/separators first
                if isinstance(value, str):
                    cleaned_value = re.sub(r'[^\d-]', '', value)
                    return int(cleaned_value) if cleaned_value else None
                elif isinstance(value, (int, float)): # Handle existing numbers
                    return int(value)
                else: return None # Cannot convert type
            elif target_type == 'float':
                 # More robust float conversion
                 if isinstance(value, str):
                      # Remove common currency/thousands separators, keep decimal point and sign
                      cleaned_value = re.sub(r'[^\d.-]', '', value)
                      # Handle case where multiple decimal points might remain after cleaning
                      if cleaned_value.count('.') > 1: cleaned_value = cleaned_value.replace('.', '', cleaned_value.count('.') - 1)
                      return float(cleaned_value) if cleaned_value and cleaned_value != '.' else None
                 elif isinstance(value, (int, float)): # Handle existing numbers
                      return float(value)
                 else: return None # Cannot convert type
            elif target_type == 'boolean':
                # Handle numeric 1/0 as boolean
                if isinstance(value, (int, float)): return bool(value)
                # Handle common string representations
                return str(value).lower().strip() in ('true', '1', 'yes', 'y', 'on')
            elif target_type == 'datetime':
                 if isinstance(value, datetime): return value # Already correct type
                 if format_str: return datetime.strptime(str(value).strip(), format_str)
                 else: return datetime.fromisoformat(str(value).strip()) # Assume ISO format if no format given
            elif target_type == 'date':
                 if isinstance(value, date): return value # Already correct type
                 if isinstance(value, datetime): return value.date() # Extract date part
                 if format_str: return datetime.strptime(str(value).strip(), format_str).date()
                 else: return date.fromisoformat(str(value).strip())
            else:
                 # Should not happen if schema validation is correct, but return original if unknown type
                 self.logger.warning(f"Unknown target type '{target_type}' specified.")
                 return value

        except (ValueError, TypeError) as e:
            # Log conversion failure and return None
            self.logger.warning(f"Type conversion failed for value '{value}' to type '{target_type}' (Format: {format_str}): {e}")
            return None


    def _clean_text(self, text: str, rules: Dict) -> str:
        """Clean text according to rules."""
        if not isinstance(text, str):
            self.logger.warning(f"Attempted to clean non-string value: {text} ({type(text).__name__})")
            return text # Return unchanged if not a string

        cleaned_text = text # Start with original
        # Apply cleaning operations in specified order
        if rules.get('trim', True): cleaned_text = cleaned_text.strip() # Default trim=True
        if rules.get('lowercase'): cleaned_text = cleaned_text.lower()
        if rules.get('uppercase'): cleaned_text = cleaned_text.upper()
        # Compile regex for efficiency if used multiple times
        if rules.get('remove_newlines', True): cleaned_text = re.sub(r'[\r\n]+', ' ', cleaned_text) # Replace one or more newlines with space
        if rules.get('remove_extra_spaces', True): cleaned_text = ' '.join(cleaned_text.split()) # Consolidate multiple spaces
        if rules.get('remove_special_chars'): cleaned_text = re.sub(r'[^\w\s-]', '', cleaned_text, flags=re.UNICODE) # Remove non-alphanumeric/space/hyphen
        # Regex replace needs careful handling
        if rules.get('regex_replace'):
            if isinstance(rules['regex_replace'], dict):
                 for pattern, replacement in rules['regex_replace'].items():
                      try:
                           cleaned_text = re.sub(pattern, replacement, cleaned_text)
                      except re.error as e:
                           self.logger.error(f"Invalid regex pattern '{pattern}' in cleaning rules: {e}")
            else:
                 self.logger.error("'regex_replace' rule must be a dictionary of pattern:replacement pairs.")

        return cleaned_text

    def _validate_field(self, value: Any, validation: Dict) -> bool:
        """Validate field against rules."""
        if validation.get('required') and value in (None, '', [], {}): # Consider empty dicts/lists as invalid if required
            self.logger.debug(f"Validation failed for value '{value}': Required field is missing/empty.")
            return False
        # Apply other rules only if value is not None (or should they apply to empty strings?)
        if value is not None:
            str_value = str(value) # Convert to string for length/pattern checks
            if 'min_length' in validation and len(str_value) < validation['min_length']:
                self.logger.debug(f"Validation failed for value '{value}': Length {len(str_value)} < min_length {validation['min_length']}.")
                return False
            if 'max_length' in validation and len(str_value) > validation['max_length']:
                 self.logger.debug(f"Validation failed for value '{value}': Length {len(str_value)} > max_length {validation['max_length']}.")
                 return False
            if 'pattern' in validation:
                 pattern = validation['pattern']
                 try:
                      if not re.match(pattern, str_value):
                           self.logger.debug(f"Validation failed for value '{value}': Does not match pattern '{pattern}'.")
                           return False
                 except re.error as e:
                      self.logger.error(f"Invalid regex pattern '{pattern}' in validation rule: {e}")
                      return False # Treat invalid pattern as validation failure? Or ignore rule? Let's fail.
        return True
