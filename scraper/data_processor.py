"""
Data transformation and cleaning utilities.
"""

from typing import Dict, List, Any
import re
from datetime import datetime
import logging

class DataProcessor:
    """Handles data cleaning and transformation."""

    def __init__(self, config: Dict = None):
        self.config = config or {}
        self.logger = logging.getLogger(__name__)

    def process(self, data: List[Dict], rules: Dict = None) -> List[Dict]:
        """Apply processing rules to dataset."""
        rules = rules or self.config.get('processing_rules', {})
        processed_data = []

        for item in data:
            try:
                processed_item = self._process_item(item.copy(), rules)
                processed_data.append(processed_item)
            except Exception as e:
                self.logger.warning(f"Failed to process item {item}: {e}")

        return processed_data

    def _process_item(self, item: Dict, rules: Dict) -> Dict:
        """Process individual data item according to rules."""
        # Field type conversions
        for field, type_info in rules.get('field_types', {}).items():
            if field in item and item[field] is not None:
                item[field] = self._convert_type(item[field], type_info)

        # Text cleaning
        for field, clean_rules in rules.get('text_cleaning', {}).items():
            if field in item and item[field] is not None:
                item[field] = self._clean_text(item[field], clean_rules)

        # Field transformations
        for field, transform in rules.get('transformations', {}).items():
            if field in item and item[field] is not None:
                item[field] = eval(transform, {'value': item[field], 'item': item})

        # Field validation
        for field, validation in rules.get('validations', {}).items():
            if field in item and not self._validate_field(item[field], validation):
                item[field] = None

        return item

    def _convert_type(self, value: Any, type_info: Dict) -> Any:
        """Convert field to specified type."""
        target_type = type_info.get('type')
        format_str = type_info.get('format')

        try:
            if target_type == 'datetime':
                return datetime.strptime(value, format_str) if format_str else datetime.fromisoformat(value)
            elif target_type == 'date':
                return datetime.strptime(value, format_str).date() if format_str else datetime.fromisoformat(value).date()
            elif target_type == 'int':
                return int(re.sub(r'[^\d-]', '', str(value)))
            elif target_type == 'float':
                return float(re.sub(r'[^\d.-]', '', str(value)))
            elif target_type == 'boolean':
                return str(value).lower() in ('true', '1', 'yes', 'y')
            elif target_type == 'string':
                return str(value).strip()
        except (ValueError, AttributeError) as e:
            self.logger.warning(f"Type conversion failed for value {value}: {e}")
            return None

        return value

    def _clean_text(self, text: str, rules: Dict) -> str:
        """Clean text according to rules."""
        if not isinstance(text, str):
            return text

        # Apply cleaning operations in specified order
        if rules.get('trim'):
            text = text.strip()
        if rules.get('lowercase'):
            text = text.lower()
        if rules.get('uppercase'):
            text = text.upper()
        if rules.get('remove_newlines'):
            text = text.replace('\n', ' ').replace('\r', ' ')
        if rules.get('remove_extra_spaces'):
            text = ' '.join(text.split())
        if rules.get('remove_special_chars'):
            text = re.sub(r'[^\w\s-]', '', text)
        if rules.get('regex_replace'):
            for pattern, replacement in rules['regex_replace'].items():
                text = re.sub(pattern, replacement, text)

        return text

    def _validate_field(self, value: Any, validation: Dict) -> bool:
        """Validate field against rules."""
        if validation.get('required') and value in (None, '', []):
            return False
        if 'min_length' in validation and len(str(value)) < validation['min_length']:
            return False
        if 'max_length' in validation and len(str(value)) > validation['max_length']:
            return False
        if 'pattern' in validation and not re.match(validation['pattern'], str(value)):
            return False
        return True
