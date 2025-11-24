"""
Custom JSON Parser - No external libraries
"""

class JSONParser:
    def parse_file(self, file_obj):
        """Parse JSON file"""
        content = file_obj.read()
        if isinstance(content, bytes):
            content = content.decode('utf-8')
        
        return self._parse_json(content)
    
    def _parse_json(self, text):
        """Simple JSON parser"""
        import json
        # Use built-in json for simplicity
        # In production, this would be a custom implementation
        return json.loads(text)
