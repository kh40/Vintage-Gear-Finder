import json
import os
from typing import Dict, Any, List

class Config:
    """Configuration management for the vintage gear scraper"""
    
    def __init__(self):
        self.config_file = "config.json"
        self.default_config = {
            "max_year": 1979,
            "max_price_percentage": 0.60,
            "min_condition": "Good",
            "search_terms": [
                "vintage guitar fender",
                "vintage guitar gibson",
                "vintage guitar martin",
                "vintage amplifier fender",
                "vintage amplifier marshall",
                "vintage amplifier vox",
                "tube amplifier vintage"
            ],
            "ebay_api_key": "",
            "reverb_api_key": "",
            "google_sheets_id": "",
            "google_credentials_json": ""
        }
        self.config = self.load_config()
    
    def load_config(self) -> Dict[str, Any]:
        """Load configuration from file or environment variables"""
        config = self.default_config.copy()
        
        # Try to load from file
        if os.path.exists(self.config_file):
            try:
                with open(self.config_file, 'r') as f:
                    file_config = json.load(f)
                    config.update(file_config)
            except Exception as e:
                print(f"Error loading config file: {e}")
        
        # Override with environment variables if they exist
        env_mappings = {
            "EBAY_API_KEY": "ebay_api_key",
            "REVERB_API_KEY": "reverb_api_key",
            "GOOGLE_SHEETS_ID": "google_sheets_id",
            "GOOGLE_CREDENTIALS_JSON": "google_credentials_json",
            "MAX_YEAR": "max_year",
            "MAX_PRICE_PERCENTAGE": "max_price_percentage",
            "MIN_CONDITION": "min_condition"
        }
        
        for env_var, config_key in env_mappings.items():
            if os.getenv(env_var):
                value = os.getenv(env_var)
                if config_key in ["max_year"]:
                    config[config_key] = int(value)
                elif config_key in ["max_price_percentage"]:
                    config[config_key] = float(value)
                else:
                    config[config_key] = value
        
        return config
    
    def save_config(self):
        """Save current configuration to file"""
        try:
            with open(self.config_file, 'w') as f:
                json.dump(self.config, f, indent=2)
        except Exception as e:
            print(f"Error saving config: {e}")
    
    def get_current_config(self) -> Dict[str, Any]:
        """Get current configuration"""
        return self.config.copy()
    
    def update_config(self, new_config: Dict[str, Any]):
        """Update configuration with new values"""
        self.config.update(new_config)
        self.save_config()
    
    def get(self, key: str, default=None):
        """Get a specific configuration value"""
        return self.config.get(key, default)
    
    def validate_config(self) -> Dict[str, List[str]]:
        """Validate configuration and return any errors"""
        errors = {
            "warnings": [],
            "errors": []
        }
        
        # Check required fields
        if not self.config.get("search_terms"):
            errors["errors"].append("At least one search term is required")
        
        # Check API keys
        if not self.config.get("ebay_api_key"):
            errors["warnings"].append("eBay API key not set - will use HTML scraping (slower and less reliable)")
        
        if not self.config.get("reverb_api_key"):
            errors["warnings"].append("Reverb API key not set - will use HTML scraping (slower and less reliable)")
        
        # Check Google Sheets config
        if not self.config.get("google_sheets_id"):
            errors["warnings"].append("Google Sheets ID not set - results won't be saved to spreadsheet")
        elif not self.config.get("google_credentials_json"):
            errors["errors"].append("Google Sheets ID provided but credentials JSON is missing")
        
        # Validate ranges
        if self.config.get("max_year", 1979) > 1979:
            errors["warnings"].append("Max year is set above 1979 - this may not return vintage items")
        
        if self.config.get("max_year", 1979) < 1920:
            errors["warnings"].append("Max year is set very low - this may return very few results")
        
        if self.config.get("max_price_percentage", 0.6) > 1.0:
            errors["warnings"].append("Max price percentage is above 100% - this may return overpriced items")
        
        if self.config.get("max_price_percentage", 0.6) < 0.1:
            errors["warnings"].append("Max price percentage is very low - this may return very few results")
        
        return errors
