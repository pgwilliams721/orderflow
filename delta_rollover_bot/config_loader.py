import yaml
from pathlib import Path
import os

# Base directory of the package
# Assuming this file is delta_rollover_bot/config_loader.py
# then PACKAGE_ROOT is the 'delta_rollover_bot' directory.
# For resolving paths relative to the project root, we might need to adjust.
# For now, config is expected to be in delta_rollover_bot/config/config.yaml
PACKAGE_ROOT = Path(__file__).resolve().parent
DEFAULT_CONFIG_PATH = PACKAGE_ROOT / "config" / "config.yaml"
TRADEOVATE_CREDENTIALS_PATH = PACKAGE_ROOT / "config" / "tradeovate_credentials.yaml" # Example

class Config:
    """
    Configuration class to load and access settings from YAML files.
    """
    def __init__(self, config_path=None, tradeovate_creds_path=None):
        if config_path is None:
            config_path = DEFAULT_CONFIG_PATH
        if tradeovate_creds_path is None:
            tradeovate_creds_path = TRADEOVATE_CREDENTIALS_PATH

        self.settings = self._load_yaml(config_path)
        self.tradeovate_credentials = self._load_yaml(tradeovate_creds_path, optional=True)

        # Merge Tradeovate credentials into main settings if they exist
        if self.tradeovate_credentials and 'api_keys' in self.settings:
            self.settings['api_keys'].update(self.tradeovate_credentials.get('tradeovate', {}))
        elif self.tradeovate_credentials:
            # if api_keys section doesn't exist, create it
            self.settings['api_keys'] = self.tradeovate_credentials.get('tradeovate', {})


    def _load_yaml(self, path: Path, optional: bool = False):
        """Loads a YAML file."""
        try:
            with open(path, 'r') as f:
                data = yaml.safe_load(f)
            return data if data else {}
        except FileNotFoundError:
            if optional:
                # print(f"Optional config file not found: {path}")
                return {}
            else:
                # print(f"Error: Configuration file not found at {path}")
                raise
        except yaml.YAMLError as e:
            # print(f"Error parsing YAML file {path}: {e}")
            raise

    def get(self, key, default=None):
        """
        Access a configuration value using dot notation for nested keys.
        Example: config.get('trading.instrument')
        """
        keys = key.split('.')
        value = self.settings
        try:
            for k in keys:
                if isinstance(value, dict):
                    value = value[k]
                else: # pragma: no cover
                    # This case should ideally not be reached if keys are correct
                    # and config structure is as expected.
                    # Added for robustness, but hard to test directly without malformed config.
                    return default
            return value
        except KeyError: # pragma: no cover
            return default
        except TypeError: # pragma: no cover
            # Handles cases where an intermediate key is not a dict
            return default

    def __getitem__(self, key):
        """Allow dictionary-style access."""
        return self.get(key)

    def __contains__(self, key):
        """Allow 'in' operator."""
        return self.get(key) is not None

# Global config instance (can be imported by other modules)
# Load default config when this module is imported.
try:
    config = Config()
except Exception as e:
    # print(f"Failed to load configuration during module import: {e}")
    # This allows the module to be imported even if config files are missing,
    # but accessing config.get() will likely fail or return defaults.
    # Specific error handling should be done by the application parts that need the config.
    config = None # Or a dummy/empty Config object

if __name__ == '__main__': # pragma: no cover
    # Example usage and basic test
    print(f"Looking for config at: {DEFAULT_CONFIG_PATH}") # pragma: no cover
    if not DEFAULT_CONFIG_PATH.exists(): # pragma: no cover
        print(f"ERROR: Default config file {DEFAULT_CONFIG_PATH} does not exist.") # pragma: no cover
        print("Please ensure 'config/config.yaml' is present in the 'delta_rollover_bot' package directory.") # pragma: no cover
    else: # pragma: no cover
        if config: # pragma: no cover
            print("Configuration loaded successfully.") # pragma: no cover
            print(f"Instrument: {config.get('trading.instrument')}") # pragma: no cover
            print(f"Bookmap API Key: {config.get('api_keys.bookmap_api_key')}") # pragma: no cover
            print(f"SpotGamma Token: {config.get('api_keys.spotgamma_api_token')}") # pragma: no cover
            print(f"Daily Loss Limit: {config.get('risk_management.daily_loss_limit_usd')}") # pragma: no cover
            print(f"Tick Size for MNQ: {config.get('instruments.MNQ.tick_size')}") # pragma: no cover

            # Example of accessing a non-existent key
            print(f"Non-existent key: {config.get('non_existent.key', 'default_value')}") # pragma: no cover

            # Test Tradeovate credentials loading (if file exists)
            if TRADEOVATE_CREDENTIALS_PATH.exists(): # pragma: no cover
                print(f"Tradeovate User (from merged creds): {config.get('api_keys.tradeovate_user')}") # pragma: no cover
            else: # pragma: no cover
                print(f"Tradeovate credentials file not found at: {TRADEOVATE_CREDENTIALS_PATH}") # pragma: no cover
                print("This is optional. Create 'delta_rollover_bot/config/tradeovate_credentials.yaml' if needed:") # pragma: no cover
                print("""\
# delta_rollover_bot/config/tradeovate_credentials.yaml
tradeovate:
  tradeovate_user: "YOUR_TRADEOVATE_USER"
  tradeovate_pass: "YOUR_TRADEOVATE_PASS"
  tradeovate_app_id: "YOUR_TRADEOVATE_APP_ID"
  tradeovate_cid: "YOUR_TRADEOVATE_CID" # Integer, not string
  tradeovate_env: "live" # or "demo"
                """) # pragma: no cover
        else: # pragma: no cover
            print("Configuration object 'config' is None. Loading must have failed.") # pragma: no cover
