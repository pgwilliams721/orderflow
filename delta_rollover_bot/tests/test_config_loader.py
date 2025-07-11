import pytest
import yaml
from pathlib import Path
from unittest.mock import patch, mock_open

from delta_rollover_bot.config_loader import Config, DEFAULT_CONFIG_PATH, TRADEOVATE_CREDENTIALS_PATH

@pytest.fixture
def mock_default_config_content(tmp_path):
    content = {
        "api_keys": {
            "bookmap_api_key": "test_bookmap_key",
            "spotgamma_api_token": "test_spotgamma_token"
        },
        "trading": {
            "instrument": "MNQ",
            "quantity": 1
        },
        "instruments": {
            "MNQ": {"tick_size": 0.25}
        }
    }
    return content

@pytest.fixture
def mock_tradeovate_config_content():
    content = {
        "tradeovate": {
            "tradeovate_user": "test_user",
            "tradeovate_pass": "test_pass",
            "tradeovate_app_id": "test_app_id",
            "tradeovate_cid": 12345
        }
    }
    return content

def test_load_valid_primary_config(mock_default_config_content, tmp_path):
    primary_config_file = tmp_path / "config.yaml"
    with open(primary_config_file, 'w') as f:
        yaml.dump(mock_default_config_content, f)
    with patch('delta_rollover_bot.config_loader.TRADEOVATE_CREDENTIALS_PATH', tmp_path / "non_existent_tradeovate.yaml"):
        cfg = Config(config_path=primary_config_file)
    assert cfg.get('api_keys.bookmap_api_key') == "test_bookmap_key"
    assert cfg.get('trading.instrument') == "MNQ"
    assert cfg.get('instruments.MNQ.tick_size') == 0.25

def test_load_valid_primary_and_tradeovate_config(mock_default_config_content, mock_tradeovate_config_content, tmp_path):
    primary_config_file = tmp_path / "config.yaml"
    tradeovate_config_file = tmp_path / "tradeovate_credentials.yaml"
    with open(primary_config_file, 'w') as f:
        yaml.dump(mock_default_config_content, f)
    with open(tradeovate_config_file, 'w') as f:
        yaml.dump(mock_tradeovate_config_content, f)
    cfg = Config(config_path=primary_config_file, tradeovate_creds_path=tradeovate_config_file)
    assert cfg.get('api_keys.bookmap_api_key') == "test_bookmap_key"
    assert cfg.get('api_keys.tradeovate_user') == "test_user"
    assert cfg.get('api_keys.tradeovate_cid') == 12345
    assert cfg.get('trading.instrument') == "MNQ"

def test_load_primary_config_tradeovate_missing_optional(mock_default_config_content, tmp_path):
    primary_config_file = tmp_path / "config.yaml"
    with open(primary_config_file, 'w') as f:
        yaml.dump(mock_default_config_content, f)
    missing_tradeovate_file = tmp_path / "missing_tradeovate.yaml"
    assert not missing_tradeovate_file.exists()
    cfg = Config(config_path=primary_config_file, tradeovate_creds_path=missing_tradeovate_file)
    assert cfg.get('api_keys.bookmap_api_key') == "test_bookmap_key"
    assert cfg.get('api_keys.tradeovate_user') is None

def test_load_missing_primary_config_raises_error(tmp_path):
    missing_primary_file = tmp_path / "non_existent_config.yaml"
    with pytest.raises(FileNotFoundError):
        Config(config_path=missing_primary_file)

def test_load_malformed_yaml_raises_error(tmp_path):
    malformed_config_file = tmp_path / "malformed.yaml"
    with open(malformed_config_file, 'w') as f:
        f.write("api_keys: \n  bookmap_api_key: test_key\n trading: - invalid_yaml")
    with pytest.raises(yaml.YAMLError):
        Config(config_path=malformed_config_file)

def test_get_nested_values(mock_default_config_content, tmp_path):
    primary_config_file = tmp_path / "config.yaml"
    with open(primary_config_file, 'w') as f:
        yaml.dump(mock_default_config_content, f)
    with patch('delta_rollover_bot.config_loader.TRADEOVATE_CREDENTIALS_PATH', tmp_path / "non_existent_tradeovate.yaml"):
         cfg = Config(config_path=primary_config_file)
    assert cfg.get('instruments.MNQ.tick_size') == 0.25
    assert cfg.get('api_keys.spotgamma_api_token') == "test_spotgamma_token"

def test_get_missing_key_returns_default(mock_default_config_content, tmp_path):
    primary_config_file = tmp_path / "config.yaml"
    with open(primary_config_file, 'w') as f:
        yaml.dump(mock_default_config_content, f)
    with patch('delta_rollover_bot.config_loader.TRADEOVATE_CREDENTIALS_PATH', tmp_path / "non_existent_tradeovate.yaml"):
        cfg = Config(config_path=primary_config_file)
    assert cfg.get('trading.non_existent_key') is None
    assert cfg.get('trading.non_existent_key', 'default_val') == 'default_val'
    assert cfg.get('non_existent_top_level.nested', 'another_default') == 'another_default'

def test_getitem_and_contains(mock_default_config_content, tmp_path):
    primary_config_file = tmp_path / "config.yaml"
    with open(primary_config_file, 'w') as f:
        yaml.dump(mock_default_config_content, f)
    with patch('delta_rollover_bot.config_loader.TRADEOVATE_CREDENTIALS_PATH', tmp_path / "non_existent_tradeovate.yaml"):
        cfg = Config(config_path=primary_config_file)
    assert cfg['trading.instrument'] == "MNQ"
    assert ('api_keys.bookmap_api_key' in cfg) is True
    assert ('trading.fake_key' in cfg) is False
    assert cfg['trading.fake_key'] is None # Correctly reflects behavior that getitem returns None for missing key

def test_load_empty_yaml_file(tmp_path):
    empty_config_file = tmp_path / "empty.yaml"
    with open(empty_config_file, 'w') as f:
        f.write("")
    empty_tradeovate_file = tmp_path / "empty_tradeovate.yaml"
    with open(empty_tradeovate_file, 'w') as f:
        f.write("")
    cfg = Config(config_path=empty_config_file, tradeovate_creds_path=empty_tradeovate_file)
    assert cfg.settings == {}
    assert cfg.get('some.key') is None

def test_load_yaml_with_only_comments(tmp_path):
    comments_config_file = tmp_path / "comments.yaml"
    with open(comments_config_file, 'w') as f:
        f.write("# This is a comment\n# Another comment\n")
    with patch('delta_rollover_bot.config_loader.TRADEOVATE_CREDENTIALS_PATH', tmp_path / "non_existent_tradeovate.yaml"):
        cfg = Config(config_path=comments_config_file)
    assert cfg.settings == {}
    assert cfg.get('some.key') is None

def test_missing_main_config_with_tradeovate_present(tmp_path):
    """
    Tests that if the main config file is missing, FileNotFoundError is raised,
    even if an optional Tradeovate config is present.
    """
    mock_tv_content = {
        "tradeovate": {
            "tradeovate_user": "test_user",
            "tradeovate_pass": "test_pass"
        }
    }
    mock_tv_path = tmp_path / "tv_creds_only.yaml"
    with open(mock_tv_path, 'w') as f:
        yaml.dump(mock_tv_content, f)
    missing_main_config_path = tmp_path / "main_config_missing.yaml"
    assert not missing_main_config_path.exists()
    with pytest.raises(FileNotFoundError):
        Config(config_path=missing_main_config_path, tradeovate_creds_path=mock_tv_path)

def test_load_tradeovate_creds_when_api_keys_section_missing_in_primary(mock_tradeovate_config_content, tmp_path):
    primary_content_no_apikeys = {
        "trading": {"instrument": "ES"}
    }
    primary_config_file = tmp_path / "config_no_apikeys.yaml"
    tradeovate_config_file = tmp_path / "tradeovate_credentials.yaml"
    with open(primary_config_file, 'w') as f:
        yaml.dump(primary_content_no_apikeys, f)
    with open(tradeovate_config_file, 'w') as f:
        yaml.dump(mock_tradeovate_config_content, f)
    cfg = Config(config_path=primary_config_file, tradeovate_creds_path=tradeovate_config_file)
    assert cfg.get('api_keys.tradeovate_user') == "test_user"
    assert cfg.get('trading.instrument') == "ES"
    assert "bookmap_api_key" not in cfg.get("api_keys", {})
