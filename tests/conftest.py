import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
SRC_PATH = ROOT / "src"

if str(SRC_PATH) not in sys.path:
    sys.path.insert(0, str(SRC_PATH))

import config
import cleaner
import validator
import loader


@pytest.fixture
def config_obj():
    return config.Config()


@pytest.fixture
def validator_obj(config_obj):
    return validator.Validate(config_obj)


@pytest.fixture
def cleaner_obj(config_obj):
    return cleaner.Clean(config_obj)


@pytest.fixture
def dao_obj(config_obj):
    return loader.Dao(config_obj)
