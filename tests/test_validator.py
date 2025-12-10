import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC_PATH = ROOT / "src"

if str(SRC_PATH) not in sys.path:
    sys.path.insert(0, str(SRC_PATH))

import pandas as pd
from pandas.api.types import is_datetime64_any_dtype
import config
import validator


def test_customers_validator_missing_required_column():
    df = pd.DataFrame(
        {
            "customer_id": [1],
            "first_name": ["Ada"],
            "last_name": ["Lovelace"],
            "age": [36],
            "created_at": ["2024-01-01"],
        }
    )

    config_obj = config.Config()
    validator_obj = validator.Validate(config_obj)
    df_validated, df_rejects = validator_obj.df_customers_validator(df)

    assert df_validated.empty
    assert "reject_reason" in df_rejects
    assert df_rejects.loc[0, "reject_reason"] == "column email is missing"


def test_customers_validator_applies_rules_and_captures_rejects():
    df = pd.DataFrame(
        {
            "customer_id": [1, 2],
            "first_name": ["Alice", "Bob"],
            "last_name": ["Smith", "Jones"],
            "age": [30, -5],
            "email": ["alice@example.com", "bob@example.com"],
            "created_at": ["2024-01-01", "2024-01-02"],
        }
    )

    config_obj = config.Config()
    validator_obj = validator.Validate(config_obj)
    df_validated, df_rejects = validator_obj.df_customers_validator(df)

    assert len(df_validated) == 1
    assert df_validated.iloc[0]["customer_id"] == 1
    assert len(df_rejects) == 1
    assert df_rejects.iloc[0]["customer_id"] == 2
    assert df_rejects.iloc[0]["reject_reason"] == "not passing age > 0 rule"


def test_sales_validator_accepts_valid_rows():
    df = pd.DataFrame(
        {
            "sale_id": [10],
            "customer_id": [1],
            "amount": [100.50],
            "currency": ["USD"],
            "ts": ["2024-01-01T00:00:00"],
        }
    )

    config_obj = config.Config()
    validator_obj = validator.Validate(config_obj)
    df_validated, df_rejects = validator_obj.df_sales_validator(df)

    assert len(df_validated) == 1
    assert df_rejects.empty
    assert is_datetime64_any_dtype(df_validated["ts"])
