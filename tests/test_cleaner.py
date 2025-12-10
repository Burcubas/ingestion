import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC_PATH = ROOT / "src"

if str(SRC_PATH) not in sys.path:
    sys.path.insert(0, str(SRC_PATH))

import cleaner
import config
import pandas as pd
from pandas.api.types import is_float_dtype, is_integer_dtype


def test_customers_clean_converts_numeric_and_string_columns():
    df = pd.DataFrame(
        {
            "customer_id": ["1"],
            "first_name": ["Ada"],
            "last_name": ["Lovelace"],
            "age": ["36"],
            "email": ["ada@example.com"],
            "created_at": ["2024-01-01"],
        }
    )

    config_obj = config.Config()
    cleaner_obj = cleaner.Clean(config_obj)
    cleaned = cleaner_obj.customers_clean(df)

    assert is_integer_dtype(cleaned["customer_id"])
    assert is_integer_dtype(cleaned["age"])
    assert cleaned["first_name"].iloc[0] == "Ada"
    assert cleaned["email"].iloc[0] == "ada@example.com"


def test_sales_clean_converts_numeric_columns():
    df = pd.DataFrame(
        {
            "sale_id": ["5"],
            "customer_id": ["1"],
            "amount": ["12.34"],
            "currency": ["USD"],
            "ts": ["2024-01-01T00:00:00Z"],
        }
    )

    config_obj = config.Config()
    cleaner_obj = cleaner.Clean(config_obj)
    cleaned = cleaner_obj.sales_clean(df)

    assert is_integer_dtype(cleaned["sale_id"])
    assert is_integer_dtype(cleaned["customer_id"])
    assert is_float_dtype(cleaned["amount"])
    assert cleaned["currency"].iloc[0] == "USD"
