import pandas as pd
from pandas.api.types import is_float_dtype, is_integer_dtype


def test_customers_clean_converts_numeric_and_string_columns(cleaner_obj):
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

    cleaned = cleaner_obj.customers_clean(df)

    assert is_integer_dtype(cleaned["customer_id"])
    assert is_integer_dtype(cleaned["age"])
    assert cleaned["first_name"].iloc[0] == "Ada"
    assert cleaned["email"].iloc[0] == "ada@example.com"


def test_sales_clean_converts_numeric_columns(cleaner_obj):
    df = pd.DataFrame(
        {
            "sale_id": ["5"],
            "customer_id": ["1"],
            "amount": ["12.34"],
            "currency": ["USD"],
            "ts": ["2024-01-01T00:00:00Z"],
        }
    )

    cleaned = cleaner_obj.sales_clean(df)

    assert is_integer_dtype(cleaned["sale_id"])
    assert is_integer_dtype(cleaned["customer_id"])
    assert is_float_dtype(cleaned["amount"])
    assert cleaned["currency"].iloc[0] == "USD"
