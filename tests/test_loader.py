import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC_PATH = ROOT / "src"

if str(SRC_PATH) not in sys.path:
    sys.path.insert(0, str(SRC_PATH))

import loader
import config


def test_connection_uses_config(monkeypatch):
    config_obj = config.Config()
    db_conf = config_obj.config_data["database"]
    called_kwargs = {}
    dummy_conn = object()

    def fake_connect(**kwargs):
        called_kwargs.update(kwargs)
        return dummy_conn

    monkeypatch.setattr(loader.psycopg2, "connect", fake_connect)

    conn = loader.Dao(config_obj).connection()

    assert conn is dummy_conn
    assert called_kwargs == {
        "host": db_conf["host"],
        "port": db_conf["port"],
        "database": db_conf["db_name"],
        "user": db_conf["user"],
        "password": db_conf["password"],
    }
