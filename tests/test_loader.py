import loader


def test_connection_uses_config(monkeypatch, dao_obj, config_obj):
    db_conf = config_obj.config_data["database"]
    called_kwargs = {}
    dummy_conn = object()

    def fake_connect(**kwargs):
        called_kwargs.update(kwargs)
        return dummy_conn

    monkeypatch.setattr(loader.psycopg2, "connect", fake_connect)

    conn = dao_obj.connection()

    assert conn is dummy_conn
    assert called_kwargs == {
        "host": db_conf["host"],
        "port": db_conf["port"],
        "database": db_conf["db_name"],
        "user": db_conf["user"],
        "password": db_conf["password"],
    }
