import psycopg2 


class Dao:
    def __init__(self, config_object):
        self.config_object = config_object

    def connection(self):
        db = self.config_object.config_data["database"]
        
        conn = psycopg2.connect(
            host = db["host"],
            port = db["port"],
            database = db["db_name"], 
            user = db["user"],
            password = db["password"]
        )
        return conn
    def creates_tables(self):
        conn = self.connection()
        cursor = conn.cursor()

        try:
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS stg_customers (
                customer_id   BIGINT PRIMARY KEY,
                first_name    TEXT NOT NULL,
                last_name     TEXT NOT NULL,
                email         TEXT,
                age           INT, 
                created_at    TIMESTAMP,
                _loaded_at    TIMESTAMP NOT NULL DEFAULT NOW()
            );

            CREATE TABLE IF NOT EXISTS stg_sales (
                sale_id       BIGINT PRIMARY KEY,
                customer_id   BIGINT NOT NULL,
                amount        NUMERIC(12,2) NOT NULL CHECK (amount >= 0),
                currency      TEXT NOT NULL,
                ts            TIMESTAMP NOT NULL,
                _loaded_at    TIMESTAMP NOT NULL DEFAULT NOW()
            );

            CREATE TABLE IF NOT EXISTS stg_rejects (
                source_name   TEXT NOT NULL,
                raw_payload   TEXT NOT NULL,
                reason        TEXT NOT NULL,
                rejected_at   TIMESTAMP NOT NULL DEFAULT NOW()
            );
            """)
            conn.commit()
        finally:
            cursor.close()
            conn.close()
    def load_customers(self, df):
        conn = self.connection()
        cursor = conn.cursor()

        sql = """
            INSERT INTO stg_customers (customer_id, first_name, last_name, email, age, created_at)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (customer_id) DO UPDATE
            SET first_name = EXCLUDED.first_name,
                last_name  = EXCLUDED.last_name,
                email      = EXCLUDED.email,
                age        = EXCLUDED.age,
                created_at = EXCLUDED.created_at;
        """

        try:
            for _, row in df.iterrows():
                cursor.execute(sql, (
                    row['customer_id'],
                    row['first_name'],
                    row['last_name'],
                    row['email'],
                    row['age'],
                    row['created_at']
                ))

            conn.commit()

        finally:
            cursor.close()
            conn.close()

    def load_sales(self, df):
        conn = self.connection()
        cursor = conn.cursor()

        sql = """
            INSERT INTO stg_sales (sale_id, customer_id, amount, currency, ts)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (sale_id) DO UPDATE
            SET customer_id = EXCLUDED.customer_id,
                amount      = EXCLUDED.amount,
                currency    = EXCLUDED.currency,
                ts          = EXCLUDED.ts;
        """
        try:
            for _, row in df.iterrows():
                cursor.execute(sql, (
                    row['sale_id'],
                    row['customer_id'],
                    row['amount'],
                    row['currency'],
                    row['ts']
                ))
            conn.commit()

        finally:
            cursor.close()
            conn.close()
        