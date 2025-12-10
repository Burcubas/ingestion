import psycopg2
import logging

logger = logging.getLogger(__name__)

class Dao:
    def __init__(self, config_object):
        self.config_object = config_object

    def connection(self):
        try:
            db = self.config_object.config_data["database"]

            logger.info("Attempting to connect to PostgreSQL database...")
            conn = psycopg2.connect(
                host=db["host"],
                port=db["port"],
                database=db["db_name"],
                user=db["user"],
                password=db["password"]
            )
            logger.info("Successfully connected to PostgreSQL database.")
            return conn

        except Exception as e:
            logger.error(f"Database connection failed")
            return None


    def creates_tables(self):
        logger.info("Starting table creation process...")

        conn = self.connection()
        if conn is None:
            logger.error("Table creation aborted: No database connection.")
            return
        
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
            logger.info("Successfully created all staging tables.")
            return True

        except Exception as e:
            logger.error(f"Error creating tables: {e}")

        finally:
            cursor.close()
            conn.close()
            logger.info("Closed DB connection after table creation.")


    def load_customers(self, df):
        logger.info("Starting customer load process...")

        conn = self.connection()
        if conn is None:
            logger.error("Customer load aborted: No database connection.")
            return
        
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
                    row["customer_id"],
                    row["first_name"],
                    row["last_name"],
                    row["email"],
                    row["age"],
                    row["created_at"]
                ))
            
            conn.commit()
            logger.info("Successfully loaded customers data.")
            return True
        except Exception as e:
            logger.error(f"Error loading customers data: {e}")

        finally:
            cursor.close()
            conn.close()
            logger.info("Closed DB connection after loading customers.")


    def load_sales(self, df):
        logger.info("Starting sales load process...")

        conn = self.connection()
        if conn is None:
            logger.error("Sales load aborted: No database connection.")
            return

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
                    row["sale_id"],
                    row["customer_id"],
                    row["amount"],
                    row["currency"],
                    row["ts"]
                ))

            conn.commit()
            logger.info("Successfully loaded sales data.")
            return True
        except Exception as e:
            logger.error(f"Error loading sales data: {e}")

        finally:
            cursor.close()
            conn.close()
            logger.info("Closed DB connection after loading sales.")


    def load_rejects(self, source_name, df_rejects):
        logger.info("Starting rejects load process...")

        conn = self.connection()
        if conn is None:
            logger.error("Rejects load aborted: No database connection.")
            return

        cursor = conn.cursor()

        sql = """
            INSERT INTO stg_rejects (source_name, raw_payload, reason)
            VALUES (%s, %s, %s);
        """

        try:
            for _, row in df_rejects.iterrows():
                reason = row.get("reject_reason", "unknown")
                raw_payload = ",".join(
                    map(str, row.drop(labels=["reject_reason"], errors="ignore").values)
                )
                cursor.execute(
                    sql,
                    (
                        source_name,
                        raw_payload,
                        reason,
                    ),
                )

            conn.commit()
            logger.info("Successfully loaded rejects data.")

        except Exception as e:
            logger.error(f"Error loading rejects data: {e}")

        finally:
            cursor.close()
            conn.close()
            logger.info("Closed DB connection after loading rejects.")
