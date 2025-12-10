import pandas as pd
import logging

logger = logging.getLogger(__name__)

class CSVRead:
    def __init__(self, config_object):
        self.config_object = config_object

    def read_customers_csv(self):
        file_path = self.config_object.config_data["sources"][0]["path"][0]["path"]
        try:
            df = pd.read_csv(file_path)
            logger.info(f"Successfully read customers CSV from: {file_path}")
            return df
        except Exception as e:
            return None

    def read_sales_csv(self):
        file_path = self.config_object.config_data["sources"][1]["path"][0]["path"]
        try:
            df = pd.read_csv(file_path)
            logger.info(f"Successfully read sales CSV from: {file_path}")
            return df
        except Exception as e:
            return None
