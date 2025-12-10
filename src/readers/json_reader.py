import pandas as pd
import logging

logger = logging.getLogger(__name__)

class JSONRead:
    def __init__(self, config_object):
        self.config_object = config_object

    def read_customers_json(self):
        file_path = self.config_object.config_data["sources"][0]["path"][1]["path"]
        try:
            df = pd.read_json(file_path)
            logger.info(f"Successfully read customers JSON from: {file_path}")
            return df
        except Exception as e:
            return None

    def read_sales_json(self):
        file_path = self.config_object.config_data["sources"][1]["path"][1]["path"]
        try:
            df = pd.read_json(file_path)
            logger.info(f"Successfully read sales JSON from: {file_path}")
            return df
        except Exception as e:
            return None