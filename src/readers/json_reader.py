import pandas as pd
import config

class JSONRead:
    def __init__(self, config_object):
        self.config_object = config_object

    def read_customers_json(self):
        try:
            df = pd.read_json(self.config_object.config_data["sources"][0]["path"][1]["path"])
            return df
        except Exception as e:
            return None
    
    def read_sales_json(self):
        try:
            df = pd.read_json(self.config_object.config_data["sources"][1]["path"][1]["path"])
            return df
        except Exception as e:
            return None
    