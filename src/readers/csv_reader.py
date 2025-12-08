import pandas as pd 

class CSVRead:
    def __init__(self, config_object):
        self.config_object = config_object

    def read_customers_csv(self):
        try:
            df = pd.read_csv(self.config_object.config_data["sources"][0]["path"][0]["path"])
            return df
        except Exception as e:
            return None
    
    def read_sales_csv(self):
        try:
            df = pd.read_csv(self.config_object.config_data["sources"][1]["path"][0]["path"])
            return df
        except Exception as e:
            return None
        return df
    
    
