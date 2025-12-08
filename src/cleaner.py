import pandas as pd
from config import Config

class Clean:
    def __init__(self, config_object):
        self.config_object = config_object

    def customers_clean(self, df):
        columns_yaml = self.config_object.config_data["sources"][0]["schema"]
        for column_name in columns_yaml.keys():
            if columns_yaml[column_name] == "int":
                df[column_name]= df[column_name].astype(int)
            elif columns_yaml[column_name] == "float":
                df[column_name] = df[column_name].astype(float)
            elif columns_yaml[column_name] == "str":
                df[column_name] = df[column_name].astype(str)
        return df
    def sales_clean(self, df):
        columns_yaml = self.config_object.config_data["sources"][1]["schema"]
        for column_name in columns_yaml.keys():
            if columns_yaml[column_name] == "int":
                df[column_name]= df[column_name].astype(int)
            elif columns_yaml[column_name] == "float":
                df[column_name] = df[column_name].astype(float)
            elif columns_yaml[column_name] == "str":
                df[column_name] = df[column_name].astype(str)
        return df      


