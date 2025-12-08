import pandas as pd
from config import Config

class Validate:
    def __init__(self, config_object):
        self.config_object = config_object
    
    def df_customers_validator(self, df):
        columns_yaml = self.config_object.config_data["sources"][0]["schema"]
        for column_name in columns_yaml.keys():
            if column_name not in df.columns:
                df["reject_reason"]=f'column {column_name} is missing'
                return (pd.DataFrame(),df)
            if columns_yaml[column_name] in ("float","int"):
                df[column_name]=pd.to_numeric(df[column_name], errors="coerce")
            elif columns_yaml[column_name] == "datetime":
                df[column_name] = pd.to_datetime(df[column_name], errors="coerce")
            
        rules_yaml = self.config_object.config_data["sources"][0]["rules"]
        df_filtered = df
        df_rejects = pd.DataFrame()

        for rule in rules_yaml:
            rule_violators = df_filtered.query(f"not ({rule["rule"]})").copy()  
            if not rule_violators.empty:
                rule_violators["reject_reason"] = f"not passing {rule["rule"]} rule"
                df_rejects = pd.concat([df_rejects, rule_violators])
            df_filtered = df_filtered.query(rule["rule"])
        
        return (df_filtered, df_rejects)
            

    def df_sales_validator(self, df):
        columns_yaml = self.config_object.config_data["sources"][1]["schema"]
        for column_name in columns_yaml.keys():
            if column_name not in df.columns:
                df["reject_reason"]=f'column {column_name} is missing'
                return (0,df)
            if columns_yaml[column_name] in ("float","int"):
                df[column_name] = pd.to_numeric(df[column_name], errors="coerce")
            elif columns_yaml[column_name] == "datetime":
                df[column_name] = pd.to_datetime(df[column_name], errors="coerce")

    
        rules_yaml = self.config_object.config_data["sources"][1]["rules"]
        df_filtered = df
        df_rejects = pd.DataFrame()

        for rule in rules_yaml:

            rule_violators = df_filtered.query(f"not ({rule["rule"]})").copy()
            
            if not rule_violators.empty:
                rule_violators['reject_reason'] = f"not passing {rule["rule"]} rule"
                df_rejects = pd.concat([df_rejects, rule_violators])
            df_filtered = df_filtered.query(rule["rule"])
        
        return (df_filtered, df_rejects)
