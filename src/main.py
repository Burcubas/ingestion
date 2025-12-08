import os
import time
from datetime import datetime
import config
from readers import csv_reader, json_reader
import validator
import cleaner
import loader



config_obj = config.Config()
csv_reader_object = csv_reader.CSVRead(config_obj) 
json_reader_object = json_reader.JSONRead(config_obj)
validator_obj = validator.Validate(config_obj)
cleaner_obj = cleaner.Clean(config_obj)
DAO_object = loader.Dao(config_obj)
DAO_object.creates_tables()

def archive_file(source_path):
    if not os.path.exists(source_path):
        return
    
    archive_dir = "data/archive"
    os.makedirs(archive_dir, exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    file_name = os.path.basename(source_path)
    file_base, file_ext = os.path.splitext(file_name)
    destination_path = os.path.join(archive_dir, f"{file_base}_{timestamp}{file_ext}")
    os.rename(source_path, destination_path)

while True:

    df_raw = csv_reader_object.read_customers_csv()
    if df_raw is not None:
        df_validated, df_rejects = validator_obj.df_customers_validator(df_raw)
        df_cleaned = cleaner_obj.customers_clean(df_validated)
        DAO_object.load_customers(df_cleaned)
        archive_file(config_obj.config_data["sources"][0]["path"][0]["path"])

    df_raw = json_reader_object.read_customers_json()
    if df_raw is not None:
        df_validated, df_rejects = validator_obj.df_customers_validator(df_raw)
        df_cleaned = cleaner_obj.customers_clean(df_validated)
        DAO_object.load_customers(df_cleaned)
        archive_file(config_obj.config_data["sources"][0]["path"][1]["path"])

    df_raw = csv_reader_object.read_sales_csv()
    if df_raw is not None:
        df_validated, df_rejects = validator_obj.df_sales_validator(df_raw)
        df_cleaned = cleaner_obj.sales_clean(df_validated)
        DAO_object.load_sales(df_cleaned)
        archive_file(config_obj.config_data["sources"][1]["path"][0]["path"])
    df_raw = json_reader_object.read_sales_json()
    if df_raw is not None:
        df_validated, df_rejects = validator_obj.df_sales_validator(df_raw)
        df_cleaned = cleaner_obj.sales_clean(df_validated)
        DAO_object.load_sales(df_cleaned)
        archive_file(config_obj.config_data["sources"][1]["path"][1]["path"])

    time.sleep(30)
