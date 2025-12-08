import yaml

class Config:
    def __init__(self):
        with open("config/etl.yml", 'r') as f:
            self.config_data = yaml.safe_load(f)
