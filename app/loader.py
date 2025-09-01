import pandas as pd
import config
from elasticsearch import Elasticsearch
class Loader:

    def __init__(self,file_path=config.csv_path,weapon_file=config.weapon_path):
        self.csv_path = file_path
        self.weapon_file=weapon_file


    def convert_csv_to_json(self):
        df = pd.read_csv(self.csv_path)

        json_data = df.to_dict(orient='records')

        return json_data

    def list_weapon_file(self):
        try:
            with open(config.weapon_path, "r") as f:
                weapons_set = {line.strip().lower() for line in f if line.strip()}
                if not weapons_set:
                    print("Weapon list file is empty")
                return weapons_set
        except FileNotFoundError:
            print(f"Weapon list file not found: {config.weapon_path}")
            return set()