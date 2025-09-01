import pandas as pd
from elasticsearch import Elasticsearch
import config



class LoadToElasticSearch:
    def __init__(self, path):
        self.df = pd.read_csv(path)
        self.data_dict = self.df.to_dict(orient="records")
        self.es = Elasticsearch(config.elastic_conn)
        self.index_name = config.index_name




    def create_index(self):
        if self.es.indices.exists(index=self.index_name):
            self.es.indices.delete(index=self.index_name)
        self.es.indices.create(index=self.index_name, body={"mappings" : config.mapping})
        print(self.es.indices.get_mapping(index=self.index_name))


    def insert_data(self):
        for i, record in enumerate(self.data_dict):
            self.es.index(index=self.index_name, document=record)
        print(f"Loaded {len(self.data_dict)} documents into '{self.index_name}'")



l = LoadToElasticSearch(config.data_path)
l.create_index()
l.insert_data()

