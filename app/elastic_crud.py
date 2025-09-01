from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
from loader import Loader
import config
from sentiment_recognition import SentimentRecognition


class ElasticCrud:

    def __init__(self,data,mapping,index_name,weapons_list):
        self.es = Elasticsearch(config.elastic_conn)
        self.mapp = mapping
        self.index_name = index_name
        self.data=data
        self.weapons_list=weapons_list


    def create_index(self):

        if self.es.indices.exists(index=self.index_name):
            self.es.indices.delete(index=self.index_name)
        self.es.indices.create(index=self.index_name, body={"mappings": self.mapp})
        print(self.es.indices.get_mapping(index=self.index_name))

    def create_data(self):
        for i, record in enumerate(self.data):
            self.es.index(index=self.index_name, document=record)

    def update_mapp(self,filed,kind):
        self.es.indices.put_mapping(
            index=self.index_name,
            body={"properties": {filed: {"type": kind}}}
        )

    def add_sentiment(self):
        # Adds a 'sentiment' field to all documents based on their text content
        self.update_mapp("sentiment","keyword")
        print(self.es.indices.get_mapping(index=self.index_name))

        res = self.es.search(
            index=self.index_name,
            scroll="2m",
            body={
                "query": {"match_all": {}},
                "size": 1000
            }
        )

        sid = res['_scroll_id']
        try:
            while res['hits']['hits']:
                for doc in res['hits']['hits']:
                    s = SentimentRecognition.sentiment_analyzer(doc['_source'].get('text', ''))
                    if s:
                        try:
                            self.es.update(index=self.index_name, id=doc['_id'], body={"doc": {"sentiment": s}})

                        except:
                            print(f"Failed to update sentiment for document {doc['_id']}")
                res = self.es.scroll(scroll_id=sid, scroll='2m')
                sid = res['_scroll_id']
        finally:
            self.es.clear_scroll(scroll_id=sid)

    from elasticsearch.helpers import bulk

    def add_weapon_list(self):
        """Updates documents with a 'weapon_list' field containing detected weapons."""
        self.update_mapp("weapon_list", "keyword")

        res = self.es.search(
            index=self.index_name,
            scroll="2m",
            body={"query": {"match_all": {}}, "size": 500}
        )
        sid = res['_scroll_id']
        actions = []
        total_updated = 0

        try:
            while res['hits']['hits']:
                for doc in res['hits']['hits']:
                    text = doc['_source'].get('text', '').lower()
                    matched_weapons = [w for w in self.weapons_list if w.lower() in text]

                    if matched_weapons:
                        actions.append({
                            "_op_type": "update",
                            "_index": self.index_name,
                            "_id": doc['_id'],
                            "doc": {"weapon_list": matched_weapons}
                        })
                        print(f"Will update document {doc['_id']} with weapons: {matched_weapons}")

                if actions:
                    bulk(self.es, actions)
                    total_updated += len(actions)
                    print(f"Bulk updated {len(actions)} documents in this batch")
                    actions = []

                res = self.es.scroll(scroll_id=sid, scroll='2m')
                sid = res['_scroll_id']

        finally:
            self.es.clear_scroll(scroll_id=sid)

        print(f"Finished updating weapon_list. Total documents updated: {total_updated}")

    #Deletes documents that are not anti-Semitic enough
    def delete_by_term(self):
        """
        Deletes all documents that have:
        - Antisemitic = 0
        - sentiment = "negative"
        - weapon_list is missing OR empty
        """
        query = {
            "query": {
                "bool": {
                    "must": [
                        {"term": {"Antisemitic": 0}},
                        {"term": {"sentiment": "negative"}}
                    ],
                    "must_not": [
                        {"exists": {"field": "weapon_list"}}
                    ]
                }
            }
        }

        res = self.es.delete_by_query(index=self.index_name, body=query, conflicts="proceed")
        print(f"Deleted {res['deleted']} documents")

    def print_one_doc_and_mapping(self):

        res = self.es.search(
            index=self.index_name,
            body={"query": {"match_all": {}}, "size": 1}
        )
        hits = res.get('hits', {}).get('hits', [])
        if not hits:
            print("No documents found in index.")
            return

        doc = hits[0]
        print("=== Document ===")
        print(f"ID: {doc['_id']}")
        print(f"Source: {doc['_source']}")


        mapping = self.es.indices.get_mapping(index=self.index_name)
        print("\n=== Index Mapping ===")
        print(mapping)