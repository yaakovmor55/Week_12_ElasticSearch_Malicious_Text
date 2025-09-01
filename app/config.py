csv_path="../data/tweets_injected 3.csv"
weapon_path="../data/weapon_list.txt"
elastic_conn="http://localhost:9200"

custom_mapping = {
    "properties": {
        "TweetID": {"type": "float"},
        "CreateDate": {
            "type": "text",
        },
        "Antisemitic": {"type": "integer"},
        "text": {"type": "text"}
    }
}


index_name="tweets_data"