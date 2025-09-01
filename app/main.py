import uvicorn
from fastapi import FastAPI
from contextlib import asynccontextmanager
from elasticsearch import Elasticsearch
import config
from loader import Loader
from elastic_crud import ElasticCrud
import threading

es = Elasticsearch(config.elastic_conn)
processing_done = False
loader = Loader()

@asynccontextmanager
async def lifespan(app: FastAPI):
    global processing_done
    processing_done = False


    crud = ElasticCrud(
        data=loader.convert_csv_to_json(),
        mapping=config.custom_mapping,
        index_name=config.index_name,
        weapons_list=loader.list_weapon_file()
    )
    crud.create_index()
    crud.create_data()


    def run_background_updates():
        global processing_done
        crud.add_sentiment()
        crud.add_weapon_list()
        processing_done = True

    threading.Thread(target=run_background_updates).start()

    yield

app = FastAPI(lifespan=lifespan)

@app.get("/antisemitic_with_weapons")
def get_antisemitic_with_weapons():
    if not processing_done:
        return {"status": "processing not finished",
                "message": "Data has not been fully processed yet."}

    query = {
        "query": {
            "bool": {
                "must": [
                    {"term": {"classification": "Antisemitic"}},
                    {"exists": {"field": "weapons_list"}}
                ]
            }
        },
        "size": 1000
    }

    res = es.search(index=config.index_name, body=query)
    return {"results": [hit["_source"] for hit in res["hits"]["hits"]]}


@app.get("/two_or_more_weapons")
def get_two_or_more_weapons():
    if not processing_done:
        return {"status": "processing not finished",
                "message": "Data has not been fully processed yet."}

    query = {
        "query": {"match_all": {}},
        "size": 1000
    }

    res = es.search(index=config.index_name, body=query)
    filtered = [hit["_source"] for hit in res["hits"]["hits"] if len(hit["_source"].get("weapons_list", [])) >= 2]
    return {"results": filtered}


if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8000)
