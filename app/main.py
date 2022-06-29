from typing import Union
from pydantic import BaseModel
from fastapi import FastAPI
from urllib.parse import urlparse
import requests
import xxhash
import json

app = FastAPI()

ES_HOST = '104.199.237.73'
ES_INDEX = 'moat_v6'

class Item(BaseModel):
    location_url: str
    referer_url : str = None
    referer_domain: str = None

@app.post("/url_tags")
async def url_tags(item: Item):

    item_dict = item.dict()

    item_dict['location_domain'] = urlparse(item_dict['location_url']).netloc

    if item_dict['referer_url'] != None :
        item_dict['referer_domain'] = urlparse(item_dict['referer_url']).netloc
    elif item_dict['referer_domain'] != None :
        item_dict['referer_url'] = "https://"+item_dict['referer_domain']
    else:
        return {"ERROR":"Either referer_url or referer_domain shoud have a value."}
    print(item_dict)

    location_data = query_elastic(item_dict['location_url'],item_dict['location_domain'])
    referer_data  = query_elastic(item_dict['referer_url'],item_dict['referer_domain'])
    data = {"location":location_data,"referer":referer_data}
    return data

def hash_url(url):
    xx_url = xxhash.xxh64(url).hexdigest()
    return xx_url

def query_elastic(url,domain):
    routing = domain
    body = query_body(url,domain)
    response = requests.get(f'http://{ES_HOST}:9200/{ES_INDEX}/_search?routing={routing}',
        headers={'Content-Type':'application/json'},
        data=json.dumps(body))
    response = json.loads(response.content.decode())
    data = response['hits']['hits'][0]['_source']
    return data

def query_body(url,domain):
    body = {
            "query":{
                "bool":{
                    "must":{
                        "term":{
                            "xx_domain": hash_url(domain)
                        }
                    },
                    "should":{
                        "term":{
                            "xx_url": hash_url(url)
                        }
                    }
                }
            },
            "from" : 0,
            "size" : 1
    }
    return body
