from client import get_client
from fastapi_elasticsearch import ElasticsearchAPIRouter
from elasticsearch import Elasticsearch
from fastapi import Request, Query, Depends
from typing import Optional, Dict
from starlette.responses import JSONResponse
from development.loaddata import create_sample_index, load_sample_data
import logging
from fastapi_elasticsearch.utils import wait_elasticsearch
from security import get_auth_header
import base64

index_name = "sample-data"
router = ElasticsearchAPIRouter(
    index_name=index_name
)

es_client = get_client()

auth_header = {"Authorization": "Basic " + base64.b64encode(b"admin:admin").decode('utf-8')}


@router.on_event("startup")
async def startup_event():
    wait_elasticsearch(es_client, headers=auth_header)
    if not es_client.indices.exists(index_name, headers=auth_header):
        logging.info(f"Index {index_name} not found. Creating one.")
        create_sample_index(es_client, index_name, headers=auth_header)
        load_sample_data(es_client, index_name, headers=auth_header)


@router.on_event("shutdown")
def shutdown_event():
    es_client.close()


@router.filter()
def filter_items():
    return {
        "term": {
            "join_field": "item"
        }
    }


@router.filter()
def filter_category(c: Optional[str] = Query(None,
                                             description="Category name to filter results.")):
    return {
        "term": {
            "category": c
        }
    } if c is not None else None


@router.matcher()
def match_fields(q: Optional[str] = Query(None,
                                          description="Query to match the document text.")):
    return {
        "multi_match": {
            "query": q,
            "fuzziness": "AUTO",
            "fields": [
                "name^2",
            ]
        }
    } if q is not None else None


@router.matcher()
def match_fragments(q: Optional[str] = Query(None,
                                             description="Query to match the document text."),
                    h: bool = Query(False,
                                    description="Highlight matched text and inner hits.")):
    if q is not None:
        matcher = {
            "has_child": {
                "type": "fragment",
                "score_mode": "max",
                "query": {
                    "bool": {
                        "minimum_should_match": 1,
                        "should": [
                            {
                                "match": {
                                    "content": {
                                        "query": q,
                                        "fuzziness": "auto"
                                    }
                                }
                            },
                            {
                                "match_phrase": {
                                    "content": {
                                        "query": q,
                                        "slop": 3,
                                        "boost": 50
                                    }
                                }
                            },
                        ]
                    }
                }
            }
        }
        if h:
            matcher["has_child"]["inner_hits"] = {
                "size": 1,
                "_source": "false",
                "highlight": {
                    "fields": {
                        "content": {
                            "fragment_size": 256,
                            "number_of_fragments": 1
                        }
                    }
                }
            }
        return matcher
    else:
        return None


@router.sorter()
def sort_by(so: Optional[str] = Query(None,
                                      description="Sort fields (uses format:'\\<field\\>,\\<direction\\>")):
    if so is not None:
        values = so.split(",")
        field = values[0]
        direction = values[1] if len(values) > 1 else "asc"
        sorter = {}
        sorter[field] = direction
        return sorter
    else:
        return None


@router.highlighter()
def highlight(q: Optional[str] = Query(None,
                                       description="Query to match the document text."),
              h: bool = Query(False,
                              description="Highlight matched text and inner hits.")):
    return {
        "name": {
            "fragment_size": 256,
            "number_of_fragments": 1
        }
    } if q is not None and h else None


@router.search_route("/search")
async def search(req: Request,
                 es_client: Elasticsearch = Depends(get_client),
                 auth_header: Dict = Depends(get_auth_header),
                 size: Optional[int] = Query(10,
                                             le=100,
                                             alias="s",
                                             description="Defines the number of hits to return."),
                 start_from: Optional[int] = Query(0,
                                                   alias="f",
                                                   description="Starting document offset."),
                 scroll: Optional[str] = Query(None,
                                               description="Period to retain the search context for scrolling."),
                 ) -> JSONResponse:
    return router.search(
        es_client=es_client,
        request=req,
        size=size,
        start_from=start_from,
        scroll=scroll,
        headers=auth_header
    )


@router.search_route("/search/debug")
async def search_debug(req: Request,
                       size: Optional[int] = Query(10,
                                                   le=100,
                                                   alias="s",
                                                   description="Defines the number of hits to return."),
                       start_from: Optional[int] = Query(0,
                                                         alias="f",
                                                         description="Starting document offset."),
                       scroll: Optional[str] = Query(None,
                                                     description="Period to retain the search context for scrolling."),
                       ):
    return router.build_query(
        request=req,
        size=size,
        start_from=start_from,
        scroll=scroll,
    )
