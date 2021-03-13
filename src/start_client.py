from elasticsearch import Elasticsearch


def start():
    return Elasticsearch(
        ["elastic-dev"],
        use_ssl=True,
        ca_certs="/euler/root-ca.pem",
        ssl_show_warn=False
    )
