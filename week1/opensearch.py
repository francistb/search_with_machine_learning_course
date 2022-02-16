from flask import g, current_app
from opensearchpy import OpenSearch

# Create an OpenSearch client instance and put it into Flask shared space for use by the application
def get_opensearch():
    if "opensearch" not in g:
        # Implement a client connection to OpenSearch so that the rest of the application can communicate with OpenSearch
        g.opensearch = OpenSearch(
            hosts=["localhost:9200"],
            use_ssl=True,
            verify_certs=False,
            ssl_show_warn=False,
            http_auth = ('admin', 'admin')
        )

    return g.opensearch
