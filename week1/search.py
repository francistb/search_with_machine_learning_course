#
# The main search hooks for the Search Flask application.
#
from flask import Blueprint, redirect, render_template, request, url_for
import json
from week1.opensearch import get_opensearch

bp = Blueprint("search", __name__, url_prefix="/search")


# Process the filters requested by the user and return a tuple that is appropriate for use in: the query, URLs displaying the filter and the display of the applied filters
# filters -- convert the URL GET structure into an OpenSearch filter query
# display_filters -- return an array of filters that are applied that is appropriate for display
# applied_filters -- return a String that is appropriate for inclusion in a URL as part of a query string.  This is basically the same as the input query string
def process_filters(filters_input):
    # Filters look like: &filter.name=regularPrice&regularPrice.key={{ agg.key }}&regularPrice.from={{ agg.from }}&regularPrice.to={{ agg.to }}
    filters = []
    display_filters = (
        []
    )  # Also create the text we will use to display the filters that are applied
    applied_filters = ""
    for filter in filters_input:
        type = request.args.get(filter + ".type")
        display_name = request.args.get(filter + ".displayName")
        filter_key = request.args.get(filter + ".key")

        # We need to capture and return what filters are already applied so they can be automatically added to any existing links we display in aggregations.jinja2
        # applied_filters += f"&filter.name={filter}&{filter}.type={type}&{filter}.displayName={display_name}"

        # TODO: IMPLEMENT AND SET filters, display_filters and applied_filters.
        # filters get used in create_query below.  display_filters gets used by display_filters.jinja2 and applied_filters gets used by aggregations.jinja2 (and any other links that would execute a search.)

        display_filters.append(f"{display_name} : {filter_key}")

        if type == "range":
            # filter.name=regularPrice&regularPrice.type=range&regularPrice.key=100.0-200.0&regularPrice.from=100.0&regularPrice.to=200.0&regularPrice.displayName=Price
            from_filter = request.args.get(filter + ".from")
            to_filter = request.args.get(filter + ".to")
            range_filter = {filter: {}}
            if from_filter:
                range_filter[filter]["gte"] = from_filter
            if to_filter:
                range_filter[filter]["lt"] = to_filter

            filters.append({"range": range_filter})
            applied_filters += f"&filter.name={filter}&{filter}.type={type}&{filter}.displayName={display_name}&{filter}.key={filter_key}&{filter}.from={from_filter}&filter.to={to_filter}"
        elif type == "terms":
            # filter.name=department&department.type=terms&department.key=PHOTO/COMMODITIES&department.displayName=Department
            terms_filter = {f"{filter}.keyword": filter_key}
            filters.append({"term": terms_filter})
            applied_filters += f"&filter.name={filter}&{filter}.type={type}&{filter}.displayName={display_name}&{filter}.key={filter_key}"

    print("Filters: {}".format(filters))
    return filters, display_filters, applied_filters


# Our main query route.  Accepts POST (via the Search box) and GETs via the clicks on aggregations/facets
@bp.route("/query", methods=["GET", "POST"])
def query():
    opensearch = (
        get_opensearch()
    )  # Load up our OpenSearch client from the opensearch.py file.
    # Put in your code to query opensearch.  Set error as appropriate.
    error = None
    user_query = None
    query_obj = None
    display_filters = None
    applied_filters = ""
    filters = None
    sort = "_score"
    sortDir = "desc"
    if request.method == "POST":  # a query has been submitted
        user_query = request.form["query"]
        if not user_query:
            user_query = "*"
        sort = request.form["sort"]
        if not sort:
            sort = "_score"
        sortDir = request.form["sortDir"]
        if not sortDir:
            sortDir = "desc"
        filters_input = request.form.get("filter.name", [])
        query_obj = create_query(user_query, filters_input, sort, sortDir)
    elif (
        request.method == "GET"
    ):  # Handle the case where there is no query or just loading the page
        user_query = request.args.get("query", "*")
        filters_input = request.args.getlist("filter.name")
        sort = request.args.get("sort", sort)
        sortDir = request.args.get("sortDir", sortDir)
        if filters_input:
            (filters, display_filters, applied_filters) = process_filters(filters_input)

        query_obj = create_query(user_query, filters, sort, sortDir)
    else:
        query_obj = create_query("*", [], sort, sortDir)

    print(f"Query:")
    print(json.dumps(query_obj))
    response = opensearch.search(query_obj, index="bbuy_products")
    # Postprocess results here if you so desire

    # print(response)

    if error is None:
        return render_template(
            "search_results.jinja2",
            query=user_query,
            search_response=response,
            display_filters=display_filters,
            applied_filters=applied_filters,
            sort=sort,
            sortDir=sortDir,
        )
    else:
        redirect(url_for("index"))


def create_query(user_query, filters, sort="_score", sortDir="desc"):
    print("Query: {} Filters: {} Sort: {}".format(user_query, filters, sort))
    query_obj = {"size": 10, "track_total_hits": True}

    if user_query and user_query != "*":
        query_obj["query"] = {
            "bool": {
                "must": [
                    {
                        "multi_match": {
                            "query": user_query,
                            "fields": [
                                "name.keyword^1000",
                                "name^10",
                                "shortDescription^5",
                                "description",
                                "features",
                                "longDescription",
                            ],
                        }
                    }
                ],
                "filter": filters,
            }
        }
    else:
        query_obj["query"] = {"match_all": {}}

    query_obj["aggs"] = {
        "regularPrice": {
            "range": {
                "field": "regularPrice",
                "ranges": [
                    {"to": 10.0},
                    {"from": 10.0, "to": 100.0},
                    {"from": 100.0, "to": 200.0},
                    {"from": 200.0},
                ],
            }
        },
        "department": {"terms": {"field": "department.keyword"}},
        "missing_images": {"missing": {"field": "image"}},
    }

    query_obj["sort"] = ({sort: sortDir},)

    return query_obj
