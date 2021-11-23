from flask import Flask, request

from LmWebServer.flask_app.occurrence import OccurrenceLayerService

app = Flask(__name__)


# .....................................................................................
@app.route('/api/v2/occ/<string:identifier>', methods=['GET'])
def occ_get(identifier):
    """Get an occurrence record from available providers.

    Args:
        identifier (str): An occurrence identifier to search for among occurrence providers.

    Returns:
        dict: A dictionary of metadata for the requested record.
    """
    api = OccurrenceLayerService()

    after_time = request.args.get('after_time', default = None, type = str)
    before_time = request.args.get('before_time', default = None, type = str)
    display_name = request.args.get('display_name', default = None, type = str)
    epsg_code = request.args.get('epsg_code', default= None, type = str) 
    minimum_number_of_points = request.args.get('minimum_number_of_points', default = 1, type = int)
    limit = request.args.get('limit', default = 100, type = int)
    offset = request.args.get('offset', default = 0, type = int)
    url_user = request.args.get('url_user', default = None, type = str) 
    status = request.args.get('status', default = None, type = str)
    gridset_id = request.args.get('gridset_id', default = None, type = str)
    fill_points = request.args.get('fill_points', default = False, type = bool)

    response = api.filter_occurrence_sets(
        path_occset_id=identifier, after_time=after_time, before_time=before_time, 
        display_name=display_name, epsg_code=epsg_code, minimum_number_of_points=minimum_number_of_points,
        limit=limit, offset=offset, url_user=url_user, status=status, gridset_id=gridset_id,
        fill_points=fill_points)
    return response

