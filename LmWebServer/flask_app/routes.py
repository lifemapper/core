from flask import Flask, redirect, render_template, request, session
from werkzeug.exceptions import BadRequest, NotFound

from LmWebServer.flask_app.occurrence import OccurrenceLayerService
from LmWebServer.flask_app.base import LmService

app = Flask(__name__.split('.')[0])

# ..........................
@app.route('/api/v2/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        req = request.form
        username = req.get('username')
        password = req.get('password')
        
        user = LmService.get_user(username)
        if user.check_password(password):
            session['username'] = user.user_id
            return user
        else:
            print('Incorrect password')
            return redirect(request.url)

    return render_template('public_html/login.html')

# .....................................................................................
@app.route('/api/v2/occ/<string:identifier>', methods=['GET', 'POST', 'DELETE'])
def occurrence(identifier):
    """Occurrence API service for GET, POST, and DELETE operations on occurrences

    Args:
        identifier (str): An occurrence identifier to search for.

    Returns:
        dict: For GET and POST operations, zero or more dictionaries of metadata for the requested or 
        posted record(s); for DELETE operations, True or False for success
        
    TODO: Why is boom post here?  Create a different service for that.
    """
    svc = OccurrenceLayerService()
    user = svc.get_user()
    user_id = user.user_id
    
    if request.method == 'POST' and request.is_json:
        boom_data = request.get_json()
        svc.post_boom_data(user_id, user.email, boom_data)

    elif request.method == 'DELETE':
        svc.delete_occurrence_set(user_id, identifier)
    
    elif request.method == 'GET':
        after_time = request.args.get('after_time', default = None, type = str)
        before_time = request.args.get('before_time', default = None, type = str)
        display_name = request.args.get('display_name', default = None, type = str)
        epsg_code = request.args.get('epsg_code', default= None, type = str) 
        minimum_number_of_points = request.args.get('minimum_number_of_points', default = 1, type = int)
        limit = request.args.get('limit', default = 100, type = int)
        offset = request.args.get('offset', default = 0, type = int)
        # url_user = request.args.get('url_user', default = None, type = str) 
        status = request.args.get('status', default = None, type = str)
        gridset_id = request.args.get('gridset_id', default = None, type = str)
        fill_points = request.args.get('fill_points', default = False, type = bool)
        
        if identifier.lower() == 'count':
            response = svc.count_occurrence_sets(
                user_id, after_time=after_time, before_time=before_time, display_name=display_name,
                epsg_code=epsg_code, minimum_number_of_points=minimum_number_of_points,
                gridset_id=gridset_id, status=status)

        elif identifier.lower() == 'web':
            response = svc.list_web_occurrence_sets(
                user_id, after_time=after_time, before_time=before_time, display_name=display_name,
                epsg_code=epsg_code, minimum_number_of_points=minimum_number_of_points, limit=limit,
                offset=offset, gridset_id=gridset_id, status=status)
            
        elif identifier is None:
            response = svc.list_occurrence_sets(
                user_id, after_time=after_time, before_time=before_time, display_name=display_name,
                epsg_code=epsg_code, minimum_number_of_points=minimum_number_of_points, limit=limit,
                offset=offset, gridset_id=gridset_id, status=status)
            
        else:
            try:
                occid = int(identifier)
            except:
                return BadRequest('{} is not a valid layer ID'.format(identifier))
            else:
                response = svc.get_occurrence_set(occid, fill_points=fill_points)
        
            
    return response

# .....................................................................................
@app.route('/api/v2/layer/<string:identifier>', methods=['GET', 'DELETE'])
def layer(identifier):
    """Layer API service for GET and DELETE operations on layers

    Path parameter:
        identifier (str): A layer identifier to search for.

    Returns:
        dict: A dictionary of metadata for the requested record.
    """
    svc = OccurrenceLayerService()
    user = svc.get_user()
    user_id = user.user_id
    
    if request.method == 'DELETE':
        svc.delete_occurrence_set(user_id, identifier)
    
    elif request.method == 'GET':
        after_time = request.args.get('after_time', default = None, type = str)
        before_time = request.args.get('before_time', default = None, type = str)
        alt_pred_code = request.args.get('alt_pred_code', default = None, type = str)
        date_code = request.args.get('date_code', default = None, type = str)
        epsg_code = request.args.get('epsg_code', default= None, type = str) 
        env_code = request.args.get('env_code', default = None, type = str)
        env_type_id = request.args.get('env_type_id', default = None, type = int)
        gcm_code = request.args.get('gcm_code', default = None, type = str)
        # layer_type: 
        layer_type = request.args.get('layer_type', default = None, type = str)
        scenario_code = request.args.get('scenario_code', default = None, type = int)
        squid = request.args.get('squid', default = None, type = str)
        limit = request.args.get('limit', default = 100, type = int)
        offset = request.args.get('offset', default = 0, type = int)
        
        if identifier is None:
            if layer_type == 1:
                response = svc.list_env_layers(
                    user_id, after_time=after_time, before_time=before_time, alt_pred_code=alt_pred_code, 
                    date_code=date_code, env_code=env_code, env_type_id=env_type_id, epsg_code=epsg_code, 
                    gcm_code=gcm_code, scenario_code=scenario_code, limit=limit, offset=offset)
            else:
                response = svc.list_layers(
                    user_id, after_time=after_time, before_time=before_time, epsg_code=epsg_code, 
                    squid=squid, limit=limit, offset=offset)
        elif identifier.lower() == 'count':
            if layer_type == 1:
                response = svc.count_env_layers(
                    user_id, after_time=after_time, before_time=before_time, alt_pred_code=alt_pred_code, 
                    date_code=date_code, env_code=env_code, env_type_id=env_type_id, epsg_code=epsg_code, 
                    gcm_code=gcm_code, scenario_code=scenario_code)
            else:
                response = svc.count_layers(
                    offset, limit, user_id=user_id, after_time=after_time, before_time=before_time,  
                    epsg=epsg_code, squid=squid)
        
        else:
            try:
                layer_id = int(identifier)
            except:
                return BadRequest('{} is not a valid occurrenceset ID'.format(identifier))
            else:
                response = svc.get_layer(user_id, layer_id, env_layer=(layer_type == 1))
        
            
    return response
