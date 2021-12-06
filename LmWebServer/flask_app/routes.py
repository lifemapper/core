import flask
# from  flask import (abort, Flask, flash, redirect, render_template, request, session, url_for)
from flask_cors import CORS
from flask_login import login_user, LoginManager
import os
from werkzeug.exceptions import BadRequest
from werkzeug.utils import secure_filename

from LmCommon.common.lmconstants import JobStatus

from LmWebServer.common.lmconstants import FALLBACK_SECRET_KEY
from LmWebServer.flask_app.base import LmService
from LmWebServer.flask_app.biotaphy_names import GBIFTaxonService
from LmWebServer.flask_app.biotaphy_points import IDigBioOccurrenceService
from LmWebServer.flask_app.gbif_parser import GBIFNamesService
from LmWebServer.flask_app.global_pam import GlobalPAMService
from LmWebServer.flask_app.layer import LayerService
from LmWebServer.flask_app.occurrence import OccurrenceLayerService
from LmWebServer.flask_app.gridset import GridsetService
from LmWebServer.flask_app.species_hint import SpeciesHintService
from LmWebServer.flask_app.open_tree import OpenTreeService
from LmWebServer.flask_app.scenario_package import ScenarioPackageService
from LmWebServer.flask_app.scenario import ScenarioService
from LmWebServer.flask_app.sdm_project import SdmProjectService
from LmWebServer.flask_app.snippet import SnippetService
from LmWebServer.flask_app.solr_raw import RawSolrService
from LmWebServer.flask_app.taxonomy import TaxonomyHintService
from LmWebServer.flask_app.tree import TreeService
from LmWebServer.flask_app.upload import UserUploadService
from LmWebServer.flask_tools.basic_auth import is_safe_url

try:
    skey = os.environ['SECRET_KEY']
except:
    skey = FALLBACK_SECRET_KEY


# TODO: Put this into the database or an environment variable
app = flask.Flask(__name__.split('.')[0])    
app.secret_key = str.encode(skey)
CORS(app)

login_manager = LoginManager()
login_manager.init_app(app)


# ..........................
@login_manager.user_loader
def load_user(user_id):
    return LmService.get_user(user_id)

# ..........................
@app.route('/')
def index():
    if 'username' in flask.session:
        return f'Logged in as {flask.session["username"]}'
    return 'You are not logged in'

# # ..........................
# @app.route('/api/login', methods=['GET', 'POST'])
# def login():
#         # Here we use a class of some kind to represent and validate our
#     # client-side form data. For example, WTForms is a library that will
#     # handle this for us, and we use a custom LoginForm to validate.
#     form = LoginForm()
#     if form.validate_on_submit():
#         user = load_user()
#         # Login and validate the user.
#         # user should be an instance of your `User` class
#         user = LmService.get_user()
#         login_user(user)
#
#         flask.flash('Logged in successfully.')
#
#         next = flask.request.args.get('next')
#         # is_safe_url should check if the url is safe for redirects.
#         # See http://flask.pocoo.org/snippets/62/ for an example.
#         if not is_safe_url(next):
#             return flask.abort(400)
#
#         return flask.redirect(next or flask.url_for('index'))
#     return flask.render_template('public_html/login.html', form=form)
#
#
#     # if request.method == 'POST':
#     #     username = request.form.get('username')
#     #     password = request.form.get('password')
#     #
#     #     user = LmService.get_user(username)
#     #     if user.check_password(password):
#     #         session['username'] = user.user_id
#     #         return user
#     #     else:
#     #         print('Incorrect password')
#     #         return redirect(request.url)
#     #
#     # return render_template('public_html/login.html')
#
# # .....................................................................................
# @app.route('/api/logout')
# def logout():
#     # remove the username from the session if it's there
#     flask.session.pop('username', None)
#     return flask.redirect(flask.url_for('index'))


# .....................................................................................
@app.route('/api/v2/layer/<string:identifier>', methods=['GET', 'DELETE'])
def layer(identifier):
    """Layer API service for GET and DELETE operations on layers

    Path parameter:
        identifier (str): A layer identifier to search for.

    Returns:
        dict: A dictionary of metadata for the requested record.
    """
    svc = LayerService()
    user = svc.get_user()
    user_id = user.user_id
    
    if flask.request.method == 'DELETE':
        svc.delete_occurrence_set(user_id, identifier)
    
    elif flask.request.method == 'GET':
        after_time = flask.request.args.get('after_time', default = None, type = float)
        before_time = flask.request.args.get('before_time', default = None, type = float)
        alt_pred_code = flask.request.args.get('alt_pred_code', default = None, type = str)
        date_code = flask.request.args.get('date_code', default = None, type = str)
        epsg_code = flask.request.args.get('epsg_code', default= None, type = str) 
        env_code = flask.request.args.get('env_code', default = None, type = str)
        env_type_id = flask.request.args.get('env_type_id', default = None, type = int)
        gcm_code = flask.request.args.get('gcm_code', default = None, type = str)
        # layer_type: 
        layer_type = flask.request.args.get('layer_type', default = None, type = str)
        scenario_code = flask.request.args.get('scenario_code', default = None, type = int)
        squid = flask.request.args.get('squid', default = None, type = str)
        limit = flask.request.args.get('limit', default = 100, type = int)
        offset = flask.request.args.get('offset', default = 0, type = int)
        
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
    
    if flask.request.method == 'POST' and flask.request.is_json:
        boom_data = flask.request.get_json()
        svc.post_boom_data(user_id, user.email, boom_data)

    elif flask.request.method == 'DELETE':
        svc.delete_occurrence_set(user_id, identifier)
    
    elif flask.request.method == 'GET':
        after_time = flask.request.args.get('after_time', default = None, type = float)
        before_time = flask.request.args.get('before_time', default = None, type = float)
        display_name = flask.request.args.get('display_name', default = None, type = str)
        epsg_code = flask.request.args.get('epsg_code', default= None, type = str) 
        minimum_number_of_points = flask.request.args.get('minimum_number_of_points', default = 1, type = int)
        limit = flask.request.args.get('limit', default = 100, type = int)
        offset = flask.request.args.get('offset', default = 0, type = int)
        status = flask.request.args.get('status', default = None, type = int)
        gridset_id = flask.request.args.get('gridset_id', default = None, type = int)
        fill_points = flask.request.args.get('fill_points', default = False, type = bool)
        
        if identifier is None:
            response = svc.list_occurrence_sets(
                user_id, after_time=after_time, before_time=before_time, display_name=display_name,
                epsg_code=epsg_code, minimum_number_of_points=minimum_number_of_points, limit=limit,
                offset=offset, gridset_id=gridset_id, status=status)

        elif identifier.lower() == 'count':
            response = svc.count_occurrence_sets(
                user_id, after_time=after_time, before_time=before_time, display_name=display_name,
                epsg_code=epsg_code, minimum_number_of_points=minimum_number_of_points,
                gridset_id=gridset_id, status=status)

        elif identifier.lower() == 'web':
            response = svc.list_web_occurrence_sets(
                user_id, after_time=after_time, before_time=before_time, display_name=display_name,
                epsg_code=epsg_code, minimum_number_of_points=minimum_number_of_points, limit=limit,
                offset=offset, gridset_id=gridset_id, status=status)
        
        else:
            try:
                occid = int(identifier)
            except:
                return BadRequest('{} is not a valid layer ID'.format(identifier))
            else:
                response = svc.get_occurrence_set(user_id, occid, fill_points=fill_points)
                
    return response

# .....................................................................................
@app.route('/api/v2/biotaphynames', methods=['POST'])
def biotaphynames():
    try:
        names_obj = flask.request.get_json()
    except: 
        return BadRequest('Names must be a JSON list')
    else:
        svc = GBIFTaxonService()
        response = svc.get_gbif_results(names_obj)
        return response

# .....................................................................................
@app.route('/api/v2/biotaphypoints', methods=['POST'])
def biotaphypoints():
    try:
        taxonids_obj = flask.request.get_json()
    except: 
        return BadRequest('Taxon IDs must be a JSON list')
    else:
        svc = IDigBioOccurrenceService()
        response = svc.get_occurrence_counts_for_taxonids(taxonids_obj)
        return response

# .....................................................................................
@app.route('/api/v2/biotaphytree', methods=['POST'])
def biotaphytree():
    try:
        taxon_names_obj = flask.request.get_json()
    except:
        return BadRequest('Taxon names must be a JSON list')
    else:
        svc =  OpenTreeService()
        svc.get_tree_for_names(taxon_names_obj)

# .....................................................................................
@app.route('/api/v2/gbifparser', methods=['POST'])
def gbifparser():
    try:
        names_obj = flask.request.get_json()
    except: 
        return BadRequest('Name list must be in JSON format')
    else:
        svc = GBIFNamesService()
        response = svc.get_gbif_names(names_obj)
        return response

# .....................................................................................
@app.route('/api/v2/globalpam', methods=['GET', 'POST'])
def globalpam():
    svc = GlobalPAMService()()
    user = svc.get_user()
    user_id = user.user_id
    
    archive_name = flask.request.args.get('display_name', default = None, type = str)
    cell_size = flask.request.args.get('cell_size', default = None, type = float)
    algorithm_code = flask.request.args.get('algorithm_code', default = None, type = str)
    bbox = flask.request.args.get('bbox', default = None, type = str)
    display_name = flask.request.args.get('display_name', default = None, type = str)
    gridset_id = flask.request.args.get('gridset_id', default = None, type = int)
    model_scenario_code = flask.request.args.get('model_scenario_code', default = None, type = str)
    prj_scen_code = flask.request.args.get('prj_scenario_code', default = None, type = str)
    point_max = flask.request.args.get('point_max', default = None, type = int)
    point_min = flask.request.args.get('point_min', default = None, type = int)
    squid = flask.request.args.get('squid', default = None, type = str)
    taxon_kingdom = flask.request.args.get('taxon_kingdom', default = None, type = str)
    taxon_phylum = flask.request.args.get('taxon_phylum', default = None, type = str)
    taxon_class = flask.request.args.get('taxon_class', default = None, type = str)
    taxon_order = flask.request.args.get('taxon_order', default = None, type = str)
    taxon_family = flask.request.args.get('taxon_family', default = None, type = str)
    taxon_genus = flask.request.args.get('taxon_genus', default = None, type = str)
    taxon_species = flask.request.args.get('taxon_species', default = None, type = str)
        
    if flask.request.method == 'POST':
        response = svc.post_pam_subset(
            user_id, archive_name, gridset_id, algorithm_code=algorithm_code, bbox=bbox, 
            display_name=display_name, gridset_id=gridset_id, model_scenario_code=model_scenario_code, 
            prj_scen_code=prj_scen_code, point_max=point_max, point_min=point_min, squid=squid, 
            taxon_kingdom=taxon_kingdom, taxon_phylum=taxon_phylum, taxon_class=taxon_class, 
            taxon_order=taxon_order, taxon_family=taxon_family, taxon_genus=taxon_genus, 
            taxon_species=taxon_species)

    elif flask.request.method == 'GET':
        response = svc.post_pam_subset(
            user_id, archive_name, cell_size=cell_size, algorithm_code=algorithm_code, bbox=bbox, 
            display_name=display_name, gridset_id=gridset_id, model_scenario_code=model_scenario_code, 
            prj_scen_code=prj_scen_code, point_max=point_max, point_min=point_min, squid=squid, 
            taxon_kingdom=taxon_kingdom, taxon_phylum=taxon_phylum, taxon_class=taxon_class, 
            taxon_order=taxon_order, taxon_family=taxon_family, taxon_genus=taxon_genus, 
            taxon_species=taxon_species)
    
        return response

# .....................................................................................
@app.route('/api/v2/gridset/<string:identifier>', methods=['GET', 'POST', 'DELETE'])
def gridset(identifier):
    svc = GridsetService()
    user = svc.get_user()
    user_id = user.user_id
    
    if flask.request.method == 'POST' and flask.request.is_json:
        gridset_data = flask.request.get_json()
        svc.post_boom_data(user_id, user.email, gridset_data)

    elif flask.request.method == 'DELETE':
        svc.delete_gridset(user_id, identifier)
    
    elif flask.request.method == 'GET':
        after_time = flask.request.args.get('after_time', default = None, type = float)
        before_time = flask.request.args.get('before_time', default = None, type = float)
        epsg_code = flask.request.args.get('epsg_code', default= None, type = str)
        meta_string = flask.request.args.get('meta_string', default= None, type = str)
        shapegrid_id = flask.request.args.get('shapegrid_id', default= None, type = int)
        limit = flask.request.args.get('limit', default = 100, type = int)
        offset = flask.request.args.get('offset', default = 0, type = int)

        if identifier is None:
            response = svc.list_gridsets(
                user_id, after_time=after_time, before_time=before_time, epsg_code=epsg_code, 
                meta_string=meta_string, shapegrid_id=shapegrid_id, limit=limit, offset=offset)

        elif identifier.lower() == 'count':
            response = svc.count_gridsets(
                user_id, after_time=after_time, before_time=before_time, epsg_code=epsg_code, 
                meta_string=meta_string, shapegrid_id=shapegrid_id)
            
        else:
            try:
                gridset_id = int(identifier)
            except:
                return BadRequest('{} is not a valid gridset ID'.format(identifier))
            else:
                response = svc.get_gridset(user_id, gridset_id)

        return response

# .....................................................................................
@app.route('/api/v2/hint', methods=['GET'])
def hint():
    svc = SpeciesHintService()
    user_id = svc.get_user()

    search_string = flask.request.args.get('search_string', default= None, type = str)
    return svc.get_hint(user_id, search_string)
    
# .....................................................................................
@app.route('/api/v2/scenpackage/<string:identifier>', methods=['GET'])
def scenpackage(identifier):
    svc = ScenarioPackageService()
    user_id = svc.get_user()
    
    scenario_package_id = flask.request.args.get('scenario_package_id', default = None, type = int)
    scenario_id = flask.request.args.get('scenario_id', default = None, type = int)
    after_time = flask.request.args.get('after_time', default = None, type = float)
    before_time = flask.request.args.get('before_time', default = None, type = float)
    epsg_code = flask.request.args.get('epsg_code', default= None, type = str) 
    limit = flask.request.args.get('limit', default = 100, type = int)
    offset = flask.request.args.get('offset', default = 0, type = int)
    
    if identifier is None:
        response = svc.list_scenario_packages(
            user_id, after_time=after_time, before_time=before_time, epsg_code=epsg_code, 
            scenario_id=scenario_id, limit=limit, offset=offset)

    elif identifier.lower() == 'count':
        response = svc.count_scenario_packages(
            user_id, after_time=after_time, before_time=before_time, epsg_code=epsg_code, scenario_id=scenario_id)
    
    else:
        try:
            scenario_package_id = int(identifier)
        except:
            return BadRequest('{} is not a valid layer ID'.format(identifier))
        else:
            response = svc.get_scenario_package(user_id, scenario_package_id)
                
    return response
    
# .....................................................................................
@app.route('/api/v2/scenario/<string:identifier>', methods=['GET'])
def scenario(identifier):
    svc = ScenarioService()
    user_id = svc.get_user_id()

    scenario_id = flask.request.args.get('scenario_id', default = None, type = int)
    after_time = flask.request.args.get('after_time', default = None, type = float)
    before_time = flask.request.args.get('before_time', default = None, type = float)
    alt_pred_code = flask.request.args.get('alt_pred_code', default= None, type = str) 
    date_code = flask.request.args.get('date_code', default= None, type = str) 
    gcm_code = flask.request.args.get('gcm_code', default= None, type = str) 
    epsg_code = flask.request.args.get('epsg_code', default= None, type = str) 
    limit = flask.request.args.get('limit', default = 100, type = int)
    offset = flask.request.args.get('offset', default = 0, type = int)

    if identifier is None:
        response = svc.list_scenarios(
            user_id, after_time=after_time, before_time=before_time, alt_pred_code=alt_pred_code, 
            date_code=date_code, gcm_code=gcm_code, epsg_code=epsg_code, limit=limit, offset=offset)

    elif identifier.lower() == 'count':
        response = svc.count_scenarios(
            user_id, after_time=after_time, before_time=before_time, alt_pred_code=alt_pred_code, 
            date_code=date_code, gcm_code=gcm_code, epsg_code=epsg_code)
    
    else:
        try:
            scenario_id = int(identifier)
        except:
            return BadRequest('{} is not a valid layer ID'.format(identifier))
        else:
            response = svc.get_scenario(user_id, scenario_id)

    return response

# .....................................................................................
@app.route('/api/v2/sdmproject/<string:identifier>', methods=['GET', 'POST', 'DELETE'])
def sdmproject(identifier):
    """SdmProject API service for GET, POST, and DELETE operations on SDM projections

    Args:
        identifier (str): An sdmproject identifier to search for.

    Returns:
        dict: For GET and POST operations, zero or more dictionaries of metadata for the requested or 
        posted record(s); for DELETE operations, True or False for success        
    """
    svc = SdmProjectService()
    user = svc.get_user()
    user_id = user.user_id
    
    if flask.request.method == 'POST' and flask.request.is_json:
        projection_data = flask.request.get_json()
        svc.post_boom_data(user_id, user.email, projection_data)

    elif flask.request.method == 'DELETE':
        svc.delete_occurrence_set(user_id, identifier)
    
    elif flask.request.method == 'GET':
        after_time = flask.request.args.get('after_time', default = None, type = float)
        before_time = flask.request.args.get('before_time', default = None, type = float)
        after_status = flask.request.args.get('after_status', default = JobStatus.COMPLETE, type = int)
        before_status = flask.request.args.get('before_status', default = JobStatus.COMPLETE, type = int)
        alg_code = flask.request.args.get('alg_code', default = None, type = str)
        display_name = flask.request.args.get('display_name', default = None, type = str)
        epsg_code = flask.request.args.get('epsg_code', default= None, type = str)
        occurrence_set_id = flask.request.args.get('occurrence_set_id', default = None, type = int)
        mdl_scenario_code = flask.request.args.get('mdl_scenario_code', default = None, type = str)
        prj_scenario_code = flask.request.args.get('prj_scenario_code', default = None, type = str)
        status = flask.request.args.get('status', default = JobStatus.COMPLETE, type = int)
        gridset_id = flask.request.args.get('gridset_id', default = None, type = int)
        limit = flask.request.args.get('limit', default = 100, type = int)
        offset = flask.request.args.get('offset', default = 0, type = int)
        atom = flask.request.args.get('atom', default = True, type = bool)
        
        if identifier is None:
            response = svc.list_projections(
                user_id, after_time=after_time, before_time=before_time, after_status=after_status, 
                before_status=before_status, alg_code=alg_code, display_name=display_name, 
                epsg_code=epsg_code, occurrence_set_id=occurrence_set_id, mdl_scenario_code=mdl_scenario_code, 
                prj_scenario_code=prj_scenario_code, status=status, gridset_id=gridset_id, 
                limit=limit, offset=offset, atom=atom)

        elif identifier.lower() == 'count':
            response = svc.count_projections(
                user_id, after_time=after_time, before_time=before_time, after_status=after_status, 
                before_status=before_status, alg_code=alg_code, display_name=display_name, 
                epsg_code=epsg_code, occurrence_set_id=occurrence_set_id, mdl_scenario_code=mdl_scenario_code, 
                prj_scenario_code=prj_scenario_code, status=status, gridset_id=gridset_id)

        else:
            try:
                projection_id = int(identifier)
            except:
                return BadRequest('{} is not a valid projection ID'.format(identifier))
            else:
                response = svc.get_occurrence_set(user_id, projection_id, atom=atom)
                
    return response

# .....................................................................................
@app.route('/api/v2/snippet', methods=['GET'])
def snippet():
    svc = SnippetService()
    user_id = svc.get_user()
    
    ident1 = flask.request.args.get('ident1', default = None, type = str)
    ident2 = flask.request.args.get('ident2', default = None, type = str)
    provider = flask.request.args.get('provider', default = None, type = str)
    collection = flask.request.args.get('collection', default = None, type = str)
    catalog_number = flask.request.args.get('catalog_number', default = None, type = str)
    operation = flask.request.args.get('operation', default = None, type = str)
    after_time = flask.request.args.get('after_time', default = None, type = float)
    before_time = flask.request.args.get('before_time', default = None, type = float)
    url = flask.request.args.get('url', default = None, type = str)
    who = flask.request.args.get('who', default = None, type = str)
    agent = flask.request.args.get('agent', default = None, type = str)
    why = flask.request.args.get('why', default = None, type = str)
    
    response = svc.get_snippet(
        user_id, ident1=ident1, ident2=ident2, provider=provider, collection=collection, 
        catalog_number=catalog_number, operation=operation, after_time=after_time, before_time=before_time, 
        url=url, who=who, agent=agent, why=why)

    return response


# .....................................................................................
@app.route('/api/v2/rawsolr', methods=['POST'])
def rawsolr():
    svc = RawSolrService()
    req_body = flask.request.get_json()
    response = svc.query_collection(req_body)
    return response

# .....................................................................................
@app.route('/api/v2/taxonomy', methods=['GET'])
def taxonomy():
    svc = TaxonomyHintService()
    req_body = flask.request.get_json()
    response = svc.query_collection(req_body)
    return response

# .....................................................................................
@app.route('/api/v2/tree/<string:identifier>', methods=['GET', 'POST', 'DELETE'])
def tree(identifier):
    """Tree API service for GET, POST, and DELETE operations on Trees

    Args:
        identifier (str): A tree identifier to search for.

    Returns:
        dict: For GET and POST operations, zero or more dictionaries of metadata for the requested or 
        posted record(s); for DELETE operations, True or False for success        
    """
    svc = TreeService()
    user_id = svc.get_user()
    
    if flask.request.method == 'POST' and flask.request.is_json:
        tree_data = flask.request.get_json()
        svc.post_tree(user_id, tree_data)

    elif flask.request.method == 'DELETE':
        svc.delete_tree(user_id, identifier)
    
    elif flask.request.method == 'GET':
        after_time = flask.request.args.get('after_time', default = None, type = float)
        before_time = flask.request.args.get('before_time', default = None, type = float)
        is_binary = flask.request.args.get('is_binary', default = None, type = bool)
        is_ultrametric = flask.request.args.get('is_ultrametric', default = None, type = bool)
        has_branch_lengths = flask.request.args.get('has_branch_lengths', default = None, type = bool)
        meta_string = flask.request.args.get('meta_string', default = None, type = str)
        name = flask.request.args.get('name', default = None, type = str)
        limit = flask.request.args.get('limit', default = 100, type = int)
        offset = flask.request.args.get('offset', default = 0, type = int)

        if identifier is None:
            response = svc.list_trees(
                user_id, after_time=after_time, before_time=before_time, is_binary=is_binary, 
                is_ultrametric=is_ultrametric, has_branch_lengths=has_branch_lengths, meta_string=meta_string, 
                name=name, limit=limit, offset=offset)

        elif identifier.lower() == 'count':
            response = svc.count_trees(
                user_id, after_time=after_time, before_time=before_time, is_binary=is_binary, 
                is_ultrametric=is_ultrametric, has_branch_lengths=has_branch_lengths, meta_string=meta_string, 
                name=name)

        else:
            try:
                tree_id = int(identifier)
            except:
                return BadRequest('{} is not a valid tree ID'.format(identifier))
            else:
                response = svc.get_tree(user_id, tree_id)
                
    return response

# .....................................................................................
def allowed_file(filename):
    allowed_exts = {'zip', 'json', 'tif', 'tiff', 'asc'}
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in allowed_exts 


@app.route('/api/v2/upload', methods=['GET', 'POST'])
def upload():
    svc = UserUploadService()

    file_name = flask.request.args.get('file_name', default = None, type = str)
    upload_type = flask.request.args.get('upload_type', default = None, type = str)
    metadata = flask.request.args.get('metadata', default = None, type = str)

    if flask.request.method == 'POST':
        # check if the post flask.request has the file part
        if 'file' not in flask.request.files:
            flask.flash('No file part')
            return flask.redirect(flask.request.url)

        upload_file = flask.request.files['file']
        # If the user does not select a file, the browser submits an empty file without a filename.
        if upload_file.filename == '':
            flask.flash('No selected file')
            return flask.redirect(flask.request.url)
        
        if upload_file:
            if allowed_file(upload_file.filename):
                if file_name is not None:
                    safe_filename = secure_filename(file_name)
                else:
                    safe_filename = secure_filename(upload_file.filename)
                
                try:
                    data = upload_file.file.read()
                except Exception as e:
                    raise BadRequest('Unable to read uploaded file ({})'.str(e))
                
        else:
            try:
                data = flask.request.get_data()
            except: 
                raise BadRequest('Unable to read data from flask.request')

    return svc.post_data(safe_filename, upload_type, metadata, data)


# .....................................................................................
@app.route('/api/v2/upload_file', methods=['GET', 'POST'])
def upload_file():
    """Test implementation from https://flask.palletsprojects.com/en/2.0.x/patterns/fileuploads/"""
    if flask.request.method == 'POST':
        # check if the post flask.request has the file part
        if 'file' not in flask.request.files:
            flask.flash('No file part')
            return flask.redirect(flask.request.url)
        
        file = flask.request.files['file']
        # If the user does not select a file, the browser submits an empty file without a filename.
        if file.filename == '':
            flask.flash('No selected file')
            return flask.redirect(flask.request.url)
        
        if file and allowed_file(file.filename):
            filename = secure_filename(file.filename)
            file.save(os.path.join(app.config['UPLOAD_FOLDER'], filename))
            return flask.redirect(flask.url_for('download_file', name=filename))
        
    return 
    
    '''
    <!doctype html>
    <title>Upload new File</title>
    <h1>Upload new File</h1>
    <form method=post enctype=multipart/form-data>
      <input type=file name=file>
      <input type=submit value=Upload>
    </form>
    '''
