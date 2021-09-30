"""Module containing functions for creating output package files

Todo:
    Split up some functions into smaller chunks
"""
from collections import defaultdict
import csv
from io import BytesIO, StringIO
import json
import os
import zipfile

import cherrypy
import mapscript

from lmpy import Matrix

from LmCommon.common.lm_xml import tostring
from LmCommon.common.lmconstants import (
    HTTPStatus, JobStatus, LMFormat, MatrixType, PamStatKeys, ENCODING)
from LmServer.common.data_locator import EarlJr
from LmServer.common.lmconstants import MAP_TEMPLATE, LMFileType
from LmServer.common.log import WebLogger
from LmServer.db.borg_scribe import BorgScribe
from LmServer.legion.gridset import Gridset
from LmWebServer.common.lmconstants import (
    DYN_PACKAGE_DIR, GRIDSET_DIR, MATRIX_DIR, MAX_PROJECTIONS, SDM_PRJ_DIR,
    STATIC_PACKAGE_PATH)
from LmWebServer.formatters.eml_formatter import make_eml
from LmWebServer.formatters.geo_json_formatter import geo_jsonify_flo
from LmWebServer.formatters.template_filler import TemplateFiller

# # ...........................................................................
PACKAGE_VERSION = '1.0.2'


# .............................................................................
def get_map_content_for_proj(prj, scribe):
    """Get a map image for a projection
    """
    ows_req = mapscript.OWSRequest()
    earl_jr = EarlJr()
    map_filename = earl_jr.get_map_filename_from_map_name(prj.map_name, prj.get_user_id())
    if not os.path.exists(map_filename):
        map_svc = scribe.get_map_service_from_map_filename(map_filename)
        if map_svc is not None and map_svc.count > 0:
            map_svc.write_map()
    map_params = [
        ('map', prj.map_name),
        ('bbox', str(prj.bbox).strip('(').strip(')')),
        ('height', '500'),
        ('layers', 'bmng,{}'.format(prj.map_layername)),
        ('request', 'GetMap'),
        ('format', 'image/png'),
        ('service', 'WMS'),
        ('srs', 'EPSG:{}'.format(prj.epsg_code)),
        ('styles', ''),
        ('version', '1.1.0'),
        ('width', '1000')
    ]
    for k, val in map_params:
        if val is not None:
            ows_req.setParameter(k, str(val))
    map_obj = mapscript.mapObj(map_filename)
    # TODO: Color
    mapscript.msIO_installStdoutToBuffer()
    _result = map_obj.OWSDispatch(ows_req)
    _content_type = mapscript.msIO_stripStdoutBufferContentType()
    content = mapscript.msIO_getStdoutBufferBytes()
    mapscript.msIO_resetHandlers()
    return content


# .............................................................................
def create_stats_meta():
    """Create a statistic metadata lookup for all stats
    """
    return {
        PamStatKeys.ALPHA: {
            'name': 'Alpha Diversity',
            'description': (
                'Alpha Diversity is the species richness (number of species '
                'present) per site')
        },
        PamStatKeys.ALPHA_PROP: {
            'name': 'Proportional Alpha Diversity',
            'description': (
                'Proportional Alpha Diversity is the proportion of the entire '
                'population of species that are present per site')
        },
        PamStatKeys.PHI: {
            'name': 'Range Size Per Site',
            'description': (
                'Range Size per site is the sum of the range sizes of each '
                'species present at each site')
        },
        PamStatKeys.PHI_AVG_PROP: {
            'name': 'Proportional Range Size Per Site',
            'description': (
                'Proportional range size per site is the sum of the range '
                'sizes at each site as a proportion of the sum of the ranges '
                'of all species in the study pool')
        },
        PamStatKeys.MNTD: {
            'name': 'Mean Nearest Taxon Distance',
            'description': (
                'Mean Nearest Taxon Distance is the average of the distance '
                'between each present species and the (phylogenetically) '
                'nearest present species for each site')
        },
        PamStatKeys.MPD: {
            'name': 'Mean Pairwise Distance',
            'description': (
                'Mean pairwise distance is the average phylogenetic distance '
                'between all species present at each site')
        },
        PamStatKeys.PEARSON: {
            'name': "Pearson's Correlation Coefficient",
            'description': ''
        },
        PamStatKeys.PD: {
            'name': 'Phylogenetic Diversity',
            'description': (
                'Phylogenetic Diversity is the sum of the branch lengths for '
                'the minimum spanning tree for all species at a site')
        },
        PamStatKeys.MNND: {
            'name': 'Mean Nearest Neighbor Distance',
            'description': (
                'Mean nearest neighbor distance is the average phylogenetic '
                'distance to the nearest neighbor of each species present at '
                'a site')
        },
        PamStatKeys.MPHYLODIST: {
            'name': 'Mean Phylogenetic Distance',
            'description': (
                'Mean phylogenetic distance is the average phylogenetic '
                'distance between all species present at a site')
        },
        PamStatKeys.SPD: {
            'name': 'Sum of Phylogenetic Distance',
            'description': (
                'Sum of phylogenetic distance is the total phylogenetic '
                'distance between all species present at a site')
        },
        PamStatKeys.OMEGA: {
            'name': 'Species Range Size',
            'description': (
                'Species range size is the number of sites where each species '
                'is present')
        },
        PamStatKeys.OMEGA_PROP: {
            'name': 'Proportional Species Range Size',
            'description': (
                'Proportional species range size the the proportion of the '
                'number of sites where each species is present to the total '
                'number of sites in the study area')
        },
        # TODO: Verify and clarify
        PamStatKeys.PSI: {
            'name': 'Species Range Richness',
            'description': (
                'Species range richness is the sum of the range sizes of '
                'all of the species present at a site')
        },
        # TODO: Verify and clarify
        PamStatKeys.PSI_AVG_PROP: {
            'name': 'Mean Proportional Species Diversity',
            'description': (
                'Mean Proportional Species Diversity is the average species '
                'range richness proportional to the total species range '
                'richness')
        },
        PamStatKeys.WHITTAKERS_BETA: {
            'name': 'Whittaker\'s Beta Diversity',
            'description': 'Whittaker\'s Beta Diversity'
        },
        PamStatKeys.LANDES_ADDATIVE_BETA: {
            'name': 'Landes Addative Beta Diveristy',
            'description': 'Landes Addative Beta Diversity'
        },
        PamStatKeys.LEGENDRES_BETA: {
            'name': 'Legendres Beta Diversity',
            'description': 'Legendres Beta Diversity'
        },
        PamStatKeys.SITES_COVARIANCE: {
            'name': 'Sites Covariance',
            'description': 'Sites covariance'
        },
        PamStatKeys.SPECIES_COVARIANCE: {
            'name': 'Species Covariance',
            'description': 'Species covariance'
        },
        PamStatKeys.SPECIES_VARIANCE_RATIO: {
            'name': 'Schluter\'s Species Variance Ratio',
            'description': 'Schluter\'s species covariance'
        },
        PamStatKeys.SITES_VARIANCE_RATIO: {
            'name': 'Schluter\'s Sites Variance Ratio',
            'description': 'Schluter\'s sites covariance'
        }
    }


# .............................................................................
def create_header_lookup(headers, squids=False, scribe=None, user_id=None):
    """Generate a header lookup to be included in the package metadata
    """

    def get_header_dict(header, idx):
        return {
            'header': header,
            'index': idx
        }

    def get_squid_header_dict(header, idx, scribe, user_id):
        taxon = scribe.get_taxon(squid=header, user_id=user_id)
        ret = get_header_dict(header, idx)

        for attrib, key in [
                ('scientific_name', 'scientific_name'),
                ('canonical_name', 'canonical_name'),
                ('rank', 'taxon_rank'),
                ('kingdom', 'taxon_kingdom'),
                ('phylum', 'taxon_phylum'),
                ('class_', 'taxon_class'),
                ('order_', 'taxon_order'),
                ('family', 'taxon_family'),
                ('genus', 'taxon_genus')]:
            val = getattr(taxon, attrib)
            if val is not None:
                ret[key] = val
        return ret

    if squids and scribe and user_id:
        return [
            get_squid_header_dict(
                headers[i], i, scribe, user_id) for i in range(len(headers))]
    return [get_header_dict(headers[i], i) for i in range(len(headers))]


# .............................................................................
def mung(data):
    """Replace a list of values with a map of non-zero values
    """
    munged = defaultdict(list)
    for i, datum in enumerate(data):
        if datum != 0:
            munged[datum].append(i)
    return munged


# .............................................................................
def _add_sdms_to_package(zip_f, projections, scribe):
    """Adds SDMs to output package and returns projection info.

    Args:
        zip_f (ZipFile): An open zip file object to add the SDMs to.
        projections (:obj:`list` of :obj:`SDMProject`): A list of projection
            objects to add to the package.
        scribe (BorgScribe): A scribe object for database queries.

    Returns:
        tuple - A tuple of occurrence set and projection info lists.
    """
    added_occ_ids = []
    occ_info = []
    prj_info = []

    sniffer = csv.Sniffer()

    for prj in projections:
        occ = prj.occ_layer
        prj_dir = os.path.join(SDM_PRJ_DIR, occ.display_name)
        # Make sure projection output file exists, then add to package
        if os.path.exists(prj.get_dlocation()):
            arc_prj_path = os.path.join(
                prj_dir, os.path.basename(prj.get_dlocation()))
            arc_prj_img_path = arc_prj_path.replace(LMFormat.GTIFF.ext, '.png')
            zip_f.write(prj.get_dlocation(), arc_prj_path)
            zip_f.writestr(
                arc_prj_img_path, get_map_content_for_proj(prj, scribe))
            scn = prj.proj_scenario
            prj_info.append(
                {
                    'prj_id': prj.get_id(),
                    'file_path': arc_prj_path,
                    'image_path': arc_prj_img_path,
                    'scenario_code': prj.proj_scenario_code,
                    'species_name': prj.species_name,
                    'algorithm_code': prj.algorithm_code,
                    'gcm_code': scn.gcm_code,
                    'alt_pred_code': scn.alt_pred_code,
                    'date_code': scn.date_code,
                    'epsg': prj.epsg_code,
                    'label': '{} {} {} {}'.format(
                        prj.display_name, prj.algorithm_code,
                        prj.proj_scenario_code, arc_prj_path)
                })

        # Add occurrence set
        if occ.get_id() not in added_occ_ids:
            arc_occ_path = os.path.join(
                prj_dir, '{}.csv'.format(occ.display_name))
            sys_occ_path = '{}{}'.format(
                os.path.splitext(occ.get_dlocation())[0], LMFormat.CSV.ext)

            # string io object
            occ_string_io = BytesIO()
            headers = list(occ.get_feature_attributes().items())
            with open(sys_occ_path) as in_f:
                # Get Delimiter
                dialect = sniffer.sniff(in_f.read(32))
                in_f.seek(0)
                delimiter = dialect.delimiter

                # Write header line
                header_line = delimiter.join(
                    [i[1][0] for i in sorted(headers)])
                occ_string_io.write(
                    '{}\n'.format(header_line).encode(ENCODING))
                # Write the rest of the lines
                for line in in_f:
                    occ_string_io.write(line.encode(ENCODING))
            occ_string_io.seek(0)
            zip_f.writestr(arc_occ_path, occ_string_io.getvalue())
            occ_string_io = None
            # zip_f.write(sys_occ_path, arc_occ_path)
            added_occ_ids.append(occ.get_id())
            occ_info.append(
                {
                    'occ_id': occ.get_id(),
                    'species_name': occ.display_name,
                    'num_points': occ.query_count,
                    'file_path': arc_occ_path
                })

    return occ_info, prj_info


# .............................................................................
def _get_known_package_files():
    """Returns known package files, both static and templated.

    Returns:
        (list, list): A list of static files and a list of template files.
    """
    static_files = []
    template_files = []
    for f_dir, _, fns in os.walk(STATIC_PACKAGE_PATH):
        for file_name in fns:
            a_path = os.path.join(f_dir, file_name)
            if a_path.lower().endswith('.template'):
                template_files.append(a_path)
            else:
                static_files.append(a_path)
    return (static_files, template_files)


# .............................................................................
def _package_gridset(gridset, include_csv=False, include_sdm=False):
    """Create a gridset package
    """
    # Initialization
    # --------------
    package_filename = gridset.get_package_location()
    scribe = BorgScribe(WebLogger())
    user_id = gridset.get_user_id()
    occ_info = None
    prj_info = None
    tree = None
    try:
        gs_name = gridset.name
    except AttributeError:
        gs_name = 'Gridset {}'.format(gridset.get_id())
    shapegrid = gridset.get_shapegrid()
    matrices = gridset.get_matrices()
    do_mcpa = False
    do_pam_stats = False
    pam = None
    anc_pam = None
    sites_cov_obs = None
    sites_obs = None
    mcpa_mtx = None
    csvs_in_folder = False

    # Open zip file
    with zipfile.ZipFile(
            package_filename, mode='w', compression=zipfile.ZIP_DEFLATED,
            allowZip64=True) as zip_f:

        # Add SDMs if we should
        # ---------------------
        if include_sdm:
            projections = scribe.list_sdm_projects(
                0, MAX_PROJECTIONS, user_id=user_id,
                after_status=JobStatus.COMPLETE - 1,
                before_status=JobStatus.COMPLETE + 1,
                gridset_id=gridset.get_id(), atom=False)
            occ_info, prj_info = _add_sdms_to_package(
                zip_f, projections, scribe)

        # Loop through matrices
        #    Add csvs if necessary
        #    Populate variables
        for mtx in matrices:
            # Only add if matrix is complete and observed scenario
            if mtx.status == JobStatus.COMPLETE and mtx.date_code == 'Curr':
                # Handle each matrix type
                if mtx.matrix_type in [MatrixType.PAM, MatrixType.ROLLING_PAM]:
                    pam = Matrix.load(mtx.get_dlocation())
                    csv_mtx_fn = os.path.join(
                        MATRIX_DIR, 'pam_{}.csv'.format(mtx.get_id()))
                elif mtx.matrix_type == MatrixType.ANC_PAM:
                    anc_pam = Matrix.load(mtx.get_dlocation())
                    csv_mtx_fn = os.path.join(
                        MATRIX_DIR, 'anc_pam_{}.csv'.format(mtx.get_id()))
                elif mtx.matrix_type == MatrixType.SITES_COV_OBSERVED:
                    sites_cov_obs = Matrix.load(mtx.get_dlocation())
                    csv_mtx_fn = os.path.join(
                        MATRIX_DIR, 'sitesCovarianceObserved_{}.csv'.format(
                            mtx.get_id()))
                elif mtx.matrix_type == MatrixType.SITES_OBSERVED:
                    sites_obs = Matrix.load(mtx.get_dlocation())
                    csv_mtx_fn = os.path.join(
                        MATRIX_DIR, 'sitesObserved_{}.csv'.format(
                            mtx.get_id()))
                    do_pam_stats = True
                elif mtx.matrix_type == MatrixType.MCPA_OUTPUTS:
                    mcpa_mtx = Matrix.load(mtx.get_dlocation())
                    csv_mtx_fn = os.path.join(
                        MATRIX_DIR, 'mcpa_{}.csv'.format(mtx.get_id()))
                    do_mcpa = True
                else:
                    csv_mtx_fn = os.path.join(
                        MATRIX_DIR, '{}.csv'.format(
                            os.path.splitext(
                                os.path.basename(mtx.get_dlocation()))[0]))

                # If we should write the CSV file, and the matrix exists, do it
                if include_csv and os.path.exists(mtx.get_dlocation()):
                    mtx_obj = Matrix.load(mtx.get_dlocation())
                    csv_mtx_str = StringIO()
                    mtx_obj.write_csv(csv_mtx_str)
                    csv_mtx_str.seek(0)
                    zip_f.writestr(csv_mtx_fn, csv_mtx_str.getvalue())
                    csv_mtx_str = None
                    csvs_in_folder = True

        # Add Generated Files
        # -------------------
        # Gridset EML
        gs_eml = tostring(make_eml(gridset))
        zip_f.writestr(
            os.path.join(
                GRIDSET_DIR, 'gridset_{}.eml'.format(
                    gridset.get_id())), gs_eml)

        # Tree
        if gridset.tree is not None and \
                gridset.tree.get_dlocation() is not None:
            tree = gridset.tree
            zip_f.write(
                tree.get_dlocation(), os.path.join(GRIDSET_DIR, 'tree.nex'))

        # Known package files
        # -------------------
        static_files, template_files = _get_known_package_files()

        # Add static files
        for static_file_name in static_files:
            r_path = static_file_name.replace(STATIC_PACKAGE_PATH, '')
            zip_f.write(static_file_name, r_path)

        # Process template files
        for template_fn in template_files:
            # Remove .template and leading static package path
            r_path = template_fn.replace(
                '.template', '').replace(
                    STATIC_PACKAGE_PATH, '').replace('//', '/')
            if r_path.startswith('/'):
                r_path = r_path[1:]
            # Should we write this template?  Default to true and change if
            #    data will not support it.
            write_template = True
            # TODO: See if there is a better way to determine what variables to
            #    send to template without just sending everything and wasting
            #    memory (would require all all file content to be generated at
            #    once and passed around, potentially hundreds of MB or more).
            if r_path.endswith('index.html'):
                temp_filler = TemplateFiller(
                    gridset_name=gs_name, do_mcpa=do_mcpa,
                    do_map_stats=do_pam_stats, do_sdm=include_sdm,
                    do_csv=csvs_in_folder, sdm_prj_dir=SDM_PRJ_DIR,
                    matrix_dir=MATRIX_DIR, package_version=PACKAGE_VERSION)
            elif r_path.endswith('browse_maps.html'):
                if prj_info is not None:
                    temp_filler = TemplateFiller(
                        gridset_name=gs_name, projections=prj_info,
                        package_version=PACKAGE_VERSION)
                else:
                    write_template = False
            elif r_path.endswith('statsHeatMap.html'):
                if do_pam_stats:
                    temp_filler = TemplateFiller(
                        gridset_name=gs_name, package_version=PACKAGE_VERSION)
                else:
                    write_template = False
            elif r_path.endswith('statsTreeMap.html'):
                if do_mcpa:
                    temp_filler = TemplateFiller(
                        gridset_name=gs_name, package_version=PACKAGE_VERSION)
                else:
                    write_template = False
            elif r_path.endswith('sdm_info.js'):
                if occ_info is not None and prj_info is not None:
                    temp_filler = TemplateFiller(
                        sdm_info=json.dumps({
                            'projections': prj_info,
                            'occurrences': occ_info
                        }, indent=4))
                else:
                    write_template = False
            elif r_path.endswith('tree.js'):
                if tree is not None:
                    with open(tree.get_dlocation()) as tree_file:
                        # Fill in tree string value this way so there are no
                        #    additional references to it and it will be cleaned
                        #    up at next loop iteration
                        temp_filler = TemplateFiller(
                            tree_string=tree_file.read())
                else:
                    write_template = False
            elif r_path.endswith('statNameLookup.json'):
                temp_filler = TemplateFiller(
                    stats_json=json.dumps(create_stats_meta(), indent=4))
            elif r_path.endswith('pam.js'):
                if pam is not None:
                    header_lookup_fn = os.path.join(
                        DYN_PACKAGE_DIR, 'squidLookup.json')
                    pam_str = StringIO()
                    geo_jsonify_flo(
                        pam_str, shapegrid.get_dlocation(), matrix=pam,
                        mtx_join_attrib=0, ident=0,
                        header_lookup_filename=header_lookup_fn,
                        transform=mung)
                    pam_str.seek(0)
                    temp_filler = TemplateFiller(pam_json=pam_str.getvalue())
                    pam_str = None
                else:
                    write_template = False
            elif r_path.endswith('squidLookup.json'):
                if pam is not None:
                    temp_filler = TemplateFiller(
                        squid_lookup=json.dumps(
                            create_header_lookup(
                                pam.get_column_headers(), squids=True,
                                scribe=scribe, user_id=user_id), indent=4))
                else:
                    write_template = False
            elif r_path.endswith('nodeLookup.js'):
                if anc_pam is not None:
                    temp_filler = TemplateFiller(
                        node_lookup_json=json.dumps(
                            create_header_lookup(
                                anc_pam.get_column_headers()), indent=4))
                else:
                    write_template = False
            elif r_path.endswith('ancPam.js'):
                if anc_pam is not None:
                    header_lookup_fn = os.path.join(
                        DYN_PACKAGE_DIR, 'nodeLookup.json')
                    anc_pam_str = StringIO()
                    geo_jsonify_flo(
                        anc_pam_str, shapegrid.get_dlocation(), matrix=anc_pam,
                        mtx_join_attrib=0, ident=0,
                        header_lookup_filename=header_lookup_fn,
                        transform=mung)
                    anc_pam_str.seek(0)
                    temp_filler = TemplateFiller(
                        anc_pam_json=anc_pam_str.getvalue())
                    anc_pam_str = None
                else:
                    write_template = False
            elif r_path.endswith('mcpaMatrix.js'):
                if mcpa_mtx is not None:
                    mtx_str = StringIO()
                    mcpa_mtx.write_csv(mtx_str)
                    mtx_str.seek(0)
                    temp_filler = TemplateFiller(mcpa_csv=mtx_str.getvalue())
                    mtx_str = None
                else:
                    write_template = False
            elif r_path.endswith('sitesCovarianceObserved.js'):
                if sites_cov_obs is not None:
                    mtx_str = StringIO()
                    mtx_2d = Matrix(
                        sites_cov_obs[:, :, 0],
                        headers={
                            '0': sites_cov_obs.getRowHeaders(),
                            '1': sites_cov_obs.getColumnHeaders()})
                    geo_jsonify_flo(
                        mtx_str, shapegrid.get_dlocation(), mtx_2d,
                        mtx_join_attrib=0, ident=0)
                    mtx_str.seek(0)
                    temp_filler = TemplateFiller(
                        sites_cov_obs_json=mtx_str.getvalue())
                    mtx_str = None
                else:
                    write_template = False
            elif r_path.endswith('sitesObserved.js'):
                if sites_obs is not None:
                    mtx_str = StringIO()
                    mtx_2d = Matrix(
                        sites_obs[:, :, 0],
                        headers={
                            '0': sites_obs.get_row_headers(),
                            '1': sites_obs.get_column_headers()})
                    geo_jsonify_flo(
                        mtx_str, shapegrid.get_dlocation(), mtx_2d,
                        mtx_join_attrib=0, ident=0)
                    mtx_str.seek(0)
                    temp_filler = TemplateFiller(
                        sites_obs_json=mtx_str.getvalue())
                    mtx_str = None
                else:
                    write_template = False

            # Fill in and write template if we should
            if write_template:
                with open(template_fn) as in_file:
                    template_str = in_file.read()
                zip_f.writestr(
                    r_path, temp_filler.fill_templated_string(template_str))


# .............................................................................
def summarize_object_statuses(summary):
    """Summarizes a summary

    Args:
        summary (:obj:`list` of :obj:`tuple` of :obj:`int`, :obj:`int`): A list
            of (status, count) tuples for an object type
    """
    complete = 0
    waiting = 0
    running = 0
    error = 0
    total = 0
    for status, count in summary:
        if status <= JobStatus.INITIALIZE:
            waiting += count
        elif status < JobStatus.COMPLETE:
            running += count
        elif status == JobStatus.COMPLETE:
            complete += count
        else:
            error += count
        total += count
    return (waiting, running, complete, error, total)


# .............................................................................
def gridset_package_formatter(gridset, include_csv=True, include_sdm=True,
                              stream=False):
    """Create a Gridset download package for the user to explore locally
    """
    # Check that it is a gridset
    if not isinstance(gridset, Gridset):
        raise cherrypy.HTTPError(
            HTTPStatus.BAD_REQUEST,
            'Only gridsets can be formatted as a package')

    package_filename = gridset.get_package_location()

    # Check to see if the package does not exist
    if not os.path.exists(package_filename):

        # Look for makeflows
        scribe = BorgScribe(WebLogger())
        scribe.open_connections()

        # Check progress counts
        gridset_id = gridset.get_id()
        # prj_summary = scribe.summarizeSDMProjectsForGridset(gridset_id)
        # mtx_summary = scribe.summarizeMatricesForGridset(gridset_id)
        mf_summary = scribe.summarize_mf_chains_for_gridset(gridset_id)
        # mc_summary = scribe.summarizeMtxColumnsForGridset(gridset_id)
        # occ_summary = scribe.summarizeOccurrenceSetsForGridset(gridset_id)

        # (waiting_prjs, running_prjs, complete_prjs, error_prjs, total_prjs
        # ) = summarize_object_statuses(prj_summary)
        # (waiting_mtxs, running_mtxs, complete_mtxs, error_mtxs, total_mtxs
        # ) = summarize_object_statuses(mtx_summary)
        (waiting_mfs, running_mfs, _, _, _
         ) = summarize_object_statuses(mf_summary)
        # (waiting_occs, running_occs, complete_occs, error_occs, total_occs
        # ) = summarize_object_statuses(occ_summary)
        # (waiting_mcs, running_mcs, complete_mcs, error_mcs, total_mcs
        # ) = summarize_object_statuses(mc_summary)
        scribe.close_connections()

        cnt = waiting_mfs + running_mfs

        # See if we can create it
        # CJG - Assume we can create package if no makeflows
        # TODO: Check other objects and raise error status if necessary
        # if cnt == 0 and all([mtx.status >= JobStatus.COMPLETE for \
        #        mtx in gridset.getMatrices() if mtx.matrixType not in [
        #            MatrixType.PAM, MatrixType.ROLLING_PAM]]):
        # Assume we will never be able to create package if makeflow errors
        # if error_mfs > 0
        if cnt == 0:
            # Create the package
            _package_gridset(
                gridset, include_csv=include_csv, include_sdm=include_sdm)
        else:
            # Not ready, so just return HTTP ACCEPTED
            cherrypy.response.status = HTTPStatus.ACCEPTED
            return None

    # Package exists, return it
    keep_chars = (' ', '.', '_')
    # Sanitize the gridset name for filename construction
    sanitized_gridset_name = ''.join(
        [c for c in gridset.name if c.isalnum() or c in keep_chars]).rstrip()
    if len(sanitized_gridset_name) == 0:
        sanitized_gridset_name = 'gridset-{}'.format(gridset.get_id())
    out_package_name = '{}{}'.format(sanitized_gridset_name, LMFormat.ZIP.ext)
    cherrypy.response.headers[
        'Content-Disposition'] = 'attachment; filename="{}"'.format(
            out_package_name)
    cherrypy.response.headers['Content-Type'] = LMFormat.ZIP.get_mime_type()

    if stream:
        cherrypy.lib.file_generator(open(package_filename, 'rb'))
    else:
        with open(package_filename, 'rb') as package_file:
            cnt = package_file.read()
        return cnt
    return None
