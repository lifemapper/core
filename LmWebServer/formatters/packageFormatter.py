"""Module containing functions for creating output package files

Todo:
    Split up some functions into smaller chunks
"""
import cherrypy
from collections import defaultdict
import json
import os
import mapscript
from StringIO import StringIO
import zipfile

from LmCommon.common.lmconstants import LMFormat, MatrixType, JobStatus,\
    HTTPStatus, PamStatKeys
from LmCommon.common.matrix import Matrix
from LmCommon.common.lmXml import tostring

from LmServer.common.log import LmPublicLogger
from LmServer.db.borgscribe import BorgScribe
from LmServer.legion.gridset import Gridset

from LmWebServer.formatters.emlFormatter import makeEml
from LmWebServer.formatters.geoJsonFormatter import geoJsonify_flo
from LmWebServer.common.lmconstants import (DYN_PACKAGE_DIR, STATIC_PACKAGE_PATH,
                        GRIDSET_DIR, MATRIX_DIR, SDM_PRJ_DIR, MAX_PROJECTIONS )
from LmServer.common.datalocator import EarlJr
from LmServer.common.lmconstants import MAP_TEMPLATE


# # .............................................................................
# # TODO: Move to lmconstants
# GRIDSET_DIR = 'gridset'
# MATRIX_DIR = os.path.join(GRIDSET_DIR, 'matrix')
# # TODO: Assmble from constants
# STATIC_PACKAGE_PATH = '/opt/lifemapper/LmWebServer/assets/gridset_package'
# DYN_PACKAGE_DIR = 'package'
# SDM_PRJ_DIR = os.path.join(GRIDSET_DIR, 'sdm')
# MAX_PROJECTIONS = 1000

# .............................................................................
def get_map_content_for_proj(prj, scribe):
    """Get a map image for a projection
    """
    ows_req = mapscript.OWSRequest()
    earl_jr = EarlJr(scribe=scribe)
    map_filename = earl_jr.getMapFilenameFromMapname(prj.mapName)
    if not os.path.exists(map_filename):
        map_svc = scribe.getMapServiceFromMapFilename(map_filename)
        if map_svc is not None and map_svc.count > 0:
            map_svc.writeMap(MAP_TEMPLATE)
    map_params = [
        ('map', prj.mapName),
        ('bbox', str(prj.bbox).strip('(').strip(')')),
        #('bgcolor', bgcolor),
        #('coverage', coverage),
        #('crs', crs),
        #('exceptions', exceptions),
        ('height', '250'),
        #('layer', layer),
        ('layers', 'bmng,{}'.format(prj.mapLayername)),
        #('point', point),
        ('request', 'GetMap'),
        ('format', 'image/png'),
        ('service', 'WMS'),
        ('srs', 'EPSG:{}'.format(prj.epsgcode)),
        ('styles', ''),
        #('transparent', transparent),
        ('version', '1.1.0'),
        ('width', '500')
    ]
    for k, v in map_params:
        if v is not None:
            ows_req.setParameter(k, str(v))
    map_obj = mapscript.mapObj(map_filename)
    # TODO: Color
    mapscript.msIO_installStdoutToBuffer()
    result = map_obj.OWSDispatch(ows_req)
    content_type = mapscript.msIO_stripStdoutBufferContentType()
    content = mapscript.msIO_getStdoutBufferBytes()
    mapscript.msIO_resetHandlers()
    return content

# .............................................................................
def get_sdm_html_page(prj_info):
    """Generate a (temporary) HTML page for viewing SDM outputs
    """
    num_prjs = len(prj_info)
    prj_images = []
    prj_labels = []
    for prj in prj_info:
        prj_images.append('<img src="{}" alt="{} - {} - {}" />'.format(
            prj['image_path'], prj['species_name'], prj['algorithm_code'],
            prj['scenario_code']))
        prj_labels.append('{} - {} - {}<br />{}'.format(
            prj['species_name'], prj['algorithm_code'], prj['scenario_code'],
            prj['image_path']))
    prj_images.append('')
    prj_labels.append('')
    
    page_html = """\
<html>
    <head>
        <title>BiotaPhy Browse Maps</title>
    </head>
    <body>
        <h2>BiotaPhy Browse Maps</h2>
        <table>
"""
    for i in range(0, num_prjs, 2):
        page_html += """\
            <tr>
                <td style="text-align: center;">
                    {}
                </td>
                <td style="text-align: center;">
                    {}
                </td>
            </tr>
            <tr>
                <th style="text-align: center;">
                    {}
                </th>
                <th style="text-align: center;">
                    {}
                </th>
            </tr>\n""".format(
                prj_images[i], prj_images[i+1], prj_labels[i], prj_labels[i+1])
    page_html += """\
        </table>
    </body>
</html>"""
    return page_html

# .............................................................................
def createIndexHtml(gridset_name, do_mcpa, do_pam_stats, do_sdm, do_csv):
    """
    @summary: Generate an index.html page for the package
    """
    mcpa_thumb = ''
    mcpa_link = ''
    map_stats_thumb = ''
    map_stats_link = ''
    sdm_text = ''
    csv_text = ''
    sdm_browse_thumb = ''
    sdm_browse_link = ''
    
    if do_mcpa:
        mcpa_thumb = """
                <td style="padding: 25px;">
                    <img src="./images/mcpa_thumb.png" alt="MCPA Thumbnail" />
                </td>
"""
        mcpa_link = """
                <td style="text-align: center;">
                    <a href="./statsTreeMap.html">MCPA with modeled data</a>
                </td>
"""

    if do_pam_stats:
        map_stats_thumb = """
                <td style="padding: 25px;">
                    <img src="./images/map_statistics_thumb.png" alt="Site Maps" />
                </td>
"""
        map_stats_link = """
                <td style="text-align: center;">
                    <a href="./statsHeatMap.html">Modeled Map Statistics</a>
                </td>
"""

    if do_sdm:
        sdm_text = """
            <p>
                SDM Model Projections, found in the {} directory 
            </p>
""".format(SDM_PRJ_DIR)
        sdm_browse_thumb = """
                <td style="padding: 25px;">
                    <img src="./images/browse_maps_thumb.png" alt="Browse Maps" />
                </td>
"""
        sdm_browse_link = """
                <td style="text-align: center;">
                    <a href="./browse_maps.html">Browse Model Maps</a>
                </td>
"""

    if do_csv:
        csv_text = """
            <p>
                Matrix CSV files, found in the {} directory 
            </p>
""".format(MATRIX_DIR)
    
    return """\
<!doctype html>
<html>
    <head>
        <meta charset="UTF-8">
        <title>BiotaPhy Results Package</title>
    </head>
    <body>
        <h2>BiotaPhy Results package 1.0.0</h2>
        <h1>Analysis Results for gridset {}</h1>
        <h3>These results were generated by the BiotaPhy project, a collaboration of</h3>
        <table>
            <tr>
                <td>
                    <img src="./images/idigbio_logo.png" alt="iDigBio" />
                </td>
                <td>
                    <img src="./images/lm_logo.png" alt="Lifemapper" />
                </td>
                <td>
                    <img src="./images/otl_logo.png" alt="Open Tree of Life" />
                </td>
            </tr>
            <tr>
                <td style="text-align: center;">
                    iDigBio
                </td>
                <td style="text-align: center;">
                    Lifemapper
                </td>
                <td style="text-align: center;">
                    Open Tree of Life
                </td>
            </tr>
        </table>
        <br />
        <p>This package contains the following:</p>
        <table>
            <tr>
{}{}{}
            </tr>
            <tr>
{}{}{}
            </tr>
        </table>
{}{}
        <p>* Due to the size of the data these pages may take some time to load.</p>
        <h4>The BiotaPhy project is supported by NSF BIO Award #1458422.</h4>
    </body>
</html>""".format(
    gridset_name, mcpa_thumb, map_stats_thumb, sdm_browse_thumb, mcpa_link,
    map_stats_link, sdm_browse_link, sdm_text, csv_text)
    
# .............................................................................
def createStatsMeta():
    """
    @summary: Create a statistic metadata lookup for all stats
    """
    return {
        PamStatKeys.ALPHA : {
            'name' : 'Alpha Diversity',
            'description' : 'Alpha Diversity is the species richness (number of species present) per site'
        },
        PamStatKeys.ALPHA_PROP : {
            'name' : 'Proportional Alpha Diversity',
            'description' : 'Proportional Alpha Diversity is the proportion of the entire population of species that are present per site'
        },
        PamStatKeys.PHI : {
            'name' : 'Range Size Per Site',
            'description' : 'Range Size per site is the sum of the range sizes of each species present at each site'
        },
        PamStatKeys.PHI_AVG_PROP : {
            'name' : 'Proportional Range Size Per Site',
            'description' : 'Proportional range size per site is the sum of the range sizes at each site as a proportion of the sum of the ranges of all species in the study pool'
        },
        PamStatKeys.MNTD : {
            'name' : 'Mean Nearest Taxon Distance',
            'description' : 'Mean Nearest Taxon Distance is the average of the distance between each present species and the (phylogenetically) nearest present species for each site'
        },
        PamStatKeys.MPD : {
            'name' : 'Mean Pairwise Distance',
            'description' : 'Mean pairwise distance is the average phylogenetic distance between all species present at each site'
        },
        PamStatKeys.PEARSON : {
            'name' : "Pearson's Correlation Coefficient",
            'description' : ''
        },
        PamStatKeys.PD : {
            'name' : 'Phylogenetic Diversity',
            'description' : 'Phylogenetic Diversity is the sum of the branch lengths for the minimum spanning tree for all species at a site'
        },
        PamStatKeys.MNND : {
            'name' : 'Mean Nearest Neighbor Distance',
            'description' : 'Mean nearest neighbor distance is the average phylogenetic distance to the nearest neighbor of each species present at a site'
        },
        PamStatKeys.MPHYLODIST : {
            'name' : 'Mean Phylogenetic Distance',
            'description' : 'Mean phylogenetic distance is the average phylogenetic distance between all species present at a site'
        },
        PamStatKeys.SPD : {
            'name' : 'Sum of Phylogenetic Distance',
            'description' : 'Sum of phylogenetic distance is the total phylogenetic distance between all species present at a site'
        },
        PamStatKeys.OMEGA : {
            'name' : 'Species Range Size',
            'description' : 'Species range size is the number of sites where each species is present'
        },
        PamStatKeys.OMEGA_PROP : {
            'name' : 'Proportional Species Range Size',
            'description' : 'Proportional species range size the the proportion of the number of sites where each species is present to the total number of sites in the study area'
        },
        # TODO: Verify and clarify
        PamStatKeys.PSI : {
            'name' : 'Species Range Richness',
            'description' : 'Species range richness is the sum of the range sizes of all of the species present at a site'
        },
        # TODO: Verify and clarify
        PamStatKeys.PSI_AVG_PROP : {
            'name' : 'Mean Proportional Species Diversity',
            'description' : 'Mean Proportional Species Diversity is the average species range richness proportional to the total species range richness'
        },
        PamStatKeys.WHITTAKERS_BETA : {
            'name' : 'Whittaker\'s Beta Diversity',
            'description' : 'Whittaker\'s Beta Diversity'
        },
        PamStatKeys.LANDES_ADDATIVE_BETA : {
            'name' : 'Landes Addative Beta Diveristy',
            'description' : 'Landes Addative Beta Diversity'
        },
        PamStatKeys.LEGENDRES_BETA : {
            'name' : 'Legendres Beta Diversity',
            'description' : 'Legendres Beta Diversity'
        },
        PamStatKeys.SITES_COVARIANCE : {
            'name' : 'Sites Covariance',
            'description' : 'Sites covariance'
        },
        PamStatKeys.SPECIES_COVARIANCE : {
            'name' : 'Species Covariance',
            'description' : 'Species covariance'
        },
        PamStatKeys.SPECIES_VARIANCE_RATIO : {
            'name' : 'Schluter\'s Species Variance Ratio',
            'description' : 'Schluter\'s species covariance'
        },
        PamStatKeys.SITES_VARIANCE_RATIO : {
            'name' : 'Schluter\'s Sites Variance Ratio',
            'description' : 'Schluter\'s sites covariance'
        }
    }

# .............................................................................
def createStatHeaderLookup():
    """
    @summary: Create a statistic header lookup for all possible stats
    """
    return {
        PamStatKeys.ALPHA : 'Alpha Diversity',
        PamStatKeys.ALPHA_PROP : 'Proportional Alpha Diversity',
        PamStatKeys.PHI : 'Range Size Per Site',
        PamStatKeys.PHI_AVG_PROP : 'Proportional Range Size Per Site',
        PamStatKeys.MNTD : 'Mean Nearest Taxon Distance',
        PamStatKeys.MPD : 'Mean Pairwise Distance',
        PamStatKeys.PEARSON : "Pearson's Correlation Coefficient",
        PamStatKeys.PD : 'Phylogenetic Diversity',
        PamStatKeys.MNND : 'Mean Nearest Neighbor Distance',
        PamStatKeys.MPHYLODIST : 'Mean Phylogenetic Distance',
        PamStatKeys.SPD : 'Sum of Phylogenetic Distance',
        PamStatKeys.OMEGA : 'Species Range Size',
        PamStatKeys.OMEGA_PROP : 'Proportional Species Range Size',
        PamStatKeys.PSI : 'Species Range Richness',
        PamStatKeys.PSI_AVG_PROP : 'Proportional Species Range Richness',
        PamStatKeys.WHITTAKERS_BETA : 'Whittaker\'s Beta Diversity',
        PamStatKeys.LANDES_ADDATIVE_BETA : 'Landes Addative Beta Diveristy',
        PamStatKeys.LEGENDRES_BETA : 'Legendres Beta Diversity',
        PamStatKeys.SITES_COVARIANCE : 'Sites Covariance',
        PamStatKeys.SPECIES_COVARIANCE : 'Species Covariance',
        PamStatKeys.SPECIES_VARIANCE_RATIO : 'Schluter\'s Species Variance Ratio',
        PamStatKeys.SITES_VARIANCE_RATIO : 'Schluter\'s Sites Variance Ratio'
    }

# .............................................................................
def createHeaderLookup(headers, squids=False, scribe=None, userId=None):
    """
    @summary: Generate a header lookup to be included in the package metadata
    """
    def getHeaderDict(header, idx):
        return {
            'header' : header,
            'index' : idx
        }
        
    def getSquidHeaderDict(header, idx, scribe, userId):
        taxon = scribe.getTaxon(squid=header, userId=userId)
        ret = getHeaderDict(header, idx)
        
        for attrib, key in [('scientificName', 'scientific_name'),
                                  ('canonicalName', 'canonical_name'),
                                  ('rank', 'taxon_rank'),
                                  ('kingdom', 'taxon_kingdom'),
                                  ('phylum', 'taxon_phylum'),
                                  ('txClass', 'taxon_class'),
                                  ('txOrder', 'taxon_order'),
                                  ('family', 'taxon_family'),
                                  ('genus', 'taxon_genus')
                                 ]:
            val = getattr(taxon, attrib)
            if val is not None:
                ret[key] = val
        return ret
    
        
    if squids and scribe and userId:
        return [
            getSquidHeaderDict(
                headers[i], i, scribe, userId) for i in xrange(len(headers))]
    else:
        return [getHeaderDict(headers[i], i) for i in xrange(len(headers))]

# .............................................................................
def mung(data):
    """
    @summary: Replace a list of values with a map from the non-zero values to
                     the indexes at which they occur
    """
    munged = defaultdict(list)
    for i, datum in enumerate(data):
        if datum != 0:
            munged[datum].append(i)
    return munged

# .............................................................................
def _get_anc_pam_content(anc_pam, shapegrid):
    """
    """
    lookup_filename = os.path.join(DYN_PACKAGE_DIR, 'nodeLookup.js')
    lookup_str = 'var nodeLookup = \n{}'.format(
        json.dumps(createHeaderLookup(anc_pam.getColumnHeaders()), indent=3))

    js_filename = os.path.join(DYN_PACKAGE_DIR, 'ancPam.js')
    
    mtx_str = StringIO()
    geoJsonify_flo(
        mtx_str, shapegrid.getDLocation(), matrix=anc_pam, mtxJoinAttrib=0,
        ident=0, headerLookupFilename=lookup_filename, transform=mung)
    mtx_str.seek(0)

    js_str = "var ancPam = JSON.parse(`{}`);".format(mtx_str.getvalue())

    # Save memory
    mtx_str = None

    return (lookup_filename, lookup_str, js_filename, js_str)
    
# .............................................................................
def _get_pam_content(pam, shapegrid, scribe, user_id):
    """Get the lookup and GeoJSON for a PAM

    Args:
        pam (:obj:`Matrix`): A PAM matrix
    """
    # Create the SQUID lookup
    lookup_filename = os.path.join(DYN_PACKAGE_DIR, 'squidLookup.json')
    # SQUID lookup content
    lookup_str = 'var squidLookup =\n{}'.format(
        json.dumps(createHeaderLookup(
            pam.getColumnHeaders(), squids=True, scribe=scribe,
            userId=user_id), indent=3))
                    
    # PAM JS filename
    js_filename = os.path.join(DYN_PACKAGE_DIR, 'pam.js')

    mtx_str = StringIO()
    geoJsonify_flo(
        mtx_str, shapegrid.getDLocation(), matrix=pam, mtxJoinAttrib=0,
        ident=0, headerLookupFilename=lookup_filename, transform=mung)
    mtx_str.seek(0)

    # PAM JS content
    js_str = "var pam = JSON.parse('{}');".format(mtx_str.getvalue())

    # Save memory
    mtx_str = None

    return (lookup_filename, lookup_str, js_filename, js_str)

# .............................................................................
def _package_gridset(gridset, include_csv=False, include_sdm=False):
    """Create a gridset package
    """
    package_filename = gridset.getPackageLocation()
    
    scribe = BorgScribe(LmPublicLogger())
    user_id = gridset.getUserId()
    
    try:
        gs_name = gridset.name
    except:
        gs_name = 'Gridset {}'.format(gridset.getId())
        
    shapegrid = gridset.getShapegrid()
    matrices = gridset.getMatrices()

    do_mcpa = False
    do_pam_stats = False
    # Loop though matrix outputs.  Set mcpa / pam stats to true if the required
    #    matrix is present and complete
    for mtx in matrices:
        if mtx.status == JobStatus.COMPLETE:
            if mtx.matrixType in [MatrixType.SITES_COV_OBSERVED,
                                  MatrixType.SITES_OBSERVED]:
                do_pam_stats = True
            elif mtx.matrixType == MatrixType.MCPA_OUTPUTS:
                do_mcpa = True

    # Create the zip file
    with zipfile.ZipFile(
        package_filename, mode='w', compression=zipfile.ZIP_DEFLATED,
        allowZip64=True) as zip_f:

        # Write static files
        for f_dir, _, fns in os.walk(STATIC_PACKAGE_PATH):
            for fn in fns:
                # Get relative and absolute paths for packaging
                a_path = os.path.join(f_dir, fn)
                r_path = a_path.replace(STATIC_PACKAGE_PATH, '')
                
                add_file = True
                # Check if html file for mcpa or stats and if we should add
                if fn.lower().find('.html') >= 0:
                    if (fn.lower().find('mcpa') >= 0 and not do_mcpa) or \
                        (fn.lower().find('stats') >= 0 and not do_pam_stats):
                        # Don't add this file
                        add_file = False
                # Don't add base index.html file
                elif fn.lower().find('index.html') >= 0:
                    add_file = False
                
                if add_file:
                    zip_f.write(a_path, r_path)
        
        # Write gridset EML
        gs_eml = tostring(makeEml(gridset))
        zip_f.writestr(
            os.path.join(
                GRIDSET_DIR, 'gridset_{}.eml'.format(gridset.getId())), gs_eml)
        
        # Write tree
        if gridset.tree is not None and gridset.tree.getDLocation() is not None:
            with open(gridset.tree.getDLocation()) as tree_file:
                tree_str = tree_file.read()
            
            zip_f.writestr(
                os.path.join(DYN_PACKAGE_DIR, 'tree.js'),
                'var taxonTree = `{}`;'.format(tree_str))
            zip_f.writestr(os.path.join(GRIDSET_DIR, 'tree.nex'), tree_str)
            # Free some memory
            tree_str = None

        # index.html page
        zip_f.writestr(
            'index.html', createIndexHtml(
                gs_name, do_mcpa, do_pam_stats, include_sdm, include_csv))
                            
        # Matrices
        stat_lookup_filename = os.path.join(
            DYN_PACKAGE_DIR, 'statNameLookup.json')
        zip_f.writestr(
            stat_lookup_filename, 'var statNameLookup =\n{}'.format(
                json.dumps(createStatsMeta(), indent=3)))

        for mtx in matrices:
            # Only add if matrix is complete and observed scenario
            # TODO: Change or do this better
            lookup_filename = None
            lookup_str = None
            js_filename = None
            js_str = None
            
            if mtx.status == JobStatus.COMPLETE and mtx.dateCode == 'Curr':
                mtx_obj = Matrix.load(mtx.getDLocation())

                if mtx.matrixType in [MatrixType.PAM, MatrixType.ROLLING_PAM]:
                    
                    (lookup_filename, lookup_str, js_filename, js_str
                     ) = _get_pam_content(mtx_obj, shapegrid, scribe, user_id) 
                    
                    csv_mtx_filename = os.path.join(
                        MATRIX_DIR, 'pam_{}.csv'.format(mtx.getId()))
                elif mtx.matrixType == MatrixType.ANC_PAM:
                    
                    (lookup_filename, lookup_str, js_filename, js_str
                     ) =_get_anc_pam_content(mtx_obj, shapegrid)

                    csv_mtx_filename = os.path.join(
                        MATRIX_DIR, 'ancPam_{}.csv'.format(mtx.getId()))
                elif mtx.matrixType in [
                    MatrixType.SITES_COV_OBSERVED, MatrixType.SITES_OBSERVED]:
                    
                    if mtx.matrixType == MatrixType.SITES_COV_OBSERVED:
                        mtx_name = 'sitesCovarianceObserved'
                    else:
                        mtx_name = 'sitesObserved'

                    js_filename = os.path.join(
                        DYN_PACKAGE_DIR, '{}.js'.format(mtx_name))
                    mtx_str = StringIO()
                    # TODO: Determine if we need to mung this data
                    
                    # We can only do geojson for 2D currently
                    mtx_2d = Matrix(
                        mtx_obj.data[:,:,0],
                        headers={'0' : mtx_obj.getHeaders(axis='0'),
                                 '1': mtx_obj.getHeaders(axis='1')})
                    
                    geoJsonify_flo(
                        mtx_str, shapegrid.getDLocation(),
                        matrix=mtx_2d,
                        mtxJoinAttrib=0, ident=0)
                    mtx_str.seek(0)
                    
                    js_str = "var {} = JSON.parse(`{}`);".format(
                        mtx_name, mtx_str.getvalue())
                    # Save memory
                    mtx_str = None
                    csv_mtx_filename = os.path.join(
                        MATRIX_DIR, '{}_{}.csv'.format(mtx_name, mtx.getId()))
                elif mtx.matrixType == MatrixType.MCPA_OUTPUTS:
                    
                    js_filename = os.path.join(
                        DYN_PACKAGE_DIR, 'mcpaMatrix.js')
                    
                    mtx_str = StringIO()
                    mtx_obj.writeCSV(mtx_str)
                    mtx_str.seek(0)

                    js_str = 'var mcpaMatrix = `{}`;'.format(
                        mtx_str.getvalue())
                    # Save memory
                    mtx_str = None
                    csv_mtx_filename = os.path.join(
                        MATRIX_DIR, 'mcpa_{}.csv'.format(mtx.getId()))
                elif mtx.matrixType == MatrixType.SPECIES_OBSERVED:
                    mtx_name = 'speciesObserved'
                    mtx_str = StringIO()
                    mtx_obj.writeCSV(mtx_str)
                    mtx_str.seek(0)
                    
                    js_filename = os.path.join(
                        DYN_PACKAGE_DIR, 'speciesObserved.js')
                    js_str = "var speciesObserved = JSON.parse(`{}`);".format(
                        mtx_str.getvalue())
                    # Save memory
                    mtx_str = None
                    csv_mtx_filename = os.path.join(
                        MATRIX_DIR, '{}_{}.csv'.format(mtx_name, mtx.getId()))
                else:
                    csv_mtx_filename = os.path.join(
                        MATRIX_DIR, '{}.csv'.format(
                            os.path.splitext(
                                os.path.basename(mtx.getDLocation()))[0]))

                # If lookup, write it
                if lookup_filename is not None and lookup_str is not None:
                    zip_f.writestr(lookup_filename, lookup_str)

                # If JS matrix, write it
                if js_filename is not None and js_str is not None:
                    zip_f.writestr(js_filename, js_str)

                # If we should include the raw CSV, do so
                if include_csv:
                    csv_mtx_str = StringIO()
                    mtx_obj.writeCSV(csv_mtx_str)
                    csv_mtx_str.seek(0)
                    zip_f.writestr(csv_mtx_filename, csv_mtx_str.getvalue())

        if include_sdm:
            added_occ_ids = []
            
            prj_info = []
            scn_info = {}
            occ_info = []
            
            for prj in scribe.listSDMProjects(
                    0, MAX_PROJECTIONS, userId=user_id,
                    afterStatus=JobStatus.COMPLETE - 1,
                    beforeStatus=JobStatus.COMPLETE + 1,
                    gridsetId=gridset.getId(),
                    atom=False):
                occ = prj.occurrenceSet
                prj_dir = os.path.join(SDM_PRJ_DIR, occ.displayName)
                # Make sure projection output file exists, then add to package
                if os.path.exists(prj.getDLocation()):
                    arc_prj_path = os.path.join(
                        prj_dir, os.path.basename(prj.getDLocation()))
                    arc_prj_img_path = arc_prj_path.replace(
                        LMFormat.GTIFF.ext, '.png')
                    #prj_eml_path = '{}{}'.format(
                    #    os.path.splitext(arc_prj_path)[0], LMFormat.EML.ext)
                    zip_f.write(prj.getDLocation(), arc_prj_path)
                    zip_f.writestr(
                        arc_prj_img_path,
                        get_map_content_for_proj(prj, scribe))
                    # EML
                    #zip_f.writestr(prj_eml_path, tostring(makeEml(prj)))
                    scn = prj.projScenario
                    prj_info.append(
                        {
                            'prj_id' : prj.getId(),
                            'file_path' : arc_prj_path,
                            'image_path' : arc_prj_img_path,
                            'scenario_code' : prj.projScenarioCode,
                            'species_name' : prj.speciesName,
                            'algorithm_code' : prj.algorithmCode,
                            'gcm_code' : scn.gcmCode,
                            'alt_pred_code' : scn.altpredCode,
                            'date_code' : scn.dateCode,
                            'epsg' : prj.epsgcode
                        })
                    
                # Add occurrence set
                if occ.getId() not in added_occ_ids:
                    arc_occ_path = os.path.join(
                        prj_dir, '{}.csv'.format(occ.displayName))
                    zip_f.write(occ.getDLocation(), arc_occ_path)
                    # TODO: Add points EML
                    added_occ_ids.append(occ.getId())
                    occ_info.append(
                        {
                            'occ_id' : occ.getId(),
                            'species_name' : occ.displayName,
                            'num_points' : occ.queryCount,
                            'file_path' : arc_occ_path
                        })
            
            # Write out JSON
            sdm_info_filename = os.path.join(
                DYN_PACKAGE_DIR, 'sdm_info.js')
            sdm_info_obj = {
                'projections' : prj_info,
                'occurrences' : occ_info
            }
            sdm_json_str = "var projection_info = JSON.parse(`{}`);".format(
                json.dumps(sdm_info_obj))
            zip_f.writestr(sdm_info_filename, sdm_json_str)
            
            # Write projection page
            zip_f.writestr('browse_maps.html', get_sdm_html_page(prj_info))

    
# .............................................................................
def gridsetPackageFormatter(gridset, includeCSV=True, includeSDM=True,
                            stream=False):
    """
    @summary: Create a Gridset download package for the user to explore locally
    @todo: Break this into smaller functions
    """
    # Check that it is a gridset
    if not isinstance(gridset, Gridset):
        raise cherrypy.HTTPError(
            HTTPStatus.BAD_REQUEST,
            'Only gridsets can be formatted as a package')
    
    package_filename = gridset.getPackageLocation()
    
    # Check to see if the package does not exist
    if not os.path.exists(package_filename):
        
        # Look for makeflows
        scribe = BorgScribe(LmPublicLogger())
        scribe.openConnections()
        cnt = scribe.countMFChains(
            gridsetId=gridset.getId(), beforeStat=JobStatus.COMPLETE)
        
        
        # See if we can create it
        if cnt == 0 and all([mtx.status >= JobStatus.COMPLETE for \
                mtx in gridset.getMatrices() if mtx.matrixType not in [
                    MatrixType.PAM, MatrixType.ROLLING_PAM]]):
            # Create the package
            _package_gridset(
                gridset, include_csv=includeCSV, include_sdm=includeSDM)
        else:
            # Not ready, so just return HTTP ACCEPTED
            cherrypy.response.status = HTTPStatus.ACCEPTED
            return

    # Package exists, return it
    out_package_name = 'gridset-{}-package.zip'.format(gridset.getId())
    cherrypy.response.headers[
        'Content-Disposition'] = 'attachment; filename="{}"'.format(
            out_package_name)
    cherrypy.response.headers['Content-Type'] = LMFormat.ZIP.getMimeType()

    if stream:
        cherrypy.lib.file_generator(open(package_filename, 'r'))
    else:
        with open(package_filename) as package_file:
            cnt = package_file.read()
        return cnt
