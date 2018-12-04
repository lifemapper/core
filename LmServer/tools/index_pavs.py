"""This script inserts PAVs into the Solr index
"""
import argparse
import json
from mx.DateTime import DateTimeFromMJD
import os

# TODO: Different logger
from LmCommon.common.matrix import Matrix
from LmCommon.compression.binaryList import compress
from LmServer.common.lmconstants import (SOLR_ARCHIVE_COLLECTION, SOLR_FIELDS)
from LmServer.common.log import ConsoleLogger
from LmServer.common.solr import buildSolrDocument, postSolrDocument
from LmServer.db.borgscribe import BorgScribe
from LmBackend.common.lmconstants import RegistryKey

# .............................................................................
def get_post_pairs(pav, prj, occ, pam, sci_name, pav_filename):
    """Gets a list of (field name, field value) tuples for the pav

    Args:
        pav : A PAV matrix column object
        prj : An SDM Projection object
        occ : An occurrence layer object
        pam : A PAM matrix object
        sci_name : A taxon object
        pav_filename : The file location of the intersected PAV data

    Returns:
        A list of (field name, field value) tuples for the PAV for posting
    """
    shapegrid = pam.getShapegrid()
    mdl_scn = prj.modelScenario
    prj_scn = prj.projScenario
    pav_mtx = Matrix.load(pav_filename)
    
    try:
        sp = sci_name.scientificName.split(' ')[1]
    except:
        sp = None

    # Mod times
    occ_mod_time = prj_mod_time = None
    
    if occ.modTime is not None:
        occ_mod_time = DateTimeFromMJD(
            occ.modTime).strftime('%Y-%m-%dT%H:%M:%SZ')
    
    if prj.modTime is not None:
        prj_mod_time = DateTimeFromMJD(
            prj.modTime).strftime('%Y-%m-%dT%H:%M:%SZ')

    # Taxonomy fields
    tax_kingdom = None
    tax_phylum = None
    tax_class = None
    tax_order = None
    tax_family = None 
    tax_genus = None

    try:
        tax_kingdom = sci_name.kingdom
        tax_phylum = sci_name.phylum
        tax_class = sci_name.txClass
        tax_order = sci_name.txOrder
        tax_family = sci_name.family
        tax_genus = sci_name.genus
    except:
        pass
    
    
    fields = [
        (SOLR_FIELDS.ID, pav.getId()),
        (SOLR_FIELDS.USER_ID, pav.getUserId()),
        (SOLR_FIELDS.DISPLAY_NAME, occ.displayName),
        (SOLR_FIELDS.SQUID, pav.squid),
        (SOLR_FIELDS.TAXON_KINGDOM, tax_kingdom),
        (SOLR_FIELDS.TAXON_PHYLUM, tax_phylum),
        (SOLR_FIELDS.TAXON_CLASS, tax_class),
        (SOLR_FIELDS.TAXON_ORDER, tax_order),
        (SOLR_FIELDS.TAXON_FAMILY, tax_family),
        (SOLR_FIELDS.TAXON_GENUS, tax_genus),
        (SOLR_FIELDS.TAXON_SPECIES, sp),
        (SOLR_FIELDS.ALGORITHM_CODE, prj.algorithmCode),
        (SOLR_FIELDS.ALGORITHM_PARAMETERS,
         prj.dumpAlgorithmParametersAsString()),
        (SOLR_FIELDS.POINT_COUNT, occ.queryCount),
        (SOLR_FIELDS.OCCURRENCE_ID, occ.getId()),
        (SOLR_FIELDS.OCCURRENCE_DATA_URL, occ.getDataUrl()),
        (SOLR_FIELDS.OCCURRENCE_META_URL, occ.metadataUrl),
        (SOLR_FIELDS.OCCURRENCE_MOD_TIME, occ_mod_time),
        (SOLR_FIELDS.MODEL_SCENARIO_CODE, mdl_scn.code),
        (SOLR_FIELDS.MODEL_SCENARIO_ID, mdl_scn.getId()),
        (SOLR_FIELDS.MODEL_SCENARIO_URL, mdl_scn.metadataUrl),
        (SOLR_FIELDS.MODEL_SCENARIO_GCM, mdl_scn.gcmCode),
        (SOLR_FIELDS.MODEL_SCENARIO_DATE_CODE, mdl_scn.dateCode),
        (SOLR_FIELDS.MODEL_SCENARIO_ALT_PRED_CODE, mdl_scn.altpredCode),
        (SOLR_FIELDS.PROJ_SCENARIO_CODE, prj_scn.code),
        (SOLR_FIELDS.PROJ_SCENARIO_ID, prj_scn.getId()),
        (SOLR_FIELDS.PROJ_SCENARIO_URL, prj_scn.metadataUrl),
        (SOLR_FIELDS.PROJ_SCENARIO_GCM, prj_scn.gcmCode),
        (SOLR_FIELDS.PROJ_SCENARIO_DATE_CODE, prj_scn.dateCode),
        (SOLR_FIELDS.PROJ_SCENARIO_ALT_PRED_CODE, prj_scn.altpredCode),
        (SOLR_FIELDS.PROJ_ID, prj.getId()),
        (SOLR_FIELDS.PROJ_META_URL, prj.metadataUrl),
        (SOLR_FIELDS.PROJ_DATA_URL, prj.getDataUrl()),
        (SOLR_FIELDS.PROJ_MOD_TIME, prj_mod_time),
        (SOLR_FIELDS.PAV_META_URL, pav.metadataUrl),
        #(SOLR_FIELDS.PAV_DATA_URL, pav.getDataUrl()),
        (SOLR_FIELDS.EPSG_CODE, prj.epsgcode),
        (SOLR_FIELDS.GRIDSET_META_URL, pam.gridsetUrl),
        (SOLR_FIELDS.GRIDSET_ID, pam.gridsetId),
        (SOLR_FIELDS.SHAPEGRID_ID, shapegrid.getId()),
        (SOLR_FIELDS.SHAPEGRID_META_URL, shapegrid.metadataUrl),
        (SOLR_FIELDS.SHAPEGRID_DATA_URL, shapegrid.getDataUrl()),
        # Compress the PAV and store the string
        (SOLR_FIELDS.COMPRESSED_PAV, compress(pav_mtx.data))
    ]

    # Process presence centroids
    rowHeaders = pav_mtx.getRowHeaders()
    
    for i in xrange(pav_mtx.data.shape[0]):
        if pav_mtx.data[i]:
            _, x, y = rowHeaders[i]
            fields.append((SOLR_FIELDS.PRESENCE, '{},{}'.format(y, x)))
    return fields

# .............................................................................
if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        prog='Lifemapper Solr index POST for Presence Absence Vectors',
        description='This script adds PAVs to the Lifemapper Solr index',
        version='2.0')
    
    parser.add_argument(
        'pavs_filename', type=str, help='A JSON file with PAV information')
    parser.add_argument(
        'post_index_filename', type=str,
        help='A file location to be used for Solr POST')
    
    args = parser.parse_args()

    with open(args.pavs_filename) as in_file:
        pav_config = json.load(in_file)

    scribe = BorgScribe(ConsoleLogger())
    scribe.openConnections()
    doc_pairs = []
    for pav_info in pav_config:
        pav_filename = pav_info[RegistryKey.PAV_FILENAME]
        pav_id = pav_info[RegistryKey.IDENTIFIER]
        proj_id = pav_info[RegistryKey.PROJECTION_ID]

        pav = scribe.getMatrixColumn(mtxcolId=pav_id)
        prj = scribe.getSDMProject(proj_id)
        occ = prj.occurrenceSet
        pam = scribe.getMatrix(mtxId=pav.parentId)
        sci_name = scribe.getTaxon(squid=pav.squid)
    
        doc_pairs.append(
            get_post_pairs(pav, prj, occ, pam, sci_name, pav_filename))
    
    # Get all the information for a POST
    doc = buildSolrDocument(doc_pairs)
    
    # Write the post document
    with open(args.post_index_filename, 'w') as out_f:
        out_f.write(doc)

    postSolrDocument(SOLR_ARCHIVE_COLLECTION, args.post_index_filename)

    scribe.closeConnections()
