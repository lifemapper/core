"""This script inserts a PAV into the Solr index
"""
import argparse
from mx.DateTime import DateTimeFromMJD
import os

# TODO: Different logger
from LmCommon.common.matrix import Matrix
from LmCommon.compression.binaryList import compress
from LmServer.common.lmconstants import (SOLR_ARCHIVE_COLLECTION, SOLR_FIELDS)
from LmServer.common.log import ConsoleLogger
from LmServer.common.solr import buildSolrDocument, postSolrDocument
from LmServer.db.borgscribe import BorgScribe

# .............................................................................
def getPostDocument(pav, prj, occ, pam, sciName, pavFname):
    """Create the Solr document to be posted

    Args:
        pav : A PAV matrix column object
        prj : A SDM Projection object
        occ : An occurrence layer object

    Returns:
        A string that can be posted to the Solr index
    """
    sg = pam.getShapegrid()
    mdlScn = prj.modelScenario
    prjScn = prj.projScenario
    pavMtx = Matrix.load(pavFname)
    try:
        sp = sciName.scientificName.split(' ')[1]
    except:
        sp = None
    
    # Mod times
    occModTime = prjModTime = None
    
    if occ.modTime is not None:
        occModTime = DateTimeFromMJD(occ.modTime).strftime('%Y-%m-%dT%H:%M:%SZ')
    
    if prj.modTime is not None:
        prjModTime = DateTimeFromMJD(prj.modTime).strftime('%Y-%m-%dT%H:%M:%SZ')

    # Taxonomy fields
    txKingdom = None
    txPhylum = None
    txClass = None
    txOrder = None
    txFamily = None
    txGenus = None
    
    try:
        txKingdom = sciName.kingdom
        txPhylum = sciName.phylum
        txClass = sciName.txClass
        txOrder = sciName.txOrder
        txFamily = sciName.family
        txGenus = sciName.genus
    except:
        pass
    
    
    fields = [
        (SOLR_FIELDS.ID, pav.getId()),
        (SOLR_FIELDS.USER_ID, pav.getUserId()),
        (SOLR_FIELDS.DISPLAY_NAME, occ.displayName),
        (SOLR_FIELDS.SQUID, pav.squid),
        (SOLR_FIELDS.TAXON_KINGDOM, txKingdom),
        (SOLR_FIELDS.TAXON_PHYLUM, txPhylum),
        (SOLR_FIELDS.TAXON_CLASS, txClass),
        (SOLR_FIELDS.TAXON_ORDER, txOrder),
        (SOLR_FIELDS.TAXON_FAMILY, txFamily),
        (SOLR_FIELDS.TAXON_GENUS, txGenus),
        (SOLR_FIELDS.TAXON_SPECIES, sp),
        (SOLR_FIELDS.ALGORITHM_CODE, prj.algorithmCode),
        (SOLR_FIELDS.ALGORITHM_PARAMETERS, prj.dumpAlgorithmParametersAsString()),
        (SOLR_FIELDS.POINT_COUNT, occ.queryCount),
        (SOLR_FIELDS.OCCURRENCE_ID, occ.getId()),
        (SOLR_FIELDS.OCCURRENCE_DATA_URL, occ.getDataUrl()),
        (SOLR_FIELDS.OCCURRENCE_META_URL, occ.metadataUrl),
        (SOLR_FIELDS.OCCURRENCE_MOD_TIME, occModTime),
        (SOLR_FIELDS.MODEL_SCENARIO_CODE, mdlScn.code),
        (SOLR_FIELDS.MODEL_SCENARIO_ID, mdlScn.getId()),
        (SOLR_FIELDS.MODEL_SCENARIO_URL, mdlScn.metadataUrl),
        (SOLR_FIELDS.MODEL_SCENARIO_GCM, mdlScn.gcmCode),
        (SOLR_FIELDS.MODEL_SCENARIO_DATE_CODE, mdlScn.dateCode),
        (SOLR_FIELDS.MODEL_SCENARIO_ALT_PRED_CODE, mdlScn.altpredCode),
        (SOLR_FIELDS.PROJ_SCENARIO_CODE, prjScn.code),
        (SOLR_FIELDS.PROJ_SCENARIO_ID, prjScn.getId()),
        (SOLR_FIELDS.PROJ_SCENARIO_URL, prjScn.metadataUrl),
        (SOLR_FIELDS.PROJ_SCENARIO_GCM, prjScn.gcmCode),
        (SOLR_FIELDS.PROJ_SCENARIO_DATE_CODE, prjScn.dateCode),
        (SOLR_FIELDS.PROJ_SCENARIO_ALT_PRED_CODE, prjScn.altpredCode),
        (SOLR_FIELDS.PROJ_ID, prj.getId()),
        (SOLR_FIELDS.PROJ_META_URL, prj.metadataUrl),
        (SOLR_FIELDS.PROJ_DATA_URL, prj.getDataUrl()),
        (SOLR_FIELDS.PROJ_MOD_TIME, prjModTime),
        (SOLR_FIELDS.PAV_META_URL, pav.metadataUrl),
        #(SOLR_FIELDS.PAV_DATA_URL, pav.getDataUrl()),
        (SOLR_FIELDS.EPSG_CODE, prj.epsgcode),
        (SOLR_FIELDS.GRIDSET_META_URL, pam.gridsetUrl),
        (SOLR_FIELDS.GRIDSET_ID, pam.gridsetId),
        (SOLR_FIELDS.SHAPEGRID_ID, sg.getId()),
        (SOLR_FIELDS.SHAPEGRID_META_URL, sg.metadataUrl),
        (SOLR_FIELDS.SHAPEGRID_DATA_URL, sg.getDataUrl()),
        # Compress the PAV and store the string
        (SOLR_FIELDS.COMPRESSED_PAV, compress(pavMtx.data))
    ]

    # Process presence centroids
    rowHeaders = pavMtx.getRowHeaders()
    
    for i in xrange(pavMtx.data.shape[0]):
        if pavMtx.data[i]:
            _, x, y = rowHeaders[i]
            fields.append((SOLR_FIELDS.PRESENCE, '{},{}'.format(y, x)))

    doc = buildSolrDocument(fields)

    return doc

# .............................................................................
if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        prog='Lifemapper Solr index POST for Presence Absence Vectors',
        description='This script adds a PAV to the Lifemapper Solr index',
        version='1.0')
    
    parser.add_argument('pavFilename', type=str, help='The PAV file to use')
    parser.add_argument('pavId', type=int, help='The matrix column id')
    parser.add_argument('projectionId', type=int, help='The projection id')
    parser.add_argument('pamId', type=int, help='The PAM id')
    parser.add_argument('pavIdxFilename', type=str, 
                              help='A temporary file to be used for Solr POST')
    
    args = parser.parse_args()
    
    if not os.path.exists(args.pavFilename):
        raise Exception(
            'The specified PAV: {}, does not exist'.format(args.pavFilename))
    if os.path.getsize(args.pavFilename) == 0:
        raise Exception(
            'The specified PAV: {}, is blank'.format(args.pavFilename))
    
    scribe = BorgScribe(ConsoleLogger())
    scribe.openConnections()
    
    pav = scribe.getMatrixColumn(mtxcolId=args.pavId)
    prj = scribe.getSDMProject(args.projectionId)
    occ = prj.occurrenceSet
    pam = scribe.getMatrix(mtxId=args.pamId)
    sciName = scribe.getTaxon(squid=pav.squid)
    
    # Get all information for POST
    doc = getPostDocument(pav, prj, occ, pam, sciName, args.pavFilename)
    
    with open(args.pavIdxFilename, 'w') as outF:
        outF.write(doc)

    postSolrDocument(SOLR_ARCHIVE_COLLECTION, args.pavIdxFilename)

    scribe.closeConnections()
    
    
