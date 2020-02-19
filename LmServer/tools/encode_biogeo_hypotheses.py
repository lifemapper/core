#! /usr/bin/env python
# -*- coding: utf-8 -*-
"""Add a tree and biogeographic hypotheses to a grid set

Todo:
     How to specify multiple hypotheses with different event fields?

"""
import argparse
import os
import sys

from LmCommon.common.config import Config
from LmCommon.common.lmconstants import (LM_USER, JobStatus, BoomKeys,
                                         MatrixType, ProcessType,
                                         SERVER_BOOM_HEADING)
from LmCommon.common.ready_file import ready_filename
from LmCommon.common.time import gmt
from LmCommon.encoding.layer_encoder import LayerEncoder
from LmServer.base.serviceobject2 import ServiceObject
from LmServer.base.utilities import is_lm_user
from LmServer.common.datalocator import EarlJr
from LmServer.common.lmconstants import LMFileType
from LmServer.common.localconstants import DEFAULT_EPSG
from LmServer.common.log import ScriptLogger
from LmServer.db.borgscribe import BorgScribe
from LmServer.legion.lmmatrix import LMMatrix
from LmServer.legion.mtxcolumn import MatrixColumn


# from LmBackend.command.server import EncodeBioGeoHypothesesCommand
# .................................
def _getBioGeoMatrix(scribe, usr, gridset, layers=[]):
    """
    @summary: Example code for encoding hypotheses to a BioGeo matrix
    @param bgMtx: The bio geographic hypotheses matrix you created
    @param layers: A list of (layer object, event field) tuples.  Event field
                            may be None
    """
    # Create the encoding data
    bgMtx = None
    try:
        bgMtxList = gridset.getBiogeographicHypotheses()
    except:
        scribe.log.info('No gridset for hypotheses')
    # TODO: There should be only one?!?
    if len(bgMtxList) > 0:
        bgMtx = bgMtxList[0]
    else:
        mtxKeywords = ['biogeographic hypotheses']
        for lyr in layers:
            kwds = []
            try:
                kwds = lyr.metadata[ServiceObject.META_KEYWORDS]
            except:
                kwds = []
            mtxKeywords.extend(kwds)
        # Add the matrix to contain biogeo hypotheses layer intersections
        meta = {ServiceObject.META_DESCRIPTION.lower():
                'Biogeographic Hypotheses for archive {}'.format(gridset.name),
                ServiceObject.META_KEYWORDS.lower(): mtxKeywords}
        tmpMtx = LMMatrix(None, matrixType=MatrixType.BIOGEO_HYPOTHESES,
                                processType=ProcessType.ENCODE_HYPOTHESES,
                                userId=usr, gridset=gridset, metadata=meta,
                                status=JobStatus.INITIALIZE,
                                statusModTime=gmt().mjd)
        bgMtx = scribe.findOrInsertMatrix(tmpMtx)
        if bgMtx is not None:
            scribe.log.info('  Found or added biogeo hypotheses matrix {}'
                            .format(bgMtx.get_id()))
        else:
            scribe.log.info('  Failed to add biogeo hypotheses matrix')
    return bgMtx


# .................................
def encodeHypothesesToMatrix(scribe, usr, gridset, successFname, layers=None):
    """
    @summary: Encoding hypotheses to a BioGeo matrix
    @note: This adds to existing encoded hypotheses
    @param scribe: An open BorgScribe object connected to the database
    @param usr: Userid for these data
    @param gridset: Gridset object for this tree data 
    @param layers: A list of (layer object, event field) tuples.  Event field
                            may be None
    """
#     allEncodings = None
    mtxCols = []
    # Find or create the matrix
    bgMtx = _getBioGeoMatrix(scribe, usr, gridset, layers)
    shapegrid = gridset.getShapegrid()
    encoder = LayerEncoder(shapegrid.get_dlocation())

    # TODO(CJ): Minimum coverage should be pulled from config or database
    min_coverage = 0.25

    for lyr in layers:
        try:
            valAttribute = lyr.lyrMetadata[MatrixColumn.INTERSECT_PARAM_VAL_NAME.lower()]
            if valAttribute is not None:
                column_name = valAttribute
            else:
                column_name = lyr.name
        except KeyError:
            valAttribute = None
            column_name = lyr.name
        new_cols = encoder.encode_biogeographic_hypothesis(
             lyr.get_dlocation(), column_name, min_coverage,
             event_field=valAttribute)
        scribe.log.info(' Encoded layer {} for eventField={}, dloc={}'
                .format(lyr.name, valAttribute, lyr.get_dlocation()))

        # Add matrix columns for the newly encoded layers
        for col_name in new_cols:
            # TODO: Fill in params and metadata
            try:
                efValue = col_name.split(' - ')[1]
            except:
                efValue = col_name

            if valAttribute is not None:
                intParams = {MatrixColumn.INTERSECT_PARAM_VAL_NAME.lower(): valAttribute,
                                 MatrixColumn.INTERSECT_PARAM_VAL_VALUE.lower(): efValue}
            else:
                intParams = None
            metadata = {
                ServiceObject.META_DESCRIPTION.lower() :
            'Encoded Helmert contrasts using the Lifemapper bioGeoContrasts module',
                ServiceObject.META_TITLE.lower() :
            'Biogeographic hypothesis column ({})'.format(col_name)}
            mc = MatrixColumn(len(mtxCols), bgMtx.get_id(), usr, layer=lyr,
                                    shapegrid=shapegrid, intersectParams=intParams,
                                    metadata=metadata, postToSolr=False,
                                    status=JobStatus.COMPLETE,
                                    statusModTime=gmt().mjd)
            updatedMC = scribe.findOrInsertMatrixColumn(mc)
            mtxCols.append(updatedMC)

        enc_mtx = encoder.get_encoded_matrix()

        bgMtx.data = enc_mtx.data
        bgMtx.setHeaders(enc_mtx.getHeaders())

    # Save matrix and update record
    bgMtx.write(overwrite=True)
    bgMtx.updateStatus(JobStatus.COMPLETE, mod_time=gmt().mjd)
    success = scribe.updateObject(bgMtx)

    msg = 'Wrote matrix {} to final location and updated db'.format(bgMtx.get_id())
    scribe.log.info(msg)
    _writeSuccessFile(msg, successFname)

    return bgMtx


# .................................
def _getBoomBioGeoParams(scribe, gridname, usr):
    epsg = DEFAULT_EPSG
    layers = []
    earl = EarlJr()
    configFname = earl.createFilename(LMFileType.BOOM_CONFIG,
                                                 objCode=gridname, usr=usr)
    if configFname is not None and os.path.exists(configFname):
        cfg = Config(site_fn=configFname)
    else:
        raise Exception('Missing config file {}'.format(configFname))

    try:
        epsg = cfg.get(SERVER_BOOM_HEADING, BoomKeys.EPSG)
    except:
        pass

    try:
        var = cfg.get(SERVER_BOOM_HEADING, BoomKeys.BIOGEO_HYPOTHESES_LAYERS)
    except:
        raise Exception('No configured Biogeographic Hypotheses layers')
    else:
        # May be one or more
        lyrnameList = [v.strip() for v in var.split(',')]
        for lname in lyrnameList:
            layers.append(scribe.getLayer(userId=usr, lyrName=lname, epsg=epsg))
    return layers


# ...............................................
def _writeSuccessFile(message, successFname):
    ready_filename(successFname, overwrite=True)
    try:
        with open(successFname, 'w') as f:
            f.write(message)
    except:
        raise

# # ...............................................
# def createEncodeBioGeoMF(scribe, usr, gridname, success_file):
#     """
#     @summary: Create a Makeflow to encode biogeographic hypotheses into a Matrix
#     """
#     scriptname, _ = os.path.splitext(os.path.basename(__file__))
#     meta = {MFChain.META_CREATED_BY: scriptname,
#                MFChain.META_DESCRIPTION:
#                             'Encode biogeographic hypotheses task for user {} grid {}'
#                             .format(usr, gridname)}
#     newMFC = MFChain(usr, priority=Priority.HIGH,
#                            metadata=meta, status=JobStatus.GENERAL,
#                            statusModTime=gmt().mjd)
#     mfChain = scribe.insertMFChain(newMFC, None)
#
#     # Create a rule from the MF
#     bgCmd = EncodeBioGeoHypothesesCommand(usr, gridname, success_file)
#
#     mfChain.addCommands([bgCmd.get_makeflow_rule(local=True)])
#     mfChain.write()
#     mfChain.updateStatus(JobStatus.INITIALIZE)
#     scribe.updateObject(mfChain)
#     return mfChain


# .............................................................................
if __name__ == '__main__':
    if not is_lm_user():
        print(("Run this script as '{}'".format(LM_USER)))
        sys.exit(2)

    parser = argparse.ArgumentParser(
        description="Encode biogeographic hypotheses into a matrix")

    # Required
    parser.add_argument('user_id', type=str,
                              help=('User owner of the tree'))
    parser.add_argument('gridset_name', type=str,
                              help="Gridset name for encoding Biogeographic Hypotheses")
    parser.add_argument('success_file', default=None,
                help=('Filename to be written on successful completion of script.'))
    # Optional
    parser.add_argument('--logname', type=str, default=None,
                help=('Basename of the logfile, without extension'))

    args = parser.parse_args()
    usr = args.user_id
    grid_name = args.gridset_name
    success_file = args.success_file
    logname = args.logname

    if logname is None:
        import time
        scriptname, _ = os.path.splitext(os.path.basename(__file__))
        secs = time.time()
        timestamp = "{}".format(time.strftime("%Y%m%d-%H%M", time.localtime(secs)))
        logname = '{}.{}'.format(scriptname, timestamp)

    import logging
    logger = ScriptLogger(logname, level=logging.INFO)
    scribe = BorgScribe(logger)
    try:
        scribe.openConnections()
        layers = _getBoomBioGeoParams(scribe, grid_name, usr)
        gridset = scribe.getGridset(userId=usr, name=grid_name, fillMatrices=True)
        if gridset and layers:
            encodeHypothesesToMatrix(scribe, usr, gridset, success_file, layers=layers)
        else:
            scribe.log.info('No gridset or layers to encode as hypotheses')
    finally:
        scribe.closeConnections()

"""
from LmServer.tools.encodeBioGeoHypotheses import *

from LmCommon.common.config import Config
from LmCommon.common.lmconstants import (LM_USER, JobStatus, 
                                            MatrixType, ProcessType, SERVER_BOOM_HEADING)
from LmCommon.common.ready_file import ready_filename
from LmCommon.encoding.layer_encoder import LayerEncoder
from LmServer.base.utilities import is_lm_user
from LmServer.common.data_locator import EarlJr
from LmServer.common.lmconstants import LMFileType
from LmServer.common.log import ScriptLogger
from LmServer.db.borg_scribe import BorgScribe
from LmServer.legion.lm_matrix import LMMatrix
from LmServer.legion.mtx_column import MatrixColumn
from LmServer.base.service_object import ServiceObject
from LmServer.common.localconstants import DEFAULT_EPSG

logname = 'encodeBioGeoHypotheses.20190111-1346'
usr = 'taffy3'
grid_name = 'sax_global' 
success_file = 'mf_517/sax_global.success' 

import logging
logger = ScriptLogger(logname, level=logging.INFO)
scribe = BorgScribe(logger)

scribe.openConnections()
layers = _getBoomBioGeoParams(scribe, grid_name, usr)
gridset = scribe.getGridset(userId=usr, name=grid_name, fillMatrices=True)
if gridset and layers:
    encodeHypothesesToMatrix(scribe, usr, gridset, success_file, layers=layers)

scribe.closeConnections()

"""

