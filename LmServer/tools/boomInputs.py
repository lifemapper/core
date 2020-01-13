"""Add a tree and biogeographic hypotheses to a grid set

@todo: How to specify multiple hypotheses with different event fields?
"""
import argparse
import mx.DateTime
import os
import sys

from LmCommon.common.config import Config
from LmCommon.common.lmconstants import (LM_USER, JobStatus, PhyloTreeKeys, 
                                         MatrixType, ProcessType, 
                                         SERVER_BOOM_HEADING, BoomKeys)
from LmCommon.encoding.layer_encoder import LayerEncoder

from LmServer.base.utilities import isLMUser
from LmServer.common.datalocator import EarlJr
from LmServer.common.lmconstants import LMFileType
from LmServer.common.log import ConsoleLogger
from LmServer.db.borgscribe import BorgScribe
from LmServer.legion.lmmatrix import LMMatrix
from LmServer.legion.mtxcolumn import MatrixColumn
from LmServer.legion.tree import Tree
from LmServer.base.serviceobject2 import ServiceObject
from LmServer.common.localconstants import DEFAULT_EPSG

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
        print ('No gridset for hypotheses')
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
        meta={ServiceObject.META_DESCRIPTION.lower(): 
                'Biogeographic Hypotheses for archive {}'.format(gridset.name),
                ServiceObject.META_KEYWORDS.lower(): mtxKeywords}
        tmpMtx = LMMatrix(None, matrixType=MatrixType.BIOGEO_HYPOTHESES, 
                                processType=ProcessType.ENCODE_HYPOTHESES,
                                userId=usr, gridset=gridset, metadata=meta,
                                status=JobStatus.INITIALIZE, 
                                statusModTime=mx.DateTime.gmt().mjd)
        bgMtx = scribe.findOrInsertMatrix(tmpMtx)
        if bgMtx is None:
            scribe.log.info('  Failed to add biogeo hypotheses matrix')
    return bgMtx

# .................................
def encodeHypothesesToMatrix(scribe, usr, gridset, layers=[]):
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
    encoder = LayerEncoder(shapegrid.getDLocation())
    
    # TODO(CJ): Minimum coverage should be pulled from config or database
    min_coverage = 0.25
    
    for lyr in layers:
        try:
            valAttribute = lyr.lyrMetadata[MatrixColumn.INTERSECT_PARAM_VAL_NAME.lower()]
            column_name = valAttribute
        except KeyError:
            valAttribute = None
            column_name = lyr.name
        new_cols = encoder.encode_biogeographic_hypothesis(
             lyr.getDLocation(), column_name, min_coverage, 
             event_field=valAttribute)
        print(('layer name={}, eventField={}, dloc={}'
                .format(lyr.name, valAttribute, lyr.getDLocation())))
        
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
            mc = MatrixColumn(len(mtxCols), bgMtx.getId(), usr, layer=lyr,
                                    shapegrid=shapegrid, intersectParams=intParams, 
                                    metadata=metadata, postToSolr=False,
                                    status=JobStatus.COMPLETE, 
                                    statusModTime=mx.DateTime.gmt().mjd)
            updatedMC = scribe.findOrInsertMatrixColumn(mc)
            mtxCols.append(updatedMC)
        
        enc_mtx = encoder.get_encoded_matrix()

        bgMtx.data = enc_mtx.data
        bgMtx.setHeaders(enc_mtx.getHeaders())
    
    # Save matrix and update record
    bgMtx.write(overwrite=True)
    bgMtx.updateStatus(JobStatus.COMPLETE, modTime=mx.DateTime.gmt().mjd)
    success = scribe.updateObject(bgMtx)
    return bgMtx

# .................................
def squidifyTree(scribe, usr, tree):
    """
    @summary: Annotate a tree with squids and node ids, then write to disk
    @note: Matching species must be present in the taxon table of the database
    @param scribe: An open BorgScribe object connected to the database
    @param usr: Userid for these data
    @param tree: Tree object 
    """
    squidDict = {}
    for label in tree.getLabels():
        # TODO: Do we always need to do this?
        taxLabel = label.replace(' ', '_')
        sno = scribe.getTaxon(userId=usr, taxonName=taxLabel)
        if sno is not None:
            squidDict[label] = sno.squid

    tree.annotateTree(PhyloTreeKeys.SQUID, squidDict)

    print("Adding interior node labels to tree")
    # Add node labels
    tree.addNodeLabels()
    
    # Update tree properties
    tree.clearDLocation()
    tree.setDLocation()
    print("Write tree to final location")
    tree.writeTree()
    tree.updateModtime(mx.DateTime.gmt().mjd)
    success = scribe.updateObject(tree)
    return tree

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

# .............................................................................
if __name__ == '__main__':
    if not isLMUser():
        print(("Run this script as '{}'".format(LM_USER)))
        sys.exit(2)

    parser = argparse.ArgumentParser(
        description="Annotate a tree with squids and node ids")

    parser.add_argument('-u', '--user', type=str, 
                              help="User name")
    parser.add_argument('-g', '--gridset_name', type=str, 
                              help="Gridset name for encoding Biogeographic Hypotheses")
    parser.add_argument('-t', '--tree_name', type=str, 
                              help="Tree name for squid, node annotation")
    
    args = parser.parse_args()
    usr = args.user
    treename = args.tree_name
    gridname = args.gridset_name
    
    scribe = BorgScribe(ConsoleLogger())
    scribe.openConnections()
    if gridname is not None:
        layers = _getBoomBioGeoParams(scribe, gridname, usr)
        gridset = scribe.getGridset(userId=usr, name=gridname, fillMatrices=True)
        if gridset and layers:
            encodeHypothesesToMatrix(scribe, usr, gridset, layers=layers)
        else:
            print ('No gridset or layers to encode as hypotheses')
    
    if treename is not None:
        baretree = Tree(treename, userId=args.user)
        tree = scribe.getTree(tree=baretree)
        decoratedtree = squidifyTree(scribe, usr, tree)
    
    scribe.closeConnections()
    
"""
import argparse
import mx.DateTime
import os
import sys

from LmServer.tools.boomInputs import (_getBoomBioGeoParams, _getBioGeoMatrix, 
      encodeHypothesesToMatrix, squidifyTree)
from LmCommon.common.config import Config
from LmCommon.common.lmconstants import (JobStatus, PhyloTreeKeys, MatrixType, 
                                                      ProcessType, SERVER_BOOM_HEADING)
from LmCommon.common.matrix import Matrix
from LmServer.base.utilities import isLMUser
from LmServer.common.datalocator import EarlJr
from LmServer.common.lmconstants import LMFileType
from LmServer.common.log import ConsoleLogger
from LmServer.db.borgscribe import BorgScribe
from LmServer.legion.lmmatrix import LMMatrix
from LmServer.legion.mtxcolumn import MatrixColumn
from LmServer.legion.tree import Tree
from LmServer.base.serviceobject2 import ServiceObject
from LmServer.common.localconstants import DEFAULT_EPSG

usr = 'biotaphy'
treename = None
gridname = 'biotaphy_heuchera_global'

scribe = BorgScribe(ConsoleLogger())
scribe.openConnections()
# Biogeo
layers = _getBoomBioGeoParams(scribe, gridname, usr)
gridset = scribe.getGridset(userId=usr, name=gridname, fillMatrices=True)
if not (gridset and layers):
    print ('No gridset or layers to encode as hypotheses')
    
# encodeHypothesesToMatrix(scribe, usr, gridset, layers=layers)
mtxCols = []
bgMtx = _getBioGeoMatrix(scribe, usr, gridset, layers)
shapegrid = gridset.getShapegrid()

encoder = LayerEncoder(shapegrid.getDLocation())

# TODO(CJ): Minimum coverage should be pulled from config or database
min_coverage = 0.25
for lyr in layers:
    try:
        valAttribute = lyr.lyrMetadata[MatrixColumn.INTERSECT_PARAM_VAL_NAME.lower()]
        column_name = valAttribute
    except KeyError:
        valAttribute = None
        column_name = lyr.name
    new_cols = encoder.encode_biogeographic_hypothesis(
         lyr.getDLocation(), column_name, min_coverage, 
         event_field=valAttribute)
    print('layer name={}, eventField={}, dloc={}'
            .format(lyr.name, valAttribute, lyr.getDLocation()))
    
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
        mc = MatrixColumn(len(mtxCols), bgMtx.getId(), usr, layer=lyr,
                                shapegrid=shapegrid, intersectParams=intParams, 
                                metadata=metadata, postToSolr=False,
                                status=JobStatus.COMPLETE, 
                                statusModTime=mx.DateTime.gmt().mjd)
        updatedMC = scribe.findOrInsertMatrixColumn(mc)
        mtxCols.append(updatedMC)
    
    enc_mtx = encoder.get_encoded_matrix()

    bgMtx.data = enc_mtx.data
    bgMtx.setHeaders(enc_mtx.getHeaders())
    
# Save matrix and update record
bgMtx.write(overwrite=True)
bgMtx.updateStatus(JobStatus.COMPLETE, modTime=mx.DateTime.gmt().mjd)
success = scribe.updateObject(bgMtx)
return bgMtx

# Tree
baretree = Tree(treename, userId=args.user)
tree = scribe.getTree(tree=baretree)
decoratedtree = squidifyTree(scribe, usr, tree)

scribe.closeConnections()
"""
    
