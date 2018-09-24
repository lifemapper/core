"""
@summary: Add a tree and biogeographic hypotheses to a grid set


@todo: How to specify multiple hypotheses with different event fields?

"""
import argparse
import mx.DateTime
import os
import sys

# from LmBackend.command.server import EncodeBioGeoHypothesesCommand

from LmCommon.common.config import Config
from LmCommon.common.lmconstants import (LM_USER, JobStatus, 
                                 MatrixType, ProcessType, SERVER_BOOM_HEADING)
from LmCommon.common.matrix import Matrix
from LmCommon.encoding.bioGeoContrasts import BioGeoEncoding
from LmServer.base.utilities import isLMUser
from LmServer.common.datalocator import EarlJr
from LmServer.common.lmconstants import LMFileType
from LmServer.common.log import ScriptLogger
from LmServer.db.borgscribe import BorgScribe
from LmServer.legion.lmmatrix import LMMatrix
from LmServer.legion.mtxcolumn import MatrixColumn
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
def encodeHypothesesToMatrix(scribe, usr, gridset, successFname, layers=[]):
   """
   @summary: Encoding hypotheses to a BioGeo matrix
   @note: This adds to existing encoded hypotheses
   @param scribe: An open BorgScribe object connected to the database
   @param usr: Userid for these data
   @param gridset: Gridset object for this tree data 
   @param layers: A list of (layer object, event field) tuples.  Event field
                     may be None
   """
#    allEncodings = None
   mtxCols = []
   # Find or create the matrix
   bgMtx = _getBioGeoMatrix(scribe, usr, gridset, layers)
   shapegrid = gridset.getShapegrid()
   for lyr in layers:
      lyrEnc = BioGeoEncoding(shapegrid.getDLocation())
      try:
         valAttribute = lyr.lyrMetadata[MatrixColumn.INTERSECT_PARAM_VAL_NAME.lower()]
      except KeyError:
         valAttribute = None
      lyrEnc.addLayers(lyr.getDLocation(), eventField=valAttribute)
      print('layer name={}, eventField={}, dloc={}'
            .format(lyr.name, valAttribute, lyr.getDLocation()))
      encMtx = lyrEnc.encodeHypotheses()
      
      # Add matrix columns for the newly encoded layers
      for col in encMtx.getColumnHeaders():
         # TODO: Fill in params and metadata
         try:
            efValue = col.split(' - ')[1]
         except:
            efValue = col

         if valAttribute is not None:
            intParams = {MatrixColumn.INTERSECT_PARAM_VAL_NAME.lower(): valAttribute,
                         MatrixColumn.INTERSECT_PARAM_VAL_VALUE.lower(): efValue}
         else:
            intParams = None
         metadata = {
            ServiceObject.META_DESCRIPTION.lower() : 
         'Encoded Helmert contrasts using the Lifemapper bioGeoContrasts module',
            ServiceObject.META_TITLE.lower() : 
         'Biogeographic hypothesis column ({})'.format(col)}
         mc = MatrixColumn(len(mtxCols), bgMtx.getId(), usr, layer=lyr,
                           shapegrid=shapegrid, intersectParams=intParams, 
                           metadata=metadata, postToSolr=False,
                           status=JobStatus.COMPLETE, 
                           statusModTime=mx.DateTime.gmt().mjd)
         updatedMC = scribe.findOrInsertMatrixColumn(mc)
         mtxCols.append(updatedMC)
      
      if bgMtx.data is None:
         allEncodings = encMtx
      else:
         # Append to previous layer encodings 
         allEncodings = Matrix.concatenate([bgMtx, encMtx], axis=1)

      bgMtx.data = allEncodings.data
      bgMtx.setHeaders(allEncodings.getHeaders())
   
   # Save matrix and update record
   bgMtx.write(overwrite=True)
   bgMtx.updateStatus(JobStatus.COMPLETE, modTime=mx.DateTime.gmt().mjd)
   success = scribe.updateObject(bgMtx)
   
   msg = 'Wrote matrix {} to final location and updated db'.format(bgMtx.getId())
   print msg
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
      cfg = Config(siteFn=configFname)
   else:
      raise Exception('Missing config file {}'.format(configFname))

   try:
      epsg = cfg.get(SERVER_BOOM_HEADING, 'SCENARIO_PACKAGE_EPSG')
   except:
      pass

   try:
      var = cfg.get(SERVER_BOOM_HEADING, 'BIOGEO_HYPOTHESES_LAYERS')
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
   if os.path.exists(successFname):
      os.remove(successFname)
   try:
      f = open(successFname, 'w')
      f.write(message)
   except:
      raise
   finally:
      f.close()


# # ...............................................
# def createEncodeBioGeoMF(scribe, usr, gridname, success_file):
#    """
#    @summary: Create a Makeflow to encode biogeographic hypotheses into a Matrix
#    """
#    scriptname, _ = os.path.splitext(os.path.basename(__file__))
#    meta = {MFChain.META_CREATED_BY: scriptname,
#            MFChain.META_DESCRIPTION: 
#                      'Encode biogeographic hypotheses task for user {} grid {}'
#                      .format(usr, gridname)}
#    newMFC = MFChain(usr, priority=Priority.HIGH, 
#                     metadata=meta, status=JobStatus.GENERAL, 
#                     statusModTime=mx.DateTime.gmt().mjd)
#    mfChain = scribe.insertMFChain(newMFC, None)
# 
#    # Create a rule from the MF 
#    bgCmd = EncodeBioGeoHypothesesCommand(usr, gridname, success_file)
# 
#    mfChain.addCommands([bgCmd.getMakeflowRule(local=True)])
#    mfChain.write()
#    mfChain.updateStatus(JobStatus.INITIALIZE)
#    scribe.updateObject(mfChain)
#    return mfChain


# .............................................................................
if __name__ == '__main__':
   if not isLMUser():
      print("Run this script as '{}'".format(LM_USER))
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
         print ('No gridset or layers to encode as hypotheses')
   finally:
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
from LmCommon.encoding.bioGeoContrasts import BioGeoEncoding
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

lyr = layers[0]
lyrEnc = BioGeoEncoding(shapegrid.getDLocation())
try:
   valAttribute = lyr.lyrMetadata[MatrixColumn.INTERSECT_PARAM_VAL_NAME.lower()]
except KeyError:
   valAttribute = None
 
lyrEnc.addLayers(lyr.getDLocation(), eventField=valAttribute)
print('layer name={}, eventField={}, dloc={}'
      .format(lyr.name, valAttribute, lyr.getDLocation()))
 
encMtx = lyrEnc.encodeHypotheses()
 
for col in encMtx.getColumnHeaders():
   try:
      efValue = col.split(' - ')[1]
   except:
      efValue = col
 
   if valAttribute is not None:
      intParams = {MatrixColumn.INTERSECT_PARAM_VAL_NAME.lower(): valAttribute,
                   MatrixColumn.INTERSECT_PARAM_VAL_VALUE.lower(): efValue}
   else:
      intParams = None
   metadata = {
      ServiceObject.META_DESCRIPTION.lower() : 
   'Encoded Helmert contrasts using the Lifemapper bioGeoContrasts module',
      ServiceObject.META_TITLE.lower() : 
   'Biogeographic hypothesis column ({})'.format(col)}
   mc = MatrixColumn(len(mtxCols), bgMtx.getId(), usr, layer=lyr,
                     shapegrid=shapegrid, intersectParams=intParams, 
                     metadata=metadata, postToSolr=False,
                     status=JobStatus.COMPLETE, 
                     statusModTime=mx.DateTime.gmt().mjd)
   updatedMC = scribe.findOrInsertMatrixColumn(mc)
   mtxCols.append(updatedMC)

# Append to previous layer encodings 
bgMtx = Matrix.concatenate([bgMtx, encMtx], axis=1)

bgMtx.clearDLocation()
bgMtx.setDLocation()
bgMtx.write()
bgMtx.updateStatus(JobStatus.COMPLETE, modTime=mx.DateTime.gmt().mjd)
success = scribe.updateObject(bgMtx)
return bgMtx

# Tree
baretree = Tree(treename, userId=args.user)
tree = scribe.getTree(tree=baretree)
decoratedtree = squidifyTree(scribe, usr, tree)

scribe.closeConnections()
"""
   
