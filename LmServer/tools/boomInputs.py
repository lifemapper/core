"""
@summary: Add a tree and biogeographic hypotheses to a grid set


@todo: How to specify multiple hypotheses with different event fields?

"""
import argparse
import mx.DateTime
import numpy as np
import os
import sys

from LmCommon.common.config import Config
from LmCommon.common.lmconstants import (JobStatus, PhyloTreeKeys, SERVER_BOOM_HEADING)
from LmCommon.common.matrix import Matrix
from LmCommon.encoding.bioGeoContrasts import BioGeoEncoding
from LmServer.base.utilities import isCorrectUser
from LmServer.common.datalocator import EarlJr
from LmServer.common.lmconstants import LMFileType
from LmServer.common.log import ConsoleLogger
from LmServer.db.borgscribe import BorgScribe
from LmServer.legion.tree import Tree
from LmServer.legion.mtxcolumn import MatrixColumn
from LmServer.base.serviceobject2 import ServiceObject
from LmServer.common.localconstants import DEFAULT_EPSG

# .................................
def encodeHypothesesToMatrix(scribe, usr, shapegrid, bgMtx, layers=[]):
   """
   @summary: Example code for encoding hypotheses to a BioGeo matrix
   @param bgMtx: The bio geographic hypotheses matrix you created
   @param layers: A list of (layer object, event field) tuples.  Event field
                     may be None
   """
   # Create the encoding data
   enc = None
   mtxCols = []
   
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
         efValue = col.split(' - ')[1]
         if valAttribute is not None:
            intParams = {MatrixColumn.INTERSECT_PARAM_VAL_NAME.lower(): valAttribute,
                         MatrixColumn.INTERSECT_PARAM_VAL_VALUE.lower(): efValue}
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
      
      # Concatenate with the existing encoded matrix
      if enc is None:
         enc = encMtx
      else:
         enc = Matrix.concatenate([enc, encMtx], axis=1)
      
   # Update biogeo matrix with new data and headers
   bgMtx.data = enc.data
   bgMtx.setHeaders(enc.getHeaders())
   
   # Save matrix and update record
   bgMtx.clearDLocation()
   bgMtx.setDLocation()
   bgMtx.write()
   bgMtx.updateStatus(JobStatus.COMPLETE, modTime=mx.DateTime.gmt().mjd)
   success = scribe.updateObject(bgMtx)
   return bgMtx

# .................................
def squidifyTree(scribe, usr, tree):
   """
   @summary: Annotate a tree with squids and node ids, then write to disk
   @note: Matching species must be present in the taxon table of the database
   """
   squidDict = {}
   for label in tree.getLabels():
      # TODO: Do we always need to do this?
      taxLabel = label.replace(' ', '_')
      sno = scribe.getTaxon(userId=usr, taxonName=taxLabel)
      if sno is not None:
         squidDict[label] = sno.squid

   tree.annotateTree(PhyloTreeKeys.SQUID, squidDict)

   print "Adding interior node labels to tree"
   # Add node labels
   tree.addNodeLabels()
   
   # Update tree properties
   tree.clearDLocation()
   tree.setDLocation()
   print "Write tree to final location"
   tree.writeTree()
   tree.updateModtime(mx.DateTime.gmt().mjd)
   success = scribe.updateObject(tree)
   return tree

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

# .............................................................................
if __name__ == '__main__':
   if not isCorrectUser():
      print("Run this script as 'lmwriter'")
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
   
#   boomInput.encodeHypothesesToMatrix(self.scribe, self.usr, shpgrid, bgMtx, 
#                                      layers=allLayers) 

   scribe = BorgScribe(ConsoleLogger())
   scribe.openConnections()
   if gridname is not None:
      bgMtx = None
      layers = _getBoomBioGeoParams(scribe, gridname, usr)
      gridset = scribe.getGridset(userId=usr, name=gridname, fillMatrices=True)
      try:
         bgMtxList = gridset.getBiogeographicHypotheses()
         # TODO: There should be only one?!?
         if len(bgMtxList) > 0:
            bgMtx = bgMtxList[0]
      except:
         print ('No gridset for hypotheses')
      else:
         if bgMtx and layers:
            encodeHypothesesToMatrix(scribe, usr, gridset.getShapegrid(), bgMtx, 
                                     layers=layers)
         else:
            print ('No biogeo matrix or layers to encode as hypotheses')
   
   if treename is not None:
      baretree = Tree(treename, userId=args.user)
      tree = scribe.getTree(tree=baretree)
      decoratedtree = squidifyTree(scribe, usr, tree)
   
   scribe.closeConnections()
   
   
   
