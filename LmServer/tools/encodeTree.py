"""
@summary: Add a tree and biogeographic hypotheses to a grid set


@todo: How to specify multiple hypotheses with different event fields?

"""
import argparse
import mx.DateTime
import os
import sys

from LmBackend.command.server import EncodeTreeCommand

from LmCommon.common.lmconstants import (LM_USER, PhyloTreeKeys, JobStatus)

from LmServer.base.utilities import isLMUser
from LmServer.common.lmconstants import Priority
from LmServer.common.log import ScriptLogger
from LmServer.db.borgscribe import BorgScribe
from LmServer.legion.processchain import MFChain
from LmServer.legion.tree import Tree


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

# ...............................................
def createEncodeTreeMF(scribe, usr, treename):
   """
   @summary: Create a Makeflow to initiate Boomer with inputs assembled 
             and configFile written by BOOMFiller.initBoom.
   """
   scriptname, _ = os.path.splitext(os.path.basename(__file__))
   meta = {MFChain.META_CREATED_BY: scriptname,
           MFChain.META_DESCRIPTION: 'Encode tree task for user {} tree {}'
   .format(usr, treename)}
   newMFC = MFChain(usr, priority=Priority.HIGH, 
                    metadata=meta, status=JobStatus.GENERAL, 
                    statusModTime=mx.DateTime.gmt().mjd)
   mfChain = scribe.insertMFChain(newMFC, None)

   # Create a rule from the MF and Arf file creation
   treeCmd = EncodeTreeCommand(usr, treename)

   mfChain.addCommands([treeCmd.getMakeflowRule(local=True)])
   mfChain.write()
   mfChain.updateStatus(JobStatus.INITIALIZE)
   scribe.updateObject(mfChain)
   return mfChain


# .............................................................................
if __name__ == '__main__':
   if not isLMUser():
      print("Run this script as '{}'".format(LM_USER))
      sys.exit(2)

   parser = argparse.ArgumentParser(
      description="Annotate a tree with squids and node ids")

   # Required
   parser.add_argument('user_id', type=str,
                       help=('User owner of the tree'))
   parser.add_argument('tree_name', type=str, 
                       help="Tree name for squid, node annotation")
   
   # Optional
   parser.add_argument('--logname', type=str, default=None,
            help=('Basename of the logfile, without extension'))
   parser.add_argument('--init_makeflow', type=bool, default=True,
                       help=("""Create a Makeflow task to encode the tree
                                with unique species identifiers (squids)."""))
   
   args = parser.parse_args()
   usr = args.user_id
   treename = args.tree_name
   logname = args.logname
   initMakeflow = args.init_makeflow
   
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
      
      if initMakeflow:
         createEncodeTreeMF(scribe, usr, treename)
      else:
         baretree = Tree(treename, userId=usr)
         tree = scribe.getTree(tree=baretree)
         decoratedtree = squidifyTree(scribe, usr, tree)
   finally:
      scribe.closeConnections()
   
"""

"""
