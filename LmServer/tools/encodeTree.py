"""
@summary: Add a tree and biogeographic hypotheses to a grid set


@todo: How to specify multiple hypotheses with different event fields?

"""
import argparse
import mx.DateTime
import os
import sys

from LmCommon.common.lmconstants import (LM_USER, PhyloTreeKeys)

from LmServer.base.utilities import isLMUser
from LmServer.common.log import ScriptLogger
from LmServer.db.borgscribe import BorgScribe
from LmServer.legion.tree import Tree

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


# .................................
def squidifyTree(scribe, usr, tree, successFname):
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
   tree.writeTree()
   
   # Update metadata
   tree.updateModtime(mx.DateTime.gmt().mjd)
   success = scribe.updateObject(tree)
   
   msg = 'Wrote tree {} to final location and updated db'.format(tree.getId())
   print msg
   _writeSuccessFile(msg, successFname)
   return tree


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
   parser.add_argument('success_file', default=None,
            help=('Filename to be written on successful completion of script.'))
   
   # Optional
   parser.add_argument('--logname', type=str, default=None,
            help=('Basename of the logfile, without extension'))
   
   args = parser.parse_args()
   usr = args.user_id
   tree_name = args.tree_name
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
      baretree = Tree(tree_name, userId=usr)
      tree = scribe.getTree(tree=baretree)
      decoratedtree = squidifyTree(scribe, usr, tree, success_file)
   finally:
      scribe.closeConnections()
   
"""

"""
