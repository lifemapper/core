"""
@summary: This script creates a workflow to rebuild the solr index for a gridset
@author: CJ Grady
@version: 1.0.0
@status: beta
@license: gpl2
@copyright: Copyright (C) 2018, University of Kansas Center for Research

          Lifemapper Project, lifemapper [at] ku [dot] edu, 
          Biodiversity Institute,
          1345 Jayhawk Boulevard, Lawrence, Kansas, 66045, USA
   
          This program is free software; you can redistribute it and/or modify 
          it under the terms of the GNU General Public License as published by 
          the Free Software Foundation; either version 2 of the License, or (at 
          your option) any later version.
  
          This program is distributed in the hope that it will be useful, but 
          WITHOUT ANY WARRANTY; without even the implied warranty of 
          MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU 
          General Public License for more details.
  
          You should have received a copy of the GNU General Public License 
          along with this program; if not, write to the Free Software 
          Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 
          02110-1301, USA.
"""
import argparse
from mx.DateTime.DateTime import gmt
import os

from LmCommon.common.lmconstants import JobStatus
from LmServer.common.lmconstants import Priority
from LmServer.common.log import ScriptLogger
from LmServer.db.borgscribe import BorgScribe
from LmServer.legion.mtxcolumn import MatrixColumn
from LmServer.legion.processchain import MFChain

# The number of matrix columns to pull at a time
GROUP_SIZE = 100

# .............................................................................
def rebuild_index_for_gridset(gridset_id):
   """
   @summary: Creates a solr index rebuild workflow for a gridset
   @param gridset_id: The identifier for the gridset to use
   """
   log = ScriptLogger('rebuild_solr_gs_{}'.format(gridset_id))
   scribe = BorgScribe(log)
   scribe.openConnections()
   
   # Get gridset and fill PAMs
   gs = scribe.getGridset(gridsetId=gridset_id, fillMatrices=True)
   user_id = gs.getUserId()
   
   # Create makeflow
   wfMeta = {
      MFChain.META_CREATED_BY : os.path.basename(__file__),
      MFChain.META_DESCRIPTION : 'Reindex PAMs for gridset {}'.format(
                                                                  gridset_id)
   }
   new_wf = MFChain(user_id, priority=Priority.REQUESTED, metadata=wfMeta,
                   status=JobStatus.GENERAL, statusModTime=gmt().mjd)
   my_wf = scribe.insertMFChain(new_wf)
   
   # TODO: Determine what work directory should be
   work_dir = my_wf.getRelativeDirectory()
   
   
   # TODO: Should this only be rolling PAMs?
   for pam in gs.getPAMs():
      matrix_id = pam.getId()
      num_columns = scribe.countMatrixColumns(userId=user_id, 
                                                            matrixId=matrix_id)
      i = 0
      while i < num_columns:
         mcs = scribe.listMatrixColumns(i, GROUP_SIZE, userId=user_id, 
                                        matrixId=matrix_id, atom=False)
         i += GROUP_SIZE
         for mc in mcs:
            # Get layer
            lyr = scribe.getLayer(lyrId=mc.layerId)
            mc.layer = lyr
            
            mc.updateStatus(JobStatus.INITIALIZE)
            scribe.updateObject(mc)
            my_wf.addCommands(mc.computeMe(workDir=work_dir))
   
   my_wf.write()
   my_wf.updateStatus(JobStatus.INITIALIZE)
   scribe.updateObject(my_wf)
   
   scribe.closeConnections()


# .............................................................................
if __name__ == '__main__':
   desc = ''.join(['This script creates a workflow to reintersect all of the',
                   ' columns for each PAM in a gridset'])
   parser = argparse.ArgumentParser(description=desc)
   
   parser.add_argument('gridset_id', type=int, 
                       help='The ID of the gridset to reintersect')
   # TODO: Consider if we need parameter indicating that we should clear index
   
   args = parser.parse_args()
   
   rebuild_index_for_gridset(args.gridset_id)
   
