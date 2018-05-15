"""
@summary: This script will reinitialize objects that have been abandoned
@note: This script will create new Makeflows for all matching objects even if 
          they are in another Makeflow.  It does not determine what has been 
          abandoned.
@author: CJ Grady
@version: 1.0
@status: alpha
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
from mx.DateTime import gmt
import os

from LmCommon.common.lmconstants import JobStatus

from LmServer.common.lmconstants import Priority
from LmServer.common.localconstants import PUBLIC_USER
from LmServer.common.log import ScriptLogger
from LmServer.db.borgscribe import BorgScribe
from LmServer.legion.processchain import MFChain

# .............................................................................
def reinitProjections(beginStatus, endStatus, groupSize, log, procParams=None,
                                           gridsetId=None, userId=PUBLIC_USER):
   """
   @summary: Reinitializes projections for computation
   """
   scribe = BorgScribe(log)
   scribe.openConnections()
   
   # TODO: Get procParams
   if procParams:
      maskLayer = scribe.getLayer(userId=userId, lyrName=procParams['mask']['mask_name'])
      procParams['mask']['mask_layer'] = maskLayer
   
   count = scribe.countSDMProjects(userId, afterStatus=beginStatus, 
                                   beforeStatus=endStatus, gridsetId=gridsetId)
   i = 0
   while i < count:
      prjs = scribe.listSDMProjects(i, groupSize, userId=userId, 
                                    afterStatus=beginStatus, 
                                    beforeStatus=endStatus, 
                                    gridsetId=gridsetId, atom=False)
      
      desc = 'Makeflow for abandoned projections'
      meta = {
         MFChain.META_CREATED_BY : os.path.basename(__file__),
         MFChain.META_DESCRIPTION : desc
      }
      
      newMFC = MFChain(userId, metadata=meta, priority=Priority.REQUESTED,
                       status=JobStatus.GENERAL, statusModTime=gmt().mjd)
      mfChain = scribe.insertMFChain(newMFC)
      scribe.updateObject(mfChain)
      
      workDir = 'mf_{}'.format(mfChain.getId())
      
      rules = []
      for prj in prjs:
         rules.extend(prj.computeMe(workDir=workDir, procParams=procParams,
                                    addOccRules=True))
      
      
      mfChain.addCommands(rules)
      mfChain.write()
      mfChain.updateStatus(JobStatus.INITIALIZE)
      scribe.updateObject(mfChain)
      log.debug('Wrote makeflow {} to {}'.format(mfChain.getId(), 
                                                 mfChain.getDLocation()))
      
      i += groupSize
   
   
   
   scribe.closeConnections()
   
   
   
# .............................................................................
if __name__ == '__main__':
   log = ScriptLogger('abandons')
   
   # TODO: Get this from an argument
   procParams = {
      'mask' : {
         'mask_name' : 'ecoreg_30sec_na',
         'code' : 'hull_region_intersect',
         'buffer' : 0.5
      }
   }
   
   parser = argparse.ArgumentParser(
               description='This script finds and reinitializes projections')
   parser.add_argument('beginStatus', type=int, 
                       help='Reset projections that have at least this status')
   parser.add_argument('endStatus', type=int, 
                       help='Reset projections that have at most this status')
   parser.add_argument('groupSize', type=int, 
                   help='Group this many projections together for computation')
   parser.add_argument('-g', '--gridsetId', type=int, 
                  help='If provided, only reset projections from this gridset')
   parser.add_argument('-u', '--userId', type=str, 
                  help='Reset projections for this user, defaults to public')
   
   args = parser.parse_args()
   
   if args.gridsetId is None:
      gsId = None
   else:
      gsId = args.gridsetId
      
   if args.userId is None:
      userId = PUBLIC_USER
   else:
      userId = args.userId 
   
   reinitProjections(args.beginStatus, args.endStatus, args.groupSize, log, 
                     procParams=procParams, gridsetId=gsId, userId=userId)
   
   