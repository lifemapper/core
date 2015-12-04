"""
@license: gpl2
@copyright: Copyright (C) 2015, University of Kansas Center for Research

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
import mx.DateTime as DT
import os

from LmCommon.common.lmconstants import DEFAULT_POST_USER
from LmCommon.common.localconstants import ARCHIVE_USER
from LmServer.common.log import ScriptLogger
from LmServer.db.scribe import Scribe
from LmServer.common.lmconstants import ReferenceType
# ...............................................
if __name__ == '__main__':
   parser = argparse.ArgumentParser(
                description="Script to determine the computational state of objects")      
   parser.add_argument('-e', "--email", type=str, 
                        help='Email recipient for summary')
   
   args = parser.parse_args()
   email = None
   if args.email:
      email = args.email
      
   oneDayAgo = DT.gmt() - (1/24.0)
   oneMonthAgo = DT.gmt() - 30
   display = {oneDayAgo: 'Day', oneMonthAgo: 'Month', None: 'Total'}

   USERS = (DEFAULT_POST_USER, ARCHIVE_USER, None)
   TIMES = (oneDayAgo, oneMonthAgo, None)
   OBJ_TYPES = ReferenceType.sdmTypes
   OBJ_TYPES.remove(ReferenceType.SDMExperiment)

   basename = os.path.splitext(os.path.basename(__file__))[0]
   logger = ScriptLogger(basename)
   scribe = Scribe(logger)
   scribe.openConnections()
   
   stats = {}
   for timeagg in TIMES:
      for usr in USERS:
         theseStats = scribe.getProgress(usrid=usr, starttime=timeagg)
         stats[timeagg][usr] = theseStats
   
   allStatii = set()
   for reftype in OBJ_TYPES:
      allStatii = allStatii.union(stats[None][None].keys())
   allStatii = list(allStatii)
   allStatii.sort()
   
   outputLines = []
   for timeagg in TIMES:
      outputLines.append('Aggregate: {}'.format(display[timeagg]))
      for usr in USERS:
         outputLines.append('')
         outputLines.append('User: {}'.format(usr))
         outputLines.append('{0: <15}{1: <15}{2: <15}{3: <15}'.format(
                            'Status', ReferenceType.name(rt) for rt in OBJ_TYPES))
         for stat in allStatii:
            line = '{0: <15}'.format(stat)
            for reftype in OBJ_TYPES: 
               line += '{0: <15}'.format(stats[timeagg][usr][stat])
            outputLines.append(line)
   output = '\n'.join(outputLines)
   logger.info(output)
   scribe.closeConnections()
         
   
   
   
   
   
   