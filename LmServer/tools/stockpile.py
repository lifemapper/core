#!/bin/bash
"""
@summary: This script updates a Lifemapper object in the database
@version: 4.0.0
@status: beta

@license: gpl2
@copyright: Copyright (C) 2017, University of Kansas Center for Research

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
import os

from LmCommon.common.lmconstants import ProcessType, LMFormat, JobStatus
from LmServer.base.layer2 import Vector, Raster
from LmServer.base.lmobj import LMError, LMObject
from LmServer.common.log import ConsoleLogger
from LmServer.db.borgscribe import BorgScribe

# .............................................................................
class Stockpile(LMObject):
   """
   @summary: Object to receive results of MF commands, update database with 
             success status.
   """
# .............................................................................
   @classmethod
   def testAndStash(cls, ptype, objId, status, successFname, outputFnameList):
      """
      @summary: Test output files and update DB with status.  Only success for
                all outputs = JobStatus.COMPLETE 
      @param ptype: LmCommon.common.lmconstants.ProcessType for the process
             being examined
      @param objId: Unique database ID for the object to update
      @param status: Status returned by the computational process??
      @param successFname: Filename to be written IFF success=True. Contains  
             final status and testing results 
      @param outputFnameList: List of output files returned by the computational 
             process
      @TODO: 'status' is currently unused. Are we getting a status file or 
             integer for updated object?
      """
      outputInfo = []
      success = True
      # Test each file
      for fname in outputFnameList:
         currSuccess, msg = cls.testFile(fname)
         if not currSuccess:
            success = False
            outputInfo.append(msg)
            
      # TODO: Override status on failure?
      if not success:
         status = JobStatus.GENERAL_ERROR
      else:
         status = JobStatus.COMPLETE

      # Update database
      scribe = BorgScribe(ConsoleLogger())
      scribe.openConnections()
      try:
         cls._updateObject(scribe, ptype, objId, status)
      except:
         # TODO: raise exception, or write info to file?
         pass
      finally:
         scribe.closeConnections()
         
      return success
      
# .............................................................................
   @classmethod
   def _updateObject(cls, scribe, ptype, objId, status):
      """
      @summary: Get object and update DB with status.  
      """
      msgs = []
      try:
         # Get object
         if ProcessType.isOccurrence(ptype):
            obj = scribe.getOccurrenceSet(occid=objId)
         elif ProcessType.isProject(ptype):
            obj = scribe.getSDMProject(objId)
         elif ptype == ProcessType.RAD_BUILDGRID:
            obj = scribe.getShapeGrid(lyrId=objId)
         elif ProcessType.isMatrix(ptype):
            obj = scribe.getMatrix(mtxId=objId)
      except Exception, e:
         msg = 'Failed to get object {} for process {}'.format(objId, ptype)
         msgs.append(msg)
         raise LMError(currargs=msg)
      
      # Update object and db record
      try:
         obj.updateStatus(status)
         scribe.updateObject(obj)
      except Exception, e:
         msg = ('Failed to update object {} for process {}, ({})'
                .format(objId, ptype, str(e)))
         msgs.append(msg)
         raise LMError(currargs=msg)

   # ...............................................
   @classmethod
   def testFile(cls, outputFname):
      success = True
      msg = None
      basename, ext = os.path.splitext(outputFname)
      if not os.path.exists(outputFname):
         msg = 'File {} does not exist'.format(outputFname)
         success = False
      elif LMFormat.isTestable(ext):
         if LMFormat.isGeo(ext):
            fileFormat = LMFormat.getFormatByExtension(ext)
            if LMFormat.isOGR(ext=ext):
               success, featCount = Vector.testVector(outputFname, 
                                                      driver=fileFormat.driver)
               if not success:
                  msg = 'File {} is not a valid {} file'.format(outputFname, 
                                                                fileFormat.driver)
               elif featCount < 1:
                  msg = 'Vector {} has no features'.format(outputFname)
                  
            elif LMFormat.isGDAL(ext=ext):
               success = Raster.testRaster(outputFname)
               if not success:
                  msg = 'File {} is not a valid GDAL file'.format(outputFname)
         else:
            f = open(outputFname, 'r')
            data = f.read()
            f.close()
            if LMFormat.isJSON(ext):
               import json
               try:
                  json.loads(data)
               except:
                  success = False
                  msg = 'File {} does not contain valid JSON'.format(outputFname)
      return success, msg

# .............................................................................
if __name__ == "__main__":
   parser = argparse.ArgumentParser(description='This script updates a Lifemapper object')
   # Inputs
   parser.add_argument('processType', type=int, 
                       help='The process type of the object to update')
   parser.add_argument('objectId', type=int, 
                       help='The id of the object to update')
   parser.add_argument('successFilename', type=str, 
                       help=('File to be created only if the job was ' + 
                             'completed successfully.'))
   parser.add_argument('objectOutput', type=str, nargs='*', 
                       help=('Files to sanity check. '))
   # Status arguments
   parser.add_argument('-s', dest='status', type=int, 
                       help='The status to update the object with')
   parser.add_argument('-f', dest='status_file', type=str, 
                       help='A file containing the new object status')
   args = parser.parse_args()
   
   # Status comes in as an integer or file 
   status = None
   if args.status is not None:
      status = args.status
   elif args.status_file is not None:
      with open(args.status_file) as statusIn:
         status = int(statusIn.read())
   ptype = args.processType
   objId = args.objectId
   successFname = args.successFilename
   outputFnameList = args.objectOutput
   
   success = Stockpile.testAndStash(ptype, objId, status, successFname, 
                                    outputFnameList)
      
   if success:
      # Only write success file if successfully updated an object with 
      #    non-error status.  Otherwise, Makeflow should stop and that will 
      #    happen without this file
      with open(args.successFilename, 'w') as successOut:
         successOut.write('1')

   
"""
Call like:
$PYTHON stockpile.py <ptype>  <objId>  <successFname>  <outputFnameList>  
               <-s status OR -f statusFname>

$PYTHON /opt/lifemapper/LmServer/tools/stockpile.py 420 504 pt_504.success /share/lm/data/archive/ryan/000/000/000/504/pt_504.shp


import os
from LmCommon.common.lmconstants import ProcessType, LMFormat, JobStatus
from LmServer.base.layer2 import Vector, Raster
from LmServer.base.lmobj import LMError, LMObject
from LmServer.common.log import ConsoleLogger
from LmServer.db.borgscribe import BorgScribe
from LmServer.tools.stockpile import *

status = 999

ptype = ProcessType.USER_TAXA_OCCURRENCE
objId = 504
successFname = 'pt_504.success'
outputFnameList = ['/share/lm/data/archive/ryan/000/000/000/504/pt_504.shp']

220 3721 prj_3856.success /share/lm/data/archive/ryan/000/000/000/532/prj_3856.tif /share/lm/data/archive/ryan/000/000/000/532/prj_3856.zip
ptype = ProcessType.OM_PROJECT
objId = 3523
successFname = 'prj_3658.success'
outputFnameList = ['/share/lm/data/archive/ryan/4326/Layers/prj_3658.tif', 
                   '/share/lm/data/archive/ryan/000/000/000/504/prj_3658.zip']
/opt/lifemapper/LmServer/tools/stockpile.py 220 3523 
stp = Stockpile()

scribe = BorgScribe(ConsoleLogger())
scribe.openConnections()

Stockpile._updateObject(scribe, ptype, objId, status)

scribe.closeConnections()

stp.testAndStash(ptype, objId, status, successFname, 
                                    outputFnameList)


"""
   