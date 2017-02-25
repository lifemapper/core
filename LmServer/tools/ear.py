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
from osgeo import gdal, ogr

from LmCommon.common.lmconstants import ProcessType, LMFormat
from LmServer.base.layer2 import Vector, Raster
from LmServer.base.lmobj import LMError, LMObject
from LmServer.common.log import ConsoleLogger
from LmServer.db.borgscribe import BorgScribe

# .............................................................................
class Ear(LMObject):
   """
   @summary: Object to construct and parse filenames and URLs.
   """
# .............................................................................
   @classmethod
   def receive(cls, ptype, objId, status, successFname, outputFnameList):
      scribe = BorgScribe(ConsoleLogger())
      scribe.openConnections()
      outputInfo = []      
      # ...............................
      # Database update
      try:
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
            outputInfo.append(msg)
            raise LMError(currargs=msg)
         # Update object and db record
         try:
            obj.updateStatus(status)
            scribe.updateObject(obj)
         except Exception, e:
            msg = 'Failed to update object {} for process {}'.format(objId, ptype)
            outputInfo.append(msg)
            raise LMError(currargs=msg)
      except:
         # TODO: raise exception, or write info to file?
         pass
      finally:
         # Close DB   
         scribe.closeConnections()
      # ...............................
      # Test outputs
      success = True
      for fname in outputFnameList:
         currSuccess, msg = cls.testFile(fname)
         if not currSuccess:
            success = False
            outputInfo.append(msg)
      # ...............................
      # Write status file
      # TODO: What do we want this file to contain
      f = open(successFname, 'w')
      f.write(str(success))
      for msg in outputInfo:
         f.write('# {}'.format(msg))
      f.close()
      
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
            if LMFormat.isOGR(ext):
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
   parser.add_argument('successFile', type=str, 
                       help=('File to be created only if the job was ' + 
                             'completed successfully.'))
   parser.add_argument('objectOutput', type=str, nargs='*', 
                       help=('Output files for sanity checks. Each process ' +
                             'type should know what these should be'))
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
   successFname = args.successFile
   outputFnameList = args.objectOutput
   
   if status is not None:
      success = Ear.receive(ptype, objId, status, successFname, outputFnameList)
      
      if success:
         # Only write success file if successfully updated an object with 
         #    non-error status.  Otherwise, Makeflow should stop and that will 
         #    happen without this file
         with open(args.successFile, 'w') as successOut:
            successOut.write('1')
   else:
      raise Exception('Must provide status or status file')
   