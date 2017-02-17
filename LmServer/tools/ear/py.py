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

from LmCommon.common.lmconstants import ProcessType
from LmServer.common.log import ConsoleLogger
from LmServer.db.borgscribe import BorgScribe

# .............................................................................
class Ear(object):
   """
   @summary: Object to construct and parse filenames and URLs.
   """
# .............................................................................
# Constructor
# .............................................................................
   def __init__(self):
      """
      @summary: Constructor for the Updater object.  
      """
      pass

   # ...............................................
   @classmethod
   def receive(cls, ptype, objId, status, successFname, outputFnameList):
      scribe = BorgScribe(ConsoleLogger())
      scribe.openConnections()
      
      # All db updates
      try:
         if ProcessType.isOccurrence(ptype):
            obj = scribe.getOccurrenceSet(occid=objId)
         elif ProcessType.isProject(ptype):
            obj = scribe.getSDMProject(objId)
         elif ptype == ProcessType.RAD_BUILDGRID:
            obj = scribe.getShapeGrid(lyrId=objId)
         elif ProcessType.isMatrix(ptype):
            obj = scribe.getMatrix(mtxId=objId)
      except Exception, e:
         scribe.log.error('Failed to get object {} for processType {}'
                          .format(objId, ptype))
      try:
         obj.updateStatus(status)
         scribe.updateObject(obj)
      except Exception, e:
         scribe.log.error('Failed to update object {} for processType {}'
                          .format(objId, ptype))
         
      scribe.closeConnections()
      
   # ...............................................
   @staticmethod
   def occur(scribe, objId, status, successFname, outputFnameList):
      try:
         occ = scribe.getOccurrenceSet(occid=objId)
         occ.updateStatus(status)
         scribe.updateOccset()
      except Exception, e:
         scribe.log.error('Failed to get or update Occurrenceset {}')
   # ...............................................
   @staticmethod
   def project(scribe, objId, status, successFname, outputFnameList):
      pass
   
   # ...............................................
   @staticmethod
   def shape(scribe, objId, status, successFname, outputFnameList):
      pass
   
   # ...............................................
   @staticmethod
   def matrix(scribe, objId, status, successFname, outputFnameList):
      pass

   # ...............................................
   @staticmethod
   def mtxVector(scribe, objId, status, successFname, outputFnameList):
      pass


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
   