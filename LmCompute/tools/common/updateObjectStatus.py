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

from LmServer.common.log import ConsoleLogger
from LmServer.db.scribe import Scribe

# .............................................................................
if __name__ == "__main__":
   parser = argparse.ArgumentParser(description="This script updates a Lifemapper object")
   # Inputs
   parser.add_argument("processType", type=str, 
                       help="The process type of the object to update")
   parser.add_argument("objectId", type=str, 
                       help="The id of the object to update")
   parser.add_argument("successFile", type=str, 
            help="This file is created if the job was completed successfully.  Otherwise, it is not.")

   parser.add_argument("objectOutput", type=str, nargs="*", 
            help="These files are the outputs for an object to sanity check.  Each process type should know how many there are")

   # Status arguments
   parser.add_argument("-s", dest="status", type=int, 
                       help="The status to update the object with")
   parser.add_argument("-f", dest="status_file", type=str, 
                       help="A file containing the new object status")
   args = parser.parse_args()
   
   status = None
   if args.status is not None:
      status = args.status
   elif args.status_file is not None:
      with open(args.status_file) as statusIn:
         status = int(statusIn.read())

   if status is not None:
      scribe = Scribe(ConsoleLogger())
      scribe.openConnections()
      
      success = False
      # Aimee: Handle each object type here
      
      # args.objectOutput will contain file paths for created files
      
      if success:
         # Only write success file if successfully updated an object with 
         #    non-error status.  Otherwise, Makeflow should stop and that will 
         #    happen without this file
         with open(args.successFile, 'w') as successOut:
            successOut.write('1')
      
      scribe.closeConnections()
   else:
      raise Exception("Must provide status or status file")
   