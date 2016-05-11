"""
@summary: This script will build job requests and write them as files
@author: CJ Grady
@status: alpha
@version: 0.1
@license: gpl2
@copyright: Copyright (C) 2016, University of Kansas Center for Research

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
from LmServer.common.lmconstants import JobFamily
from LmServer.common.log import ScriptLogger
from LmServer.db.scribe import Scribe
from LmWebServer.formatters.jobFormatter import JobFormatter

# .............................................................................
if __name__ == "__main__":
   
   parser = argparse.ArgumentParser(prog="Lifemapper Job Request creator",
                      description="Creates a job request file from the inputs",
                      version="1.0.0")
   parser.add_argument('jobFamily', type=int, 
      choices=[JobFamily.SDM, JobFamily.RAD],
      help="Generate a job request from this family (%s - SDM, %s - RAD)" % (
                                                 JobFamily.SDM, JobFamily.RAD))
   parser.add_argument('jobId', type=int, 
                       help="Generate a job request for this job id")
   parser.add_argument('-f', nargs='+', type=str,
                    help="Write the output here.  Write to std out if missing")
   
   args = parser.parse_args()
   
   jobFam = args.jobFamily
   jobId = args.jobId
   fn = args.f
   
   scribe = Scribe(ScriptLogger)
   scribe.openConnections()
   
   job = scribe.getJob(jobFam, jobId)
   
   scribe.closeConnections()
   
   jf = JobFormatter(job)
   
   output = str(jf.format())
   
   if fn is not None:
      with open(fn, 'w') as outF:
         outF.write(output)
   else:
      print(output)
      