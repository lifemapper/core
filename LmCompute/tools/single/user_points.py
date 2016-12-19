#!/bin/bash
"""
@summary: This script attempts to generate a Lifemapper occurrence set from 
             user CSV
@author: CJ Grady
@version: 4.0.0
@status: beta

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
from LmCompute.plugins.single.csvocc.csvoccRunners import CSVRetrieverRunner

# .............................................................................
if __name__ == "__main__":
   
   # Set up the argument parser
   parser = argparse.ArgumentParser(
      description="This script attempts to generate a Lifemapper occurrence set from user CSV") 
   
   parser.add_argument('-n', '--job_name', dest='jobName', type=str,
                               help="Use this as the name of the job (for logging and work directory creation).  If omitted, one will be generated")
   parser.add_argument('-o', '--out_dir', dest='outDir', type=str, 
                               help="Write the final outputs to this directory")
   parser.add_argument('-w', '--work_dir', dest='workDir', type=str, 
                               help="The workspace directory where the work directory should be created.  If omitted, will use current directory")
   parser.add_argument('-l', '--log_file', dest='logFn', type=str, 
                               help="Where to log outputs (don't if omitted)")
   parser.add_argument('-ll', '--log_level', dest='logLevel', type=str, 
                               help="What level to log at", 
                               choices=['info', 'debug', 'warn', 'error', 'critical'])
   parser.add_argument('-s', '--status_fn', dest='statusFn', type=str,
                       help="If this is not None, output the status of the job here")
   parser.add_argument('--metrics', type=str, dest='metricsFn', 
                               help="If provided, write metrics to this file")
   parser.add_argument('--cleanup', type=bool, dest='cleanUp', 
                               help="Clean up outputs or not", 
                               choices=[True, False])
   parser.add_argument('jobXml', type=str, 
                               help="Job configuration information XML file")
   
   args = parser.parse_args()
   
   job = CSVRetrieverRunner(args.jobXml, jobName=args.jobName, 
               outDir=args.outDir, workDir=args.workDir, 
               metricsFn=args.metricsFn, logFn=args.logFn, 
               logLevel=args.logLevel, statusFn=args.statusFn)
   job.run()
   