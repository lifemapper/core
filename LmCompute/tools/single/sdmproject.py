#!/bin/bash
"""
@summary: This script attempts to generate a Maxent projection from the given 
             Lifemapper inputs
@author: CJ Grady
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
from LmCompute.plugins.single.maxent.meRunners import MEProjectionRunner
from LmCompute.plugins.single.openModeller.omRunners import OMProjectionRunner

# .............................................................................
if __name__ == "__main__":
   
   # Set up the argument parser
   parser = argparse.ArgumentParser(
      description='This script generates a SDM projection from Lifemapper inputs') 
   
   parser.add_argument('-n', '--job_name', type=str,
                       help='Use this as the name of the job (for logging and work directory creation).  If omitted, one will be generated')
   parser.add_argument('-p', '--process_type', type=int,
                       choices=[ProcessType.ATT_PROJECT, ProcessType.OM_PROJECT],
                       help='Type of SDM to run, ProcessType.ATT_MODEL or ProcessType.OM_MODEL')
   parser.add_argument('-o', '--out_dir', type=str, 
                       help='Write the final outputs to this directory')
   parser.add_argument('-w', '--work_dir', type=str, 
                       help='The workspace directory where the work directory should be created.  If omitted, will use current directory')
   parser.add_argument('-l', '--log_file', type=str, 
                       help='Optional output logfile')
   parser.add_argument('-ll', '--log_level', type=str, 
                       help='Log level for optional logging', 
                       choices=['info', 'debug', 'warn', 'error', 'critical'])
   parser.add_argument('-s', '--status_file', type=str,
                       help='Optional output job status file')
   parser.add_argument('--metrics_file', type=str, 
                       help='Optional output metrics file')
   parser.add_argument('--cleanup', type=bool, 
                       help='Clean up outputs or not', 
                       choices=[True, False])
   parser.add_argument('jobXml', type=str, 
                       help='Job configuration information XML file')
   
   args = parser.parse_args()
   
   if args.process_type == ProcessType.ATT_PROJECT:
      job = MEProjectionRunner(args.jobXml, jobName=args.job_name, 
                               outDir=args.out_dir, workDir=args.work_dir, 
                               metricsFn=args.metrics_file, logFn=args.log_file, 
                               logLevel=args.log_level, 
                               statusFn=args.status_file)
   else:
      job = OMProjectionRunner(args.jobXml, jobName=args.job_name, 
                               outDir=args.out_dir, workDir=args.work_dir, 
                               metricsFn=args.metrics_file, logFn=args.log_file, 
                               logLevel=args.log_level, 
                               statusFn=args.status_file)
   job.run()
   