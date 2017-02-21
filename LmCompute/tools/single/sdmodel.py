#!/bin/bash
"""
@summary: This script attempts to generate a Maxent model from the given 
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
import json
from LmCommon.common.lmconstants import ProcessType
from LmCompute.plugins.single.maxent.meRunners import MaxentModel
from LmCompute.plugins.single.openModeller.omRunners import OpenModellerModel

# .............................................................................
if __name__ == '__main__':
   parser = argparse.ArgumentParser(
      description='This script generates a SDM ruleset from Lifemapper inputs') 
   
   parser.add_argument('processType', type=int, 
                       choices=[ProcessType.ATT_MODEL, ProcessType.OM_MODEL],
                       help="The Lifemapper process type of this model")
   parser.add_argument('jobName', type=str, help="A name for this model")
   parser.add_argument('pointsFn', type=str, 
          help="File location of occurrence set shapefile to use for modeling")
   parser.add_argument('layersJsonFile', type=str, 
          help="JSON file containing layer information for modeling")
   parser.add_argument('rulesetFn', type=str, 
                       help="File location to write the output ruleset")
   parser.add_argument('paramsJsonFile', type=str, 
          help="JSON file containing algorithm parameter information")

   # Optional arguments
   parser.add_argument('-p', '--package_file', type=str, 
                  help="If provided, write the model package to this location")
   parser.add_argument('-w', '--work_dir', type=str, 
                       help='Path for work directory creation. Defaults to current directory')
   parser.add_argument('--metrics_file', type=str, 
                       help='Optional output metrics file')
   parser.add_argument('-l', '--log_file', type=str, 
                       help='Optional output logfile')
   parser.add_argument('-s', '--status_file', type=str,
                       help='Optional output job status file')
   
   args = parser.parse_args()
   
   # Get algorithm parameters
   with open(args.paramsJson) as paramsIn:
      paramsJson = json.load(paramsIn)
   
   with open(args.layersJsonFile) as layersIn:
      layersJson = json.load(layersIn)
   
   if args.processType == ProcessType.ATT_MODEL:
      job = MaxentModel(args.jobName, args.pointsFn, layersJson, 
                        args.rulesetFn, paramsJson=paramsJson, 
                        packageFn=args.package_file, workDir=args.work_dir,
                        metricsFn=args.metrics_file, logFn=args.log_file,
                        statusFn=args.status_file)
   else:
      job = OpenModellerModel(args.jobName, args.pointsFn, layersJson, 
                        args.rulesetFn, paramsJson, 
                        packageFn=args.package_file, workDir=args.work_dir,
                        metricsFn=args.metrics_file, logFn=args.log_file,
                        statusFn=args.status_file)
   
   job.run()
   
