#!/bin/bash
"""
@summary: This script attempts to generate a Lifemapper occurrence set from 
             user CSV
@author: CJ Grady
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
import ast

from LmCommon.common.occparse import OccDataParser
from LmCompute.plugins.single.occurrences.csvOcc import createUserShapefile

# .............................................................................
if __name__ == "__main__":
   
   # Set up the argument parser
   parser = argparse.ArgumentParser(
      description="This script attempts to generate a Lifemapper occurrence set from user CSV") 
   
   parser.add_argument('pointsCsvFn', type=str,
                       help="A path to a raw user CSV file")
   parser.add_argument('metadataFile', type=str, 
                       help="CSV file of occurrence set metadata")
   parser.add_argument('outFile', type=str, 
                  help="The file location to write the shapefile for modeling")
   parser.add_argument('bigFile', type=str, 
           help="The file location to write the full occurrence set shapefile")
   parser.add_argument('maxPoints', type=int, 
               help="The maximum number of points for the modelable shapefile")
   args = parser.parse_args()
   
   meta, _, doMatchHeader = OccDataParser.readMetadata(args.metadataFile)
   createUserShapefile(args.pointsCsvFn, meta, args.outFile, 
                       args.bigFile, args.maxPoints)
   
"""
$PYTHON /opt/lifemapper/LmCompute/tools/single/user_points.py \
        /share/lm/data/archive/biotaphytest/000/000/006/277/pt_6277.csv \
        /share/lm/data/archive/biotaphytest/dirtyNAPlants_1m.meta \
        /tmp/mf_6418/pt_6277/pt_6277.shp \
        /tmp/mf_6418/pt_6277/bigpt_6277.shp \
        500


import json
import os
from LmCommon.common.apiquery import BisonAPI, IdigbioAPI
from LmCommon.common.occparse import OccDataParser
from LmCommon.shapes.createshape import ShapeShifter
from LmCommon.common.lmconstants import JobStatus, ProcessType
from LmCommon.common.readyfile import readyFilename
from LmCompute.common.lmObj import LmException
from LmCompute.common.log import LmComputeLogger
from LmCompute.plugins.single.occurrences.csvOcc import parseCsvData

pointCsvFn = '/share/lm/data/archive/biotaphytest/000/000/006/277/pt_6277.csv'
metafname='/share/lm/data/archive/biotaphytest/dirtyNAPlants_1m.meta'
outFile = '/tmp/mf_6418/pt_6277/pt_6277.shp'
bigFile = '/tmp/mf_6418/pt_6277/bigpt_6277.shp'
readyFilename(outFile, overwrite=True)
readyFilename(bigFile, overwrite=True)
maxPoints = 500



with open(pointCsvFn) as inF:
   csvInputBlob = inF.read()

meta, _, doMatchHeader = OccDataParser.readMetadata(metafname)
header = None
if doMatchHeader:
   header = True
   
(fieldIndexMeta, filters, idIdx, xIdx, yIdx, ptIdx, groupByIdx, 
  nameIdx) = OccDataParser.getMetadata(meta, header)
  
count = len(csvInputBlob.split('\n')) - 2

parseCsvData(csvInputBlob, ProcessType.USER_TAXA_OCCURRENCE, outFile, 
            bigFile, count, maxPoints, metadata=meta, isUser=True)

logger = LmComputeLogger('testpoints')
shaper = ShapeShifter(ProcessType.USER_TAXA_OCCURRENCE, csvInputBlob, 21, logger=logger, metadata=meta)
op = shaper.op

outDs = bigDs = None             
outDs = shaper._createDataset(outFile)
outLyr = shaper._addUserFieldDef(outDs)
lyrDef = outLyr.GetLayerDefn()

shaper.processType == ProcessType.USER_TAXA_OCCURRENCE




"""