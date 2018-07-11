#!/bin/bash
"""
@summary: This script attempts to generate a Lifemapper occurrence set from 
             a GBIF csv dump
@author: CJ Grady
@version: 4.0.0
@status: beta

@license: gpl2
@copyright: Copyright (C) 2018, University of Kansas Center for Research

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

from LmCompute.plugins.single.occurrences.csvOcc import createGBIFShapefile

# .............................................................................
if __name__ == "__main__":
   
   # Set up the argument parser
   parser = argparse.ArgumentParser(
      description="This script attempts to generate a Lifemapper occurrence set from a GBIF csv dump") 
   
   parser.add_argument('-s', '--status_filename', type=str, 
                       help='Write job status to this file')
   
   parser.add_argument('pointsCsvFn', type=str, 
                       help="A path to a CSV file with raw GBIF points")
   parser.add_argument('pointCount', type=int, 
                       help="A reported count of entries in the CSV file")
   
   parser.add_argument('outFile', type=str, 
                  help="The file location to write the shapefile for modeling")
   parser.add_argument('bigFile', type=str, 
           help="The file location to write the full occurrence set shapefile")
   parser.add_argument('maxPoints', type=int, 
               help="The maximum number of points for the modelable shapefile")
   args = parser.parse_args()
   
   createGBIFShapefile(args.pointsCsvFn, args.outFile, args.bigFile, 
                       args.pointCount, args.maxPoints, 
                       statusFname=args.status_filename)
   
"""
from LmCompute.plugins.single.occurrences.csvOcc import createGBIFShapefile
from LmCommon.common.lmconstants import JobStatus, ProcessType
from LmCommon.common.apiquery import *
from LmCommon.shapes.createshape import ShapeShifter
from LmCommon.common.lmconstants import JobStatus, ProcessType
from LmCompute.common.lmObj import LmException
from LmCompute.common.log import LmComputeLogger
from LmServer.db.borgscribe import BorgScribe

from LmCommon.common.readyfile import readyFilename
from LmCompute.plugins.single.occurrences.csvOcc import *
logger = LmComputeLogger('crap')

maxPoints = 500
processType=ProcessType.GBIF_TAXA_OCCURRENCE

pointsCsvFn = '/share/lm/data/archive/kubi/000/000/000/001/pt_1.csv'
outFile = '/tmp/pt_1.shp'
bigFile = '/tmp/bigpt_1.shp'
with open(pointsCsvFn) as inF:
   rawData = inF.readlines()
count = len(rawData)

parseCsvData(''.join(rawData), ProcessType.GBIF_TAXA_OCCURRENCE, outFile, 
                       bigFile, len(rawData), maxPoints)


readyFilename(outFile, overwrite=True)
readyFilename(bigFile, overwrite=True)
shaper = ShapeShifter(processType, ''.join(rawData), count, logger=logger, metadata=None)


outDs = shaper._createDataset(outFile)
outLyr = shaper._addFieldDef(outDs)
lyrDef = outLyr.GetLayerDefn()
recDict = shaper._getRecord()
while recDict is not None:
   shaper._createFillFeat(lyrDef, recDict, outLyr)
   recDict = shaper._getRecord()


shaper.writeOccurrences(outFile, maxPoints=maxPoints, bigfname=bigFile, isUser=False)



                       


success = False
badrecs = 0
while not success:
   try:
      tmpDict = shaper._reader.next()
      success = True
   except StopIteration, e:
      success = True
   except Exception, e:
      print str(e)
      badrecs += 1


"""