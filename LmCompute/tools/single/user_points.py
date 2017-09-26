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
                       help="JSON file of occurrence set metadata")
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
        /share/lm/data/archive/biotaphy/000/000/000/006/pt_6.csv \
        /share/lm/data/archive/biotaphy/heuchera_all.meta \
        mf_18/pt_6/pt_6.shp mf_18/pt_6/bigpt_6.shp 500



import os

from LmCommon.common.lmconstants import (ENCODING, BISON, BISON_QUERY,
               GBIF, GBIF_QUERY, IDIGBIO, IDIGBIO_QUERY, PROVIDER_FIELD_COMMON, 
               LM_ID_FIELD, LM_WKT_FIELD, ProcessType, JobStatus,
               DWCNames, LMFormat, DEFAULT_EPSG)
from types import UnicodeType, StringType

from LmCommon.common.apiquery import BisonAPI, IdigbioAPI
from LmCommon.shapes.createshape import ShapeShifter
from LmCommon.common.lmconstants import JobStatus, ProcessType
from LmCommon.common.readyfile import readyFilename
from LmCompute.common.lmObj import LmException
from LmCompute.common.log import LmComputeLogger
from LmCommon.common.occparse import OccDataParser

infname = '/share/lm/data/archive/biotaphy/000/000/000/006/pt_6.csv'
inmeta = '/share/lm/data/archive/biotaphy/heuchera_all.meta'
outFile = 'mf_18/pt_6/pt_6.shp'
bigFile = 'mf_18/pt_6/big_6.shp'
mxpts = 500
meta, _, doMatchHeader = OccDataParser.readMetadata(inmeta)                   
pointCsvFn = infname
with open(pointCsvFn) as inF:
   csvInputBlob = inF.read()


count = len(csvInputBlob.split('\n')) - 2

readyFilename(outFile, overwrite=True)
readyFilename(bigFile, overwrite=True)
logger = LmComputeLogger('testpoints')
shaper = ShapeShifter(ProcessType.USER_TAXA_OCCURRENCE, csvInputBlob, 21, logger=logger, metadata=meta)
op = shaper.op

outDs = bigDs = None             
outDs = shaper._createDataset(outFile)
outLyr = shaper._addUserFieldDef(outDs)
lyrDef = outLyr.GetLayerDefn()

shaper.processType == ProcessType.USER_TAXA_OCCURRENCE

recDict = shaper._getRecord()

feat = ogr.Feature(lyrDef)
x = recDict[op.xFieldName]
y = recDict[op.yFieldName]
wkt = 'POINT ({} {})'.format(x, y)
feat.SetField(LM_WKT_FIELD, wkt)
geom = ogr.CreateGeometryFromWkt(wkt)
feat.SetGeometryDirectly(geom)

for name in recDict.keys():
   fldname = shaper._lookup(name)
   if fldname is not None:
      val = recDict[name]
      if val is not None and val != 'None':
         if isinstance(val, UnicodeType):
            val = fromUnicode(val)
         feat.SetField(fldname, val)


"""