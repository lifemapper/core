"""
@summary: Module containing GBIF functions
@author: Aimee Stewart
@version: 3.0.0
@status: beta

@license: gpl2
@copyright: Copyright (C) 2015, University of Kansas Center for Research

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
import os
from osgeo import ogr, osr
import StringIO

from LmCommon.common.createshape import ShapeShifter
from LmCommon.common.lmconstants import OutputFormat, ProcessType, JobStatus
from LmCompute.common.lmObj import LmException

# .............................................................................
# PUBLIC
# .............................................................................

# .............................................................................
def parseCSVData(log, count, csvInputBlob, metadata, basePath, env, maxPoints):
   """
   @summary: Parses a CSV-format data set and saves it to a shapefile in the 
                specified location
   @param csvInputBlob: A string of CSV data
   @param basePath: A directory where the shapefile should be stored
   @param maxPoints: The maximum number of points to include if subsetting
   @return: The name of the file where the data is stored (.shp extension)
   @rtype: String
   """
   # First line is column names
   if csvInputBlob is None or len(csvInputBlob.strip()) <= 1:
      raise LmException(JobStatus.OCC_NO_POINTS_ERROR, 
                        "The CSV provided was empty")
   if env is not None:
      outfilename = env.getTemporaryFilename(OutputFormat.SHAPE, base=basePath)
   else:
      outfilename = os.path.join(basePath, 'testocc.shp')
      
   subsetOutfilename = None
   subsetIndices = None
   
   if count > maxPoints:
      if env is not None:
         subsetOutfilename = env.getTemporaryFilename(OutputFormat.SHAPE, base=basePath)
      else:
         subsetOutfilename = os.path.join(basePath, 'subset_testocc.shp')
         
   shaper = ShapeShifter(ProcessType.USER_TAXA_OCCURRENCE, csvInputBlob, count, 
                         logger=log, metadata=metadata)
   shaper.writeUserOccurrences(outfilename, maxPoints=maxPoints, 
                               subsetfname=subsetOutfilename)

   return outfilename, subsetOutfilename

# ...............................................
if __name__ == '__main__':
   from LmCompute.common.log import RetrieverLogger
   PRAGMA_META = {'gbifid': ('gbifid', 'integer', 'id'),
                 'datasetkey': ('datasetkey', 'string'), 
                 'occurrenceid': ('occurid', 'string'),
                 'kingdom': ('kingdom', 'string'),
                 'phylum': ('phylum', 'string'),
                 'class': ('class', 'string'),
                 'order': ('order', 'string'),
                 'family': ('family', 'string'),
                 'genus': ('genus', 'string'),
                 'species': ('species', 'string', 'dataname'),
                 'infraspecificepithet': ('isepithet', 'string'), 
                 'taxonrank': ('taxonrank', 'string', ['SUBSPECIES', 'SPECIES']),
                 'scientificname': ('sciname', 'string'),
                 'countrycode': ('cntrycode', 'string'),
                 'locality': ('locality', 'string'),
                 'publishingorgkey': ('puborgkey', 'string'),
                 'decimallatitude': ('dec_lat', 'real', 'latitude'),
                 'decimallongitude': ('dec_long', 'real', 'longitude'),
                 'elevation': ('elevation', 'real'),
                 'elevationaccuracy': ('elev_acc', 'real'),
                 'depth': ('depth', 'real'),
                 'depthaccuracy': ('depth_acc', 'real'),
                 'eventdate': ('eventdate', 'string'),
                 'day': ('day', 'integer'),
                 'month': ('month', 'integer'),
                 'year': ('year', 'integer'),
                 'taxonkey': ('taxonkey', 'integer', 'groupby'),
                 'specieskey': ('specieskey', 'integer'),
                 'basisofrecord': ('basisofrec', 'string'),
                 'institutioncode': ('inst_code', 'string'),
                 'collectioncode': ('coll_code', 'string'),
                 'catalognumber': ('catnum', 'string'),
                 'recordnumber': ('recnum', 'string'),
                 'identifiedby': ('idby', 'string'),
                 'rights': ('rights', 'string'),
                 'rightsholder': ('rightshold', 'string'),
                 'recordedby': ('rec_by', 'string'),
                 'typestatus': ('typestatus', 'string'),
                 'establishmentmeans': ('estabmeans', 'string'),
                 'lastinterpreted': ('lastinterp', 'string'),
                 'mediatype': ('mediatype', 'string'),
                 'issue': ('issue', 'string') }


   fname = '/share/lmserver/data/archive/pragma/000/000/002/291/pt_2291.csv'
   basepth = '/tmp/'
   env = None
   maxPoints = 1000000
   
   f = open(fname)
   csvblob = f.read()
   f.close()
   
   # data must have a header
   count = len(csvblob) - 1
   
   log = RetrieverLogger('csvocc')
   fname, tmp = parseCSVData(log, count, csvblob, PRAGMA_META, basepth, env, 
                             maxPoints)
   print fname, tmp