# !/bin/bash
"""
@summary: This script retrieves the convex hull of an occurrence set from the 
             object and writes it to a new shapefile
@author: CJ Grady
@version: 4.0.0
@status: beta

@license: gpl2
@copyright: Copyright (C) 2019, University of Kansas Center for Research

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

from LmCommon.common.lmconstants import LMFormat
from LmServer.common.log import ConsoleLogger
from LmServer.db.borgscribe import BorgScribe
from osgeo import ogr

# .............................................................................
if __name__ == "__main__":
   # Set up the argument parser
   parser = argparse.ArgumentParser(
      description='This script writes the convex hull for an occurrence set')

   parser.add_argument('occId', type=int, help='The occurrence set database id')
   parser.add_argument('outFile', type=str,
                       help='The file location to write the shapefile')
   parser.add_argument('-b', '--buffer', type=float,
                       help='How big the buffer should be (in map units)')

   args = parser.parse_args()

   scribe = BorgScribe(ConsoleLogger())
   scribe.openConnections()

   occ = scribe.getOccurrenceSet(occId=args.occId)
   occ.readShapefile()

   chWkt = occ.getConvexHullWkt(convexHullBuffer=args.buffer)

   scribe.closeConnections()

   drv = ogr.GetDriverByName(LMFormat.getDefaultOGR().driver)
   ds = drv.CreateDataSource(args.outFile)
   lyr = ds.CreateLayer('', None, ogr.wkbPolygon)
   lyr.CreateField(ogr.FieldDefn('id', ogr.OFTInteger))
   defn = lyr.GetLayerDefn()

   feat = ogr.Feature(defn)
   feat.SetField('id', 1)
   geom = ogr.CreateGeometryFromWkt(chWkt)
   feat.SetGeometry(geom)
   lyr.CreateFeature(feat)

   feat = None
   geom = None
   lyr = None
   ds = None
