"""
@summary: Module functions for converting object to GeoJSON
@author: CJ Grady
@version: 2.0
@status: alpha
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
import cherrypy
import json
import ogr

from LmCommon.common.lmconstants import LMFormat, MatrixType
from LmCommon.common.matrix import Matrix

from LmServer.base.layer2 import Vector
from LmServer.legion.lmmatrix import LMMatrix
from LmServer.legion.occlayer import OccurrenceLayer
from LmServer.legion.shapegrid import ShapeGrid

# .............................................................................
def geoJsonify(shpFilename, matrix=None, mtxJoinAttrib=None):
   """
   @summary: Creates GeoJSON for the features in a shapefile.  If a matrix is 
                provided, attempt to join the features contained.
   """
   attLookup = {}
   
   # Build matrix lookup
   if matrix is not None:
      colHeaders = matrix.getColumnHeaders()
      rowHeaders = matrix.getRowHeaders()
      
      # Define a cast function, necessary if the matrix is full of booleans 
      #    because they cannot be encoded correctly for JSON
      if matrix.data.dtype == bool:
         castFunc = lambda x: int(x)
      else:
         castFunc = lambda x: x
      
      for i in range(len(rowHeaders)):
         
         joinAtt = rowHeaders[i][mtxJoinAttrib]
         
         attLookup[joinAtt] = {}
         
         for j in range(len(colHeaders)):
            try:
               attLookup[joinAtt][colHeaders[j]] = castFunc(matrix.data[i,j])
            except:
               pass
   
   # Build features list
   features = []
   drv = ogr.GetDriverByName(LMFormat.getDefaultOGR().driver)
   ds = drv.Open(shpFilename, 0)
   lyr = ds.GetLayer()
   for feat in lyr:
      # Get the GeoJSON for the feature
      ft = json.loads(feat.ExportToJson())
      joinAttrib = feat.GetFID()
      
      # Join matrix attributes
      if attLookup.has_key(joinAttrib):
         print ft
         if not ft.has_key('properties'):
            ft['properties'] = {}
         ft['properties'].update(attLookup[joinAttrib])
      
      # Add feature to features list
      features.append(ft)
   ds = None

   doc = {
      'type' : 'FeatureCollection',
      'features' : features
   }

   return doc

# .............................................................................
def geoJsonObjectFormatter(obj):
   """
   @summary: Looks at object and converts to JSON based on its type
   """
   response = _formatObject(obj)
   
   return json.dumps(response, indent=3)

# .............................................................................
def _formatObject(obj):
   """
   @summary: Helper method to format an individual object based on its type
   """
   cherrypy.response.headers['Content-Type'] = LMFormat.GEO_JSON.getMimeType()
   if isinstance(obj, (OccurrenceLayer, ShapeGrid, Vector)):
      cherrypy.response.headers['Content-Disposition'] = 'attachment; filename="{}.geojson"'.format(obj.name)
      return geoJsonify(obj.getDLocation())
   elif isinstance(obj, LMMatrix):
      if obj.matrixType in (MatrixType.PAM, MatrixType.ROLLING_PAM, 
                            MatrixType.ANC_PAM, MatrixType.SITES_COV_OBSERVED, 
                            MatrixType.SITES_COV_RANDOM, 
                            MatrixType.SITES_OBSERVED, MatrixType.SITES_RANDOM):
         sg = obj.getGridset().getShapegrid()
         mtx = Matrix.load(obj.getDLocation())
         cherrypy.response.headers['Content-Disposition'] = 'attachment; filename="mtx_{}.geojson"'.format(obj.getId())
         return geoJsonify(sg.getDLocation(), matrix=mtx, mtxJoinAttrib=0)
      else:
         raise TypeError, 'Cannot format matrix type: {}'.format(obj.matrixType)
   else:
      raise TypeError, "Cannot format object of type: {}".format(type(obj))
