"""
@summary: This module contains constants used by the Lifemapper web services
@author: CJ Grady
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
import os
from LmServer.base.utilities import getColor, getMjdTimeFromISO8601
from LmServer.common.lmconstants import SESSION_DIR
from LmServer.common.localconstants import SCRATCH_PATH

# CherryPy constants
SESSION_PATH = os.path.join(SCRATCH_PATH, SESSION_DIR)
SESSION_KEY = '_cp_username'
REFERER_KEY = 'lm_referer'

# HTTP Methods
class HTTPMethod(object):
   DELETE = 'DELETE'
   GET = 'GET'
   POST = 'POST'
   PUT = 'PUT'


# This constant is used for processing query parameters.  If no 'processIn' 
#    key, just take the parameter as it comes in
# Note: The dictionary keys are the .lower() version of the parameter names.
#          The 'name' value of each key is what it gets translated to
# The point of this structure is to allow query parameters to be case-insensitive
QUERY_PARAMETERS = {
   'aftertime' : {
      'name' : 'afterTime',
      'processIn' : getMjdTimeFromISO8601
   },
   'algorithmcode' : {
      'name' : 'algorithmCode',
   },
   'altpredcode' : {
      'name' : 'altPredCode'
   },
   'beforetime' : {
      'name' : 'beforeTime',
      'processIn' : getMjdTimeFromISO8601
   },
   'bbox' : {
      # Comes in as a comma separated list, turn it into a tuple of floats
      'name' : 'bbox',
      'processIn' : lambda x: [float(i) for i in x.split(',')]
   },
   'bgcolor' : {
      'name' : 'bgcolor',
      'processIn' : lambda x: getColor(x, allowRamp=False)
   },
   'cellsides' : {
      'name' : 'cellSides',
      'processIn' : int
   },
   'color' : {
      'name' : 'color',
      'processIn' : lambda x: getColor(x, allowRamp=True)
   },
   'crs' : {
      # TODO: Consider processing the EPSG here
      'name' : 'crs'
   },
   'datecode' : {
      'name' : 'dateCode'
   },
   'displayname' : {
      'name' : 'displayName'
   },
   'envcode' : {
      'name' : 'envCode'
   },
   'envtypeid' : {
      'name' : 'envTypeId',
      'processIn' : int
   },
   'exceptions' : {
      'name' : 'exceptions'
   },
   'epsgcode' : {
      'name' : 'epsgCode',
      'processIn' : int
   },
   'format' : {
      # TODO: Forward to respFormat since format is reserved
      'name' : 'respFormat',
   },
   'gcmcode' : {
      'name' : 'gcmCode',
   },
   'height' : {
      'name' : 'height',
      'processIn' : int
   },
   'layers' : {
      'name' : 'layers',
      'processIn' : lambda x: [i for i in x.split(',')]
   },
   'layertype' : {
      'name' : 'layerType',
      'processIn' : int
   },
   'limit' : {
      'name' : 'limit',
      'processIn' : lambda x: max(1, int(x)) # Integer, minimum is one
   },
   'mapname' : {
      'name' : 'mapName'
   },
   'modelscenariocode' : {
      'name' : 'modelScenarioCode'
   },
   'minimumnumberofpoints' : {
      'name' : 'minimumNumberOfPoints',
      'processIn' : lambda x: max(1, int(x)) # Integer, minimum is one
   },
   'occurrencesetid' : {
      'name' : 'occurrenceSetId',
      'processIn' : int
   },
   'offset' : {
      'name' : 'offset',
      'processIn' : lambda x: max(0, int(x)) # Integer, minimum is zero
   },
   'pathlayerid' : {
      'name' : 'pathLayerId'
   },
   'pathoccsetid' : {
      'name' : 'pathOccSetId'
   },
   'pathprojectionid' : {
      'name' : 'pathProjectionId'
   },
   'pathscenarioid' : {
      'name' : 'pathScenarioId'
   },
   'projectionscenariocode' : {
      'name' : 'projectionScenarioCode'
   },
   'public' : {
      'name' : 'public',
      'processIn' : lambda x: bool(int(x)) # Zero is false, one is true
   },
   'request' : {
      'name' : 'request'
   },
   'scenarioid' : {
      'name' : 'scenarioId',
      'processIn' : int
   },
   'service' : {
      'name' : 'service'
   },
   'sld' : {
      'name' : 'sld'
   },
   'sldbody' : {
      'name' : 'sld_body'
   },
   'srs' : {
      # TODO: Forward to crs for WMS 1.3.0?
      'name' : 'srs'
   },
   'status' : {
      'name' : 'status',
      'processIn' : int
   },
   'styles' : {
      'name' : 'styles',
      'processIn' : lambda x: [i for i in x.split(',')]
   },
   'time' : {
      'name' : 'time'
   },
   'transparent' : {
      'name' : 'transparent',
      'processIn' : lambda x: bool(x.lower() == 'true')
   },
   'version' : {
      'name' : 'version'
   },
   'width' : {
      'name' : 'width',
      'processIn' : int
   },
   # Authentication parameters
   'address1' : {
      'name' : 'address1'
   },
   'address2' : {
      'name' : 'address2'
   },
   'address3' : {
      'name' : 'address3'
   },
   'phone' : {
      'name' : 'phone'
   },
   'email' : {
      'name' : 'email'
   },
   'firstname' : {
      'name' : 'firstName'
   },
   'institution' : {
      'name' : 'institution'
   },
   'lastname' : {
      'name' : 'lastName'
   },
   'pword' : {
      'name' : 'pword'
   },
   'pword1' : {
      'name' : 'pword1'
   },
   'userid' : {
      'name' : 'userId'
   },
}


# Web object interfaces
CSV_INTERFACE = "csv"
GEOTIFF_INTERFACE = "gtiff"
JSON_INTERFACE = "json"
KML_INTERFACE = "kml"
OGC_INTERFACE = "ogc"
SHAPEFILE_INTERFACE = "shapefile"

# Kml
KML_NAMESPACE = "http://earth.google.com/kml/2.2"
KML_NS_PREFIX = None

# TODO: These probably shouldn't be here
# Projections scaling constants.  Used for mapping of archive projections
#   These are sent to Maxent job runners in a post processing step
SCALE_PROJECTION_MINIMUM = 0
SCALE_PROJECTION_MAXIMUM = 100
SCALE_DATA_TYPE = "int"

