"""
    Module containing vendor-specific-parameters (VSPs) for Lifemapper W*S services
    and the filter parameters that will be applied when they are invoked. 
    @todo: this is not currently used, but will need to be updated for querying
           occurrence and projection data.
@license: gpl2
@copyright: Copyright (C) 2014, University of Kansas Center for Research

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
from LmServer.base.utilities import getMjdTimeFromISO8601
from LmServer.common.lmconstants import SESSION_DIR, WEB_DIR
from LmServer.common.localconstants import APP_PATH, SCRATCH_PATH

# Service path constants (some of these are still in LmServer.common.lmconstants
STATIC_DIR = 'static'

# CherryPy constants
SESSION_PATH = os.path.join(SCRATCH_PATH, SESSION_DIR)
SESSION_KEY = '_cp_username'
REFERER_KEY = 'lm_referer'


STATIC_PATH = os.path.join(APP_PATH, WEB_DIR, STATIC_DIR)

# External namespaces
STMML_NAMESPACE = "http://www.xml-cml.org/schema/stmml"
STMML_NS_PREFIX = "stmml"

XSI_NAMESPACE = "http://www.w3.org/2001/XMLSchema-instance"
XSI_NS_PREFIX = "xsi" # This is a well-known namespace in ElementTree and will automatically be set to xsi

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
   'cellsides' : {
      'name' : 'cellSides',
      'processIn' : int
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
   'epsgcode' : {
      'name' : 'epsgCode',
      'processIn' : int
   },
   'gcmcode' : {
      'name' : 'gcmCode',
   },
   'layertype' : {
      'name' : 'layerType',
      'processIn' : int
   },
   'limit' : {
      'name' : 'limit',
      'processIn' : lambda x: max(1, int(x)) # Integer, minimum is one
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
   'scenarioid' : {
      'name' : 'scenarioId',
      'processIn' : int
   },
   'status' : {
      'name' : 'status',
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


class QueryParamNames:
   AFTER_TIME = {
      "displayName"   : "After Time",
      "documentation" : "Format: YYYY-MM-DD",
      "multiplicity"  : "1",
      "name"          : "afterTime",
      "type"          : "date",
   }
   ALGO_CODE = {
      "displayName"   : "Algorithm Code",
      "documentation" : "",
      "multiplicity"  : "1",
      "name"          : "algorithmCode",
      "options"       : [
                           {
                              "name"  : "Artificial Neural Networks",
                              "value" : "ANN",
                           },
                           {
                              "name"  : "Aquamaps",
                              "value" : "AQUAMAPS",
                           },
                           {
                              "name"  : "Bioclim",
                              "value" : "BIOCLIM",
                           },
                           {
                              "name"  : "Climate Space Model - Broken Stick Implementation",
                              "value" : "CSMBS",
                           },
                           {
                              "name"  : "GARP - DesktopGARP Implementation",
                              "value" : "DG_GARP",
                           },
                           {
                              "name"  : "GARP Best Subsets - DesktopGARP Implementation",
                              "value" : "DG_GARP_BS",
                           },
                           {
                              "name"  : "Environmental Distance",
                              "value" : "ENVDIST",
                           },
                           {
                              "name"  : "GARP - openModeller Implementation",
                              "value" : "GARP",
                           },
                           {
                              "name"  : "GARP Best Subsets - openModeller Implementation",
                              "value" : "GARP_BS",
                           },
                           {
                              "name"  : "Maximum Entropy - ATT Implementation",
                              "value" : "ATT_MAXENT"
                           },
                           {
                              "name"  : "Maximum Entropy - openModeller Implementation",
                              "value" : "MAXENT"
                           },
                           {
                              "name"  : "Support Vector Machines",
                              "value" : "SVM",
                           },
                        ],
      "type"          : "string",
   }
   ALGORITHM_PARAMETERS_ID = {
      "displayName"   : "Algorithm Parameters Set Id",
      "documentation" : "",
      "multiplicity"  : "1",
      "name"          : "algorithmParametersId",
      "type"          : "integer",
   }
   ANCILLARY_VALUE_ID = {
      "displayName"   : "Ancillary Value Id",
      "documentation" : "",
      "multiplicity"  : "1",
      "name"          : "ancillaryValueId",
      "type"          : "integer",
   }
   BEFORE_TIME = {
      "displayName"   : "Before Time",
      "documentation" : "Format: YYYY-MM-DD",
      "multiplicity"  : "1",
      "name"          : "beforeTime",
      "type"          : "date",
   }
   BUCKET_ID = {
      "displayName"   : "Bucket Id",
      "documentation" : "",
      "multiplicity"  : "1",
      "name"          : "bucketId",
      "type"          : "integer",
   }
   CELL_SIDES = {
      "displayName"   : "Cell Shape",
      "documentation" : "",
      "multiplicity"  : "1",
      "name"          : "cellSides",
      "options"       : [
                           {
                              "name"  : "Square",
                              "value" : "4",
                           },
                           {
                              "name"  : "Hexagon",
                              "value" : "6",
                           },
                        ],
      "type"          : "integer",
   }
   DISPLAY_NAME = {
      "displayName"   : "Display Name",
      "documentation" : "",
      "multiplicity"  : "1",
      "name"          : "displayName",
      "type"          : "string",
   }
   EPSG_CODE = {
      "displayName"   : "EPSG Code",
      "documentation" : "",
      "multiplicity"  : "1",
      "name"          : "epsgCode",
      "type"          : "string",
   }
   EXPERIMENT_ID = {
      "displayName"   : "Experiment Id",
      "documentation" : "",
      "multiplicity"  : "1",
      "name"          : "experimentId",
      "type"          : "integer",
   }
   EXPERIMENT_NAME = {
      "displayName"   : "Experiment Name",
      "documentation" : "",
      "multiplicity"  : "1",
      "name"          : "experimentName",
      "type"          : "string",
   }
   FILL_POINTS = {
      "displayName"   : "Fill Points",
      "documentation" : "",
      "multiplicity"  : "1",
      "name"          : "fillPoints",
      "type"          : "integer",
      "options"       : [
                           {
                              "name" : "True",
                              "value" : "1"
                           },
                           {
                              "name" : "False",
                              "value" : "0"
                           }
                        ]
   }
   FULL_OBJECTS = {
      "displayName"   : "Full Objects",
      "documentation" : "",
      "multiplicity"  : "1",
      "name"          : "fullObjects",
      "type"          : "integer",
      "options"       : [
                           {
                              "name" : "True",
                              "value" : "1"
                           },
                           {
                              "name" : "False",
                              "value" : "0"
                           }
                        ]
   }
   HAS_PROJECTIONS = {
      "displayName"   : "Has Projections",
      "documentation" : "",
      "multiplicity"  : "1",
      "name"          : "hasProjections",
      "type"          : "integer",
      "options"       : [
                           {
                              "name" : "True",
                              "value" : "1"
                           },
                           {
                              "name" : "False",
                              "value" : "0"
                           }
                        ],
   }
   IS_CATEGORICAL = {
      "displayName"   : "Is Categorical",
      "documentation" : "",
      "multiplicity"  : "1",
      "name"          : "isCategorical",
      "type"          : "integer",
      "options"       : [
                           {
                              "name" : "True",
                              "value" : "1"
                           },
                           {
                              "name" : "False",
                              "value" : "0"
                           }
                        ]
   }
   IS_RANDOMIZED = {
      "displayName"   : "Is Randomized",
      "documentation" : "",
      "multiplicity"  : "1",
      "name"          : "randomized",
      "type"          : "integer",
      "options"       : [
                           {
                              "name" : "True",
                              "value" : "1"
                           },
                           {
                              "name" : "False",
                              "value" : "0"
                           }
                        ],
   }
   KEYWORD = {
      "displayName"   : "Keyword",
      "documentation" : "",
      "multiplicity"  : "4",
      "name"          : "keyword",
      "type"          : "string",
   }
   LAYER_ID = {
      "displayName"   : "Layer Id",
      "documentation" : "",
      "multiplicity"  : "1",
      "name"          : "layerId",
      "type"          : "integer",
   }
   LAYER_NAME = {
      "displayName"   : "Layer Name",
      "documentation" : "",
      "multiplicity"  : "1",
      "name"          : "layerName",
      "type"          : "string",
   }
   MATCHING_SCENARIO = {
      "displayName"   : "Matching Scenario Id",
      "documentation" : "",
      "multiplicity"  : "1",
      "name"          : "matchingScenario",
      "type"          : "integer",
   }
   MAX_RETURNED = {
      "displayName"   : "Maximum Features Returned",
      "documentation" : "",
      "multiplicity"  : "1",
      "name"          : "maxReturned",
      "type"          : "integer",
   }
   MIN_POINTS = {
      "displayName"   : "Minimum Number Of Points",
      "documentation" : "Value should be greater than zero",
      "multiplicity"  : "1",
      "name"          : "minimumNumberOfPoints",
      "type"          : "integer",
   }
   MODEL_ID = {
      "displayName"   : "Model Id",
      "documentation" : "",
      "multiplicity"  : "1",
      "name"          : "modelId",
      "type"          : "integer",
   }
   MODEL_STATUS = {
      "displayName"   : "Model Status",
      "documentation" : "",
      "multiplicity"  : "1",
      "name"          : "status",
      "options"       : [
                           {
                              "name"  : "Initialized",
                              "value" : "1",
                           },
                           {
                              "name"  : "Completed",
                              "value" : "300",
                           },
                           {
                              "name"  : "Obsolete",
                              "value" : "60",
                           },
                        ],
      "type"          : "integer",
   }
   OCCURRENCE_SET_ID = {
      "displayName"   : "Occurrence Set Id",
      "documentation" : "",
      "multiplicity"  : "1",
      "name"          : "occurrenceSetId",
      "type"          : "integer",
   }
   PAGE = {
      "displayName"   : "Page of Results",
      "documentation" : "Should be greater than or equal to zero",
      "multiplicity"  : "1",
      "name"          : "page",
      "type"          : "integer",
   }
   PARAMS_TYPE = {
      "displayName"   : "Algorithm Parameter Set Type",
      "documentation" : "",
      "multiplicity"  : "1",
      "name"          : "paramsType",
      "options"       : [
                           {
                              "name"  : "Default Algorithm Parameters",
                              "value" : "default",
                           },
                           {
                              "name"  : "Unique Algorithm Parameters",
                              "value" : "unique",
                           },
                        ],
      "type"          : "string",
   }
   PER_PAGE = {
      "displayName"   : "Results Per Page",
      "documentation" : "Value should be between 1 and 100",
      "multiplicity"  : "1",
      "name"          : "perPage",
      "type"          : "integer",
   }
   PRESENCE_ABSENCE_ID = {
      "displayName"   : "Presence Absence Id",
      "documentation" : "",
      "multiplicity"  : "1",
      "name"          : "presenceAbsenceId",
      "type"          : "integer",
   }
   PROJECTION_STATUS = {
      "displayName"   : "Projection Status",
      "documentation" : "",
      "multiplicity"  : "1",
      "name"          : "status",
      "options"       : [
                           {
                              "name"  : "Initialized",
                              "value" : "1",
                           },
                           {
                              "name"  : "Completed",
                              "value" : "300",
                           },
                           {
                              "name"  : "Obsolete",
                              "value" : "60",
                           },
                        ],
      "type"          : "integer",
   }
   PUBLIC = {
      "displayName" : "Use public objects",
      "documentation" : "",
      "multiplicity" : "1",
      "name"          : "public",
      "options"       : [
                           {
                              "name" : "True",
                              "value" : "1"
                           },
                           {
                              "name" : "False",
                              "value" : "0"
                           }
                        ],
      "type"          : "integer"
   }
   RANDOM_METHOD = {
      "displayName"   : "Randomization Method",
      "documentation" : "",
      "multiplicity"  : "1",
      "name"          : "randomMethod",
      "options"       : [
                           {
                              "name"  : "Not Random",
                              "value" : "0",
                           },
                           {
                              "name"  : "Swap",
                              "value" : "1",
                           },
                           {
                              "name"  : "Splotch",
                              "value" : "2",
                           },
                        ],
      "type"          : "integer",
   }
   SCENARIO_ID = {
      "displayName"   : "Scenario Id",
      "documentation" : "",
      "multiplicity"  : "1",
      "name"          : "scenarioId",
      "type"          : "integer", 
   }
   SHAPEGRID_ID = {
      "displayName"   : "Shapegrid Id",
      "documentation" : "",
      "multiplicity"  : "1",
      "name"          : "shapegridId",
      "type"          : "integer",
   }
   SHAPEGRID_NAME = {
      "displayName"   : "Shapegrid Name",
      "documentation" : "",
      "multiplicity"  : "1",
      "name"          : "shapegridName",
      "type"          : "string",
   }
   TYPE_CODE = {
      "displayName"   : "Type Code",
      "documentation" : "Code categorizing raster data to allow layer matching",
      "multiplicity"  : "1",
      "name"          : "typeCode",
      "type"          : "string",
   }
   

# Address this.  At least use constants.     
HTTP_ERRORS = {
               400 : {
                  "msg" : "Bad request",
                  "title" : "400 - Bad Request"
               },
               401: {
                  "msg" : """\
<div class="loginDiv" align="center">
   <form action="/login" method="post">
      <div align="center">
         <table>
            <tr>
               <td style="text-align: right;">
                  User Name: 
               </td>
               <td style="text-align: left;">
                  <input type="text" name="username" />
               </td>
            </tr>
            <tr>
               <td style="text-align: right;">
                  Password:
               </td>
               <td style="text-align: left;">
                  <input type="password" name="pword" />
               </td>
            </tr>
         </table>
         <span class="notification">Invalid user name / password combination.</span>
      </div>
      <input type="submit" value="Log In" /><br /><br />
      New user? <a href="/signup">Sign up</a> here!<br /><br />
      Forgot your password? Contact us at lifemapper at ku dot edu.<br />
   </form>
</div>""",
                  "title" : "401 - Authentication Failure"
               },
               403 : {
                  "msg" : """\
<div class="loginDiv" align="center">
   <form action="/login" method="post">
      <div align="center">
         <table>
            <tr>
               <td style="text-align: right;">
                  User Name: 
               </td>
               <td style="text-align: left;">
                  <input type="text" name="username" />
               </td>
            </tr>
            <tr>
               <td style="text-align: right;">
                  Password:
               </td>
               <td style="text-align: left;">
                  <input type="password" name="pword" />
               </td>
            </tr>
         </table>
      </div>
      <input type="submit" value="Log In" /><br /><br />
      New user? <a href="/signup">Sign up</a> here!<br /><br />
      Forgot your password? Contact us at lifemapper at ku dot edu.<br />
   </form>
</div>""",
                  "title" : "403 - Not authenticated"
               },
               404 : {
                  "msg" : """\
               <h1>Page Not Found</h1>
               <br /><br />
               <p>
                  Sorry, the page you requested could not be found.  Please
                  check the url.  If you followed a link to this page, let us
                  know by sending an email to: lifemapper at ku dot edu.
               </p>""",
                  "title" : "404 - Page not found" 
               },
               409 : {
                  "msg" : "Conflict",
                  "title" : "409 - Conflict"
               },
               500 : {
                  "msg" : ''.join((
                           "The error you encountered has been logged by ",
                           "Lifemapper.  If you were in the middle of a ",
                           "workflow and wish to seek immediate resolution, "
                           "please send an email to: [lifemapper at ku dot ",
                           "edu] and mention the approximate time the error ",
                           "occurred and what you were working on.  Thank ",
                           "you.")),
                  "title" : "500 - An error occurred"
               },
               503 : {
                  "msg" : "The service is unavailable at this time",
                  "title" : "503 - Service Unavailable"
               }
              }
   
# Lifemapper Lucene Constants
LUCENE_INDEX_DIR = "luceneIndex"
LUCENE_LISTEN_ADDRESS = ("localhost", 12987)
LUCENE_MAX_START_TRIES = 30

# Web services interfaces constants

# These are the service mount points
# TODO: Add to the ancillary and presence absence layers names when we remove 
#         the sub-objects from experiments
SERVICE_MOUNTS = {
   "rad" : {
      "experiments" : ["experiments", "exps"],
      "anclayers" : ["anclayers"],
      "buckets" : ["buckets", "bkts"],
      "layers" : ["layers", "lyrs"],
      "palayers" : ["palayers"],
      "pamsums" : ["pamsums", "pss"],
      "shapegrids" : ["shapegrids"],
   },
   "sdm" : {
      "experiments" : ["experiments", "exps", "models"],
      "layers" : ["layers", "lyrs"],
      "occurrences" : ["occurrences", "occ"],
      "projections" : ["projections", "prjs", "projs"],
      "scenarios" : ["scenarios", "climateScenarios", "scns"],
      "typecodes" : ["typecodes"]
   } 
}

ASCIIGRID_INTERFACE = "aaigrid"
ASCII_OLD_INTERFACE = "ascii"
ATOM_INTERFACE = "atom"
CSV_INTERFACE = "csv"
EML_INTERFACE = "eml"
GEOTIFF_INTERFACE = "gtiff"
HTML_INTERFACE = "html"
INDICES_INTERFACE = "indices"
JSON_INTERFACE = "json"
KML_INTERFACE = "kml"
MODEL_INTERFACE = "model"
OGC_INTERFACE = "ogc"
PACKAGE_INTERFACE = "package"
PRESENCE_INTERFACE = "presence"
PROV_INTERFACE = "prov"
RAW_INTERFACE = "raw"
SHAPEFILE_INTERFACE = "shapefile"
STATISTICS_INTERFACE = "statistics"
STATUS_INTERFACE = "status"
TIFF_OLD_INTERFACE = "tiff"
WCS_INTERFACE = "wcs"
WFS_INTERFACE = "wfs"
WMS_INTERFACE = "wms"
XML_INTERFACE = "xml"

DEFAULT_INTERFACE = HTML_INTERFACE

# Atom
ATOM_NAMESPACE = "http://www.w3.org/2005/Atom"
ATOM_NS_PREFIX = "atom"

# Kml
KML_NAMESPACE = "http://earth.google.com/kml/2.2"
KML_NS_PREFIX = None


# Projections scaling constants.  Used for mapping of archive projections
#   These are sent to Maxent job runners in a post processing step
SCALE_PROJECTION_MINIMUM = 0
SCALE_PROJECTION_MAXIMUM = 100
SCALE_DATA_TYPE = "int"

