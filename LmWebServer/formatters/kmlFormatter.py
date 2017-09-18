"""
@summary: Module containing KML Formatter class and helping functions
@note: This needs to be cleaned up a lot.  This is a patch to get us through
          until we can spend some time on this
@author: CJ Grady
@version: 1.0
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
import cherrypy
from osgeo import ogr
from LmCommon.common.lmconstants import LMFormat
from LmCommon.common.lmXml import (CDATA, Element, register_namespace, 
                                  setDefaultNamespace, SubElement, tostring)
#from LmCommon.common.lmconstants import ENCODING, HTTPStatus

from LmServer.legion.occlayer import OccurrenceLayer
from LmServer.legion.sdmproj import SDMProjection
from LmServer.common.localconstants import WEBSERVICES_ROOT
from LmServer.base.utilities import formatTimeHuman
#from LmServer.base.layer2 import Raster
#from LmServer.base.lmobj import LmHTTPError
#from LmServer.base.serviceobject import ServiceObject
#from LmServer.base.utilities import escapeString, formatTimeHuman
#from LmServer.common.datalocator import EarlJr
from LmServer.common.lmconstants import OccurrenceFieldNames
#from LmServer.common.localconstants import WEBSERVICES_ROOT
#from LmServer.sdm.occlayer import OccurrenceLayer
#from LmServer.sdm.sdmexperiment import SDMExperiment
#from LmServer.sdm.sdmprojection import SDMProjection

#from LmWebServer.formatters.formatter import Formatter, FormatterResponse

KML_NS = "http://www.opengis.net/kml/2.2"

# .............................................................................
def addOccurrenceSet(parent, occ):
   """
   @summary: Adds an SDM Occurrence Set to the KML output
   @param parent: The parent element to add it to
   @param occ: The occurrence set object to add
   """
   SubElement(parent, "name", 
           value='{} points (Occ Id: {})'.format(occ.displayName, occ.getId()))
   SubElement(parent, "open", value="1")
   SubElement(parent, "description", 
           value='{} points (Occ Id: {})'.format(occ.displayName, occ.getId()))
   
   
   # TODO: Look at feature attributes and decide what to read
   for pt in occ.features:
      addPoint(parent, pt)
   
# .............................................................................
def addPoint(parent, point):
   """
   """
   name = getNameForPoint(point)
   lat, lon = getLatLonForPoint(point)

   pmEl = SubElement(parent, "Placemark")
   SubElement(pmEl, "name", value=name)
   SubElement(pmEl, "styleUrl", value="#lmUserOccurrenceBalloon")
   
   ptEl = SubElement(pmEl, "Point")
   SubElement(ptEl, "coordinates", value="%s,%s,0" % (lon, lat))

   ext = SubElement(pmEl, "ExtendedData")
   latEl = SubElement(ext, "Data", attrib={"name": "latitude"})
   SubElement(latEl, "value", value=lat)
   
   lonEl = SubElement(ext, "Data", attrib={"name": "longitude"})
   SubElement(lonEl, "value", value=lon)
      
# .............................................................................
def addProjection(parent, prj, visibility, indent=0):
   """
   @summary: Adds a projection to the KML output
   @param parent: The parent element to add it to
   @param point: The projection to add
   """
   prjName = "Lifemapper projection %s - %s" % (prj.getId(), prj.speciesName)
   if indent == 0:
      SubElement(parent, "name", value=prjName)
      SubElement(parent, "description", value=prjName)
   
   # Ground Overlay
   goEl = SubElement(parent, "GroundOverlay")
   SubElement(goEl, "styleUrl", value="#lmProjectionBalloon")
   SubElement(goEl, "name", value=prjName)
   SubElement(goEl, "visibility", value=visibility)
   
   # Look at
   lookAt = SubElement(goEl, "LookAt")
   SubElement(lookAt, "latitude", value="0.0")
   SubElement(lookAt, "longitude", value="0.0")
   SubElement(lookAt, "altitude", value="0.0")
   SubElement(lookAt, "range", value="500000")
   SubElement(lookAt, "tilt", value="0.0")
   SubElement(lookAt, "heading", value="0.0")
   
   # Icon
   iconEl = SubElement(goEl, "Icon")
   
   mapUrl = prj._earlJr.constructLMMapRequest('{}/{}{}'.format(
                               WEBSERVICES_ROOT, 'api/v2/ogc', prj._mapPrefix), 
                                            400, 200, prj.bbox, color='ff0000')
   SubElement(iconEl, "href", value=mapUrl)
   
   # Latitude Longitude Box
   latLonBoxEl = SubElement(goEl, "LatLonBox")
   SubElement(latLonBoxEl, "north", value=prj.bbox[3])
   SubElement(latLonBoxEl, "south", value=prj.bbox[1])
   SubElement(latLonBoxEl, "west", value=prj.bbox[0])
   SubElement(latLonBoxEl, "east", value=prj.bbox[2])
   SubElement(latLonBoxEl, "rotation", value="0.0")
   
   # Extended Data
   extData = SubElement(goEl, "ExtendedData")
   
   lastModEl = SubElement(extData, "Data", attrib={"name": "lastModified"})
   SubElement(lastModEl, "value", value=formatTimeHuman(prj.modTime))

   scnTitleEl = SubElement(extData, "Data", attrib={"name": "scenarioTitle"})
   # TODO: Get the title for this scenario
   SubElement(scnTitleEl, "value", value=prj._projScenario.code)


# .............................................................................
def getKML(myObj):
   """
   @summary: Gets a KML document for the object
   @param obj: The object to return in KML
   """
   register_namespace('', KML_NS)
   setDefaultNamespace(KML_NS)
   root = Element("kml")
   doc = SubElement(root, "Document")
   SubElement(doc, "styleUrl", value="#lmBalloon")
   
   # lmBalloon style
   lmBalloon = SubElement(doc, "Style", attrib={"id": "lmBalloon"})
   
   # Nested parent elements that don't add extra attributes
   SubElement(SubElement(lmBalloon, "BalloonStyle"), "text").append(
      CDATA("""\
               <table>
                  <tr>
                     <td>
                        <img src="{WEBSITE}/images/lmlogosmall.jpg" />
                     </td>
                     <td>
                        <h3>$[name]</h3>
                     </td>
                  </tr>
                  <tr>
                     <td colspan="2">
                        $[description]
                     </td>
                  </tr>
               </table>""".format(WEBSITE=WEBSERVICES_ROOT)))

   # lmLayerBalloon style
   lmLayerBalloon = SubElement(doc, "Style", attrib={"id": "lmLayerBalloon"})

   # Nested parent elements that don't add extra attributes
   SubElement(SubElement(lmLayerBalloon, "BalloonStyle"), "text").append(
      CDATA("""\
               <table>
                  <tr>
                     <td>
                        <img src="{WEBSITE}/images/lmlogosmall.jpg" />
                     </td>
                     <td>
                        <h3>$[name]</h3>
                     </td>
                  </tr>
                  <tr>
                     <td colspan="2">
                        <table width="300">
                           <tr>
                              <th align="right">
                                 Title:
                              </th>
                              <td>
                                 $[layerTitle]
                              </td>
                           </tr>
                           <tr>
                              <th align="right">
                                 Last Modified:
                              </th>
                              <td>
                                 $[lastModified]
                              </td>
                           </tr>
                        </table>
                     </td>
                  </tr>
               </table>""".format(WEBSITE=WEBSERVICES_ROOT)))

   # lmGbifOccurrenceBalloon
   lmGbif = SubElement(doc, "Style", attrib={"id": "lmGbifOccurrenceBalloon"})
   #SubElement(
   #     # Nested parent elements that don't add extra attributes
   #     SubElement(SubElement(lmGbif, "IconStyle"), "Icon"), 
   #     "href", value="%s/images/pushpin.png" % WEBSERVICES_ROOT)

   # Nested parent elements that don't add extra attributes
   SubElement(SubElement(lmGbif, "BalloonStyle"), "text").append(CDATA("""\
               <table>
                  <tr>
                     <td>
                        <img src="{WEBSITE}/images/lmlogosmall.jpg" />
                     </td>
                     <td>
                        <h3>$[name]</h3>
                     </td>
                  </tr>
                  <tr>
                     <td colspan="2">
                        <table width="300">
                           <tr>
                              <th align="right">
                                 Provider:
                              </th>
                              <td>
                                 $[providerName]
                              </td>
                           </tr>
                           <tr>
                              <th align="right">
                                 Resource:
                              </th>
                              <td>
                                 $[resourceName]
                              </td>
                           </tr>
                           <tr>
                              <th align="right">
                                 Latitude:
                              </th>
                              <td>
                                 $[latitude]
                              </td>
                           </tr>
                           <tr>
                              <th align="right">
                                 Longitude:
                              </th>
                              <td>
                                 $[longitude]
                              </td>
                           </tr>
                           <tr>
                              <th align="right">
                                 Collector:
                              </th>
                              <td>
                                 $[collector]
                              </td>
                           </tr>
                           <tr>
                              <th align="right">
                                 Collection Date:
                              </th>
                              <td>
                                 $[colDate]
                              </td>
                           </tr>
                        </table>
                     </td>
                  </tr>
               </table>""".format(WEBSITE=WEBSERVICES_ROOT)))

   # lmUserOccurrenceBalloon
   lmUser = SubElement(doc, "Style", attrib={"id": "lmUserOccurrenceBalloon"})
   #SubElement(
   #     # Nested parent elements that don't add extra attributes
   #     SubElement(SubElement(lmUser, "IconStyle"), "Icon"), 
   #     "href", value="%s/images/pushpin.png" % WEBSERVICES_ROOT)

   # Nested parent elements that don't add extra attributes
   SubElement(SubElement(lmUser, "BalloonStyle"), "text").append(CDATA("""\
               <table>
                  <tr>
                     <td>
                        <img src="{WEBSITE}/images/lmlogosmall.jpg" />
                     </td>
                     <td>
                        <h3>$[name]</h3>
                     </td>
                  </tr>
                  <tr>
                     <td colspan="2">
                        <table width="300">
                           <tr>
                              <th align="right">
                                 Latitude:
                              </th>
                              <td>
                                 $[latitude]
                              </td>
                           </tr>
                           <tr>
                              <th align="right">
                                 Longitude:
                              </th>
                              <td>
                                 $[longitude]
                              </td>
                           </tr>
                        </table>
                     </td>
                  </tr>
               </table>""".format(WEBSITE=WEBSERVICES_ROOT)))

   # lmProjectionBalloon
   lmPrj = SubElement(doc, "Style", attrib={"id": "lmProjectionBalloon"})

   # Nested parent elements that don't add extra attributes
   SubElement(SubElement(lmPrj, "BalloonStyle"), "text").append(CDATA("""\
               <table>
                  <tr>
                     <td>
                        <img src="{WEBSITE}/images/lmlogosmall.jpg" />
                     </td>
                     <td>
                        <h3>$[name]</h3>
                     </td>
                  </tr>
                  <tr>
                     <td colspan="2">
                        <table width="300">
                           <tr>
                              <th align="right">
                                 Scenario Title:
                              </th>
                              <td>
                                 $[scenarioTitle]
                              </td>
                           </tr>
                           <tr>
                              <th align="right">
                                 Last Modified:
                              </th>
                              <td>
                                 $[lastModified]
                              </td>
                           </tr>
                        </table>
                     </td>
                  </tr>
               </table>""".format(WEBSITE=WEBSERVICES_ROOT)))

   # Add object
   if isinstance(myObj, SDMProjection):
      addProjection(doc, myObj, 1)
   elif isinstance(myObj, OccurrenceLayer):
      #myObj.readData(doReadData=True)
      myObj.readShapefile()
      addOccurrenceSet(doc, myObj)
      
   temp = tostring(root)
   temp = temp.replace('&lt;', '<')
   temp = temp.replace('&gt;', '>')
   return temp

# .............................................................................
def kmlObjectFormatter(obj):
   """
   @summary: Looks at object and converts to KML based on its type
   """
   #cherrypy.response.headers['Content-Type'] = LMFormat.JSON.getMimeType()
   cherrypy.response.headers['Content-Type'] = LMFormat.KML.getMimeType()
   cherrypy.response.headers['Content-Disposition'] = 'attachment; filename="{}.kml"'.format(obj.name)
   kmlStr = getKML(obj)
   return kmlStr





# .............................................................................
def getNameForPoint(pt):
   """
   @summary: Get a name for a point.  Tries to get the taxon name, falls back 
                to local id
   @param pt: A point object
   """
   name = None
   
   try:
      return pt.sciname
   except:
      try:
         return pt.occurid
      except:
         pass
   
   for att in OccurrenceFieldNames.DATANAME:
      try:
         name = pt.__getattribute__(att)
         return name
      except:
         pass
   
   # If no data name fields were available
   for att in OccurrenceFieldNames.LOCAL_ID:
      try:
         name = pt.__getattribute__(att)
         return name
      except:
         pass
   
   # Return unknown if we can't find a name
   return "Unknown"

# .............................................................................
def getLatLonForPoint(pt):
   """
   @summary: Get's the x and y for a point
   @param pt: A point object
   @note: Tries to get this from the geometry first, falls back to attributes
   """
   # Try wkt first
   wkt = None
   for att in OccurrenceFieldNames.GEOMETRY_WKT:
      try:
         wkt = pt._attrib[att]
         break
      except:
         pass
      
   if wkt is not None:
      lon, lat, _ = ogr.CreateGeometryFromWkt(wkt).GetPoint()
      return lat, lon
   else:
      # Find lat and lon
      lat = None
      for att in OccurrenceFieldNames.LATITUDE:
         try:
            lat = pt._attrib[att]
            break
         except:
            pass
      
      lon = None
      for att in OccurrenceFieldNames.LONGITUDE:
         try:
            lon = pt._attrib[att]
            break
         except:
            pass
      
      if lat is not None and lon is not None:
         return lat, lon
   
   # Raise exception if we get to here without determining lat and lon
   raise Exception, "Could not retrieve latitude and / or longitude for point"

# .............................................................................
def getLocalIdForPoint(pt):
   """
   @summary: Get a local id for a point.
   @param pt: A point object
   """
   localId = None
   
   # If no data name fields were available
   for att in OccurrenceFieldNames.LOCAL_ID:
      try:
         localId = pt.__getattribute__(att)
         return localId
      except:
         pass
   
   # Return unknown if we can't find a name
   return "Unknown"
