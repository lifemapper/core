"""
@summary: Module containing KML Formatter class and helping functions
@author: CJ Grady
@version: 1.0
@status: beta
@note: Part of the Factory pattern
@see: Formatter
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
from osgeo import ogr
from LmCommon.common.lmXml import CDATA, Element, register_namespace, \
                                  setDefaultNamespace, SubElement, tostring
from LmCommon.common.localconstants import ENCODING, WEBSERVICES_ROOT

from LmServer.base.layer import Raster
from LmServer.base.lmobj import LMError
from LmServer.base.serviceobject import ServiceObject
from LmServer.base.utilities import escapeString, formatTimeHuman
from LmServer.common.datalocator import EarlJr
from LmServer.sdm.occlayer import OccurrenceLayer
from LmServer.sdm.sdmexperiment import SDMExperiment
from LmServer.sdm.sdmprojection import SDMProjection

from LmWebServer.formatters.formatter import Formatter, FormatterResponse

KML_NS = "http://www.opengis.net/kml/2.2"

# .............................................................................
class KmlFormatter(Formatter):
   """
   @summary: Formatter class for KML output
   """
   # ..................................
   def format(self):
      """
      @summary: Formats the object
      @return: A response containing the content and metadata of the format 
                  operation
      @rtype: FormatterResponse
      """
      if not isinstance(self.obj, ServiceObject):
         raise LMError("Can't format %s object as KML" % str(self.obj.__class__))
      kml = getKml(self.obj)
      try:
         name = self.obj.serviceType[:-1]
      except:
         name = "items"
      
      ct = "application/vnd.google-earth.kml+xml"
      fn = "%s%s.kml" % (name, self.obj.getId())
      headers = {"Content-Disposition" : 'attachment; filename="%s"' % fn}
      
      return FormatterResponse(kml, contentType=ct, filename=fn, 
                                                         otherHeaders=headers)


# .............................................................................
def addSdmExperiment(parent, exp):
   """
   @summary: Adds an SDM experiment to the KML output
   @param exp: The SDM experiment to add
   @param parent: The parent ElementTree element to add it to
   """
   SubElement(parent, "name", 
                                value="Lifemapper experiment %s" % exp.getId())
   SubElement(parent, "styleUrl", value="#lmBalloon")
   SubElement(parent, "open", value="1")

   # Extended Data
   extData = SubElement(parent, "ExtendedData")
   el1 = SubElement(extData, "Data", attrib={"name": "metadataUrl"})
   SubElement(el1, "value", value=exp.metadataUrl)
   el2 = SubElement(extData, "Data", attrib={"name": "lastModified"})
   SubElement(el2, "displayName", value="Last Modified")
   SubElement(el2, "value", value=formatTimeHuman(exp.statusModTime))
   
   # Description
   SubElement(parent, "description", 
                                value="Lifemapper experiment %s" % exp.getId())

   # Projections Folder
   prjFolderEl = SubElement(parent, "Folder")
   SubElement(prjFolderEl, "name", 
                           value="Projections for experiment %s" % exp.getId())
   SubElement(prjFolderEl, "styleUrl", value="#lmBalloon")
   SubElement(prjFolderEl, "open", value="1")
   SubElement(prjFolderEl, "description", 
                           value="Projections for experiment %s" % exp.getId())
   for i in xrange(len(exp.projections)):
      addProjection(prjFolderEl, exp.projections[i], int(i == 0), 1)

   # Occurrence Set Folder
   occFolderEl = SubElement(parent, "Folder")
   SubElement(occFolderEl, "styleUrl", value="#lmBalloon")
   SubElement(occFolderEl, "description", 
                        value="Occurrence Set for Experiment %s" % exp.getId())
   addOccurrenceSet(occFolderEl, exp.model.occurrenceSet)

# .............................................................................
def addOccurrenceSet(parent, occ):
   """
   @summary: Adds an SDM Occurrence Set to the KML output
   @param parent: The parent element to add it to
   @param occ: The occurrence set object to add
   """
   SubElement(parent, "name", 
                            value="Points for occurrence set %s" % occ.getId())
   SubElement(parent, "open", value="1")
   SubElement(parent, "description", 
                 value="Lifemapper points for occurrence set %s" % occ.getId())
   
   # Add points
   if occ.fromGbif:
      for pt in occ.features:
         addGbifPoint(parent, pt)
   else:
      for pt in occ.features:
         addUserPoint(parent, pt)

# .............................................................................
def addGbifPoint(parent, point):
   """
   @summary: Adds an occurrence point to the KML output
   @param parent: The parent element to add it to
   @param point: The point to add
   """
   # Name
   try:
      name = point.canname
   except:
      name = point.sciname

   # Latitude
   try:
      latitude = point.lat
   except:
      try:
         latitude = point.latitude
      except:
         latitude = point.dec_lat

   # Longitude
   try:
      longitude = point.lon
   except:
      try:
         longitude = point.longitude
      except:
         longitude = point.dec_long
   
   # Provider name
   try:
      providerName = point.provname
   except:
      try:
         providerName = point.instcode
      except:
         providerName = None

   if providerName is not None:
      providerName = unicode(providerName, ENCODING)

   # Resource name
   try:
      resourceName = point.resname
   except:
      try:
         resourceName = point.collcode
      except:
         resourceName = None
   
   if resourceName is not None:
      resourceName = unicode(resourceName, ENCODING)
   
   # Collector
   try:
      collector = point.collectr
   except:
      try:
         collector = point.coll
      except:
         collector = point.rec_by

   if collector is not None:
      collector = unicode(collector, ENCODING)
   
   # Collection Date
   try:
      colDate = formatTimeHuman(point.colldate)
   except:
      # If the year is not provided, set to unknown.  If there is a year, try 
      #    to get month and day but fall back to January if no month and the 
      #    first day of the month if none provided
      try:
         year = point.year
         try:
            month = point.month
         except:
            month = "1"
         try:
            day = point.day
         except:
            day = "1"
         colDate = "%s-%s-%s" % (year, month, day)
      except:
         colDate = "Unknown"

   latitude = float(latitude)
   longitude = float(longitude)
   
   # Throw out bad data
   if latitude >= -90.0 and latitude <= 90.0 and longitude >= -180.0 and longitude <= 180.0:
      # Placemark
      pmEl = SubElement(parent, "Placemark")
      SubElement(pmEl, "name", value=escapeString(name, "xml"))
      SubElement(pmEl, "styleUrl", value="#lmGbifOccurrenceBalloon")
      
      # Point
      ptEl = SubElement(pmEl, "Point")
      SubElement(ptEl, "coordinates", value="%s,%s,0" % (longitude, latitude))
      
      # Extended data
      extDataEl = SubElement(pmEl, "ExtendedData")
      
      # Latitude
      latEl = SubElement(extDataEl, "Data", attrib={"name": "latitude"})
      SubElement(latEl, "value", value=latitude)
      
      # Longitude
      lonEl = SubElement(extDataEl, "Data", attrib={"name": "longitude"})
      SubElement(lonEl, "value", value=longitude)
      
      # Provider name
      pnEl = SubElement(extDataEl, "Data", attrib={"name": "providerName"})
      SubElement(pnEl, "value", value=escapeString(providerName, "xml"))
         
      # Resource name
      rnEl = SubElement(extDataEl, "Data", attrib={"name": "resourceName"})
      SubElement(rnEl, "value", value=escapeString(resourceName, "xml"))
   
      # Collector
      colEl = SubElement(extDataEl, "Data", attrib={"name": "collector"})
      SubElement(colEl, "value", value=escapeString(collector, "xml"))
   
      # Collection Date
      colDateEl = SubElement(extDataEl, "Data", attrib={"name": "colDate"})
      SubElement(colDateEl, "value", value=colDate)

# .............................................................................
def addUserPoint(parent, point):
   """
   @summary: Adds an occurrence point to the KML output
   @param parent: The parent element to add it to
   @param point: The point to add
   """
   try:
      name = "Point %s" % point.localId
   except:
      try:
         name = "Point %s" % point.localid
      except:
         name = point.canname
   
   lat, lon = getLatLonFromPointWkt(point.geomwkt)
   
#    try:
#       lat = point.latitude
#    except:
#       lat = point.lat
#       
#    try:
#       lon = point.longitude
#    except:
#       lon = point.lon

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
   ej = EarlJr()
   iconEl = SubElement(goEl, "Icon")
   SubElement(iconEl, "href", value=ej.constructLMMapRequest(
                                       "%s/ogc" % prj.metadataUrl,
                                       width=800, height=400, bbox=prj.bbox))
   
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
   SubElement(lastModEl, "value", value=formatTimeHuman(prj.statusModTime))

   scnTitleEl = SubElement(extData, "Data", attrib={"name": "scenarioTitle"})
   SubElement(scnTitleEl, "value", value=prj._scenario.title)

# .............................................................................
def addLayer(parent, lyr, visibility, indent=0):
   """
   @summary: Adds a layer to the KML output
   @param parent: The parent element to add it to
   @param point: The layer to add
   """
   lyrName = "Lifemapper layer %s" % lyr.getId()
   if indent == 0:
      SubElement(parent, "name", value=lyrName)
      SubElement(parent, "description", value=lyrName)
   
   # Ground Overlay
   goEl = SubElement(parent, "GroundOverlay")
   SubElement(goEl, "styleUrl", value="#lmLayerBalloon")
   SubElement(goEl, "name", value=lyrName)
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
   ej = EarlJr()
   SubElement(iconEl, "href", value=ej.constructLMMapRequest(
                                       "%s/ogc" % lyr.metadataUrl, width=800, 
                                       height=400, bbox=lyr.bbox))
   
   # Latitude Longitude Box
   latLonBoxEl = SubElement(goEl, "LatLonBox")
   SubElement(latLonBoxEl, "north", value=lyr.bbox[3])
   SubElement(latLonBoxEl, "south", value=lyr.bbox[1])
   SubElement(latLonBoxEl, "west", value=lyr.bbox[0])
   SubElement(latLonBoxEl, "east", value=lyr.bbox[2])
   SubElement(latLonBoxEl, "rotation", value="0.0")
   
   # Extended Data
   extData = SubElement(goEl, "ExtendedData")
   
   lastModEl = SubElement(extData, "Data", attrib={"name": "lastModified"})
   SubElement(lastModEl, "value", value=formatTimeHuman(lyr.modTime))

   scnTitleEl = SubElement(extData, "Data", attrib={"name": "layerTitle"})
   SubElement(scnTitleEl, "value", value=lyr.title)

# .............................................................................
def getKml(obj):
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
   SubElement(
        # Nested parent elements that don't add extra attributes
        SubElement(SubElement(lmGbif, "IconStyle"), "Icon"), 
        "href", value="%s/images/pushpin.png" % WEBSERVICES_ROOT)

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
   SubElement(
        # Nested parent elements that don't add extra attributes
        SubElement(SubElement(lmUser, "IconStyle"), "Icon"), 
        "href", value="%s/images/pushpin.png" % WEBSERVICES_ROOT)

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
   if isinstance(obj, SDMExperiment):
      addSdmExperiment(doc, obj)
   elif isinstance(obj, SDMProjection):
      addProjection(doc, obj, 1)
   elif isinstance(obj, Raster):
      addLayer(doc, obj, 1)
   elif isinstance(obj, OccurrenceLayer):
      addOccurrenceSet(doc, obj)
      
   temp = tostring(root)
   temp = temp.replace('&lt;', '<')
   temp = temp.replace('&gt;', '>')
   return temp

# .............................................................................
def getLatLonFromPointWkt(ptWkt):
   """
   @summary: Gets the latitude and longitude values for a point from the 
                geometry wkt
   @param ptWkt: The well known text for the point
   """
   lon, lat, z = ogr.CreateGeometryFromWkt(ptWkt).GetPoint()
   return lat, lon
