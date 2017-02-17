"""
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
from types import StringType, UnicodeType
import os

from LmCommon.common.lmconstants import OutputFormat

from LmServer.common.localconstants import (APP_PATH, ARCHIVE_USER, 
                                    OGC_SERVICE_URL, WEBSERVICES_ROOT)
from LmServer.common.lmconstants import (DEFAULT_SRS, WEB_DIR,
   LMFileType, FileFix, SERVICES_PREFIX, GENERIC_LAYER_NAME_PREFIX,
   OCC_NAME_PREFIX, PRJ_PREFIX, MapPrefix, DEFAULT_WMS_FORMAT, 
   DEFAULT_WCS_FORMAT, MAP_TEMPLATE, MAP_DIR, ARCHIVE_PATH, USER_LAYER_DIR, 
   MODEL_DEPTH, NAME_SEPARATOR, MAP_KEY, WMS_LAYER_KEY, WCS_LAYER_KEY, 
   RAD_EXPERIMENT_DIR_PREFIX)
from LmServer.base.lmobj import LMError, LMObject
         
# .............................................................................
class EarlJr(LMObject):
   """
   @summary: Object to construct and parse filenames and URLs.
   """
# .............................................................................
# Constructor
# .............................................................................
   def __init__(self):
      """
      @summary: Constructor for the Earl object.  
      """
      LMObject.__init__(self)
      self.ogcUrl = OGC_SERVICE_URL

# ...............................................
   def createLayername(self, occsetId=None, projId=None, lyrId=None):
      """
      @summary: Return the base filename of an Archive data layer
      @param occsetId: Id of the OccurrenceSet data.
      @param projId: Id of the Projection layer.
      @note: Check prjId first, because occsetId may be sent in either case
      """
      if projId is not None:
         basename = NAME_SEPARATOR.join([PRJ_PREFIX, str(projId)])
      elif occsetId is not None:
         basename = NAME_SEPARATOR.join([OCC_NAME_PREFIX, str(occsetId)])
      elif lyrId is not None:
         basename = NAME_SEPARATOR.join([GENERIC_LAYER_NAME_PREFIX, str(lyrId)])
      else:
         raise LMError(currargs='Must supply OccsetId or ProjId for layer name')
      return basename
   
# ...............................................
   def createSDMProjectTitle(self, userId, taxaname, algCode, mdlscenCode, 
                            prjscenCode):
      name = 'Taxa {} modeled with {} and {} projected onto {}'.format(
               taxaname, algCode, mdlscenCode, prjscenCode )
      return name

# ...............................................
   def _parseSDMId(self, idstr, parts):
      """
      Recursive method to return a list of 3 digit strings from a multi-digit
      string.
      @note: replaces _parseSpeciesId
      """
      if len(idstr) > 1:
         self._parseSDMId(idstr[:-3], parts)
      
      if len(idstr) > 0:
         lastpart = idstr[-3:]
         for i in range(3-len(lastpart)):
            lastpart = ''.join(['0', lastpart])
         parts.append(lastpart)
      return parts
   
# ...............................................
   def createDataPath(self, usr, filetype, occsetId=None, epsg=None, 
                      gridsetId=None):
      """
      @note: /ARCHIVE_PATH/userId/
                 contains config files, MF docs, trees, attributes ...
             /ARCHIVE_PATH/userId/xxx/xxx/xxx/xxx
                 contains experiment data common to occurrenceId xxxxxxxxxxxx
             /ARCHIVE_PATH/userId/MAP_DIR/
                 contains maps
             /ARCHIVE_PATH/userId/<epsg>/USER_LAYER_DIR/
                 contains user layers common to epsg 
             /ARCHIVE_PATH/userId/<epsg>/RAD_<xxx>/
                 contains computed data for RAD gridset xxx
      """
      if usr is None :
         raise LMError('createDataPath requires userId')
      pth = os.path.join(ARCHIVE_PATH, usr)
      
      # General user documents go directly in user directory
      if LMFileType.isUserSpace(filetype):
         pass
      
      # OccurrenceSet path overrides general map path for SDM maps
      elif LMFileType.isSDM(filetype):
         if occsetId is not None:
            dirparts = self._parseSDMId(str(occsetId), [])
            for i in range(MODEL_DEPTH - len(dirparts)):
               dirparts.insert(0, '000')
            pth = os.path.join(pth, *dirparts)
         else:
            raise LMError('Missing occurrenceSetId for SDM filepath')
         
      # Maps go under user dir
      elif LMFileType.isMap(filetype):
         pth = os.path.join(pth, MAP_DIR)
                             
      elif not (LMFileType.isUserLayer(filetype) or LMFileType.isRAD(filetype)):
         raise LMError('Unknown filetype {}'.format(filetype))

      # Rest, User Layer and RAD data, are separated by epsg
      if epsg is None:
         raise LMError('Missing epsg for filetype {}'.format(filetype))
      else:
         pthparts = [pth, str(epsg)]
         # multi-purpose layers
         if LMFileType.isUserLayer(filetype):
            pthparts.append(USER_LAYER_DIR)
         # RAD gridsets
         elif LMFileType.isRAD(filetype):
            if gridsetId is not None:
               pthparts.append(RAD_EXPERIMENT_DIR_PREFIX + str(gridsetId))
            else:
               raise LMError('Missing gridsetId {}'.format(filetype))

      pth = os.path.join(*pthparts)
      return pth

# ...............................................
   def createOtherLayerFilename(self, usr, epsg, lyrName, ext):
      """
      @summary: Return the base filename of a Non-SDM-experiment Layer file 
      @param usr: Id of the User.
      @param epsg: EPSG code of the layer file
      @param occsetId: OccurrenceSet Id if this is for an OccurrenceSet layer.
      @param lyrName: Name of the layer.
      @param ext: File extentsion of this layer.
      """
      pth = self.createDataPath(usr, LMFileType.USER_LAYER, epsg=epsg)
      lyrName = lyrName + ext
      return os.path.join(pth, lyrName)

# ...............................................
   def createBasename(self, ftype, 
                      gridsetId=None, objCode=None, 
                      lyrname=None, usr=None, epsg=None):
      """
      @summary: Return the base filename for given filetype and parameters 
      @param ftype: LmServer.common.lmconstants.LMFileType
      @param objCode: Object database Id or unique code for non-db items
      @param lyrname: Layer name 
      @param usr: User database Id
      @param epsg: File or object EPSG code
      """
      basename = None
      
      nameparts = []
      # Prefix
      if FileFix.PREFIX[ftype] is not None:
         nameparts.append(FileFix.PREFIX[ftype])
      # Non-auto-generated Maps (not scenario or SDM)
      if ftype == LMFileType.OTHER_MAP:
         nameparts.extend([usr, epsg])
      # User layers
      elif LMFileType.isUserLayer(ftype):
         nameparts.append(lyrname)
      # All non-map, non-user-layer files use objCode 
      else:
         nameparts.append(objCode)
         
      fileparts = [str(p) for p in nameparts if p is not None ]
      try:
         basename = NAME_SEPARATOR.join(fileparts)
      except Exception, e:
         raise LMError('Bad type %s or parameters; (%s)' % (str(ftype), str(e)))
      return basename

# ...............................................
   def createFilename(self, ftype, 
                      occsetId=None, gridsetId=None, objCode=None, 
                      lyrname=None, usr=None, epsg=None, pth=None):
      """
      @summary: Return the absolute filename for given filetype and parameters 
      @copydoc LmServer.common.datalocator.EarlJr::createBasename()
      @param occsetId: SDM OccurrenceLayer database Id, used for path
      @param gridsetId: RAD Gridset database Id, used for path
      @param pth: File storage path, overrides calculated path
      """
      basename = self.createBasename(ftype, objCode=objCode, lyrname=lyrname, 
                                     usr=usr, epsg=epsg)      
      if pth is None:
         pth = self.createDataPath(usr, ftype, gridsetId=gridsetId,
                                   epsg=epsg, occsetId=occsetId)
      filename = os.path.join(pth, basename + FileFix.EXTENSION[ftype])
      return filename
   
# ...............................................
   def getMapFilenameAndUserFromMapname(self, mapname):         
      if mapname == MAP_TEMPLATE:
         pth = self._createStaticMapPath()
         usr = None
      else:
         scencode, occsetId, radexpId, bucketId, usr, ancillary, num = \
                     self._parseMapname(mapname)
         if usr is None:
            usr = self._findUserForObject(scencode=scencode, occsetId=occsetId, 
                                          radexpId=radexpId)
         if occsetId is not None:
            pth = self.createDataPath(usr, LMFileType.SDM_MAP, 
                                      occsetId=occsetId)
         elif scencode is not None:
            pth = self.createDataPath(usr, LMFileType.SCENARIO_MAP)
         elif ancillary:
            pth = self._createStaticMapPath()
         else:
            pth = self.createDataPath(usr, LMFileType.OTHER_MAP)
      if not mapname.endswith(OutputFormat.MAP):
         mapname = mapname+OutputFormat.MAP
      mapfname = os.path.join(pth, mapname)
      return mapfname, usr
   
# ...............................................
   def constructLMMetadataUrl(self, serviceType, objectId, moduleType=None,  
                              parentMetadataUrl=None):
      """
      @summary Return the REST service url for data in the Lifemapper Archive 
               or UserData for the given user and service.
      @param moduleType: LM module for this service, i.e. 'sdm' or 'rad'
      @param serviceType: LM service for this service, i.e. 'bucket' or 'model'
      @param parentMetadataUrl: The nested structure of this object's parent objects.
               The nested structure will begin with a '/', and take a form like: 
                  /grandParentClassType/grandParentId/parentClassType/parentId
      @param interface: The format in which to return the results, 
      """
      prefix = self.createWebServicePrefix()
      postfix = self.createWebServicePostfix(serviceType, objectId, 
                                             moduleType=moduleType,  
                                             parentMetadataUrl=parentMetadataUrl)
      url = '{}{}'.format(prefix, postfix)
      return url

# ...............................................
   def createWebServicePostfix(self, serviceType, objectId, moduleType=None,  
                              parentMetadataUrl=None):
      """
      @summary Return the relative REST service url for data in the 
               Lifemapper Archive for the given object and service (with 
               leading '/').
      @param moduleType: LM module for this service, i.e. 'sdm' or 'rad'
      @param serviceType: LM service for this service, i.e. 'bucket' or 'model'
      @param parentMetadataUrl: The nested structure of this object's parent objects.
               The nested structure will begin with a '/', and take a form like: 
                  /grandParentClassType/grandParentId/parentClassType/parentId
      """
      if parentMetadataUrl is not None:
         relativeprefix = ''
         prefix = self.createWebServicePrefix()
         if parentMetadataUrl.startswith(prefix):
            relativeprefix = parentMetadataUrl[len(prefix):]
         urlpath = '%s/%s/%s' % (relativeprefix, serviceType, objectId)
      else:
         urlpath = '/{}/{}/{}'.format(moduleType, serviceType, objectId)
      return urlpath

# ...............................................
   def createWebServicePrefix(self):
      """
      @summary Return the REST service url for Lifemapper web services (without 
               trailing '/').
      """
      url = '{}/{}'.format(WEBSERVICES_ROOT, SERVICES_PREFIX)
      return url


# ...............................................
   def _getOWSParams(self, mapprefix, owsLayerKey, bbox):
      params = []
      if bbox:
         coordStrings = [str(b) for b in bbox]
         bbstr = ','.join(coordStrings)
      else:
         bbstr = '-180,-90,180,90'
      params.append(('bbox', bbstr))   
      mapname = lyrname = None
      svcurl_rest = mapprefix.split('?')
      svcurl = svcurl_rest[0]
      if len(svcurl_rest) == 2:
         pairs = svcurl_rest[1].split('&')
         for kv in pairs:
            k, v = kv.split('=')
            k = k.lower()
            if k  == MAP_KEY:
               mapname = v
            elif k == WMS_LAYER_KEY:
               lyrname = v
            elif k == WCS_LAYER_KEY:
               lyrname = v   
         if mapname is not None:
            params.append(('map', mapname))
         if lyrname is not None:
            params.append((owsLayerKey, lyrname))
      return svcurl, params

# ...............................................
   def _getGETQuery(self, urlprefix, paramTpls):
      """
      Method to construct a GET query from a URL endpoint and a list of  
      of key-value tuples. URL endpoint concludes in either a '?' or a key/value 
      pair (i.e. id=25)
      @note: using list of tuples to ensure that the order of the parameters
             is always the same so we can string compare GET Queries
      """
      kvSep = '&'
      paramsSep = '?'
       
      pairs = []
      for key, val in paramTpls:
         if isinstance(val, (StringType, UnicodeType)):
            val = val.replace(' ', '%20')
         pairs.append('%s=%s' % (key, val))
       
      # Don't end in key/value pair separator
      if urlprefix.endswith(paramsSep) or urlprefix.endswith('&amp;') :
         raise LMError(['Improperly formatted URL prefix %s' % urlprefix])
      # If url/key-value-pair separator isn't present, append it
      elif urlprefix.find(paramsSep) == -1:
         urlprefix = urlprefix + '?'
      # > one key/value pair on the urlprefix, add separator before more pairs
      elif not urlprefix.endswith('?') and pairs:
         urlprefix = urlprefix + kvSep
       
      return urlprefix + kvSep.join(pairs)

# ...............................................
   def constructLMMapRequest(self, mapprefix, width, height, bbox, color=None,
                        srs=DEFAULT_SRS, format=DEFAULT_WMS_FORMAT):
      """
      @summary Return a GET query for the Lifemapper WMS GetMap request
      @param mapprefix: Lifemapper layer metadataUrl with 'ogc' format
      @param width: requested width for resulting image 
      @param height: requested height for resulting image 
      @param bbox: tuple in the form (minx, miny, maxx, maxy) delineating the 
                   geographic limits of the query.
      @param color: (optional) color in hex format RRGGBB or predefined palette 
             name. Color is applied only to Occurrences or Projection. Valid 
             palette names: 'gray', 'red', 'green', 'blue', 'bluered', 
             'bluegreen', 'greenred'. 
      @param srs: (optional) string indicating Spatial Reference System, 
             default is 'epsg:4326'
      @param format: (optional) image file format, default is 'image/png'
      """
      params = [('request', 'GetMap'),
                ('service', 'WMS'),
                ('version', '1.1.0'),
                ('srs', srs),
                ('format', format),
                ('width', width),
                ('height', height),
                ('styles', '')]
      url, moreparams = self._getOWSParams(mapprefix, 'layers', bbox)
      params.extend(moreparams)
      if color is not None:
         params.append(('color', color))
      wmsUrl = self._getGETQuery(url, params)
      return wmsUrl   

# ...............................................
   def constructLMRasterRequest(self, mapprefix, bbox, resolution=1, 
                                format=DEFAULT_WCS_FORMAT, crs=DEFAULT_SRS):
      """
      @summary Return a GET query for the Lifemapper WCS GetCoverage request
      @param mapprefix: Lifemapper layer metadataUrl with 'ogc' format
      @param bbox: tuple delineating the geographic limits of the query.
      @param resolution: (optional) spatial resolution along the x and y axes of 
                         the Coordinate Reference System (CRS). The values are  
                         given in the units appropriate to each axis of the CRS.
                         Default is 1.
      @param format: raster format for query output, default=image/tiff
      @param crs: (optional) string indicating Coordinate Reference System, 
             default is 'epsg:4326'
      """
      params = [('request', 'GetCoverage'),
                ('service', 'WCS'),
                ('version', '1.0.0'),
                ('crs', crs),
                ('format', format),
                ('resx', resolution),
                ('resy', resolution)]
      url, moreparams = self._getOWSParams(mapprefix, 'coverage', bbox)
      params.extend(moreparams)

      wcsUrl = self._getGETQuery(url, params)
      return wcsUrl   

# ...............................................
   # TODO: Replace this with constructMapPrefixNew
   def constructMapPrefix(self, mapname=None, ftype=None, scenarioCode=None,
                          occsetId=None, modelId=None, projId=None,
                          radexpId=None, bucketId=None, pamsumId=None,
                          lyrname=None, usr=None, epsg=None):
      """
      @summary: Construct a Lifemapper URL (prefix) for a map or map layer, 
                including 'ogc' format, '?', map=<mapname> key/value pair, and
                optional layers=<layername> pair.
      """
      if mapname is not None:
         if mapname.endswith(OutputFormat.MAP):
            mapname = mapname[:-1*len(OutputFormat.MAP)]
      else:
         if ftype in (LMFileType.SCENARIO_MAP,
                      LMFileType.OTHER_MAP, LMFileType.SDM_MAP, 
                      LMFileType.SHAPEGRID):
            mapname = self.createBasename(ftype, objCode=scenarioCode, 
                                          occsetId=occsetId, bucketId=bucketId, 
                                          usr=usr, epsg=epsg)
         else:
            raise LMError('Invalid LMFileType %s' % ftype)
         
      prefix = '%s?map=%s' % (self.ogcUrl, mapname)
      if lyrname is None and (occsetId is not None or projId is not None):
         lyrname = self.createLayername(occsetId=occsetId, projId=projId)
      if lyrname is not None:
         prefix += '&layers=%s' % lyrname
      return prefix

# ...............................................
   def constructMapPrefixNew(self, urlprefix=None, mapname=None, ftype=None, 
                             objCode=None, lyrname=None, usr=None, epsg=None):
      """
      @summary: Construct a Lifemapper URL (prefix) for a map or map layer, 
                including 'ogc' format, '?', map=<mapname> key/value pair, and
                optional layers=<layername> pair.
      @param urlprefix: optional urlprefix
      @param mapname: optional mapname
      @param ftype: LmServer.common.lmconstants.LMFileType
      @param occsetId: SDM OccurrenceLayer database Id, used for path/filename
      @param objCode: Object database Id or unique code for non-db items
      @param lyrname: Layer name 
      @param usr: User database Id
      @note: ignoring shapegrid maps for now
      @note: optional layer name must be provided fully formed
      """
      if mapname is not None:
         if mapname.endswith(OutputFormat.MAP):
            mapname = mapname[:-1*len(OutputFormat.MAP)]
      else:
         if LMFileType.isMap(ftype):
            mapname = self.createBasename(ftype, objCode=objCode, usr=usr, 
                                          epsg=epsg)
         else:
            raise LMError('Invalid LMFileType %s' % ftype)
      
      if urlprefix is None:
         urlprefix = self.ogcUrl
      fullprefix = '?map={}'.format(mapname)
         
      if lyrname is not None:
         fullprefix += '&layers={}'.format(lyrname)
         
      return fullprefix

# ...............................................
   def _createStaticMapPath(self):
      pth = os.path.join(APP_PATH, WEB_DIR, MAP_DIR)
      return pth

# ...............................................
   def _parseDataPathParts(self, parts):
      occsetId = epsg = radId = bckId = None
      usr = parts[0]
      rem = parts[1:]
      if len(rem) == 4:
         try:
            occsetId = int(''.join(parts))
         except:
            pass
      else:
         # Everything else begins with epsgcode (returned as string)
         epsg = rem[0]
         rem = rem[1:]
         if len(rem) >= 1:
            if rem[0] == USER_LAYER_DIR:
               isLayers = True
            elif rem[0].startswith(RAD_EXPERIMENT_DIR_PREFIX):
               dirname = rem[0]
               try:
                  radId = int(dirname[len(RAD_EXPERIMENT_DIR_PREFIX):])
               except:
                  raise LMError('Invalid RAD experiment id %s' % dirname)
               if len(rem) > 1:
                  try:
                     bktId = int(rem[1])
                  except:
                     raise LMError('Invalid RAD bucket id %s' % rem[1])
      return usr, occsetId, epsg, radId, bckId 

# ...............................................
   def _parseNewDataPath(self, fullpath):
      """
      @todo: UNFINISHED?
      @summary Return the relevant information from an absolute path to 
               LM-stored input/output data
      @note: /ARCHIVE_PATH/userId/xxx/xxx/xxx/xxx
                 contains experiment data common to occurrenceId xxxxxxxxxxxx
             /ARCHIVE_PATH/userId/maps/
                 contains all non-SDM mapfiles 
             /ARCHIVE_PATH/userId/epsg/Layers/
                 contains layers sharing epsg code
             /ARCHIVE_PATH/userId/epsg/RADxxx 
                 contains experiment level data (layer indexes, tree files, etc)
             /ARCHIVE_PATH/userId/epsg/RADxxx/bucketId
                 contains bucket level data (pam, statistics, etc)
      """
      usr = occsetId = epsg = radId = bckId = None
      ancPth = self._createStaticMapPath()
      
      if fullpath.startswith(ancPth):
         pass
      elif fullpath.startswith(ARCHIVE_PATH):
         pth = fullpath[len(ARCHIVE_PATH):]
         parts = pth.split(os.path.sep)
         # Remove empty string from leading path separator
         if '' in parts:
            parts.remove('')
         # Check last entry - if ends w/ trailing slash, this is a directory 
         last = parts[len(parts)-1]
         if last == '':
            parts = parts[:-1]
            
         usr, occsetId, epsg, radId, bckId = self._parseDataPathParts(parts)
            
      return usr, occsetId, epsg, radId, bckId

# ...............................................
   def parseMapFilename(self, mapfname):
      fullpath, fname = os.path.split(mapfname)
      mapname, ext = os.path.splitext(fname)

      usr, occsetId, epsg, radexpId, bucketId = self._parseNewDataPath(fullpath) 
      scencode, occsetId, radexpId, bucketId, usr2, ancillary, num \
               = self._parseMapname(mapname)
      if usr is None: usr = usr2

      return (mapname, ancillary, usr, epsg, occsetId, radexpId, bucketId, scencode)
   
# ...............................................
   def _findUserForObject(self, scencode=None, occsetId=None, radexpId=None):
      from LmServer.common.log import ConsoleLogger
      from LmServer.db.scribe import Scribe
      scribe = Scribe(ConsoleLogger())
      scribe.openConnections()
      usr = scribe.findUserForObject(scencode=scencode, occsetId=occsetId, 
                                     radexpId=radexpId)
      scribe.closeConnections()
      return usr
   
# ...............................................
   def _parseMapname(self, mapname):
      scencode = occsetId = radexpId = bucketId = usr = num = None
      ancillary = False
      # Remove extension
      if mapname.endswith(OutputFormat.MAP):
         mapname = mapname[:-1*len(OutputFormat.MAP)]
         
      parts = mapname.split(NAME_SEPARATOR)

      if parts[0] == MapPrefix.SCEN:
         scencode = parts[1]
         
      elif parts[0] == MapPrefix.SDM:
         occsetIdStr = parts[1]
         try:
            occsetId = int(occsetIdStr)
         except:
            msg = 'Improper Archive Data mapname %; ' % mapname
            msg += 'Should be %s + OccurrenceSetId' % MapPrefix.SDM
            raise LMError(currargs=msg, doTrace=True)
         
      elif parts[0] == MapPrefix.RAD:
         try:
            radexpId = int(parts[1])
            bucketId = int(parts[2])
         except:
            msg = 'Improper RAD mapname %; ' % mapname
            msg += 'Should be %s + experimentId + bucketId' % MapPrefix.RAD
            raise LMError(currargs=msg, doTrace=True)
         
      elif parts[0] == MapPrefix.USER:
         usr = parts[1]
         try:
            num = int(parts[2])
         except:
            pass

      elif mapname.startswith(MapPrefix.ANC):
         ancillary = True
         usr = ARCHIVE_USER 
         
      else:
         msg = 'Improper mapname %s - ' % mapname
         msg += '  requires prefix %s, %s, %s, or %s' % (MapPrefix.SCEN, 
                              MapPrefix.SDM, MapPrefix.USER, MapPrefix.ANC)
         raise LMError(currargs=msg, doTrace=True)
      return scencode, occsetId, radexpId, bucketId, usr, ancillary, num
