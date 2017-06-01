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

from LmBackend.common.lmobj import LMError, LMObject
from LmCommon.common.lmconstants import LMFormat
from LmServer.common.localconstants import APP_PATH, PUBLIC_USER
from LmServer.common.lmconstants import (DEFAULT_SRS, WEB_DIR, 
   LMFileType, FileFix, GENERIC_LAYER_NAME_PREFIX,
   OCC_NAME_PREFIX, PRJ_PREFIX, MapPrefix, DEFAULT_WMS_FORMAT, 
   DEFAULT_WCS_FORMAT, MAP_TEMPLATE, MAP_DIR, ARCHIVE_PATH, USER_LAYER_DIR, 
   MODEL_DEPTH, NAME_SEPARATOR, MAP_KEY, WMS_LAYER_KEY, WCS_LAYER_KEY, 
   RAD_EXPERIMENT_DIR_PREFIX, USER_MAKEFLOW_DIR, API_URL, OGC_SERVICE_URL)
         
# .............................................................................
class EarlJr(LMObject):
   """
   @summary: Object to construct and parse filenames and URLs.
   """
# .............................................................................
# Constructor
# .............................................................................
   def __init__(self, scribe=None):
      """
      @summary: Constructor for the Earl object.  
      """
      LMObject.__init__(self)
      self.ogcUrl = OGC_SERVICE_URL
      self._scribe = scribe

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
                 contains config files, trees, attributes ...
             /ARCHIVE_PATH/userId/makeflow
                 contains MF docs
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
      
      elif filetype == LMFileType.MF_DOCUMENT:
         pth = os.path.join(pth, USER_MAKEFLOW_DIR)
      
      # OccurrenceSet path overrides general map path for SDM maps
      elif LMFileType.isSDM(filetype):
         if occsetId is not None:
            dirparts = self._parseSDMId(str(occsetId), [])
            for i in range(MODEL_DEPTH - len(dirparts)):
               dirparts.insert(0, '000')
            pth = os.path.join(pth, *dirparts)
         else:
            raise LMError('Missing occurrenceSetId for SDM filepath')
         
      # All non-SDM Maps go under user map dir
      elif LMFileType.isMap(filetype):
         pth = os.path.join(pth, MAP_DIR)
                             
      else:
         if not (LMFileType.isUserLayer(filetype) or LMFileType.isRAD(filetype)):
            raise LMError('Unknown filetype {}'.format(filetype))

         # Rest, User Layer and RAD data, are separated by epsg
         if epsg is not None:
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
   def getTopLevelUserSDMPaths(self, usr):
      """
      @note: /ARCHIVE_PATH/userId/
                 contains config files, trees, attributes ...
             /ARCHIVE_PATH/userId/xxx/xxx/xxx/xxx
                 contains experiment data common to occurrenceId xxxxxxxxxxxx
      """
      sdmPaths = []
      if usr is None :
         raise LMError('getTopLevelUserSDMPaths requires userId')
      pth = os.path.join(ARCHIVE_PATH, usr)
      
      contents = os.listdir(pth)
      for name in contents:
         fulldir = os.path.join(pth, name)
         # SDM dirs are 3-digit integers, EPSG codes are a 4-digit integer
         if len(name) == 3 and os.path.isdir(fulldir):
            try:
               int(name)
            except:
               pass
            else:
               sdmPaths.append(fulldir)
         
      return sdmPaths

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
   def createBasename(self, ftype, objCode=None, 
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
      # User Maps for unconnected user layers (not SCENARIO_MAP or SDM_MAP)
      if ftype == LMFileType.OTHER_MAP:
         nameparts.extend([usr, epsg])
      # User layers
      elif LMFileType.isUserLayer(ftype):
         nameparts.append(lyrname)
      # All non-user-layer files use objCode 
      # SDM_MAP occurrencesetId, RAD_MAP gridsetId, SCENARIO_MAP scenarioCode
      elif objCode:
         nameparts.append(objCode)
         
      else:
         return None
         
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
      if basename is None:
         filename = None
      else:
         if pth is None:
            pth = self.createDataPath(usr, ftype, gridsetId=gridsetId,
                                      epsg=epsg, occsetId=occsetId)
         filename = os.path.join(pth, basename + FileFix.EXTENSION[ftype])
      return filename
   
# ...............................................
   def getMapFilenameFromMapname(self, mapname):         
      if mapname == MAP_TEMPLATE:
         pth = self._createStaticMapPath()
         usr = None
      else:
         (fileType, scencode, occsetId, gridsetId, usr, ancillary, 
          epsg) = self._parseMapname(mapname)
         
         if usr is None:
            usr = self._findUserForObject(scenCode=scencode, occId=occsetId)
         
         if ancillary:
            pth = self._createStaticMapPath()
         else:
            pth = self.createDataPath(usr, fileType, 
                                      occsetId=occsetId, gridsetId=gridsetId)
      
      if not mapname.endswith(LMFormat.MAP.ext):
         mapname = mapname+LMFormat.MAP.ext
      mapfname = os.path.join(pth, mapname)
      return mapfname, usr
   
# ...............................................
   def constructLMDataUrl(self, serviceType, objectId, interface, parentMetadataUrl=None):
      """
      @summary Return the REST service url for data in the Lifemapper Archive 
               or UserData for the given user and service.
      @param serviceType: LM service for this service, i.e. 'bucket' or 'model'
      @param objectId: The unique database id for requested object
      @param format: The data format in which to return the results, 
      @param parentMetadataUrl: The nested structure of this object's parent objects.
               The nested structure will begin with a '/', and take a form like: 
                  /grandParentClassType/grandParentId/parentClassType/parentId
      """
      postfix = self._createWebServicePostfix(serviceType, objectId, 
                                             parentMetadataUrl=parentMetadataUrl,
                                             interface=interface)
      url = '/'.join((API_URL, postfix))
      return url

# ...............................................
   def constructLMMetadataUrl(self, serviceType, objectId, 
                              parentMetadataUrl=None):
      """
      @summary Return the REST service url for data in the Lifemapper Archive 
               or UserData for the given user and service.
      @param serviceType: LM service for this service, i.e. 'bucket' or 'model'
      @param objectId: The unique database id for requested object
      @param parentMetadataUrl: The nested structure of this object's parent objects.
               The nested structure will begin with a '/', and take a form like: 
                  /grandParentClassType/grandParentId/parentClassType/parentId
      @param interface: The format in which to return the results, 
      """
      postfix = self._createWebServicePostfix(serviceType, objectId, 
                                             parentMetadataUrl=parentMetadataUrl)
      url = '/'.join((API_URL, postfix))
      return url

# ...............................................
   def _createWebServicePostfix(self, serviceType, objectId, 
                               parentMetadataUrl=None, interface=None):
      """
      @summary Return the relative REST service url for data in the 
               Lifemapper Archive for the given object and service (with 
               or without leading '/').
      @param serviceType: LM service for this service, i.e. 'bucket' or 'model'
      @param objectId: The unique database id for requested object
      @param parentMetadataUrl: The nested structure of this object's parent objects.
               The nested structure will begin with a '/', and take a form like: 
                  /grandParentClassType/grandParentId/parentClassType/parentId
      @param dataformat: The data format in which to return the results, 
      """
      parts = [serviceType, str(objectId)]
      if parentMetadataUrl is not None:
         if not parentMetadataUrl.startswith(API_URL):
            raise LMError('Parent URL {} does not start with local prefix {}'
                          .format(parentMetadataUrl, API_URL))
         else:
            relativeprefix = parentMetadataUrl[len(API_URL):]
            parts.insert(0, relativeprefix)
      if interface is not None:
         parts.append(interface)
      urlpath = '/'.join(parts)
      return urlpath


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
         if mapname.endswith(LMFormat.MAP.ext):
            mapname = mapname[:-1*len(LMFormat.MAP.ext)]
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
      occsetId = epsg = gridsetId = None
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
                  gridsetId = int(dirname[len(RAD_EXPERIMENT_DIR_PREFIX):])
               except:
                  raise LMError('Invalid RAD gridset id %s' % dirname)
      return usr, occsetId, epsg, gridsetId 

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
                 contains gridset level data
      """
      usr = occsetId = epsg = gridsetId = None
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
            
         usr, occsetId, epsg, gridsetId = self._parseDataPathParts(parts)
            
      return usr, occsetId, epsg, gridsetId

# ...............................................
   def parseMapFilename(self, mapfname):
      fullpath, fname = os.path.split(mapfname)
      mapname, ext = os.path.splitext(fname)

      usr, occsetId, epsg, _ = self._parseNewDataPath(fullpath) 
      (fileType, scencode, occsetId, gridsetId, usr2, ancillary, 
       epsg2) = self._parseMapname(mapname)
      if usr is None: usr = usr2
      if epsg is None: epsg = epsg2

      return (mapname, ancillary, usr, epsg, occsetId, gridsetId, scencode)
   
# ...............................................
   def _findUserForObject(self, layerId=None, scenCode=None, occId=None, 
                         matrixId=None, gridsetId=None, mfprocessId=None):
      if self._scribe is not None:
         usr = self._scribe.findUserForObject(layerId=layerId, scenCode=scenCode, 
                                        occId=occId, matrixId=matrixId, 
                                        gridsetId=gridsetId, mfprocessId=mfprocessId)
      else:
         from LmServer.common.log import ConsoleLogger
         from LmServer.db.borgscribe import BorgScribe
         scribe = BorgScribe(ConsoleLogger())
         scribe.openConnections()
         usr = scribe.findUserForObject(layerId=layerId, scenCode=scenCode, 
                                        occId=occId, matrixId=matrixId, 
                                        gridsetId=gridsetId, mfprocessId=mfprocessId)
         scribe.closeConnections()
      return usr
   
# ...............................................
   def _parseMapname(self, mapname):
      fileType = scencode = occsetId = usr = epsg = gridsetId = None
      ancillary = False
      # Remove extension
      if mapname.endswith(LMFormat.MAP.ext):
         mapname = mapname[:-1*len(LMFormat.MAP.ext)]
         
      parts = mapname.split(NAME_SEPARATOR)
      
      fileType = FileFix.getMaptypeFromName(prefix=parts[0])

      # RAD_MAP mapname = rad_<gridsetId>
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
      # RAD_MAP mapname = rad_<gridsetId>
      elif parts[0] == MapPrefix.RAD:
         try:
            gridsetId = int(parts[1])
         except:
            msg = 'Improper RAD mapname %; ' % mapname
            msg += 'Should be %s + gridsetId' % MapPrefix.RAD
            raise LMError(currargs=msg, doTrace=True)
         
      # User maps are usr_<usr>_<epsg>
      elif parts[0] == MapPrefix.USER:
         usr = parts[1]
         try:
            epsg = int(parts[2])
         except:
            pass

      elif mapname.startswith(MapPrefix.ANC):
         ancillary = True
         usr = PUBLIC_USER 
         
      else:
         msg = 'Improper mapname %s - ' % mapname
         msg += '  requires prefix %s, %s, %s, or %s' % (MapPrefix.SCEN, 
                              MapPrefix.SDM, MapPrefix.USER, MapPrefix.ANC)
         raise LMError(currargs=msg, doTrace=True)
      return fileType, scencode, occsetId, gridsetId, usr, ancillary, epsg
