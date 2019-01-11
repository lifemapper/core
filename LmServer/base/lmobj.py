"""
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
from collections import namedtuple
from fractions import Fraction
import inspect
import mx.DateTime
from osgeo.osr import CoordinateTransformation, SpatialReference
from types import TupleType, ListType, FloatType, IntType, StringType, UnicodeType

from LmBackend.common.lmobj import LMObject, LMError
from LmCommon.common.lmconstants import (LegalMapUnits, DEFAULT_EPSG,
    DEFAULT_MAPUNITS)
from LmServer.common.localconstants import SMTP_SENDER

# ............................................................................
# ............................................................................
class LMAbstractObject(LMObject):
    """
    Base class for all abstract objects in the Lifemapper project
    """    
# ...............................................
    def abstract(self):
        """
        Returns an error with the method name incorrectly called on the abstract 
        class.
        """
        caller = inspect.getouterframes(inspect.currentframe())[1][3]
        raise NotImplementedError(caller + ' must be implemented in subclass')

# ............................................................................
# ............................................................................
class LMSpatialObject(LMObject):
    """
    Class to ensure that bounding boxes are consistent and logical
    """
    
    def __init__(self, epsgcode, bbox, mapunits):
        """
        @summary Constructor for the LMSpatialObject class
        @param epsgcode: Integer code for EPSG indicating the SRS to use.
        @param bbox: Sequence in the form (minX, minY, maxX, maxY)
                 or string in the form 'minX, minY, maxX, maxY'
        @param mapunits: mapunits of measurement. These are keywords as used in 
                 mapserver, choice of LmCommon.common.lmconstants.LegalMapUnits
                 described in http://mapserver.gis.umn.edu/docs/reference/mapfile/mapObj)
        """
        self._epsg = None
        self._setEPSG(epsgcode)
        self._bbox = None
        self._setBBox(bbox)
        self._mapunits = None 
        self._setUnits(mapunits)
        LMObject.__init__(self)

# ...............................................
    def createSRSFromEPSG(self, epsgcode=None):
        srs = SpatialReference()
        if epsgcode is not None:
            srs.ImportFromEPSG(epsgcode)
        else:
            srs.ImportFromEPSG(self._epsg)
        return srs

# ...............................................
    def getSRS(self):
        raise LMError(currargs='getSRS is only implemented on subclasses')

    def getSRSAsWkt(self):
        try:
            srs = self.getSRS()
        except Exception, e:
            raise
        else:
            wkt = srs.ExportToWkt()
            return wkt 

    def getSRSAsString(self):
        """
        @summary: Get SRS suitable for a Mapserver file
        """
        if self._epsg is not None:
            return 'epsg:' + str(self._epsg)
        
# .............................................................................
    def _getEPSG(self):
        return self._epsg

    def _setEPSG(self, epsg=None):
        if epsg == None:
            self._epsg = None
        else:
            if isinstance(epsg, (StringType, UnicodeType)): 
                try:
                    epsg = int(epsg)
                    self._epsg = epsg
                except:
                    raise LMError('Invalid epsg code {}'.format(epsg))
            elif isinstance(epsg, IntType):
                self._epsg = epsg
            elif isinstance(epsg, ListType) or isinstance(epsg, TupleType):
                epsgcodes = set(epsg).discard(None)
                if len(epsgcodes) == 1:
                    self._epsg = epsgcodes[0]
                else:
                    raise LMError('LMSpatialObject may only contain a single EPSG code'
                                      .format(epsgcodes))
            else:
                raise LMError('Invalid EPSG code {} type {}'.format(epsg, type(epsg)))
    epsgcode = property(_getEPSG, _setEPSG)
            
# ...............................................
    def _setUnits(self, mapunits):
        """
        @summary Set the units parameter for the layer
        @param mapunits: The new units type
        @raise LMError: If the new units type is not one of the pre-determined 
                    legal unit types (feet, inches, kilometers, meters, miles, dd, ds)
        """
        if mapunits is None or mapunits == '':
            self._mapunits = None
        else:
            mapunits = mapunits.lower()
            try:
                LegalMapUnits.index(mapunits)
            except:
                raise LMError(['Illegal Unit type', mapunits])
            else:
                self._mapunits = mapunits
    
    def _getUnits(self):
        """
        @todo: REMOVE THIS HACK!
                 Add mapunits to Occ table (and Scenario?), handle on construction.
        """
        if self._mapunits is None and self._epsg == DEFAULT_EPSG:
            self._mapunits = DEFAULT_MAPUNITS
        return self._mapunits

    mapUnits = property(_getUnits, _setUnits)
                
# ..............................................................................
    def getHeightWidthByBBox(self, bbox=None, limitWidth=1000):
        if bbox is None or len(bbox) != 4:
            bbox = self._bbox
        ratio = (bbox[3] - bbox[1]) / (bbox[2] - bbox[0])
        frac = Fraction(ratio).limit_denominator(limitWidth)
        return frac.numerator, frac.denominator
    
# ..............................................................................
    def _getBBox(self):
        """
        @summary Gets the bounding box as a tuple in the 
                    format (minX, minY, maxX, maxY)
        @return Bounding box tuple
        """
        return self._bbox

    def _setBBox(self, bbox):
        """
        @summary Sets the bounding box
        @param bbox : geographic boundary in one of 2 formats:
                          - a list or tuple in the format [minX, minY, maxX, maxY]
                          - a string in the format 'minX, minY, maxX, maxY'
        @exception LMError: Thrown when the bounding box has values that are 
                                  out of legal range
        """
        if bbox is None:
            self._bbox = bbox
            
        else:
            try:
                if (isinstance(bbox, (StringType, UnicodeType))):
                    bbox = tuple([float(b) for b in bbox.split(',')])
                elif isinstance(bbox, ListType):
                    bbox = tuple(bbox)
                    
                if (isinstance(bbox, TupleType) and len(bbox) == 4):
                    for v in bbox:
                        if not(isinstance(v, FloatType) or isinstance(v, IntType)):
                            raise LMError('Invalid bounding box type(s) {}'.format(bbox))
                        
                    if LMSpatialObject._checkBounds(bbox):
                        self._bbox = bbox
                    else:
                        # TODO: replace with LMError as soon as bad bboxes are cleared out
                        print('Invalid bounding box boundaries {}'.format(bbox))
                        self._bbox = None
                else:
                    raise LMError('Invalid BBox: require 4-tuple in format (minX, minY, maxX, maxY)')
            except LMError, e:
                print('Invalid bounding box boundaries {}'.format(bbox))
                self._bbox = None
            except Exception, e:
                print('Invalid bounding box boundaries {}'.format(bbox))
                self._bbox = None


# ..............................................................................
    ## Tuple representation of bounding box, i.e. (minX, minY, maxX, maxY)
    bbox = property(_getBBox, _setBBox)

# ...............................................
    def _getBoundsAsStrings(self):
        """
        Get the minx, miny, maxx, maxy values of the dataset as a comma-separated 
        string.  The float values are rounded to 2 digits past the decimal.
        """
        bstrLst = None
        if self._bbox is not None:
            bstrLst = ['{0:.2f}'.format(b) for b in self._bbox]
        return bstrLst

# ...............................................
    @classmethod
    def getExtentAsString(cls, bboxList, separator=' '):
        """
        Get the minx, miny, maxx, maxy values of the dataset as a string of 
        separator separated values.  Values are rounded to 2 digits past the decimal.
        """
        bboxStr = None
        if bboxList is not None:
            bboxStr = ('{:.2f}{sep}{:.2f}{sep}{:.2f}{sep}{:.2f}'
            .format(bboxList[0], bboxList[1], bboxList[2], bboxList[3], 
                      sep=separator))
        return bboxStr

# ..............................................................................
    def getCSVExtentString(self):
        """
        @summary Returns the bounding box values as a comma-delimited string,
        @return: String in the format 'minX,minY,maxX,maxY'
        @note: Used in bbox value in database records
        """
        bboxStr = LMSpatialObject.getExtentAsString(self._bbox, separator=',')
        return bboxStr

# ..............................................................................
    def getSSVExtentString(self):
        """
        @summary Returns the bounding box values as a space-delimited string,
        @return: String in the format 'minX  minY  maxX  maxY'
        @note: Used in EXTENT parameter in Mapserver mapfiles
        """
        bboxStr = LMSpatialObject.getExtentAsString(self._bbox, separator=' ')
        return bboxStr

# ..............................................................................
    def getMinX(self):
        """
        @summary Gets the minimum x value
        @return Minimum x value of the tuple
        """
        if self._bbox is not None:
            return self._bbox[0]
        else:
            return None
    
    def getMinY(self):
        """
        @summary Gets the minimum y value
        @return Minimum y value of the tuple
        """
        if self._bbox is not None:
            return self._bbox[1]
        else:
            return None
    
    def getMaxX(self):
        """
        @summary Gets the maximum x value
        @return Maximum x value of the tuple
        """
        if self._bbox is not None:
            return self._bbox[2]
        else:
            return None
    
    def getMaxY(self):
        """
        @summary Gets the maximum y value
        @return Maximum y value of the tuple
        """
        if self._bbox is not None:
            return self._bbox[3]
        else:
            return None
        
    minX = property(getMinX)
    minY = property(getMinY)
    maxX = property(getMaxX)
    maxY = property(getMaxY)

# ..............................................................................
    def getWkt(self):
        """
        @summary: Get Well-Known-Text (WKT) describing the bounding box polygon.
        @return: String representing WKT for this polygon.
        @note: Make sure there are no extra spaces so can compare strings with 
                 those returned by postgis ASTEXT(geom) function.
        @note: Does ASTEXT always return bbox starting with lower left corner?
        """
        if self._bbox is not None:
            coordStrings = [str(b) for b in self._bbox]
            corners = ','.join([' '.join([coordStrings[0], coordStrings[1]]),
                                        ' '.join([coordStrings[0], coordStrings[3]]),
                                        ' '.join([coordStrings[2], coordStrings[3]]),
                                        ' '.join([coordStrings[2], coordStrings[1]]),
                                        ' '.join([coordStrings[0], coordStrings[1]])])
            wkt = 'POLYGON(({}))'.format(corners);
            return wkt
        else:
            return None            

# ..............................................................................
    def getLLUR(self):
        """
        @summary: Return lower left and upper right points.
        @return: String in the format 'minX minY, maxX maxY'
        """
        llur = '{} {}, {} {}'.format(self.getMinX(), self.getMinY(), 
                                         self.getMaxX(), self.getMaxY())
        return llur
            
# ..............................................................................
    def equalExtents(self, spObj):
        """
        @summary Tests equality of two bounding boxes.
        @param spObj: Another SpatialObject
        @return True or false, if bboxes are equal
        """
        result = (isinstance(spObj, LMSpatialObject) and
                     (self.minX == spObj.minX) and
                     (self.minY == spObj.minY) and
                     (self.maxX == spObj.maxX) and
                     (self.maxY == spObj.maxY))
        return result

# ..............................................................................
# Static/Class Methods
# ..............................................................................\
    @staticmethod
    def intersectBoundingBoxes(bboxSeq):
        """
        @summary Method to find the intersection of a sequence of bounding boxes.
        @param bboxSeq: List of tuples representing (minx, miny, maxx, maxy)
        @return tuple in the form (minx, miny, maxx, maxy) representing the 
                  intersection of the list of bounding boxes
        @exception LMError: Thrown if the resulting bounding box has invalid 
                                     boundaries
        """
        if bboxSeq and bboxSeq.count(None) > 0:
            bboxSeq = filter(lambda item: item is not None, bboxSeq)
        if bboxSeq:
            minXs = [bb[0] for bb in bboxSeq]
            minYs = [bb[1] for bb in bboxSeq]
            maxXs = [bb[2] for bb in bboxSeq]
            maxYs = [bb[3] for bb in bboxSeq]
            bounds = (max(minXs), max(minYs), min(maxXs), min(maxYs))
            if LMSpatialObject._checkBounds(bounds):
                return bounds
            else:
                print('Non-intersecting bounding boxes, bounds: {}'.format(bounds))
                return None
        else:
            return None
    
    @staticmethod
    def unionBoundingBoxes(bboxSeq):
        """
        @summary Method to find the union of a sequence of bounding boxes.
        @param bboxSeq: List of tuples representing (minx, miny, maxx, maxy)
        @return tuple in the form (minx, miny, maxx, maxy) representing the 
                  union of the list of bounding boxes
        @exception LMError: Thrown if the resulting bounding box has invalid 
                                     boundaries
        """
        if bboxSeq and bboxSeq.count(None) > 0:
            bboxSeq = filter(lambda item: item is not None, bboxSeq)
        if bboxSeq:
            minXs = [bb[0] for bb in bboxSeq]
            minYs = [bb[1] for bb in bboxSeq]
            maxXs = [bb[2] for bb in bboxSeq]
            maxYs = [bb[3] for bb in bboxSeq]
            bounds = (min(minXs), min(minYs), max(maxXs), max(maxYs))
            if LMSpatialObject._checkBounds(bounds):
                return bounds
            else:
                raise LMError('Invalid bounding box boundaries {}'.format(bboxSeq))
        else:
            return None
        
    # ..............................................................................
    @staticmethod
    def _checkBounds(bbox):
        """
        @summary Checks the bounds of a tuple to make sure that they are within
                    the legal range for a bounding box
        @param bbox: The tuple of 4 values representing minX, minY, maxX, maxY
        @return True or False, based on the legality of each element of the 
                  bounding box
        @note: this checks for min <= max because rounding of lat/long values may
                 make very small bounding boxes appear to be a single point.
        """
        minX, minY, maxX, maxY = bbox
        return ((minX <= maxX) and (minY <= maxY))
    
    # ..............................................................................
    @staticmethod
    def processWkt(wkt):
        """
        @summary: Processes Well-Known Text for an SRS and returns an object
        """
        if wkt.startswith('GEOGCS'):
            return LMSpatialObject._processGEOGCS(wkt)
        else:
            return LMSpatialObject._processPROJCS(wkt)

    # ..............................................................................
    @staticmethod
    def _processPROJCS(prjcsStr):
        """
        @summary: Processes a projected coordinate system's WKT into an object
        """
        PrjCS = namedtuple('PROJCS', ['name', 'geogcs', 'projectionName', 'parameters', 'unit'])
        Parameter = namedtuple('Parameter', ['name', 'value'])
        
        # Name
        name = prjcsStr.split('"')[1]
        
        # GeoGCS
        geocsStr = "GEOGCS{}".format(prjcsStr.split('GEOGCS')[1].split('PROJECTION')[0])
        geocs = LMSpatialObject._processGEOGCS(geocsStr)
        
        # Projection Name
        try:
            prjName = prjcsStr.split('PROJECTION')[1].split('"')[1]
        except:
            prjName = ""
        
        # Parameters
        parameters = []
        parametersGroup = prjcsStr.split('PARAMETER')
        
        try:
            for param in parametersGroup[1:]: # Cut out beginning string
                n = param.split('"')[1]
                v = param.split(']')[0].split(',')[1]
                parameters.append(Parameter(name=n, value=v))
        except:
            pass
        
        # Unit
        unit = prjcsStr.split('UNIT')[-1].split('"')[1]
        if unit.lower() == "metre": # Must match for EML
            unit = "meter"
        elif unit == "Degree":
            unit = "degree"
        
        ret = PrjCS(name, geocs, prjName, parameters, unit)
        return ret
    
    # ..............................................................................
    @staticmethod
    def _processGEOGCS(geocsStr):
        """
        @summary: Processes a geographic coordinate system's WKT into an object
        """
        GeoCS = namedtuple('GEOGCS', ['name', 'datum', 'spheroid', 'primeMeridian', 'unit'])
        Spheroid = namedtuple('Spheroid', ['name', 'semiAxisMajor', 'denomFlatRatio'])
        PrimeM = namedtuple('PrimeMeridian', ['name', 'longitude'])
        
        # Name
        name = geocsStr.split('"')[1]
        
        # Datum
        datumString = geocsStr.split('DATUM')[1].split('PRIMEM')[0]
        datum = datumString.split('"')[1]
        
        # Spheroid
        spheroidString = datumString.split('SPHEROID')[1]
        spheroidParts = spheroidString.split(',')
        spheroid = Spheroid(
                                  name=spheroidParts[0].split('"')[1],
                                  semiAxisMajor=float(spheroidParts[1]),
                                  denomFlatRatio=float(spheroidParts[2].split(']')[0])
                                 )
        
        # Prime Meridian
        pmString = geocsStr.split('PRIMEM')[1].split('UNIT')[0]
        primeM = PrimeM(name=pmString.split('"')[1],
                             longitude=float(pmString.split(',')[1].split(']')[0]))
    
        # Unit
        unit = geocsStr.split('UNIT')[1].split('"')[1]
        if unit.lower() == "metre": # Must match for EML
            unit = "meter"
        elif unit == "Degree":
            unit = "degree"
        
        ret = GeoCS(name, datum, spheroid, primeM, unit)
        return ret
    
    # ..............................................................................
    @staticmethod
    def translatePoints(points, srcWKT=None, srcEPSG=None, dstWKT=None, dstEPSG=None):
        """
        @summary: Translates a list of (x, y) pairs from one coordinate system to 
                         another
        @param points: A list of points [(x, y)+]
        @param srcWKT: The Well-Known Text for the source coordinate system
        @param srcEPSG: The EPSG code of the source coordinate system
        @param dstWKT: The Well-Known Text for the destination coordinate system
        @param dstEPSG: The EPSG code of the destination coordinate system
        @note: Use either srcWKT or srcEPSG, srcWKT has priority
        @note: Use either dstWKT or dstEPSG, dstWKT has priority
        """
        srcSR = SpatialReference()
        if srcWKT is not None:
            srcSR.ImportFromWkt(srcWKT)
        elif srcEPSG is not None:
            srcSR.ImportFromEPSG(srcEPSG)
        else:
            raise Exception("Either srcWKT or srcEPSG must be specified")
    
        dstSR = SpatialReference()
        if dstWKT is not None:
            dstSR.ImportFromWkt(dstWKT)
        elif dstEPSG is not None:
            dstSR.ImportFromEPSG(dstEPSG)
        else:
            raise Exception("Either dstWKT or dstEPSG must be specified")
        
        transPoints = []
        
        trans = CoordinateTransformation(srcSR, dstSR)
        for pt in points:
            x, y, _ = trans.TransformPoint(pt[0], pt[1])
            transPoints.append((x, y))
        trans = None
        return transPoints
    


# ============================================================================
class LmHTTPError(LMError):
    """
    @summary: Error class for HTTP errors.  Could be a wrapper for LMErrors as 
                     they are passed back out of the system or for permission issues
    @note: Wrapper method for LMError to add http status code
    @note: These errors are caught by the web server and then transformed so 
                 that the user knows what went wrong (usually in the case of 4xx
                 errors).  The code should be one of the standard status response
                 codes specified at: http://www.w3.org/Protocols/rfc2616/rfc2616-sec10.html
    @note: If the proper status code is unknown (common for general errors), use
                 500.  It is for internal server error.
    @note: Status codes are found in LmCommon.common.lmconstants.HTTPStatus
    """
    
    def __init__(self, code, msg=None, currargs=None, prevargs=None, lineno=None, 
                     doTrace=False):
        """
        @summary: Constructor for the LmHTTPError class
        @param code: (optional - HTTPException does not have a code) 
                         The HTTP error code of this error
        @param msg: A message to return in the headers
        """
        LMError.__init__(self, currargs=currargs, prevargs=prevargs, 
                              lineno=lineno, doTrace=doTrace)
        self.code = code
        self.msg = msg
    
    # ......................................
    def __repr__(self):
        return "LmHTTPError {} ({})".format(self.code, self.msg)

    # ......................................
    def __str__(self):
        return "LmHTTPError {} ({})".format(self.code, self.msg)

# ============================================================================
class LMMissingDataError(LMError):
    """
    @summary: Error class for missing data errors.  
    @note: Wrapper method for LMError allowing code to handle missing data.
    """
    
    def __init__(self, code, msg=None, currargs=None, prevargs=None, lineno=None, 
                     doTrace=False):
        """
        @summary: Constructor for the LmHTTPError class
        @param code: (optional - HTTPException does not have a code) 
                         The HTTP error code of this error
        @param msg: A message to return in the headers
        """
        LMError.__init__(self, currargs=currargs, prevargs=prevargs, 
                              lineno=lineno, doTrace=doTrace)
        self.code = code
        self.msg = msg
    
    # ......................................
    def __repr__(self):
        return "{} [] ({})".format(self.__class__.__name__, self.code, self.msg)

    # ......................................
    def __str__(self):
        return "{} {} ({})".format(self.__class__.__name__, self.code, self.msg)

# .............................................................................
class LMMessage(list, LMObject):
    """
    @summary: Object used for communication, by email, text, etc
    """
    def __init__(self, body, toAddressList, fromAddress=SMTP_SENDER, subject=None):
        """
        @summary: Constructor
        @param body: The body of the message
        @param fromAddress: Sender address
        @param toAddressList: List of recipient addresses 
        @param subject: (optional) Subject line for the message 
        """
        self.body = body
        self.fromAddress = fromAddress 
        self.toAddresses = toAddressList
        self.subject = subject
