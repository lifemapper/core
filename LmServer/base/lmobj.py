"""
"""
from collections import namedtuple
from fractions import Fraction
import inspect

from LmBackend.common.lmobj import LMObject, LMError
from LmCommon.common.lmconstants import (LEGAL_MAP_UNITS, DEFAULT_EPSG,
    DEFAULT_MAPUNITS)
from LmServer.common.localconstants import SMTP_SENDER
from osgeo.osr import CoordinateTransformation, SpatialReference


# ............................................................................
# ............................................................................
class LMAbstractObject(LMObject):
    """
    Base class for all abstract objects in the Lifemapper project
    """

# ...............................................
    def abstract(self):
        """
        Raises:
            NotImplementedError: for calling method on the abstract class.
        """
        caller = inspect.getouterframes(inspect.currentframe())[1][3]
        raise NotImplementedError(caller + ' must be implemented in subclass')


# ............................................................................
# ............................................................................
class LMSpatialObject(LMObject):
    """Superclass for all spatial objects to ensure that bounding boxes are 
       consistent and logical
    """

    def __init__(self, epsgcode, bbox, mapunits):
        """LMSpatialObject superclass constructor

        Args:
            epsgcode (int): EPSG code indicating the SRS to use
            bbox: spatial extent of data
                sequence in the form (minX, minY, maxX, maxY)
                or comma-delimited string in the form 'minX, minY, maxX, maxY'
            mapunits: units of measurement for the data. 
                These are keywords as used in  mapserver, choice of 
                LmCommon.common.lmconstants.LegalMapUnits
                described in http://mapserver.gis.umn.edu/docs/reference/mapfile/mapObj)
        """
        self._epsg = None
        self._set_epsg(epsgcode)
        self._bbox = None
        self._set_bbox(bbox)
        self._mapunits = None
        self._setUnits(mapunits)
        super().__init__(self)

# ...............................................
    def create_srs_from_epsg(self, epsgcode=None):
        srs = SpatialReference()
        if epsgcode is not None:
            srs.ImportFromEPSG(epsgcode)
        else:
            srs.ImportFromEPSG(self._epsg)
        return srs

# ...............................................
    def get_srs(self):
        raise LMError('get_srs is only implemented on subclasses')

    def get_srs_as_wkt(self):
        """Gets an SRS as Well-Known Text
        
        Returns:
            string representation of an SRS
        """
        try:
            srs = self.get_srs()
        except Exception as e:
            raise
        else:
            wkt = srs.ExportToWkt()
            return wkt

    def get_srs_as_string(self):
        """Gets an SRS suitable for a Mapserver file
        
        Returns:
            string representation of an SRS
        """
        if self._epsg is not None:
            return 'epsg:' + str(self._epsg)

# .............................................................................
    def _get_epsg(self):
        return self._epsg

    def _set_epsg(self, epsg=None):
        if epsg == None:
            self._epsg = None
        else:
            if isinstance(epsg, str):
                try:
                    epsg = int(epsg)
                    self._epsg = epsg
                except:
                    raise LMError('Invalid epsg code {}'.format(epsg))
            elif isinstance(epsg, int):
                self._epsg = epsg
            elif isinstance(epsg, (tuple, list)):
                epsgcodes = set(epsg).discard(None)
                if len(epsgcodes) == 1:
                    self._epsg = epsgcodes[0]
                else:
                    raise LMError('LMSpatialObject may only contain a single EPSG code'
                                      .format(epsgcodes))
            else:
                raise LMError('Invalid EPSG code {} type {}'.format(epsg, type(epsg)))

    epsgcode = property(_get_epsg, _set_epsg)

# ...............................................
    def _set_units(self, mapunits):
        """Sets the map units for the layer
        
        Args:
            mapunits: map units type
            
        Raises:
            LMError: If the new units type is not one of the pre-defined
                LEGAL_MAP_UNITS (feet, inches, kilometers, meters, miles, dd, ds)
        """
        if mapunits is None or mapunits == '':
            self._mapunits = None
        else:
            mapunits = mapunits.lower()
            try:
                LEGAL_MAP_UNITS.index(mapunits)
            except:
                raise LMError(['Illegal Unit type', mapunits])
            else:
                self._mapunits = mapunits

    def _get_units(self):
        """Returns the map units type for the layer
        
        Note: 
            REMOVE THIS HACK! Add mapunits to db, handle on construction.
        """
        if self._mapunits is None and self._epsg == DEFAULT_EPSG:
            self._mapunits = DEFAULT_MAPUNITS
        return self._mapunits

    mapUnits = property(_get_units, _set_units)

# ..............................................................................
    def get_dimensions_by_bbox(self, bbox=None, limitWidth=1000):
        if bbox is None or len(bbox) != 4:
            bbox = self._bbox
        ratio = (bbox[3] - bbox[1]) / (bbox[2] - bbox[0])
        frac = Fraction(ratio).limit_denominator(limitWidth)
        return frac.numerator, frac.denominator

# ..............................................................................
    def _get_bbox(self):
        """
        @summary Gets the bounding box as a tuple in the 
                    format (minX, minY, maxX, maxY)
        @return Bounding box tuple
        """
        return self._bbox

    def _set_bbox(self, bbox):
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
                # In python3, all strings are unicode
                if isinstance(bbox, str):
                    bbox = tuple([float(b) for b in bbox.split(',')])
                elif isinstance(bbox, list):
                    bbox = tuple(bbox)

                if (isinstance(bbox, tuple) and len(bbox) == 4):
                    for v in bbox:
                        if not(isinstance(v, (float, int))):
                            raise LMError('Invalid bounding box type(s) {}'.format(bbox))

                    if LMSpatialObject._check_bounds(bbox):
                        self._bbox = bbox
                    else:
                        # TODO: replace with LMError as soon as bad bboxes are cleared out
                        print(('Invalid bounding box boundaries {}'.format(bbox)))
                        self._bbox = None
                else:
                    raise LMError('Invalid BBox: require 4-tuple in format (minX, minY, maxX, maxY)')
            except LMError as e:
                print(('Invalid bounding box boundaries {}'.format(bbox)))
                self._bbox = None
            except Exception as e:
                print(('Invalid bounding box boundaries {}'.format(bbox)))
                self._bbox = None

# ..............................................................................
    # # Tuple representation of bounding box, i.e. (minX, minY, maxX, maxY)
    bbox = property(_get_bbox, _set_bbox)

# # ...............................................
#     def _get_bounds_as_strings(self):
#         """
#         Get the minx, miny, maxx, maxy values of the dataset as a comma-separated 
#         string.  The float values are rounded to 2 digits past the decimal.
#         """
#         bstrLst = None
#         if self._bbox is not None:
#             bstrLst = ['{0:.2f}'.format(b) for b in self._bbox]
#         return bstrLst

# ...............................................
    @classmethod
    def get_extent_string(cls, bboxList, separator=' '):
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
    def get_csv_extent_string(self):
        """
        @summary Returns the bounding box values as a comma-delimited string,
        @return: String in the format 'minX,minY,maxX,maxY'
        @note: Used in bbox value in database records
        """
        bboxStr = LMSpatialObject.get_extent_string(self._bbox, separator=',')
        return bboxStr

# ..............................................................................
    def get_ssv_extent_string(self):
        """
        @summary Returns the bounding box values as a space-delimited string,
        @return: String in the format 'minX  minY  maxX  maxY'
        @note: Used in EXTENT parameter in Mapserver mapfiles
        """
        bboxStr = LMSpatialObject.get_extent_string(self._bbox, separator=' ')
        return bboxStr

# ..............................................................................
    @property
    def min_x(self):
        """
        @summary Gets the minimum x value
        @return Minimum x value of the tuple
        """
        if self._bbox is not None:
            return self._bbox[0]
        else:
            return None

    @property
    def min_y(self):
        """
        @summary Gets the minimum y value
        @return Minimum y value of the tuple
        """
        if self._bbox is not None:
            return self._bbox[1]
        else:
            return None

    @property
    def max_x(self):
        """
        @summary Gets the maximum x value
        @return Maximum x value of the tuple
        """
        if self._bbox is not None:
            return self._bbox[2]
        else:
            return None

    @property
    def max_y(self):
        """
        @summary Gets the maximum y value
        @return Maximum y value of the tuple
        """
        if self._bbox is not None:
            return self._bbox[3]
        else:
            return None

# ..............................................................................
    def get_wkt(self):
        """Gets Well-Known-Text (WKT) describing the bounding box polygon.
        
        Returns:
            String representing WKT for this polygon.
            
        Notes:
            Make sure there are no extra spaces so can compare strings with 
            those returned by postgis ASTEXT(geom) function.
        
            Does ASTEXT always return bbox starting with lower left corner?
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

# # ..............................................................................
#     def getLLUR(self):
#         """
#         @summary: Return lower left and upper right points.
#         @return: String in the format 'minX minY, maxX maxY'
#         """
#         llur = '{} {}, {} {}'.format(self.getMinX(), self.getMinY(),
#                                          self.getMaxX(), self.getMaxY())
#         return llur
# 
# # ..............................................................................
#     def equalExtents(self, spObj):
#         """
#         @summary Tests equality of two bounding boxes.
#         @param spObj: Another SpatialObject
#         @return True or false, if bboxes are equal
#         """
#         result = (isinstance(spObj, LMSpatialObject) and
#                      (self.minX == spObj.minX) and
#                      (self.minY == spObj.minY) and
#                      (self.maxX == spObj.maxX) and
#                      (self.maxY == spObj.maxY))
#         return result

# ..............................................................................
# Static/Class Methods
# ..............................................................................\
    @staticmethod
    def intersect_bboxes(bbox_seq):
        """
        @summary Method to find the intersection of a sequence of bounding boxes.
        @param bbox_seq: List of tuples representing (minx, miny, maxx, maxy)
        @return tuple in the form (minx, miny, maxx, maxy) representing the 
                  intersection of the list of bounding boxes
        @exception LMError: Thrown if the resulting bounding box has invalid 
                                     boundaries
        """
        if bbox_seq and bbox_seq.count(None) > 0:
            bbox_seq = [item for item in bbox_seq if item is not None]
        if bbox_seq:
            minXs = [bb[0] for bb in bbox_seq]
            minYs = [bb[1] for bb in bbox_seq]
            maxXs = [bb[2] for bb in bbox_seq]
            maxYs = [bb[3] for bb in bbox_seq]
            bounds = (max(minXs), max(minYs), min(maxXs), min(maxYs))
            if LMSpatialObject._check_bounds(bounds):
                return bounds
            else:
                print(('Non-intersecting bounding boxes, bounds: {}'.format(bounds)))
                return None
        else:
            return None

    @staticmethod
    def union_bboxes(bbox_seq):
        """Method to find the union of a sequence of bounding boxes.
        
        Args:
            bbox_seq: sequence of sequences of 4 numeric values 
                representing min_x, min_y, max_x, max_y

        Returns:
            tuple in the form (minx, miny, maxx, maxy) representing the 
            union of the bounding boxes
                  
        Raises:
            LMError: if the resulting bounding box has invalid 
            boundaries
        """
        if bbox_seq and bbox_seq.count(None) > 0:
            bbox_seq = [item for item in bbox_seq if item is not None]
        if bbox_seq:
            min_x_lst = [bb[0] for bb in bbox_seq]
            min_y_lst = [bb[1] for bb in bbox_seq]
            max_x_lst = [bb[2] for bb in bbox_seq]
            max_y_lst = [bb[3] for bb in bbox_seq]
            bounds = (min(min_x_lst), min(min_y_lst), max(max_x_lst), max(max_y_lst))
            if LMSpatialObject._check_bounds(bounds):
                return bounds
            else:
                raise LMError('Invalid bounding box boundaries {}'.format(bbox_seq))
        else:
            return None

    # ..............................................................................
    @staticmethod
    def _check_bounds(bbox):
        """
        @summary Checks the bounds of a tuple to make sure that they are within
                    the legal range for a bounding box
        @param bbox: The tuple of 4 values representing minX, minY, maxX, maxY
        @return True or False, based on the legality of each element of the 
                  bounding box
        @note: this checks for min <= max because rounding of lat/long values may
                 make very small bounding boxes appear to be a single point.
        """
        min_x, min_y, max_x, max_y = bbox
        return ((min_x <= max_x) and (min_y <= max_y))

    # ..............................................................................
    @staticmethod
    def process_wkt(wkt):
        """Processes Well-Known Text for a projected or geographic coordinate 
           system into an object
           
        Args:
            wkt (str): Well Known Text for a projected or geographic coordinate system
            
        Returns: 
            A named tuple containing elements defining the coordinate system
        """
        if wkt.startswith('GEOGCS'):
            return LMSpatialObject._process_geog_cs(wkt)
        else:
            return LMSpatialObject._process_proj_cs(wkt)

    # ..............................................................................
    @staticmethod
    def _process_proj_cs(prj_wkt):
        """Processes a projected coordinate system's WKT into an object
        
        Args:
            prj_wkt (str): Well Known Text (WKT) for a projected coordinate system
        """
        ProjCS = namedtuple(
            'PROJCS', ['name', 'geogcs', 'projectionName', 'parameters', 'unit'])
        ProjParam = namedtuple('Parameter', ['name', 'value'])

        # Name
        name = prj_wkt.split('"')[1]
        # GeoGCS
        geocs = "GEOGCS{}".format(prj_wkt.split('GEOGCS')[1].split('PROJECTION')[0])
        geocs = LMSpatialObject._processGEOGCS(geocs)
        # Projection Name
        try:
            pname = prj_wkt.split('PROJECTION')[1].split('"')[1]
        except:
            pname = ""
        # Parameters
        params = []
        params_group = prj_wkt.split('PARAMETER')
        try:
            for p in params_group[1:]:  # Cut out beginning string
                n = p.split('"')[1]
                v = p.split(']')[0].split(',')[1]
                params.append(ProjParam(name=n, value=v))
        except:
            pass
        # Unit
        unit = prj_wkt.split('UNIT')[-1].split('"')[1]
        if unit.lower() == "metre":  # Must match for EML
            unit = "meter"
        elif unit == "Degree":
            unit = "degree"

        return ProjCS(name, geocs, pname, params, unit)

    # ..............................................................................
    @staticmethod
    def _process_geog_cs(geocs_wkt):
        GeoCS = namedtuple('GEOGCS', ['name', 'datum', 'spheroid', 'primeMeridian', 'unit'])
        Spheroid = namedtuple('Spheroid', ['name', 'semiAxisMajor', 'denomFlatRatio'])
        PrimeMeridian = namedtuple('PrimeMeridian', ['name', 'longitude'])

        # Name
        name = geocs_wkt.split('"')[1]
        # Datum
        datumString = geocs_wkt.split('DATUM')[1].split('PRIMEM')[0]
        datum = datumString.split('"')[1]
        # Spheroid
        spheroid_str = datumString.split('SPHEROID')[1]
        spheroid_parts = spheroid_str.split(',')
        spheroid = Spheroid(
            name=spheroid_parts[0].split('"')[1],
            semiAxisMajor=float(spheroid_parts[1]),
            denomFlatRatio=float(spheroid_parts[2].split(']')[0]))
        # Prime Meridian
        pm = geocs_wkt.split('PRIMEM')[1].split('UNIT')[0]
        prime_meridian = PrimeMeridian(
            name=pm.split('"')[1],
            longitude=float(pm.split(',')[1].split(']')[0]))
        # Unit
        unit = geocs_wkt.split('UNIT')[1].split('"')[1]
        if unit.lower() == "metre":  # Must match for EML
            unit = "meter"
        elif unit == "Degree":
            unit = "degree"

        return GeoCS(name, datum, spheroid, prime_meridian, unit)

    # ..............................................................................
    @staticmethod
    def translate_points(points, src_wkt=None, src_epsg=None, dst_wkt=None, dst_epsg=None):
        """Translates a list of (x, y) pairs from one coordinate system to another
        
        Args:
            points: A sequence of one or more coordinate pairs, 
                i.e. [(x,y), (x,y) ...]
            src_wkt: Well-Known Text for the source coordinate system
            src_epsg: EPSG code of the source coordinate system
            dst_wkt: Well-Known Text for the destination coordinate system
            dst_epsg: EPSG code of the destination coordinate system
            
        Notes:
            Must provider either src_wkt or src_epsg, src_wkt has priority
            Must provide either dst_wkt or dst_epsg, dst_wkt has priority
        """
        src_spref = SpatialReference()
        if src_wkt is not None:
            src_spref.ImportFromWkt(src_wkt)
        elif src_epsg is not None:
            src_spref.ImportFromEPSG(src_epsg)
        else:
            raise Exception("Either src_wkt or src_epsg must be specified")

        dst_spref = SpatialReference()
        if dst_wkt is not None:
            dst_spref.ImportFromWkt(dst_wkt)
        elif dst_epsg is not None:
            dst_spref.ImportFromEPSG(dst_epsg)
        else:
            raise Exception("Either dst_wkt or dst_epsg must be specified")

        transPoints = []

        trans = CoordinateTransformation(src_spref, dst_spref)
        for pt in points:
            x, y, _ = trans.TransformPoint(pt[0], pt[1])
            transPoints.append((x, y))
        trans = None
        return transPoints


# ============================================================================
class LmHTTPError(LMError):
    """Error class for HTTP errors.  
    
    Notes:
        Wrapper method for LMError to add http status code
        
        These errors are caught by the web server and then transformed so 
        that the user knows what went wrong (usually in the case of 4xx
        errors).  The code should be one of the standard status response
        codes specified at: http://www.w3.org/Protocols/rfc2616/rfc2616-sec10.html

        If the proper status code is unknown (common for general errors), use
        500.  It is for internal server error.
        
        Status codes are found in LmCommon.common.lmconstants.HTTPStatus
    """

    def __init__(self, code, *args, msg=None, line_num=None, do_trace=False, **kwargs):
        """Constructor for the LmHTTPError class
        
        Args:
            code: HTTP error code of this error.
            msg: A message to return in the headers
        Note:
            HTTPException does not have a code 
        """
        LMError.__init__(
            self, *args, code=code, msg=msg, line_num=line_num, do_trace=do_trace)
        self.code = code
        self.msg = msg
        self.args = self.args + (self.code, self.msg)


# .............................................................................
class LMMessage(list, LMObject):
    """Class for communication, by email, text, etc
    """

    def __init__(self, body, to_addresses, from_address=SMTP_SENDER, subject=None):
        """Constructor for LMMessage class
        
        Args:
            body: body of the message
            from_address: Sender address
            to_addresses: List of recipient addresses
            subject: (optional) Subject line for the message 
        """
        self.body = body
        self.from_address = from_address
        self.to_addresses = to_addresses
        self.subject = subject
