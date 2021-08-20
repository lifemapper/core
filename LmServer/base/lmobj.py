"""Module containing Lifemapper object base classes.
"""
from collections import namedtuple
from fractions import Fraction
import inspect

from osgeo.osr import CoordinateTransformation, SpatialReference

from LmBackend.common.lmobj import LMObject, LMError
from LmCommon.common.lmconstants import (
    DEFAULT_EPSG, DEFAULT_MAPUNITS, LEGAL_MAP_UNITS)
from LmServer.common.localconstants import SMTP_SENDER


# ............................................................................
# ............................................................................
class LMAbstractObject(LMObject):
    """Base class for all abstract objects in the Lifemapper project"""
    # ................................
    @staticmethod
    def abstract():
        """
        Raises:
            NotImplementedError: for calling method on the abstract class.
        """
        caller = inspect.getouterframes(inspect.currentframe())[1][3]
        raise NotImplementedError(caller + ' must be implemented in subclass')


# ............................................................................
class LMSpatialObject(LMObject):
    """Superclass for all Lifemapper spatial objects"""
    # ................................
    def __init__(self, epsg_code, bbox, map_units):
        """LMSpatialObject superclass constructor

        Args:
            epsg_code (int): EPSG code indicating the SRS to use
            bbox: spatial extent of data
                sequence in the form (minX, minY, maxX, maxY)
                or comma-delimited string in the form 'minX, minY, maxX, maxY'
            map_units: units of measurement for the data. These are keywords as
                used in  mapserver, choice of LegalMapUnits described in
                    http://mapserver.gis.umn.edu/docs/reference/mapfile/mapObj)
        """
        self._epsg = None
        self._set_epsg(epsg_code)
        self._bbox = None
        self._set_bbox(bbox)
        self._map_units = None
        self._set_units(map_units)

    # ................................
    def create_srs_from_epsg(self, epsg_code=None):
        """Create a spatial reference system object from an epsg code
        """
        srs = SpatialReference()
        if epsg_code is not None:
            srs.ImportFromEPSG(epsg_code)
        else:
            srs.ImportFromEPSG(self._epsg)
        return srs

    # ................................
    @staticmethod
    def get_srs():
        """Get the spatial reference system"""
        raise LMError('get_srs is only implemented on subclasses')

    # ................................
    def get_srs_as_wkt(self):
        """Gets an SRS as Well-Known Text

        Returns:
            string representation of an SRS
        """
        try:
            srs = self.get_srs()
        except Exception as err:
            raise LMError(err)

        return srs.ExportToWkt()

    # ................................
    def get_srs_as_string(self):
        """Gets an SRS suitable for a Mapserver file

        Returns:
            string representation of an SRS
        """
        if self._epsg is not None:
            return 'epsg:' + str(self._epsg)
        return None

    # ................................
    def _get_epsg(self):
        return self._epsg

    # ................................
    def _set_epsg(self, epsg=None):
        if epsg is None:
            self._epsg = None
        else:
            if isinstance(epsg, str):
                try:
                    epsg = int(epsg)
                    self._epsg = epsg
                except Exception:
                    raise LMError('Invalid epsg code {}'.format(epsg))
            elif isinstance(epsg, int):
                self._epsg = epsg
            elif isinstance(epsg, (tuple, list)):
                epsg_codes = set(epsg).discard(None)
                if len(epsg_codes) == 1:
                    self._epsg = epsg_codes[0]
                else:
                    raise LMError(
                        'LMSpatialObject may only contain a single EPSG code')
            else:
                raise LMError(
                    'Invalid EPSG code {} type {}'.format(epsg, type(epsg)))

    epsg_code = property(_get_epsg, _set_epsg)

    # ................................
    def _set_units(self, map_units):
        """Sets the map units for the layer

        Args:
            map_units: map units type

        Raises:
            LMError: If the new units type is not one of the pre-defined
                LEGAL_MAP_UNITS (feet, inches, kilometers, meters, miles, dd,
                ds)
        """
        if map_units is None or map_units == '':
            self._map_units = None
        else:
            map_units = map_units.lower()
            try:
                LEGAL_MAP_UNITS.index(map_units)
            except Exception:
                raise LMError(['Illegal Unit type', map_units])
            else:
                self._map_units = map_units

    # ................................
    def _get_units(self):
        """Returns the map units type for the layer

        Note:
            REMOVE THIS HACK! Add map_units to db, handle on construction.
        """
        if self._map_units is None and self._epsg == DEFAULT_EPSG:
            self._map_units = DEFAULT_MAPUNITS
        return self._map_units

    map_units = property(_get_units, _set_units)

    # ................................
    def get_dimensions_by_bbox(self, bbox=None, limit_width=1000):
        """Get the dimensions by the bounding box"""
        if bbox is None or len(bbox) != 4:
            bbox = self._bbox
        ratio = (bbox[3] - bbox[1]) / (bbox[2] - bbox[0])
        frac = Fraction(ratio).limit_denominator(limit_width)
        return frac.numerator, frac.denominator

    # ................................
    def _get_bbox(self):
        """Gets the bounding box tuple"""
        return self._bbox

    # ................................
    def _set_bbox(self, bbox):
        """Sets the bounding box"""
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
                    for val in bbox:
                        if not isinstance(val, (float, int)):
                            raise LMError(
                                'Invalid bounding box type(s) {}'.format(bbox))

                    if LMSpatialObject._check_bounds(bbox):
                        self._bbox = bbox
                    else:
                        # TODO: replace with LMError as soon as bad bboxes are
                        #    cleared out
                        print((
                            'Invalid bounding box boundaries {}'.format(bbox)))
                        self._bbox = None
                else:
                    raise LMError(
                        ('Invalid BBox: require 4-tuple in format (minX, minY,'
                         ' maxX, maxY)'))
            except LMError:
                print(('Invalid bounding box boundaries {}'.format(bbox)))
                self._bbox = None
            except Exception:
                print(('Invalid bounding box boundaries {}'.format(bbox)))
                self._bbox = None

    # ................................
    # # Tuple representation of bounding box, i.e. (minX, minY, maxX, maxY)
    bbox = property(_get_bbox, _set_bbox)

    # ................................
    @classmethod
    def get_extent_string(cls, bbox_list, separator=' '):
        """Get the bbox of the dataset as a string of separated values."""
        bbox_str = None
        if bbox_list is not None:
            bbox_str = ('{:.2f}{sep}{:.2f}{sep}{:.2f}{sep}{:.2f}'.format(
                bbox_list[0], bbox_list[1], bbox_list[2], bbox_list[3],
                sep=separator))
        return bbox_str

    # ................................
    def get_csv_extent_string(self):
        """Returns the bounding box values as a comma-delimited string

        Returns:
            str - String in the format 'minX,minY,maxX,maxY'

        Note:
            Used in bbox value in database records
        """
        return LMSpatialObject.get_extent_string(self._bbox, separator=',')

    # ................................
    def get_ssv_extent_string(self):
        """Returns the bounding box values as a space-delimited string,

        Returns:
            str - String in the format 'minX  minY  maxX  maxY'

        Note:
            Used in EXTENT parameter in Mapserver mapfiles
        """
        return LMSpatialObject.get_extent_string(self._bbox, separator=' ')

    # ................................
    @property
    def min_x(self):
        """Gets the minimum x value"""
        if self._bbox is not None:
            return self._bbox[0]

        return None

    # ................................
    @property
    def min_y(self):
        """Gets the minimum y value"""
        if self._bbox is not None:
            return self._bbox[1]

        return None

    # ................................
    @property
    def max_x(self):
        """Gets the maximum x value"""
        if self._bbox is not None:
            return self._bbox[2]

        return None

    # ................................
    @property
    def max_y(self):
        """Gets the maximum y value"""
        if self._bbox is not None:
            return self._bbox[3]

        return None

    # ................................
    def get_wkt(self):
        """Gets Well-Known-Text (WKT) describing the bounding box polygon.

        Returns:
            String representing WKT for this polygon.

        Notes:
            - Make sure there are no extra spaces so can compare strings with
                those returned by postgis ASTEXT(geom) function.
            - Does ASTEXT always return bbox starting with lower left corner?
        """
        if self._bbox is not None:
            min_x, min_y, max_x, max_y = self._bbox
            corners = ['{} {}'.format(min_x, min_y),
                       '{} {}'.format(min_x, max_y),
                       '{} {}'.format(max_x, max_y),
                       '{} {}'.format(max_x, min_y),
                       '{} {}'.format(min_x, min_y)]
            return 'POLYGON(({}))'.format(','.join(corners))

        return None

    # ................................
    @staticmethod
    def intersect_bboxes(bbox_seq):
        """Method to find the intersection of a sequence of bounding boxes.

        Args:
            bbox_seq: List of tuples representing (minx, miny, maxx, maxy)

        Returns:
            tuple in the form (minx, miny, maxx, maxy) representing the
                intersection of the list of bounding boxes

        Raises:
            LMError: Thrown if the resulting bounding box has invalid
                boundaries
        """
        if bbox_seq and bbox_seq.count(None) > 0:
            bbox_seq = [item for item in bbox_seq if item is not None]
        if bbox_seq:
            min_xs = [bb[0] for bb in bbox_seq]
            min_ys = [bb[1] for bb in bbox_seq]
            max_xs = [bb[2] for bb in bbox_seq]
            max_ys = [bb[3] for bb in bbox_seq]
            bounds = (max(min_xs), max(min_ys), min(max_xs), min(max_ys))
            if LMSpatialObject._check_bounds(bounds):
                return bounds

            print(('Non-intersecting bounding boxes, bounds: {}'.format(
                bounds)))
            return None

        return None

    # ................................
    @staticmethod
    def union_bboxes(bbox_seq):
        """Method to find the union of a sequence of bounding boxes.

        Args:
            bbox_seq: sequence of sequences of 4 numeric values
                representing min_x, min_y, max_x, max_y

        Returns:
            tuple in the form (minx, miny, maxx, maxy) representing the union
                of the bounding boxes

        Raises:
            LMError: if the resulting bounding box has invalid boundaries
        """
        if bbox_seq and bbox_seq.count(None) > 0:
            bbox_seq = [item for item in bbox_seq if item is not None]
        if bbox_seq:
            min_x_lst = [bb[0] for bb in bbox_seq]
            min_y_lst = [bb[1] for bb in bbox_seq]
            max_x_lst = [bb[2] for bb in bbox_seq]
            max_y_lst = [bb[3] for bb in bbox_seq]
            bounds = (
                min(min_x_lst), min(min_y_lst), max(max_x_lst), max(max_y_lst))
            if LMSpatialObject._check_bounds(bounds):
                return bounds

            raise LMError(
                'Invalid bounding box boundaries {}'.format(bbox_seq))

        return None

    # ................................
    @staticmethod
    def _check_bounds(bbox):
        """Checks the bounds to make sure they are legal

        Args:
            bbox: The tuple of 4 values representing minX, minY, maxX, maxY

        Returns:
            bool - Indication if bounds are legal.

        Note:
            This checks for min <= max because rounding of lat/long values may
                make very small bounding boxes appear to be a single point.
        """
        min_x, min_y, max_x, max_y = bbox
        return (min_x <= max_x) and (min_y <= max_y)

    # ................................
    @staticmethod
    def process_wkt(wkt):
        """Processes Well-Known Text for a coordinate system.

        Args:
            wkt (str): Well Known Text for a projected or geographic coordinate
                system

        Returns:
            A named tuple containing elements defining the coordinate system
        """
        if wkt.startswith('GEOGCS'):
            return LMSpatialObject._process_geog_cs(wkt)

        return LMSpatialObject._process_proj_cs(wkt)

    # ................................
    @staticmethod
    def _process_proj_cs(prj_wkt):
        """Processes a projected coordinate system's WKT into an object

        Args:
            prj_wkt (str): Well Known Text (WKT) for a projected coordinate
                system
        """
        ProjCS = namedtuple(
            'PROJCS',
            ['name', 'geogcs', 'projectionName', 'parameters', 'unit'])
        ProjParam = namedtuple('Parameter', ['name', 'value'])

        # Name
        name = prj_wkt.split('"')[1]
        # GeoGCS
        geocs = "GEOGCS{}".format(
            prj_wkt.split('GEOGCS')[1].split('PROJECTION')[0])
        geocs = LMSpatialObject._process_geog_cs(geocs)
        # Projection Name
        try:
            pname = prj_wkt.split('PROJECTION')[1].split('"')[1]
        except Exception:
            pname = ""
        # Parameters
        params = []
        params_group = prj_wkt.split('PARAMETER')
        try:
            for prm in params_group[1:]:  # Cut out beginning string
                name = prm.split('"')[1]
                val = prm.split(']')[0].split(',')[1]
                params.append(ProjParam(name=name, value=val))
        except Exception:
            pass
        # Unit
        unit = prj_wkt.split('UNIT')[-1].split('"')[1]
        if unit.lower() == "metre":  # Must match for EML
            unit = "meter"
        elif unit == "Degree":
            unit = "degree"

        return ProjCS(name, geocs, pname, params, unit)

    # ................................
    @staticmethod
    def _process_geog_cs(geocs_wkt):
        GeoCS = namedtuple(
            'GEOGCS', ['name', 'datum', 'spheroid', 'prime_meridian', 'unit'])
        Spheroid = namedtuple(
            'Spheroid', ['name', 'semi_axis_major', 'denom_flat_ratio'])
        PrimeMeridian = namedtuple('PrimeMeridian', ['name', 'longitude'])

        # Name
        name = geocs_wkt.split('"')[1]
        # Datum
        datum_string = geocs_wkt.split('DATUM')[1].split('PRIMEM')[0]
        datum = datum_string.split('"')[1]
        # Spheroid
        spheroid_str = datum_string.split('SPHEROID')[1]
        spheroid_parts = spheroid_str.split(',')
        spheroid = Spheroid(
            name=spheroid_parts[0].split('"')[1],
            semi_axis_major=float(spheroid_parts[1]),
            denom_flat_ratio=float(spheroid_parts[2].split(']')[0]))
        # Prime Meridian
        pm_parts = geocs_wkt.split('PRIMEM')[1].split('UNIT')[0]
        prime_meridian = PrimeMeridian(
            name=pm_parts.split('"')[1],
            longitude=float(pm_parts.split(',')[1].split(']')[0]))
        # Unit
        unit = geocs_wkt.split('UNIT')[1].split('"')[1]
        if unit.lower() == "metre":  # Must match for EML
            unit = "meter"
        elif unit == "Degree":
            unit = "degree"

        return GeoCS(name, datum, spheroid, prime_meridian, unit)

    # ................................
    @staticmethod
    def translate_points(points, src_wkt=None, src_epsg=None, dst_wkt=None,
                         dst_epsg=None):
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

        trans_points = []

        trans = CoordinateTransformation(src_spref, dst_spref)
        for point in points:
            x_coord, y_coord, _ = trans.TransformPoint(point[0], point[1])
            trans_points.append((x_coord, y_coord))
        trans = None
        return trans_points


# .............................................................................
class LmHTTPError(LMError):
    """Error class for HTTP errors.

    Notes:
        Wrapper method for LMError to add http status code

        These errors are caught by the web server and then transformed so
        that the user knows what went wrong (usually in the case of 4xx
        errors).  The code should be one of the standard status response
        codes specified at:
            http://www.w3.org/Protocols/rfc2616/rfc2616-sec10.html

        If the proper status code is unknown (common for general errors), use
        500.  It is for internal server error.

        Status codes are found in LmCommon.common.lmconstants.HTTPStatus
    """

    def __init__(self, code, *args, msg=None, line_num=None, do_trace=False,
                 **kwargs):
        """Constructor for the LmHTTPError class

        Args:
            code: HTTP error code of this error.
            msg: A message to return in the headers
        Note:
            HTTPException does not have a code
        """
        LMError.__init__(
            self, *args, code=code, msg=msg, line_num=line_num,
            do_trace=do_trace)
        self.code = code
        self.msg = msg
        self.args = self.args + (self.code, self.msg)


# .............................................................................
class LMMessage(LMObject):
    """Class for communication, by email, text, etc"""

    def __init__(self, body, to_addresses, from_address=SMTP_SENDER,
                 subject=None):
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
