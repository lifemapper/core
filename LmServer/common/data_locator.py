"""Module containing tool for locating data
"""
import os

from LmBackend.common.lmobj import LMError, LMObject
from LmCommon.common.lmconstants import LMFormat, DEFAULT_GLOBAL_EXTENT
from LmServer.base.lmobj import LMSpatialObject
from LmServer.common.lmconstants import (
    API_URL, ARCHIVE_PATH, DEFAULT_SRS, DEFAULT_WCS_FORMAT, DEFAULT_WMS_FORMAT,
    FileFix, GENERIC_LAYER_NAME_PREFIX, LMFileType, LOG_PATH, MAP_DIR, MAP_KEY,
    MAP_TEMPLATE, MapPrefix, MODEL_DEPTH, NAME_SEPARATOR, OCC_NAME_PREFIX,
    OGC_SERVICE_URL, PRJ_PREFIX, RAD_EXPERIMENT_DIR_PREFIX, USER_LAYER_DIR,
    USER_MAKEFLOW_DIR, USER_TEMP_DIR, WCS_LAYER_KEY, WEB_DIR, WMS_LAYER_KEY)
from LmServer.common.localconstants import APP_PATH, PUBLIC_USER


# .............................................................................
class EarlJr(LMObject):
    """Object to construct and parse filenames and URLs.
    """

    # ................................
    def __init__(self, scribe=None):
        """Constructor"""
        self.ogc_url = OGC_SERVICE_URL
        self._scribe = scribe

    # ................................
    @staticmethod
    def create_start_walken_filename(user, archive_name):
        """Create the start walken filename"""
        name = '{}_{}'.format(user, archive_name)
        fname = os.path.join(LOG_PATH, 'start.{}.txt'.format(name))
        return fname

    # ................................
    @staticmethod
    def create_layer_name(occ_set_id=None, proj_id=None, lyr_id=None):
        """Return the base filename of an Archive data layer

        Args:
            occ_set_id: Id of the OccurrenceSet data.
            proj_id: Id of the Projection layer.

        Note:
            Check proj_id first, because occ_set_id may be sent in either case
        """
        if proj_id is not None:
            base_name = NAME_SEPARATOR.join([PRJ_PREFIX, str(proj_id)])
        elif occ_set_id is not None:
            base_name = NAME_SEPARATOR.join([OCC_NAME_PREFIX, str(occ_set_id)])
        elif lyr_id is not None:
            base_name = NAME_SEPARATOR.join(
                [GENERIC_LAYER_NAME_PREFIX, str(lyr_id)])
        else:
            raise LMError('Must supply OccsetId or ProjId for layer name')
        return base_name

    # ................................
    @staticmethod
    def create_sdm_project_title(user_id, taxa_name, alg_code, mdl_scen_code,
                                 prj_scen_code):
        """Create a title for an SDM projection"""
        return '{} modeled with {} projected onto {}'.format(
            taxa_name, alg_code, prj_scen_code)

    # ................................
    @staticmethod
    def _parse_sdm_id(id_str, group_size=3):
        """Return a list of 3-digit strings from a multi-digit string."""
        num_zeros = group_size - len(id_str) % group_size
        if num_zeros >= group_size:
            num_zeros -= group_size
        id_str = (num_zeros * '0') + id_str
        return [
            id_str[i:i+group_size] for i in range(0, len(id_str), group_size)]

    # ................................
    def create_data_path(self, usr, file_type, occ_set_id=None, epsg=None,
                         gridset_id=None):
        """Create a data path for the provided inputs

        Note:
            - Returns path without trailing /
            - /ARCHIVE_PATH/user_id
                contains config files, trees, attributes ...
              /ARCHIVE_PATH/user_id/makeflow
                contains MF docs
              /ARCHIVE_PATH/user_id/xxx/xxx/xxx/xxx
                contains experiment data common to occurrenceId xxxxxxxxxxxx
              /ARCHIVE_PATH/user_id/MAP_DIR
                contains maps
              /ARCHIVE_PATH/user_id/<epsg>/USER_LAYER_DIR
                contains user layers common to epsg

        Todo:
            - Add gridset-related directory
                /ARCHIVE_PATH/user_id/<epsg>/RAD_<xxx>
                    contains computed data for gridset xxx
        """
        if usr is None:
            raise LMError('createDataPath requires user_id')
        pth = os.path.join(ARCHIVE_PATH, usr)

        # General user documents go directly in user directory
        if LMFileType.is_user_space(file_type):
            pass

        elif file_type == LMFileType.TEMP_USER_DATA:
            pth = os.path.join(pth, USER_TEMP_DIR)

        elif file_type == LMFileType.MF_DOCUMENT:
            pth = os.path.join(pth, USER_MAKEFLOW_DIR)

        # OccurrenceSet path overrides general map path for SDM maps
        elif LMFileType.is_sdm(file_type):
            if occ_set_id is not None:
                dir_parts = self._parse_sdm_id(str(occ_set_id))
                for _ in range(MODEL_DEPTH - len(dir_parts)):
                    dir_parts.insert(0, '000')
                pth = os.path.join(pth, *dir_parts)
            else:
                raise LMError('Missing occ_layer_id for SDM filepath')

        # All non-SDM Maps go under user map dir
        elif LMFileType.is_map(file_type):
            pth = os.path.join(pth, MAP_DIR)

        else:
            if not (LMFileType.is_user_layer(file_type) or
                    LMFileType.is_rad(file_type)):
                raise LMError('Unknown filetype {}'.format(file_type))

            # Rest, User Layer and RAD data, are separated by epsg
            if epsg is not None:
                pth_parts = [pth, str(epsg)]
                # multi-purpose layers
                if LMFileType.is_user_layer(file_type):
                    pth_parts.append(USER_LAYER_DIR)
                # RAD gridsets
                elif LMFileType.is_rad(file_type):
                    if gridset_id is not None:
                        pth_parts.append(
                            RAD_EXPERIMENT_DIR_PREFIX + str(gridset_id))
                    else:
                        raise LMError('Missing gridsetId {}'.format(file_type))
                pth = os.path.join(*pth_parts)

        return pth

    # ................................
    @staticmethod
    def get_top_level_user_sdm_paths(usr):
        """Get the top level user sdm path

        Note:
            - /ARCHIVE_PATH/user_id/
                contains config files, trees, attributes ...
              /ARCHIVE_PATH/user_id/xxx/xxx/xxx/xxx
                contains experiment data common to occurrenceId xxxxxxxxxxxx
        """
        sdm_paths = []
        if usr is None:
            raise LMError('getTopLevelUserSDMPaths requires user_id')
        pth = os.path.join(ARCHIVE_PATH, usr)

        contents = os.listdir(pth)
        for name in contents:
            full_dir = os.path.join(pth, name)
            # SDM dirs are 3-digit integers, EPSG codes are a 4-digit integer
            if len(name) == 3 and os.path.isdir(full_dir):
                try:
                    int(name)
                except ValueError:
                    pass
                else:
                    sdm_paths.append(full_dir)
        return sdm_paths

    # ................................
    def create_other_layer_filename(self, usr, epsg, lyr_name, ext):
        """Return the base filename of a Non-SDM-experiment Layer file

        Args:
            usr: Id of the User.
            epsg: EPSG code of the layer file
            occ_set_id: OccurrenceSet Id if this is for an OccurrenceSet layer.
            lyr_name: Name of the layer.
            ext: File extentsion of this layer.
        """
        pth = self.create_data_path(usr, LMFileType.USER_LAYER, epsg=epsg)
        return os.path.join(pth, lyr_name + ext)

    # ................................
    @staticmethod
    def create_basename(f_type, obj_code=None, lyr_name=None, usr=None,
                        epsg=None):
        """Return the base filename for given filetype and parameters

        Args:
            f_type: LmServer.common.lmconstants.LMFileType
            obj_code: Object database Id or unique code for non-db items, for
                maps:
                    - SDM_MAP occurrencesetId
                    - RAD_MAP gridsetId
                    - SCENARIO_MAP scenarioCode
            lyr_name: Layer name
            usr: User database Id
            epsg: File or object EPSG code
        """
        base_name = None

        name_parts = []
        # Prefix
        if FileFix.PREFIX[f_type] is not None:
            name_parts.append(FileFix.PREFIX[f_type])

        # MAPs
        if f_type in (LMFileType.SCENARIO_MAP, LMFileType.RAD_MAP):
            name_parts.append(usr)
        # User Maps for unconnected user layers (not SCENARIO_MAP or SDM_MAP)
        elif f_type == LMFileType.OTHER_MAP:
            name_parts.extend([usr, epsg])

        # User layers
        if LMFileType.is_user_layer(f_type):
            name_parts.append(lyr_name)
        # All non-UserLayer files add objCode
        elif obj_code:
            name_parts.append(obj_code)

        else:
            return None

        file_parts = [str(p) for p in name_parts if p is not None]
        try:
            base_name = NAME_SEPARATOR.join(file_parts)
        except Exception as err:
            raise LMError(
                'Bad type {} or parameters; ({})'.format(
                    str(f_type), str(err)))
        return base_name

    # ................................
    def create_filename(self, f_type, occ_set_id=None, gridset_id=None,
                        obj_code=None, lyr_name=None, usr=None, epsg=None,
                        pth=None):
        """Return the absolute filename for given filetype and parameters

        Args:
            f_type: LmServer.common.lmconstants.LMFileType
            occ_set_id: SDM OccurrenceLayer database Id, used for path
            gridset_id: RAD Gridset database Id, used for path
            obj_code: ScenarioCode or database Id for primary object contained
                or organizing the file contents
            pth: File storage path, overrides calculated path
        """
        if occ_set_id is not None and obj_code is None:
            obj_code = occ_set_id
        base_name = self.create_basename(
            f_type, obj_code=obj_code, lyr_name=lyr_name, usr=usr, epsg=epsg)
        if base_name is None:
            filename = None
        else:
            if pth is None:
                pth = self.create_data_path(
                    usr, f_type, gridset_id=gridset_id, epsg=epsg,
                    occ_set_id=occ_set_id)
                filename = os.path.join(
                    pth, base_name + FileFix.EXTENSION[f_type])
        return filename
        
    # ................................
    def get_map_filename_from_map_name(self, map_name, user_id):
        """Get the map filename from the map name

        Args:
            map_name: name for the map, used in Mapserver mapfile
            user_id: needed for SDM or RAD maps - it cannot be retrieved from the map name

        Returns:
            absolute pathname for the mapfile
        """
        pth = self._create_static_map_path()
        if map_name == MAP_TEMPLATE:
            usr = None
        else:
            (file_type, _, occ_set_id, gridset_id, usr, ancillary, _
             ) = self._parse_map_name(map_name)
            if usr is None:
                usr = user_id

            if not ancillary:
                pth = self.create_data_path(
                    usr, file_type, occ_set_id=occ_set_id,
                    gridset_id=gridset_id)

        if not map_name.endswith(LMFormat.MAP.ext):
            map_name = map_name + LMFormat.MAP.ext
        return os.path.join(pth, map_name)

    # ................................
    @staticmethod
    def get_map_template_filename():
        pth = EarlJr._create_static_map_path()
        map_template_filename = os.path.join(pth, MAP_TEMPLATE + LMFormat.MAP.ext)
        return map_template_filename

    # ................................
    def construct_lm_data_url(self, service_type, object_id, interface,
                              parent_metadata_url=None):
        """Return the REST service url for data in the Lifemapper archive

        Args:
            service_type: LM service for this service, i.e. 'bucket' or 'model'
            object_id: The unique database id for requested object
            parent_metadata_url: The nested structure of this object's parent
                objects.  The nested structure will begin with a '/', and take
                a form like: /{parent_class}/{parent id}/{class}/{id}
        """
        postfix = self._create_web_service_postfix(
            service_type, object_id, parent_metadata_url=parent_metadata_url,
            interface=interface)
        url = '/'.join((API_URL, postfix))
        return url

    # ................................
    def construct_lm_metadata_url(self, service_type, object_id,
                                  parent_metadata_url=None):
        """Return the REST service url for data in the Lifemapper archive

        Args:
            service_type: LM service for this service, i.e. 'bucket' or 'model'
            object_id: The unique database id for requested object
            parent_metadata_url: The nested structure of this object's parent
                objects.  The nested structure will begin with a '/', and take
                a form like: /{parent_class}/{parent id}/{class}/{id}

        Returns:
            a URL for the service object REST API
        """
        postfix = self._create_web_service_postfix(
            service_type, object_id, parent_metadata_url=parent_metadata_url)
        url = '/'.join((API_URL, postfix))
        return url

    # ................................
    @staticmethod
    def _create_web_service_postfix(service_type, object_id,
                                    parent_metadata_url=None, interface=None):
        """Return the relative REST service url without leading ROOT

        Args:
            service_type: The Lifemapper service type
            object_id: The unique database identifier for the requested object.
            parent_metadata_url: The nested structure of this object's parent
                objects.
        """
        parts = [service_type, str(object_id)]
        if parent_metadata_url is not None:
            if not parent_metadata_url.startswith(API_URL):
                raise LMError(
                    'Parent URL {} does not start with local prefix {}'.format(
                        parent_metadata_url, API_URL))

            relative_prefix = parent_metadata_url[len(API_URL):]
            parts.insert(0, relative_prefix)
        if interface is not None:
            parts.append(interface)
        return '/'.join(parts)

    # ................................
    @staticmethod
    def _get_ows_params(map_prefix, ows_layer_key, bbox):
        params = []
        if not bbox:
            bbox = DEFAULT_GLOBAL_EXTENT
        bbstr = LMSpatialObject.get_extent_string(bbox, separator=',')
        params.append(('bbox', bbstr))
        map_name = lyr_name = None
        svc_url_rest = map_prefix.split('?')
        svc_url = svc_url_rest[0]
        if len(svc_url_rest) == 2:
            pairs = svc_url_rest[1].split('&')
            for key_val in pairs:
                k, val = key_val.split('=')
                k = k.lower()
                if k == MAP_KEY:
                    map_name = val
                elif k == WMS_LAYER_KEY:
                    lyr_name = val
                elif k == WCS_LAYER_KEY:
                    lyr_name = val
            if map_name is not None:
                params.append(('map', map_name))
            if lyr_name is not None:
                params.append((ows_layer_key, lyr_name))
        return svc_url, params

    # ................................
    @staticmethod
    def _get_GET_query(url_prefix, param_tpls):
        """Method to construct an HTTP GET query

        Method to construct a GET query from a URL endpoint and a list of
        of key-value tuples. URL endpoint concludes in either a '?' or a
        key/value pair (i.e. id=25)

        Note:
            Using list of tuples to ensure that the order of the parameters
                is always the same so we can string compare GET Queries
        """
        kv_sep = '&'
        params_sep = '?'

        pairs = []
        for key, val in param_tpls:
            if isinstance(val, str):
                val = val.replace(' ', '%20')
            pairs.append('{}={}'.format(key, val))

        # Don't end in key/value pair separator
        if url_prefix.endswith(params_sep) or url_prefix.endswith('&amp;'):
            raise LMError(
                'Improperly formatted URL prefix {}'.format(url_prefix))
        # If url/key-value-pair separator isn't present, append it
        if url_prefix.find(params_sep) == -1:
            url_prefix = url_prefix + '?'
        # > one key/value pair on the urlprefix, add separator before more
        #    pairs
        elif not url_prefix.endswith('?') and pairs:
            url_prefix = url_prefix + kv_sep

        return url_prefix + kv_sep.join(pairs)

    # ................................
    def construct_lm_map_request(self, map_prefix, width, height, bbox,
                                 color=None, srs=DEFAULT_SRS,
                                 format_=DEFAULT_WMS_FORMAT):
        """Return a GET query for the Lifemapper WMS GetMap request

        Args:
            map_prefix: Lifemapper layer metadata_url with 'ogc' format
            width: requested width for resulting image
            height: requested height for resulting image
            bbox: tuple in the form (minx, miny, maxx, maxy) delineating the
                geographic limits of the query.
            color: (optional) color in hex format RRGGBB or predefined palette
                name. Color is applied only to Occurrences or Projection. Valid
                palette names: 'gray', 'red', 'green', 'blue', 'bluered',
               'bluegreen', 'greenred'.
            srs: (optional) string indicating Spatial Reference System, default
                is 'epsg:4326'
            format_: (optional) image file format, default is 'image/png'
        """
        params = [
            ('request', 'GetMap'), ('service', 'WMS'), ('version', '1.1.0'),
            ('srs', srs), ('format', format_), ('width', width),
            ('height', height), ('styles', '')]
        url, more_params = self._get_ows_params(map_prefix, 'layers', bbox)
        params.extend(more_params)
        if color is not None:
            params.append(('color', color))
        return self._get_GET_query(url, params)

    # ................................
    def construct_lm_raster_request(self, map_prefix, bbox, resolution=1,
                                    format_=DEFAULT_WCS_FORMAT,
                                    crs=DEFAULT_SRS):
        """Return a GET query for the Lifemapper WCS GetCoverage request

        Args:
            map_prefix: Lifemapper layer metadata_url with 'ogc' format
            bbox: tuple delineating the geographic limits of the query.
            resolution: (optional) spatial resolution along the x and y axes of
                the Coordinate Reference System (CRS). The values are given in
                the units appropriate to each axis of the CRS.  Default is 1.
            format_: raster format for query output, default=image/tiff
            crs: (optional) string indicating Coordinate Reference System,
                default is 'epsg:4326'
        """
        params = [
            ('request', 'GetCoverage'), ('service', 'WCS'),
            ('version', '1.0.0'), ('crs', crs), ('format', format_),
            ('resx', resolution), ('resy', resolution)]
        url, more_params = self._get_ows_params(map_prefix, 'coverage', bbox)
        params.extend(more_params)

        return self._get_GET_query(url, params)

    # ................................
    def construct_map_prefix_new(self, url_prefix=None, map_name=None,
                                 f_type=None, obj_code=None, lyr_name=None,
                                 usr=None, epsg=None):
        """Construct a Lifemapper URL prefix

        Args:
            url_prefix: optional urlprefix
            map_name: optional mapname
            f_type: LmServer.common.lmconstants.LMFileType
            occ_set_id: SDM OccurrenceLayer database Id, used for path/filename
            obj_code: Object database Id or unique code for non-db items
            lyr_name: Layer name
            usr: User database Id

        Note:
            - Ignoring shapegrid maps for now
            - Optional layer name must be provided fully formed
        """
        if map_name is not None:
            if map_name.endswith(LMFormat.MAP.ext):
                map_name = map_name[:-1 * len(LMFormat.MAP.ext)]
        else:
            if LMFileType.is_map(f_type):
                map_name = self.create_basename(
                    f_type, obj_code=obj_code, usr=usr, epsg=epsg)
            else:
                raise LMError('Invalid LMFileType {}'.format(f_type))

        if url_prefix is None:
            url_prefix = self.ogc_url
        full_prefix = '?map={}'.format(map_name)

        if lyr_name is not None:
            full_prefix += '&layers={}'.format(lyr_name)

        return full_prefix

    # ................................
    @staticmethod
    def _create_static_map_path():
        pth = os.path.join(APP_PATH, WEB_DIR, MAP_DIR)
        return pth

    # ................................
    @staticmethod
    def _parse_data_path_parts(parts):
        occ_set_id = epsg = gridset_id = None
        is_layers = non_sdm_map = False
        usr = parts[0]
        rem = parts[1:]
        if len(rem) == 4:
            try:
                occ_set_id = int(''.join(parts))
            except ValueError:
                pass
        elif len(rem) == 1 and rem[0] == MAP_DIR:
            non_sdm_map = True
        else:
            # Everything else begins with epsgcode (returned as string)
            epsg = rem[0]
            rem = rem[1:]
            if len(rem) >= 1:
                if rem[0] == USER_LAYER_DIR:
                    is_layers = True
                elif rem[0].startswith(RAD_EXPERIMENT_DIR_PREFIX):
                    dir_name = rem[0]
                    try:
                        gridset_id = int(
                            dir_name[len(RAD_EXPERIMENT_DIR_PREFIX):])
                    except ValueError:
                        raise LMError(
                            'Invalid RAD gridset id {}'.format(dir_name))
        return usr, occ_set_id, epsg, gridset_id, is_layers, non_sdm_map

    # ................................
    def _parse_new_data_path(self, full_path):
        usr = occ_set_id = epsg = gridset_id = None
        anc_pth = self._create_static_map_path()

        if full_path.startswith(anc_pth):
            pass
        elif full_path.startswith(ARCHIVE_PATH):
            pth = full_path[len(ARCHIVE_PATH):]
            parts = pth.split(os.path.sep)
            # Remove empty string from leading path separator
            if '' in parts:
                parts.remove('')
            # Check last entry - if ends w/ trailing slash, this is a directory
            last = parts[len(parts) - 1]
            if last == '':
                parts = parts[:-1]

            (usr, occ_set_id, epsg, gridset_id, is_layers, non_sdm_map
             ) = self._parse_data_path_parts(parts)

        return usr, occ_set_id, epsg, gridset_id, is_layers, non_sdm_map

    # ................................
    def parse_map_filename(self, map_f_name):
        """Parse a map filename

        Args:
            map_f_name: absolute filename for the mapfile

        Returns:
            A tuple containing map_name, ancillary flag, user id, epsg code,
                db id of OccurrenceLayer, db id of Gridset, scenario code
        """
        full_path, fname = os.path.split(map_f_name)
        map_name, _ = os.path.splitext(fname)
        # Get path info
        (usr, _, epsg, _, _, _) = self._parse_new_data_path(full_path)
        # Get filename info
        (_, scen_code, occ_set_id, gridset_id, usr2, ancillary,
         epsg2) = self._parse_map_name(map_name)

        usr = usr2 if usr is None else None
        epsg = epsg2 if epsg is None else None

        return (
            map_name, ancillary, usr, epsg, occ_set_id, gridset_id, scen_code)

    # ................................
    def parse_map_name(self, map_name):
        scen_code = occ_set_id = usr = epsg = gridset_id = None
        # Remove extension
        if map_name.endswith(LMFormat.MAP.ext):
            map_name = map_name[:-1 * len(LMFormat.MAP.ext)]

        parts = map_name.split(NAME_SEPARATOR)

        file_type = FileFix.get_map_type_from_name(prefix=parts[0])

        # SCENARIO_MAP mapname = scen_<user>_<scencode>
        if parts[0] == MapPrefix.SCEN:
            usr = parts[1]
            scen_code = parts[2]

        elif parts[0] == MapPrefix.SDM:
            occ_set_id_str = parts[1]
            try:
                occ_set_id = int(occ_set_id_str)
            except ValueError:
                raise LMError(
                    'Improper archive data map name {}; {}'.format(
                        map_name, 'Should be {} + occrrence set id'.format(
                            MapPrefix.SDM)), do_trace=True)
        # RAD_MAP mapname = rad_<gridsetId>
        elif parts[0] == MapPrefix.RAD:
            try:
                gridset_id = int(parts[1])
            except (TypeError, ValueError) as err:
                raise LMError(
                    'Improper map name {}; should be {} + gridset id'.format(
                        map_name, MapPrefix.RAD), err, do_trace=True)

        # User maps are usr_<usr>_<epsg>
        elif parts[0] == MapPrefix.USER:
            usr = parts[1]
            try:
                epsg = int(parts[2])
            except (TypeError, ValueError):
                pass

        elif map_name.startswith(MapPrefix.ANC):
            usr = PUBLIC_USER

        else:
            raise LMError(
                'Improper map name {} - {}'.format(
                    map_name, 'requires prefix {}, {}, {}, or {}'.format(
                        MapPrefix.SCEN, MapPrefix.SDM, MapPrefix.USER,
                        MapPrefix.ANC)), do_trace=True)
        return (
            file_type, scen_code, occ_set_id, gridset_id, usr, epsg)

    # ................................
    def get_map_path_from_parts(
            self, file_type, user_id=None, occ_set_id=None, gridset_id=None):
        """Get the map path from type,  and data elements

        Args:
            map_name: name for the map, used in Mapserver mapfile

        Returns:
            absolute pathname for the mapfile
        """
        if file_type == LMFileType.ANCILLARY_MAP:
            pth = self._create_static_map_path()
        else:
            pth = self.create_data_path(
                user_id, file_type, occ_set_id=occ_set_id,
                gridset_id=gridset_id)
        return pth

    # ................................
    @staticmethod
    def _parse_map_name(map_name):
        scen_code = occ_set_id = usr = epsg = gridset_id = None
        ancillary = False
        # Remove extension
        if map_name.endswith(LMFormat.MAP.ext):
            map_name = map_name[:-1 * len(LMFormat.MAP.ext)]

        parts = map_name.split(NAME_SEPARATOR)

        file_type = FileFix.get_map_type_from_name(prefix=parts[0])

        # SCENARIO_MAP mapname = scen_<user>_<scencode>
        if parts[0] == MapPrefix.SCEN:
            usr = parts[1]
            scen_code = parts[2]

        elif parts[0] == MapPrefix.SDM:
            occ_set_id_str = parts[1]
            try:
                occ_set_id = int(occ_set_id_str)
            except ValueError:
                raise LMError(
                    'Improper archive data map name {}; {}'.format(
                        map_name, 'Should be {} + occrrence set id'.format(
                            MapPrefix.SDM)), do_trace=True)
        # RAD_MAP mapname = rad_<gridsetId>
        elif parts[0] == MapPrefix.RAD:
            try:
                gridset_id = int(parts[1])
            except (TypeError, ValueError) as err:
                raise LMError(
                    'Improper map name {}; should be {} + gridset id'.format(
                        map_name, MapPrefix.RAD), err, do_trace=True)

        # User maps are usr_<usr>_<epsg>
        elif parts[0] == MapPrefix.USER:
            usr = parts[1]
            try:
                epsg = int(parts[2])
            except (TypeError, ValueError):
                pass

        elif map_name.startswith(MapPrefix.ANC):
            ancillary = True
            usr = PUBLIC_USER

        else:
            raise LMError(
                'Improper map name {} - {}'.format(
                    map_name, 'requires prefix {}, {}, {}, or {}'.format(
                        MapPrefix.SCEN, MapPrefix.SDM, MapPrefix.USER,
                        MapPrefix.ANC)), do_trace=True)
        return (
            file_type, scen_code, occ_set_id, gridset_id, usr, ancillary, epsg)
"""
from LmBackend.common.lmobj import LMError, LMObject
from LmCommon.common.lmconstants import LMFormat, DEFAULT_GLOBAL_EXTENT
from LmServer.base.lmobj import LMSpatialObject
from LmServer.common.lmconstants import (
    API_URL, ARCHIVE_PATH, DEFAULT_SRS, DEFAULT_WCS_FORMAT, DEFAULT_WMS_FORMAT,
    FileFix, GENERIC_LAYER_NAME_PREFIX, LMFileType, LOG_PATH, MAP_DIR, MAP_KEY,
    MAP_TEMPLATE, MapPrefix, MODEL_DEPTH, NAME_SEPARATOR, OCC_NAME_PREFIX,
    OGC_SERVICE_URL, PRJ_PREFIX, RAD_EXPERIMENT_DIR_PREFIX, USER_LAYER_DIR,
    USER_MAKEFLOW_DIR, USER_TEMP_DIR, WCS_LAYER_KEY, WEB_DIR, WMS_LAYER_KEY)
from LmServer.common.localconstants import APP_PATH, PUBLIC_USER
from LmServer.common.log import ConsoleLogger

from LmServer.common.data_locator import EarlJr
from LmServer.db.borg_scribe import BorgScribe


logger = ConsoleLogger()
scribe = BorgScribe(logger)
earl_jr = EarlJr(scribe=scribe)


map_name = 'data_334027'




"""