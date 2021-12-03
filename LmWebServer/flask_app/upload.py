"""This module provides a user upload service for specific data types

Todo:
    * Make much more robust.  This is a minimum to get something working and
        discover limitations
    * Use sub-services for different upload types rather than query parameter
"""
from flask import make_response, Response
from http import HTTPStatus
from io import BytesIO
import json
import os
import werkzeug.exceptions as WEXC
import zipfile

from lmpy import TreeWrapper

from LmCommon.common.lmconstants import (
    DEFAULT_POST_USER, DEFAULT_TREE_SCHEMA, LMFormat,PhyloTreeKeys)
from LmCommon.common.ready_file import ready_filename

from LmServer.common.data_locator import EarlJr
from LmServer.common.lmconstants import ENV_DATA_PATH, LMFileType
from LmServer.common.localconstants import PUBLIC_USER

from LmWebServer.common.lmconstants import HTTPMethod
from LmWebServer.common.localconstants import MAX_ANON_UPLOAD_SIZE
from LmWebServer.services.api.v2.base import LmService
from LmWebServer.services.common.access_control import check_user_permission
from LmWebServer.services.cp_tools.lm_format import lm_formatter

# TODO: Move to constants
BIOGEO_UPLOAD = 'biogeo'
CLIMATE_UPLOAD = 'climate'
OCCURRENCE_UPLOAD = 'occurrence'
TREE_UPLOAD = 'tree'


# .............................................................................
class UserUploadService(LmService):
    """This class is responsible for data uploads to a user space."""

    # ................................
    @lm_formatter
    def post_data(self, file_name=None, upload_type=None, metadata=None, indata=None, **params):
        """Posts the new file to the user's space"""
        if not check_user_permission(self.get_user_id(), self, HTTPMethod.POST):
            raise WEXC.Forbidden('Only logged in users can upload here')

        if upload_type is None:
            raise WEXC.BadRequest('Must provide upload type')
        
        if upload_type.lower() == TREE_UPLOAD:
            return self._upload_tree(file_name, indata)
        
        elif upload_type.lower() == BIOGEO_UPLOAD:
            return self._upload_biogeo(file_name, indata)
        
        elif upload_type.lower() == OCCURRENCE_UPLOAD:
            return self._upload_occurrence_data(file_name, metadata, indata)
        
        elif upload_type.lower() == CLIMATE_UPLOAD:
            return self._upload_climate_data(file_name, indata)

        else:
            raise WEXC.BadRequest('Unknown upload type: {}'.format(upload_type))


    # ................................
    def _get_user_temp_dir(self, user_id):
        """Get the user's workspace directory

        Todo:
            * Change this to use something at a lower level.  This is using
                the same path construction as the getBoomPackage script
        """
        earl = EarlJr()
        pth = earl.create_data_path(user_id, LMFileType.TMP_JSON)
        if not os.path.exists(pth):
            os.makedirs(pth)
        return pth

    # ................................
    def _upload_biogeo(self, package_filename, indata):
        """Write the biogeographic hypotheses to the user's workspace

        Args:
            package_filename (str): The name of the biogeographic hypotheses
                package
            upload_file: The uploaded data file

        Todo:
            * Sanity checking
            * More docs
        """
        # Determine where to write the files
        out_dir = os.path.join(self.get_user_dir(), package_filename)
        if not os.path.exists(out_dir):
            os.makedirs(out_dir)

        instr = BytesIO()
        instr.write(indata)
        instr.seek(0)

        valid_extensions = [LMFormat.JSON.ext]
        valid_extensions.extend(LMFormat.SHAPE.get_extensions())

        # Unzip files and name provided name
        with zipfile.ZipFile(instr, allowZip64=True) as zip_f:
            for zfname in zip_f.namelist():
                # fn = os.path.basename(zfname)
                _, ext = os.path.splitext(zfname)
                if ext in valid_extensions:
                    out_fn = os.path.join(out_dir, os.path.basename(zfname))
                    if os.path.exists(out_fn):
                        raise WEXC.Conflict('{} exists, {}'.format(out_fn, zfname))

                    # zipF.extract(zfname, outFn)
                    with zip_f.open(zfname) as zip_f2:
                        with open(out_fn, 'wb') as out_f:
                            for line in zip_f2:
                                out_f.write(line)

        outdata = {
            'package_name': package_filename,
            'upload_type': BIOGEO_UPLOAD,
            'status': HTTPStatus.ACCEPTED
        }
        return make_response(outdata, HTTPStatus.ACCEPTED)

    # ................................
    @staticmethod
    def _upload_climate_data(climate_data_filename, indata):
        """Write the climate data to the layers space

        Args:
            climate_data_filename: The name of the directory to unzip files in

        Todo:
            Sanity checking
        """
        out_dir = os.path.join(ENV_DATA_PATH, climate_data_filename)
        with zipfile.ZipFile(indata, allowZip64=True) as zip_f:
            for zf_name in zip_f.namelist():
                _, ext = os.path.splitext(zf_name)
                out_fn = os.path.join(
                    out_dir, '{}{}'.format(climate_data_filename, ext))
                ready_filename(out_fn)
                if os.path.exists(out_fn):
                    raise WEXC.Conflict('{}{} exists'.format(climate_data_filename, ext))
                zip_f.extract(zf_name, out_fn)

        outdata = {
            'package_name': climate_data_filename,
            'upload_type': CLIMATE_UPLOAD,
            'status': HTTPStatus.ACCEPTED
        }
        return make_response(outdata, HTTPStatus.ACCEPTED)

    # ................................
    def _upload_occurrence_data(self, package_name, metadata, indata):
        """Write the occurrence data to the user's workspace

        Args:
            package_name: The name of the occurrence data
            metadata: A JSON document with metadata about the CSV data

        Todo:
            Sanity checking
            Use constants
            Case insensitive
        """
        self.log.debug('In occ upload')
        # If the package name ends in .csv, strip it
        if package_name.lower().find(LMFormat.CSV.ext) > 0:
            package_name = package_name[
                :package_name.lower().find(LMFormat.CSV.ext)]
        csv_filename = os.path.join(
            self._get_user_temp_dir(), '{}{}'.format(
                package_name, LMFormat.CSV.ext))
        meta_filename = os.path.join(
            self._get_user_temp_dir(),
            '{}{}'.format(package_name, LMFormat.JSON.ext))

        # Check to see if files exist
        if os.path.exists(csv_filename):
            raise WEXC.Conflict('{} exists'.format(os.path.basename(csv_filename)))
        if os.path.exists(meta_filename):
            raise WEXC.Conflict('{} exists'.format(os.path.basename(meta_filename)))

        # Process metadata
        if metadata is None:
            raise WEXC.BadRequest('Must provide metadata with occurrence data upload')

        m_stringio = BytesIO()
        m_stringio.write(metadata.encode())
        m_stringio.seek(0)
        metadata = json.load(m_stringio)
        self.log.debug('Metadata: {}'.format(metadata))
        if 'field' not in list(
                metadata.keys()) or 'role' not in list(metadata.keys()):
            raise WEXC.BadRequest('Metadata not in expected format')

        header_row = indata.split('\n'.encode())[0]
        meta_obj = {}
        # Check for delimiter
        if 'delimiter' in list(metadata.keys()):
            delim = metadata['delimiter']
        else:
            delim = ','
        meta_obj['delimiter'] = delim
        headers = header_row.split(delim.encode())
        short_names = []

        roles = metadata['role']
        for fld in metadata['field']:
            if fld['field_type'].lower() == 'string':
                field_type = 'string'  # 4
            elif fld['field_type'].lower() == 'integer':
                field_type = 'integer'  # 0
            elif fld['field_type'].lower() == 'real':
                field_type = 'real'  # 2
            else:
                raise WEXC.BadRequest('Field type: {} is unknown'.format(fld['field_type']))
            field_idx = fld['key']

            # If short name is None or has zero-length, get from csv
            short_name = fld['short_name']
            if short_name is None or len(short_name) == 0:
                short_name = headers[int(fld['key'])].strip()
            # If short name is too long
            i = 0
            if len(short_name) > 9:
                test_name = short_name[:9] + str(i)
                while test_name in short_names:
                    i += 1
                    test_name = short_name[:9] + str(i)
                    self.log.debug(
                        'Trying test name: {}'.format(test_name))
                short_names.append(test_name)
                short_name = test_name
            field_obj = {
                'type': field_type,
                'name': short_name
            }
            if 'geopoint' in list(roles.keys()) and fld[
                    'key'] == roles['geopoint']:
                field_obj['role'] = 'geopoint'
            elif 'taxa_name' in list(roles.keys()) and fld[
                    'key'] == roles['taxa_name']:
                field_obj['role'] = 'taxaname'
            elif 'latitude' in list(roles.keys()) and fld[
                    'key'] == roles['latitude']:
                field_obj['role'] = 'latitude'
            elif 'longitude' in list(roles.keys()) and fld[
                    'key'] == roles['longitude']:
                field_obj['role'] = 'longitude'
            elif 'unique_id' in list(roles.keys()) and fld[
                    'key'] == roles['unique_id']:
                field_obj['role'] = 'uniqueid'
            elif 'group_by' in list(roles.keys()) and fld[
                    'key'] == roles['group_by']:
                field_obj['role'] = 'groupby'
            meta_obj[field_idx] = field_obj

        with open(meta_filename, 'wt') as out_f:
            json.dump(meta_obj, out_f)

        # Process file
        instr = BytesIO()
        instr.write(indata)
        instr.seek(0)
        csv_done = False

        if zipfile.is_zipfile(instr):
            with zipfile.ZipFile(instr, allowZip64=True) as zip_f:
                for z_fname in zip_f.namelist():
                    _, ext = os.path.splitext(z_fname)
                    if ext == LMFormat.CSV.ext:
                        # TODO: We could extend here and process more than one
                        if csv_done:
                            raise WEXC.BadRequest('Must only provide one .csv file')
                        # Determine if we are dealing with anonymous user
                        #    once instead of checking at every line
                        anon_user = self.get_user_id() == DEFAULT_POST_USER
                        with zip_f.open(z_fname) as z_f:
                            with open(csv_filename, 'w') as out_f:
                                num_lines = 0
                                for line in z_f:
                                    num_lines += 1
                                    if (anon_user and
                                            num_lines >= MAX_ANON_UPLOAD_SIZE):
                                        fail_to_upload = True
                                        break
                                    out_f.write(line)
                            if fail_to_upload:
                                os.remove(csv_filename)
                                raise WEXC.RequestEntityTooLarge(
                                    'Anonymous users may upload occurrence data less than {} lines'.format(
                                        MAX_ANON_UPLOAD_SIZE))
                        csv_done = True
        else:
            if self.get_user_id() == DEFAULT_POST_USER and \
                    len(indata.split('\n'.encode())) > MAX_ANON_UPLOAD_SIZE:
                raise WEXC.RequestEntityTooLarge(
                    'Anonymous users may only upload occurrence data less than {} lines'.format(
                        MAX_ANON_UPLOAD_SIZE))
            with open(csv_filename, 'w') as out_f:
                out_f.write(indata.decode())

        # Return
        outdata = {
            'package_name': package_name,
            'upload_type': OCCURRENCE_UPLOAD,
            'status': HTTPStatus.ACCEPTED
        }
        return make_response(outdata, HTTPStatus.ACCEPTED)

    # ................................
    def _upload_tree(self, tree_name, indata):
        """Write the tree to the user's work space

        Todo:
            * Sanity checking
            * Insert tree into database?  Let boom do it?
        """
        tree_base_name, _ = os.path.splitext(tree_name)
        tree_name = '{}{}'.format(tree_base_name, LMFormat.NEXUS.ext)
        # Check to see if file already exists, fail if it does
        out_tree_filename = os.path.join(self._get_user_temp_dir(), tree_name)
        if not os.path.exists(out_tree_filename):
            # Make sure the user directory exists
            ready_filename(out_tree_filename)

            for schema in ['newick', 'nexus', 'phyloxml']:
                try:
                    self.log.debug(indata.decode())
                    tree = TreeWrapper.get(data=indata.decode(), schema=schema)
                    # Add squids
                    squid_dict = {}
                    user_id = self.get_user_id()

                    if user_id == PUBLIC_USER:
                        user_id = DEFAULT_POST_USER
                    for label in tree.get_labels():
                        sno = self.scribe.get_taxon(
                            user_id=user_id, taxon_name=label)
                        if sno is not None:
                            squid_dict[label] = sno.squid
                    tree.annotate_tree_tips(PhyloTreeKeys.SQUID, squid_dict)
                    # Add internal node labels
                    tree.add_node_labels()
                    tree.write(
                        path=out_tree_filename, schema=DEFAULT_TREE_SCHEMA)
                    break
                except Exception as err:
                    # Uncomment for debugging purposes
                    # self.log.debug(err)
                    pass
        else:
            raise WEXC.Conflict('Tree with this name already exists in the user space')
        # Set HTTP status code
        outdata = {
            'file_name': tree_name,
            'upload_type': TREE_UPLOAD,
            'status': HTTPStatus.ACCEPTED
        }
        return make_response(outdata, HTTPStatus.ACCEPTED)
