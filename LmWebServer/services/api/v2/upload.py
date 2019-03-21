#! /usr/bin/env python
# -*- coding: utf-8 -*-
"""This module provides a user upload service for specific data types

Todo:
    * Make much more robust.  This is a minimum to get something working and
        discover limitations
Todo: Use sub-services for different upload types rather than query parameter
"""
import cherrypy
import dendropy
import json
import os
from StringIO import StringIO
import zipfile

from LmCommon.common.lmconstants import HTTPStatus, LMFormat, DEFAULT_POST_USER
from LmCommon.common.readyfile import readyFilename
from LmServer.common.lmconstants import ENV_DATA_PATH
from LmWebServer.common.lmconstants import HTTPMethod
from LmWebServer.services.api.v2.base import LmService
from LmWebServer.services.common.accessControl import checkUserPermission
from LmWebServer.services.cpTools.lmFormat import lmFormatter
from LmServer.common.datalocator import EarlJr
from LmServer.common.localconstants import PUBLIC_USER
from LmServer.common.lmconstants import LMFileType

# TODO: Move to constants
BIOGEO_UPLOAD = 'biogeo'
CLIMATE_UPLOAD = 'climate'
OCCURRENCE_UPLOAD = 'occurrence'
TREE_UPLOAD = 'tree'


# .............................................................................
@cherrypy.expose
class UserUploadService(LmService):
    """This class is responsible for data uploads to a user space.

    Note:
        * This dispatcher is responsible for calling the correct method.
    """
    # ................................
    @lmFormatter
    def POST(self, fileName=None, uploadType=None, metadata=None, file=None,
             **params):
        """Posts the new file to the user's space
        """
        if checkUserPermission(self.getUserId(), self, HTTPMethod.POST):
            if uploadType is None:
                raise cherrypy.HTTPError(
                    HTTPStatus.BAD_REQUEST, 'Must provide upload type')
            if uploadType.lower() == TREE_UPLOAD:
                return self._upload_tree(fileName, file)
            elif uploadType.lower() == BIOGEO_UPLOAD:
                return self._upload_biogeo(fileName, file)
            elif uploadType.lower() == OCCURRENCE_UPLOAD:
                return self._upload_occurrence_data(fileName, metadata, file)
            elif uploadType.lower() == CLIMATE_UPLOAD:
                return self._upload_climate_data(fileName)
            else:
                raise cherrypy.HTTPError(
                    HTTPStatus.BAD_REQUEST,
                    'Unknown upload type: {}'.format(uploadType))
        else:
            raise cherrypy.HTTPError(
                HTTPStatus.FORBIDDEN, 'Only logged in users can upload here')
        
    # ................................
    def _get_user_dir(self):
        """Get the user's workspace directory

        Todo:
            * Change this to use something at a lower level.  This is using
                the same path construction as the getBoomPackage script
        """
        earl = EarlJr()
        userId = self.getUserId()
        if userId == PUBLIC_USER:
            userId = DEFAULT_POST_USER
        pth = earl.createDataPath(userId, LMFileType.TMP_JSON)
        return pth
    
    # ................................
    def _upload_biogeo(self, package_filename, upload_file):
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
        outDir = os.path.join(
            self._get_user_dir(), 'hypotheses', package_filename)
        if not os.path.exists(outDir):
            os.makedirs(outDir)

        instr = StringIO()
        instr.write(upload_file.file.read())
        instr.seek(0)
        
        valid_extensions = [LMFormat.JSON.ext]
        valid_extensions.extend(LMFormat.SHAPE.getExtensions())
        
        # Unzip files and name provided name
        with zipfile.ZipFile(instr, allowZip64=True) as zipF:
            for zfname in zipF.namelist():
                #fn = os.path.basename(zfname)
                _, ext = os.path.splitext(zfname)
                if ext in valid_extensions:
                    outFn = os.path.join(outDir, os.path.basename(zfname))
                    if os.path.exists(outFn):
                        raise cherrypy.HTTPError(
                            HTTPStatus.CONFLICT,
                            '{} exists, {}'.format(outFn, zfname))
                    else:
                        #zipF.extract(zfname, outFn)
                        with zipF.open(zfname) as zf:
                            with open(outFn, 'w') as outF:
                                for line in zf:
                                    outF.write(line)
            
        return {
            'package_name' : package_filename,
            'upload_type' : BIOGEO_UPLOAD,
            'status' : HTTPStatus.ACCEPTED
        }
            
    # ................................
    def _upload_climate_data(self, climateDataFilename):
        """
        @summary: Write the climate data to the layers space
        @param climateDataFilename: The name of the directory to unzip files in
        @todo: Sanity checking
        """
        outDir = os.path.join(ENV_DATA_PATH, climateDataFilename)
        with zipfile.ZipFile(cherrypy.request.body, allowZip64=True) as zipF:
            for zfname in zipF.namelist():
                _, ext = os.path.splitext(zfname)
                outFn = os.path.join(outDir, '{}{}'.format(
                                              climateDataFilename, ext))
                readyFilename(outFn)
                if os.path.exists(outFn):
                    raise cherrypy.HTTPError(
                        HTTPStatus.CONFLICT,
                        '{}{} exists'.format(climateDataFilename, ext))
                else:
                    zipF.extract(zfname, outFn)
        
        return {
            'package_name' : climateDataFilename,
            'upload_type' : CLIMATE_UPLOAD,
            'status' : HTTPStatus.ACCEPTED
        }
            
    # ................................
    def _upload_occurrence_data(self, packageName, metadata, upload_file):
        """
        @summary: Write the occurrence data to the user's workspace
        @param packageName: The name of the occurrence data
        @param metadata: A JSON document with metadata about the CSV data
        @todo: Sanity checking
        @todo: Use constants
        @todo: Case insensitive
        """
        self.log.debug('In occ upload')
        csvFilename = os.path.join(
            self._get_user_dir(), '{}{}'.format(packageName, LMFormat.CSV.ext))
        metaFilename = os.path.join(
            self._get_user_dir(),
            '{}{}'.format(packageName, LMFormat.JSON.ext))
        
        # Check to see if files exist
        if os.path.exists(csvFilename):
            raise cherrypy.HTTPError(
                HTTPStatus.CONFLICT,
                '{} exists'.format(os.path.basename(csvFilename)))
        if os.path.exists(metaFilename):
            raise cherrypy.HTTPError(
                HTTPStatus.CONFLICT,
                '{} exists'.format(os.path.basename(metaFilename)))
        
        # Process metadata
        if metadata is None:
            raise cherrypy.HTTPError(
                HTTPStatus.BAD_REQUEST,
                'Must provide metadata with occurrence data upload')
        else:
            m_stringio = StringIO()
            m_stringio.write(metadata)
            m_stringio.seek(0)
            metadata = json.load(m_stringio)
            self.log.debug('Metadata: {}'.format(metadata))
            if 'field' not in metadata.keys() or 'role' not in metadata.keys():
                raise cherrypy.HTTPError(
                    HTTPStatus.BAD_REQUEST, 'Metadata not in expected format')
            else:
                meta_obj = {}
                roles = metadata['role']
                for f in metadata['field']:
                    if f['fieldType'].lower() == 'string':
                        field_type = 4
                    elif f['fieldType'].lower() == 'integer':
                        field_type = 0
                    elif f['fieldType'].lower() == 'real':
                        field_type = 2
                    else:
                        raise cherrypy.HTTPError(
                            HTTPStatus.BAD_REQUEST,
                            'Field type: {} is unknown'.format(f['fieldType']))
                    field_idx = f['key']
                    field_obj = {
                        'type' : field_type,
                        'name' : f['shortName']
                    }
                    if 'geopoint' in roles.keys() and f[
                            'key'] == roles['geopoint']:
                        field_obj['role'] = 'geopoint'
                    elif 'groupBy' in roles.keys() and f[
                            'key'] == roles['groupBy']:
                        field_obj['role'] = 'groupby'
                    elif 'latitude' in roles.keys() and f[
                            'key'] == roles['latitude']:
                        field_obj['role'] = 'latitude'
                    elif 'longitude' in roles.keys() and f[
                            'key'] == roles['longitude']:
                        field_obj['role'] = 'longitude'
                    elif 'taxaName' in roles.keys() and f[
                            'key'] == roles['taxaName']:
                        field_obj['role'] = 'taxaName'
                    elif 'uniqueId' in roles.keys() and f[
                            'key'] == roles['uniqueId']:
                        field_obj['role'] = 'uniqueId'
                    meta_obj[field_idx] = field_obj
            
                with open(metaFilename, 'w') as outF:
                    json.dump(meta_obj, outF)
        # Process file
        instr = StringIO()
        data = upload_file.file.read()
        #data = cherrypy.request.body.read()
        instr.write(data)
        instr.seek(0)
        csv_done = False

        if zipfile.is_zipfile(instr):
            with zipfile.ZipFile(instr, allowZip64=True) as zip_f:
                for z_fname in zip_f.namelist():
                    _, ext = os.path.splitext(z_fname)
                    if ext == LMFormat.CSV.ext:
                        # TODO: We could extend here and process more than one
                        if csv_done:
                            raise cherrypy.HTTPError(
                                HTTPStatus.BAD_REQUEST,
                                'Must only provide one .csv file')
                        else:
                            with zip_f.open(z_fname) as zf:
                                with open(csvFilename, 'w') as outF:
                                    for line in zf:
                                        outF.write(line)
                        csv_done = True
        else:
            with open(csvFilename, 'w') as out_f:
                out_f.write(data)
                    
        # Return
        return {
            'package_name' : packageName,
            'upload_type' : OCCURRENCE_UPLOAD,
            'status' : HTTPStatus.ACCEPTED
        }
            
    # ................................
    def _upload_tree(self, tree_name, upload_file):
        """Write the tree to the user's work space

        Todo:
            * Sanity checking
            * Insert tree into database?  Let boom do it?
        """
        # Check to see if file already exists, fail if it does
        out_tree_filename = os.path.join(
            self._get_user_dir(), '{}{}'.format(tree_name, LMFormat.NEXUS.ext))
        if not os.path.exists(out_tree_filename):
            # Make sure the user directory exists
            readyFilename(out_tree_filename)
            data = upload_file.file.read()
            for schema in ['newick', 'nexus', 'phyloxml']:
                try:
                    tree = dendropy.Tree.get(data=data, schema=schema)
                    with open(out_tree_filename, 'w') as out_f:
                        out_f.write(tree.as_string('nexus'))
                    break
                except Exception as e:
                    pass
        else:
            raise cherrypy.HTTPError(
                HTTPStatus.CONFLICT,
                'Tree with this name already exists in the user space')
        return {
            'file_name' : tree_name,
            'upload_type' : TREE_UPLOAD,
            'status' : HTTPStatus.ACCEPTED
        }
