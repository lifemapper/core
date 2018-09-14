"""
@summary: This module provides a user upload service for specific data types
@author: CJ Grady
@version: 2.0
@status: alpha
@license: gpl2
@copyright: Copyright (C) 2018, University of Kansas Center for Research

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
@todo: Make much more robust.  This is a minimum to get something working and 
             discover limitations
"""
import cherrypy
import os
from StringIO import StringIO
import zipfile

from LmCommon.common.lmconstants import HTTPStatus, LMFormat, DEFAULT_POST_USER
from LmCommon.common.readyfile import readyFilename
from LmServer.common.lmconstants import ARCHIVE_PATH, ENV_DATA_PATH
from LmWebServer.common.lmconstants import HTTPMethod
from LmWebServer.services.api.v2.base import LmService
from LmWebServer.services.common.accessControl import checkUserPermission
from LmWebServer.services.cpTools.lmFormat import lmFormatter
from LmServer.common.localconstants import PUBLIC_USER

BIOGEO_UPLOAD = 'biogeo'
CLIMATE_UPLOAD = 'climate'
OCCURRENCE_UPLOAD = 'occurrence'
TREE_UPLOAD = 'tree'

# .............................................................................
@cherrypy.expose
class UserUploadService(LmService):
    """
    @summary: This class is responsible for data uploads to a user space.  The
                     dispatcher is responsible for calling the correct method.
    """
    # ................................
    @lmFormatter
    def POST(self, fileName=None, uploadType=None, metadata=None, **params):
        """
        @summary: Posts a new file to the user's space
        @todo: Add parameters to available
        
        tree
         - file name
        biogeo hypotheses
         - file name
        occurrence data
        climate data
         - 
        """
        if checkUserPermission(self.getUserId(), self, HTTPMethod.POST):
            if uploadType.lower() == TREE_UPLOAD:
                return self._upload_tree(fileName)
            elif uploadType.lower() == BIOGEO_UPLOAD:
                return self._upload_biogeo(fileName)
            elif uploadType.lower() == OCCURRENCE_UPLOAD:
                return self._upload_occurrence_data(fileName, metadata)
            elif uploadType.lower() == CLIMATE_UPLOAD:
                return self._upload_climate_data(fileName)
            else:
                raise cherrypy.HTTPError(HTTPStatus.BAD_REQUEST, 
                                            'Unknown upload type: {}'.format(uploadType))
        else:
            raise cherrypy.HTTPError(HTTPStatus.FORBIDDEN, 
                                             'Only logged in users can upload here')
        
    # ................................
    def _get_user_dir(self):
        """
        @summary: Get the user's workspace directory
        @todo: Change this to use something at a lower level.  This is using the
                     same path construction as the getBoomPackage script
        """
        userId = self.getUserId()
        if userId == PUBLIC_USER:
            userId = DEFAULT_POST_USER
        return os.path.join(ARCHIVE_PATH, userId)
    
    # ................................
    def _upload_biogeo(self, bioGeoFilename):
        """
        @summary: Write the biogeographic hypotheses to the user's workspace
        @param bioGeoFilename: The name of the biogeographic hypotheses package
        @todo: Sanity checking
        """
        # Determine where to write the files
        outDir = os.path.join(self._get_user_dir(), 'hypotheses', bioGeoFilename)
        if not os.path.exists(outDir):
            os.makedirs(outDir)
            
        instr = StringIO()
        instr.write(cherrypy.request.body.read())
        instr.seek(0)
        
        # Unzip files and name provided name
        with zipfile.ZipFile(instr, allowZip64=True) as zipF:
            for zfname in zipF.namelist():
                #fn = os.path.basename(zfname)
                _, ext = os.path.splitext(zfname)
                if ext in LMFormat.SHAPE.getExtensions():
                    outFn = os.path.join(outDir, os.path.basename(zfname))
                    if os.path.exists(outFn):
                        raise cherrypy.HTTPError(HTTPStatus.CONFLICT,
                                                     '{} exists, {}'.format(outFn, zfname))
                    else:
                        #zipF.extract(zfname, outFn)
                        with zipF.open(zfname) as zf:
                            with open(outFn, 'w') as outF:
                                for line in zf:
                                    outF.write(line)
            
        return {
            'package_name' : bioGeoFilename,
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
                    raise cherrypy.HTTPError(HTTPStatus.CONFLICT, 
                                          '{}{} exists'.format(climateDataFilename, ext))
                else:
                    zipF.extract(zfname, outFn)
        
        return {
            'package_name' : climateDataFilename,
            'upload_type' : CLIMATE_UPLOAD,
            'status' : HTTPStatus.ACCEPTED
        }
            
    # ................................
    def _upload_occurrence_data(self, packageName, metadata):
        """
        @summary: Write the occurrence data to the user's workspace
        @param packageName: The name of the occurrence data
        @param metadata: A JSON document with metadata about the CSV data
        @todo: Sanity checking
        @todo: Use constants
        @todo: Case insensitive
        """
        
        csvFilename = os.path.join(self._get_user_dir(), 
                                    '{}{}'.format(packageName, LMFormat.CSV.ext))
        metaFilename = os.path.join(self._get_user_dir(), 
                             '{}{}'.format(packageName, LMFormat.METADATA.ext))
        
        # Check to see if files exist
        if os.path.exists(csvFilename):
            raise cherrypy.HTTPError(HTTPStatus.CONFLICT, 
                                      '{} exists'.format(os.path.basename(csvFilename)))
        if os.path.exists(metaFilename):
            raise cherrypy.HTTPError(HTTPStatus.CONFLICT, 
                                    '{} exists'.format(os.path.basename(metaFilename)))
        
        # Process metadata
        if metadata is None:
            raise cherrypy.HTTPError(HTTPStatus.BAD_REQUEST, 
                                    'Must provide metadata with occurrence data upload')
        else:
            meta_str = ''
            if 'field' not in metadata.keys() or 'role' not in metadata.keys():
                raise cherrypy.HTTPError(HTTPStatus.BAD_REQUEST, 
                                                  'Metadata not in expected format')
            else:
                for f in metadata['field']:
                    line = '{}, {}, {}'.format(f['key'], 
                                               f['shortName'], f['fieldType'])
                    if f['key'] == f['role']['geopoint']:
                        line += ', geopoint'
                    elif f['key'] == f['role']['groupBy']:
                        line += ', groupby'
                    elif f['key'] == f['role']['latitude']:
                        line += ', latitude'
                    elif f['key'] == f['role']['longitude']:
                        line += ', longitude'
                    elif f['key'] == f['role']['taxaName']:
                        line += ', taxaname'
                    elif f['key'] == f['role']['uniqueId']:
                        line += ', uniqueid'
                    meta_str += line
                    meta_str += '\n'
            
                with open(metaFilename, 'w') as outF:
                    outF.write(meta_str)
            
        # Process file
        instr = StringIO()
        instr.write(cherrypy.request.body.read())
        instr.seek(0)
        csv_done = False

        if zipfile.is_zipfile(instr):
            with zipfile.ZipFile(instr, allowZip64=True) as zip_f:
                for z_fname in zip_f.namelist():
                    _, ext = os.path.splitext(z_fname)
                    if ext == LMFormat.CSV.ext:
                        # TODO: We could extend here and process more than one
                        if csv_done:
                            raise cherrypy.HTTPError(HTTPStatus.BAD_REQUEST,
                                             'Must only provide one .csv file')
                        else:
                            with zip_f.open(z_fname) as zf:
                                with open(csvFilename, 'w') as outF:
                                    for line in zf:
                                        outF.write(line)
                        csv_done = True
                    
        # Return
        return {
            'package_name' : packageName,
            'upload_type' : OCCURRENCE_UPLOAD,
            'status' : HTTPStatus.ACCEPTED
        }
            
    # ................................
    def _upload_tree(self, treeFilename):
        """
        @summary: Write the tree to the user's work space
        @param treeFilename: The file name to use for writing
        @todo: Sanity checking
        @todo: Insert tree into database?  Let boom do it?
        """
        # Check to see if file already exists, fail if it does
        outTreeFilename = os.path.join(self._get_user_dir(), treeFilename)
        if not os.path.exists(outTreeFilename):
            # Make sure the user directory exists
            readyFilename(outTreeFilename)
            with open(outTreeFilename, 'w') as outF:
                for chunk in cherrypy.request.body:
                    outF.write(chunk)
        else:
            raise cherrypy.HTTPError(HTTPStatus.CONFLICT, 
                                'Tree with this name already exists in the user space')
        return {
            'file_name' : treeFilename,
            'upload_type' : TREE_UPLOAD,
            'status' : HTTPStatus.ACCEPTED
        }
            
