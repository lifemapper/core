"""
@summary: This module provides a user upload service for specific data types
@author: CJ Grady
@version: 2.0
@status: alpha
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
@todo: Make much more robust.  This is a minimum to get something working and 
          discover limitations
"""
import cherrypy
import os
import zipfile

from LmCommon.common.lmconstants import HTTPStatus
from LmCommon.common.readyfile import readyFilename
from LmServer.common.lmconstants import ARCHIVE_PATH, ENV_DATA_PATH
from LmWebServer.common.lmconstants import HTTPMethod
from LmWebServer.services.api.v2.base import LmService
from LmWebServer.services.common.accessControl import checkUserPermission
from LmWebServer.services.cpTools.lmFormat import lmFormatter

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
   def POST(self, fileName=None, uploadType=None):
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
            return self._upload_occurrence_data(fileName)
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
      return os.path.join(ARCHIVE_PATH, self.getUserId())
   
   # ................................
   def _upload_biogeo(self, bioGeoFilename):
      """
      @summary: Write the biogeographic hypotheses to the user's workspace
      @param bioGeoFilename: The name of the biogeographic hypotheses package
      @todo: Sanity checking
      """
      outDir = os.path.join(self._get_user_dir(), bioGeoFilename)
      
      with zipfile.ZipFile(cherrypy.request.body, allowZip64=True) as zipF:
         for zfname in zipF.namelist():
            _, ext = os.path.splitext(zfname)
            outFn = os.path.join(outDir, '{}{}'.format(bioGeoFilename, ext))
            readyFilename(outFn)
            if os.path.exists(outFn):
               raise cherrypy.HTTPError(HTTPStatus.CONFLICT, 
                                '{}{} exists'.format(bioGeoFilename, ext))
            else:
               zipF.extract(zfname, outFn)
      
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
   def _upload_occurrence_data(self, occurrenceFilename):
      """
      @summary: Write the occurrence data to the user's workspace
      @param occurrenceFilename: The name of the occurrence data
      @todo: Sanity checking
      """
      with zipfile.ZipFile(cherrypy.request.body, allowZip64=True) as zipF:
         for zfname in zipF.namelist():
            _, ext = os.path.splitext(zfname)
            outFn = os.path.join(self._get_user_dir(), 
                                 '{}{}'.format(occurrenceFilename, ext))
            readyFilename(outFn)
            if os.path.exists(outFn):
               raise cherrypy.HTTPError(HTTPStatus.CONFLICT, 
                                '{}{} exists'.format(occurrenceFilename, ext))
            else:
               zipF.extract(zfname, outFn)
      
      return {
         'package_name' : occurrenceFilename,
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
         
