#! /usr/bin/env python
# -*- coding: utf-8 -*-
"""This module provides REST services for matrices
"""
import cherrypy

from LmCommon.common.lmconstants import HTTPStatus, JobStatus
from LmWebServer.common.lmconstants import HTTPMethod
from LmWebServer.services.api.v2.base import LmService
from LmWebServer.services.api.v2.matrixColumn import MatrixColumnService
from LmWebServer.services.common.accessControl import checkUserPermission
from LmWebServer.services.cpTools.lmFormat import lmFormatter

# .............................................................................
@cherrypy.expose
@cherrypy.popargs('pathMatrixId')
class MatrixService(LmService):
    """This class is reponsible for matrix services.
    """
    column = MatrixColumnService()
    
    # ................................
    def DELETE(self, pathGridSetId, pathMatrixId):
        """
        @summary: Attempts to delete a matrix
        @param pathMatrixId: The id of the matrix to delete
        """
        mtx = self.scribe.getMatrix(mtxId=pathMatrixId)

        if mtx is None:
            raise cherrypy.HTTPError(
                HTTPStatus.NOT_FOUND, 'Matrix not found')
        
        # If allowed to, delete
        if checkUserPermission(self.getUserId(), mtx, HTTPMethod.DELETE):
            success = self.scribe.deleteObject(mtx)
            if success:
                cherrypy.response.status = HTTPStatus.NO_CONTENT
                return 
            else:
                # TODO: How can this happen?  Make sure we catch those cases and 
                #             respond appropriately.  We don't want 500 errors
                raise cherrypy.HTTPError(
                    HTTPStatus.INTERNAL_SERVER_ERROR,
                    'Failed to delete matrix')
        else:
            raise cherrypy.HTTPError(
                HTTPStatus.FORBIDDEN,
                'User does not have permission to delete this matrix')

    # ................................
    @lmFormatter
    def GET(self, pathGridSetId, pathMatrixId=None, afterTime=None,
            altPredCode=None, beforeTime=None, dateCode=None, epsgCode=None,
            gcmCode=None, keyword=None, limit=100, matrixType=None, offset=0,
            urlUser=None, status=None, **params):
        """
        @summary: Performs a GET request.  If a matrix id is provided,
                         attempt to return that item.  If not, return a list of 
                         matrices that match the provided parameters
        """
        if pathMatrixId is None:
            return self._listMatrices(
                self.getUserId(urlUser=urlUser), gridSetId=pathGridSetId,
                afterTime=afterTime, altPredCode=altPredCode,
                beforeTime=beforeTime, dateCode=dateCode, epsgCode=epsgCode,
                gcmCode=gcmCode, keyword=keyword, limit=limit,
                matrixType=matrixType, offset=offset, status=status)
        elif pathMatrixId.lower() == 'count':
            return self._countMatrices(
                self.getUserId(urlUser=urlUser), gridSetId=pathGridSetId,
                afterTime=afterTime, altPredCode=altPredCode,
                beforeTime=beforeTime, dateCode=dateCode, epsgCode=epsgCode,
                gcmCode=gcmCode, keyword=keyword, matrixType=matrixType,
                status=status)
        else:
            return self._getMatrix(pathGridSetId, pathMatrixId)
        
    # ................................
    def _countMatrices(self, userId, gridSetId, afterTime=None,
                       altPredCode=None, beforeTime=None, dateCode=None,
                       epsgCode=None, gcmCode=None, keyword=None,
                       matrixType=None, status=None):
        """
        @summary: Count matrix objects matching the specified criteria
        @param userId: The user to count matrices for.  Note that this may not 
                                be the same user logged into the system
        @param afterTime: (optional) Return matrices modified after this time 
                                    (Modified Julian Day)
        @param beforeTime: (optional) Return matrices modified before this time 
                                     (Modified Julian Day)
        @param epsgCode: (optional) Return matrices with this EPSG code
        """
        afterStatus = None
        beforeStatus = None

        # Process status parameter
        if status:
            if status < JobStatus.COMPLETE:
                beforeStatus = JobStatus.COMPLETE - 1
            elif status == JobStatus.COMPLETE:
                beforeStatus = JobStatus.COMPLETE + 1
                afterStatus = JobStatus.COMPLETE - 1
            else:
                afterStatus = status - 1

        mtxCount = self.scribe.countMatrices(
            userId=userId, matrixType=matrixType, gcmCode=gcmCode,
            altpredCode=altPredCode, dateCode=dateCode, keyword=keyword,
            gridsetId=gridSetId, afterTime=afterTime, beforeTime=beforeTime,
            epsg=epsgCode, afterStatus=afterStatus, beforeStatus=beforeStatus)
        # Format return
        # Set headers
        return {"count" : mtxCount}

    # ................................
    def _getMatrix(self, pathGridSetId, pathMatrixId):
        """
        @summary: Attempt to get a matrix
        """
        mtx = self.scribe.getMatrix(
            gridsetId=pathGridSetId, mtxId=pathMatrixId)
        if mtx is None:
            raise cherrypy.HTTPError(
                HTTPStatus.NOT_FOUND,
                'matrix {} was not found'.format(pathMatrixId))
        if checkUserPermission(self.getUserId(), mtx, HTTPMethod.GET):
            return mtx
        else:
            raise cherrypy.HTTPError(
                HTTPStatus.FORBIDDEN,
                'User {} does not have permission to access matrix {}'.format(
                    self.getUserId(), pathMatrixId))
    
    # ................................
    def _listMatrices(self, userId, gridSetId, afterTime=None,
                      altPredCode=None, beforeTime=None, dateCode=None,
                      epsgCode=None, gcmCode=None, keyword=None, limit=100,
                      matrixType=None, offset=0, status=None):
        """
        @summary: Count matrix objects matching the specified criteria
        @param userId: The user to count matrices for.  Note that this may not 
                                be the same user logged into the system
        @param afterTime: (optional) Return matrices modified after this time 
                                    (Modified Julian Day)
        @param beforeTime: (optional) Return matrices modified before this time 
                                     (Modified Julian Day)
        @param epsgCode: (optional) Return matrices with this EPSG code
        @param limit: (optional) Return this number of matrices, at most
        @param offset: (optional) Offset the returned matrices by this number
        """
        afterStatus = None
        beforeStatus = None

        # Process status parameter
        if status:
            if status < JobStatus.COMPLETE:
                beforeStatus = JobStatus.COMPLETE - 1
            elif status == JobStatus.COMPLETE:
                beforeStatus = JobStatus.COMPLETE + 1
                afterStatus = JobStatus.COMPLETE - 1
            else:
                afterStatus = status - 1

        mtxAtoms = self.scribe.listMatrices(
            offset, limit, userId=userId, matrixType=matrixType,
            gcmCode=gcmCode, altpredCode=altPredCode, dateCode=dateCode,
            keyword=keyword, gridsetId=gridSetId, afterTime=afterTime,
            beforeTime=beforeTime, epsg=epsgCode, afterStatus=afterStatus,
            beforeStatus=beforeStatus)
        # Format return
        # Set headers
        return mtxAtoms
