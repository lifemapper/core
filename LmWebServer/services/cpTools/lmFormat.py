"""This tool provides output formatting for service calls based on headers

This module provides a tool for formatting outputs of service calls based on
the accept headers of the request
"""
import cherrypy

from LmCommon.common.lmconstants import (
    CSV_INTERFACE, HTTPStatus, JSON_INTERFACE, LMFormat, SHAPEFILE_INTERFACE)

from LmServer.common.localconstants import PUBLIC_USER
from LmServer.common.lmconstants import SnippetOperations
from LmServer.common.snippet import SnippetShooter

from LmWebServer.formatters.fileFormatter import (
    csvObjectFormatter, file_formatter, gtiffObjectFormatter,
    shapefileObjectFormatter)
from LmWebServer.formatters.geoJsonFormatter import geoJsonObjectFormatter
from LmWebServer.formatters.jsonFormatter import jsonObjectFormatter
from LmWebServer.formatters.kmlFormatter import kmlObjectFormatter
from LmWebServer.formatters.packageFormatter import gridsetPackageFormatter
from LmWebServer.formatters.progress_formatter import progress_object_formatter

# .............................................................................
def lmFormatter(f):
    """Wrapper method for formatting service objects

    Use this as a decorator for methods that return objects that should be sent
    through formatting before being returned
    """
    def wrapper(*args, **kwargs):
        """Wrapper function
        """
        # Call the handler and get the object result
        handler_result = f(*args, **kwargs)
        
        accept_headers = cherrypy.request.headers.get('Accept')
        
        try:
            raw_headers = accept_headers.split(',')
            valued_accepts = []
            for h in raw_headers:
                if len(h.split(';')) > 1:
                    mime, val = h.split(';')
                    valued_accepts.append(
                        (mime.strip(), float(val.strip('q='))))
                else:
                    valued_accepts.append((h.strip(), 1.0))
        except:
            valued_accepts = [('*/*', 1.0)]
        
        sorted_accepts = sorted(
            valued_accepts, key=lambda x: x[1], reverse=True)
        
        for ah, _ in sorted_accepts:
            try:
                if ah == LMFormat.GEO_JSON.getMimeType():
                    return geoJsonObjectFormatter(handler_result)
                # If JSON or default
                if ah in [LMFormat.JSON.getMimeType(), '*/*']:
                    shootSnippets(handler_result, SnippetOperations.VIEWED, 
                                      JSON_INTERFACE)
                    return jsonObjectFormatter(handler_result)
                elif ah == LMFormat.KML.getMimeType():
                    return kmlObjectFormatter(handler_result)
                elif ah == LMFormat.GTIFF.getMimeType():
                    return gtiffObjectFormatter(handler_result)
                elif ah == LMFormat.SHAPE.getMimeType():
                    shootSnippets(
                        handler_result, SnippetOperations.DOWNLOADED,
                        SHAPEFILE_INTERFACE)
                    return shapefileObjectFormatter(handler_result)
                elif ah == LMFormat.CSV.getMimeType():
                    shootSnippets(
                        handler_result, SnippetOperations.DOWNLOADED,
                        CSV_INTERFACE)
                    return csvObjectFormatter(handler_result)
                elif ah == LMFormat.NEWICK.getMimeType():
                    raise cherrypy.HTTPError(
                        HTTPStatus.BAD_REQUEST,
                        'Newick response not enabled yet')
                    # TODO: Use dendropy to convert nexus to newick
                    return file_formatter(handler_result.getDLocation())
                elif ah == LMFormat.NEXUS.getMimeType():
                    return file_formatter(handler_result.getDLocation())
                elif ah == LMFormat.ZIP.getMimeType():
                    # TODO: use constants
                    try:
                        csvs = cherrypy.request.params.get('includeCSVs')
                    except:
                        csvs = True
                        
                    try:
                        sdms = cherrypy.request.params.get('includeSDMs')
                    except:
                        sdms = True
                    
                    return gridsetPackageFormatter(
                        handler_result, includeCSV=csvs, includeSDM=sdms)
                elif ah == LMFormat.PROGRESS.getMimeType():
                    obj_type, obj_id, detail = handler_result
                    return progress_object_formatter(
                        obj_type, obj_id, detail=detail)
            except Exception, e:
                # Ignore and try next accept header
                raise cherrypy.HTTPError(
                    HTTPStatus.INTERNAL_SERVER_ERROR,
                    'Failed: {}'.format(str(e)))
        # If we cannot find an acceptable formatter, raise HTTP error
        raise cherrypy.HTTPError(
            HTTPStatus.NOT_ACCEPTABLE, 'Could not find an acceptable format')
                
        return jsonObjectFormatter(handler_result)
    
    return wrapper

# .............................................................................
def shootSnippets(obj, operation, formatString):
    """
    @summary: Attempt to shoot snippets for downloads / viewings / etc
    """
    # Only shoot public data snippets
    try:
        if obj.getUserId() == PUBLIC_USER:
            shooter = SnippetShooter()
            shooter.addSnippets(
                obj, operation, url='{}/{}'.format(
                    obj.metadataUrl, formatString),
                who='user', agent='webService', why='request')
            shooter.shootSnippets()
    except:
        pass
        