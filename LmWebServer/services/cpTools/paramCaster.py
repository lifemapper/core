"""This function ensures that query parameters are case-insensitive

This dispatcher casts query parameters appropriately so that they can be used
    directly by functions down the line.  We handle them all here to prevent
    code redundancy.
"""

import cherrypy

from LmWebServer.common.lmconstants import (
    QP_NAME_KEY, QP_PROCESS_KEY, QUERY_PARAMETERS)

# .............................................................................
def castParameters():
    """Casts the provided parameters to match what we expect

    Cast the provided parameters and change the names to match what we expect.
    This allows query parameter names to be case-insensitive and of the type we
    expect for processing.
    """
    newParameters = {}
    inParams = cherrypy.request.params
    for key in inParams:
        # Conver the key to lower case and remove any underscores
        modKey = key.replace('_', '').lower()
        if QUERY_PARAMETERS.has_key(modKey):
            qp = QUERY_PARAMETERS[modKey]
            if qp.has_key(QP_PROCESS_KEY):
                # If we have a processing instruction, do it
                newParameters[qp[QP_NAME_KEY]] = qp[
                    QP_PROCESS_KEY](inParams[key])
            else:
                # If not, just set to what was passed in but for new parameter
                #    name
                newParameters[qp[QP_NAME_KEY]] = inParams[key]

    # Set the request parameters to the new values
    cherrypy.request.params = newParameters
