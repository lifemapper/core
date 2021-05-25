"""This function ensures that query parameters are case-insensitive

This dispatcher casts query parameters appropriately so that they can be used
    directly by functions down the line.  We handle them all here to prevent
    code redundancy.
"""
import cherrypy

from LmWebServer.common.lmconstants import (
    QP_NAME_KEY, QP_PROCESS_KEY, QUERY_PARAMETERS)


# .............................................................................
def cast_parameters():
    """Casts the provided parameters to match what we expect

    Cast the provided parameters and change the names to match what we expect.
    This allows query parameter names to be case-insensitive and of the type we
    expect for processing.
    """
    new_parameters = {}
    in_params = cherrypy.request.params

    for key in in_params:
        # Convert the key to lower case and remove any underscores
        mod_key = key.replace('_', '').lower()
        if mod_key in QUERY_PARAMETERS:
            query_param = QUERY_PARAMETERS[mod_key]
            if QP_PROCESS_KEY in query_param:
                # If we have a processing instruction, do it
                new_parameters[query_param[QP_NAME_KEY]
                               ] = query_param[QP_PROCESS_KEY](in_params[key])
            else:
                # If not, just set to what was passed in but for new parameter
                #    name
                new_parameters[query_param[QP_NAME_KEY]] = in_params[key]

    # Set the request parameters to the new values
    cherrypy.request.params = new_parameters
