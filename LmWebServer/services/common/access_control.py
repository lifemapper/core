"""This module provides access control methods for Lifemapper objects
"""
from LmCommon.common.lmconstants import DEFAULT_POST_USER
from LmServer.common.localconstants import PUBLIC_USER
from LmWebServer.common.lmconstants import HTTPMethod


# .............................................................................
def check_user_permission(session_user, obj, method):
    """Checks that the user has permission to perform the desired action

    Args:
        session_user: The user logged into Lifemapper
        obj: The object to check permission for
        method: The HTTP method used
    """
    # For now, the HTTP method requires the user_id for the object to match:
    #        GET - session user, public, or anonymous
    #        PUT - session user - can't update public or anonymous
    #        DELETE - session user - can't delete public or anonymous
    if method.upper() in [HTTPMethod.DELETE, HTTPMethod.PUT, HTTPMethod.POST]:
        valid_users = [session_user]
    elif method.upper() == HTTPMethod.GET:
        valid_users = [session_user, PUBLIC_USER, DEFAULT_POST_USER]
    else:
        raise Exception("Unknown HTTP method: {}".format(method.upper()))

    # Return boolean indicating if the object's user id is in the valid user
    #     list for the HTTP method
    return obj.get_user_id() in valid_users
