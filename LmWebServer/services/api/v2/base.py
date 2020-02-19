#! /usr/bin/env python
# -*- coding: utf-8 -*-
"""The module provides a base Lifemapper service class
"""
import cherrypy

from LmCommon.common.lmconstants import DEFAULT_POST_USER
from LmServer.common.localconstants import PUBLIC_USER
from LmServer.common.log import WebLogger
from LmServer.db.borg_scribe import BorgScribe


# .............................................................................
class LmService:
    """This is the base Lifemapper service object

    This is the base Lifemapper service object that the services can inherit
    from.  It is responsible for getting a database connection and logger that
    can be used for the service.
    """

    # ..........................
    def __init__(self):
        """Constructor

        The constructor is only responsible for getting a logger, user and a
        scribe instance for the service.  We do that here in a simple base
        class in case we decide that we need to use a different mechanism (such
        as a CherryPy Tool)
        """
        log = WebLogger()
        # self.scribe = cherrypy.thread_data.scribeRetriever.get_scribe()
        self.scribe = BorgScribe(log)
        self.scribe.open_connections()
        # self.log = cherrypy.session.log
        self.log = log

    # ..........................
    @staticmethod
    def get_user_id(url_user=None):
        """Gets the user id for the service call.

        Gets the user id for the service call.  If urlUser is provided, try
        that first.  Then try the session and finally fall back to the
        PUBLIC_USER
        """
        # Check to see if we should use url user
        if url_user is not None:
            if url_user.lower() == 'public'.lower():
                return PUBLIC_USER
            if url_user.lower() == DEFAULT_POST_USER.lower():
                return DEFAULT_POST_USER
        # Try to get the user from the session
        try:
            return cherrypy.session.user
        except Exception:
            # Fall back to PUBLIC_USER
            return PUBLIC_USER

    # ..........................
    @staticmethod
    def OPTIONS():
        """Common options request for all services (needed for CORS)
        """
        return
