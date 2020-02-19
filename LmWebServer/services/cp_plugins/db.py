"""This module provides hooks for CherryPy threads to DB connections

The db plugin module provides a hook for CherryPy threads so that each will
have its own database connection.

Note:
    * This may need to evolve to be a true 'CherryPy Plugin' but we will
        classify it as a plugin because it is attached to a thread and that
        can / will exist for multiple requests.  Whereas a CherryPy Tool only
        exists for one request.
    * psycopg2 is supposed to be thread safe, so this is not really necessary
          and we could use a tool, but this will limit the overall number of
          connections.
"""
import cherrypy

from LmServer.common.log import WebLogger
from LmServer.db.borg_scribe import BorgScribe


# .............................................................................
class _ScribeRetriever:
    """Provide a single public method for getting a scribe connection.

    This class provides a single public method for getting a scribe connection.
    The purpose of this class is to handle any errors that may occur with that
    connection between requests and get a fresh connection if necessary
   """

    # ..........................
    def __init__(self):
        log = WebLogger()
        self.scribe = BorgScribe(log)
        self.scribe.open_connections()

    # ..........................
    def get_scribe(self):
        """Returns a log and scribe for the request

        Todo:
            * Add checks to see if the scribe exists and has an open
                connection, if it does not, refresh and send a new one
        """
        return self.scribe


# .............................................................................
def connect_db(thread_index):
    """Sets up a database connection and logger for a thread

    Args:
        thread_index: A thread index provided by CherryPy
    """
    cherrypy.thread_data.scribeRetriever = _ScribeRetriever()


# .............................................................................
def disconnect_db(thread_index):
    """Attempt to close a database connection on thread close
    """
    scribe = cherrypy.thread_data.scribeRetriever.get_scribe()

    scribe.closeConnections()
