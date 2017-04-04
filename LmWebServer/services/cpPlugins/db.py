"""
@summary: The db plugin module provides a hook for CherryPy threads so that each
             will have its own database connection.
@note: This may need to evolve to be a true 'CherryPy Plugin' but we will 
          classify it as a plugin because it is attached to a thread and that
          can / will exist for multiple requests.  Whereas a CherryPy Tool only
          exists for one request.
@note: psycopg2 is supposed to be thread safe, so this is not really necessary
          and we could use a tool, but this will limit the overall number of
          connections.
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
"""
import cherrypy

from LmServer.common.log import LmPublicLogger
from LmServer.db.borgscribe import BorgScribe

# .............................................................................
class _ScribeRetriever(object):
   """
   @summary: This class provides a single public method for getting a scribe
                connection.  The purpose of this class is to handle any errors
                that may occur with that connection between requests and get a
                fresh connection if necessary
   """
   # ..........................
   def __init__(self):
      log = LmPublicLogger()
      self.scribe = BorgScribe(log)
      self.scribe.openConnections()
      
   # ..........................
   def getScribe(self):
      """
      @summary: This method returns a log instance and a scribe instance that 
                   may be used by a request
      @todo: Add checks to see if the scribe exists and has an open connection,
                if it does not, refresh and send a new one
      """
      return self.scribe
   
# .............................................................................
def connectDb(threadIndex):
   """
   @summary: Sets up a database connection for a thread and gives the thread a 
                logger
   @param threadIndex: A thread index provided by CherryPy
   """
   cherrypy.thread_data.scribeRetriever = _ScribeRetriever()
   
# .............................................................................
def disconnectDb(threadIndex):
   """
   @summary: Attempt to close a database connection on thread close
   """
   scribe = cherrypy.thread_data.scribeRetriever.getScribe()
   
   scribe.closeConnections()
   
