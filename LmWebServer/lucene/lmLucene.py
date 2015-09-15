"""
@summary: Lifemapper Lucene Module
@author: CJ Grady
@version: 3.0
@note: Originally adapted from test_pipe_service.py module from Dave
@license: gpl2
@copyright: Copyright (C) 2014, University of Kansas Center for Research

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
import os
from processing import connection
import socket
import subprocess
import sys
import time

from LmServer.base.lmobj import LMObject
import LmServer.common.jsonTree as lmJson
from LmServer.common.lmconstants import WEB_MODULES_PATH
from LmServer.common.localconstants import APP_PATH
from LmServer.common.log import LuceneLogger
from LmServer.db.scribe import Scribe

from LmWebServer.common.lmconstants import LUCENE_INDEX_DIR, LUCENE_PATH, \
                                 LUCENE_LISTEN_ADDRESS, LUCENE_MAX_START_TRIES
from LmWebServer.common.localconstants import LM_LIB_PATH
from LmWebServer.lucene.lucene4 import LmLucene4 as LmLucene


# Command to run the service
# 'python' on hera links to python 2.7
SERVICE_COMMAND = ('python', os.path.join(APP_PATH, WEB_MODULES_PATH, 
                                          LUCENE_PATH, 'lmLucene.py'), '!start')

log = LuceneLogger()

# ............................................................................
class LmLuceneClient(LMObject):
   """
   @summary: Client class that connects to running Lucene listener process
   """
   # ...................................
   def __init__(self):
      """
      @summary: Constructor
      """
      LMObject.__init__(self)
      self.client = None
    
   # ...................................
   def _initClient(self):
      """
      @summary: Initializes client
      """
      started = False
      numTries = 0
       
      while numTries < LUCENE_MAX_START_TRIES and self.client is None:
         try:
            self.client = connection.Client(LUCENE_LISTEN_ADDRESS)
         except socket.error:
            if not started:
               log.error(' '.join(("Listener has not been started.",
                                   "Number of tries:", str(numTries))))
               log.info("Attempting to start listener...")
               #started = startListener()
            else:
               time.sleep(0.5)
         numTries = numTries + 1
      if self.client is None:
         raise Exception("Client could not connect...")
    
   # ...................................
   def _sendCommand(self, msg):
      """
      @summary: Sends command to listener
      @param msg: A dictionary object of parameters to be processed by listener
      @return: Returns response from listener
      """
      self._initClient()
      self.client.send(msg)
      response = self.client.recv()
      self.client.close()
      return response
    
   # ...................................
   def buildIndex(self):
      """
      @summary: Sends a command to the listener to (re)build the species index
      @return: Response from listener
      """
      msg = {
               "op" : "build",
            }
      return self._sendCommand(msg)
    
   # ...................................
   def getPid(self):
      """
      @summary: Sends a command to the listener to return it's process id
      @return: The process id of the listener
      """
      msg = {
               "op" : "pid",
            }
      return self._sendCommand(msg)
    
   # ...................................
   def querySpecies(self, query, maxCount, frmt, columns):
      """
      @summary: Sends a command to the listener to query the species index
      @param query: The string of letters to query for
      @param maxCount: The maximum number of results to return 
                          (if <= 0, return all)
      @param frmt: The format to return results in. 
                        (autocomplete, json, jsonNew)
      @param columns: The number of columns to return results in
      @return: Response from listener
      """
      msg = {
               "op"      : "species",
               "query"   : query,
               "max"     : maxCount,
               "format"  : frmt
            }
      return self._sendCommand(msg)
    
   # ...................................
   def shutdown(self):
      """
      @summary: Sends a command to the listener to shut down
      @return: Response from listener
      """
      msg = {
               "op" : "quit"
            }
      return self._sendCommand(msg)
 
# .............................................................................
class LmLuceneListener(LMObject):
   """
   @summary: The LmLuceneListener class runs and waits for connections and 
                Lucene related requests
   """
   
   # ...................................
   def __init__(self, address=LUCENE_LISTEN_ADDRESS):
      """
      @summary: LmLuceneListener constructor
      @param address: THe address to listen on
      """
      self.service = connection.Listener(address=address)
      self.msgPollTimeout = 1.0
      self.methods = {
                        'build'     : self._buildIndex,
                        'pid'       : self._getPid,
                        'species'   : self._searchIndex,
                     }
      LMObject.__init__(self)
      storeDir = '/'.join((LM_LIB_PATH, LUCENE_INDEX_DIR))
      self.lmLucene = LmLucene(storeDir, log=log)
      log.debug("Lucene store directory: %s" % storeDir)
   
   # ...................................
   def handleConnection(self):
      """
      @summary: Handles a client connection and performs that request
      @return: Boolean value if the listener should quit or not
      """
      doQuit = False
      # Wait for a connection
      conn = self.service.accept()
      try:
         # Expect data within self.msgPollTimeout seconds
         hasData = conn.poll(self.msgPollTimeout)
          
         # Handle data if available, otherwise terminate connection
         if hasData:
            msg = conn.recv()
            #log.debug("Received: %s" % str(msg))
            log.debug("Received request")
            try:
               response, doQuit = self.processMessage(msg)
            except Exception, e:
               log.error("An error occurred: %s" % str(e))
               response = str(e)
               doQuit = False
            conn.send(response)
            log.debug("Sent response")
      except EOFError, e:
         log("EOFError: %s" % str(e))
      conn.close()
      return doQuit
    
   # ...................................
   def listen(self):
      """
      @summary: Listens for connections
      """
      done = False
      while not done:
         try:
            done = self.handleConnection()
            if done:
               log.debug("Exiting by quit message.")
               result = self.service.close()
               log.debug("Result of close call: %s" % str(result))
         except KeyboardInterrupt:
            done = True
            log.debug("Exiting by keyboard break.")
            result = self.service.close()
            log.debug("Result of close call: %s" % str(result))
      log.info("Closed.")
 
   # ...................................
   def processMessage(self, msg):
      """
      @summary: Processes client request
      @param msg: A dictionary of parameters to use for request
      @return: Response for request
      @return: Boolean indicating if the listener should quit
      """
      operation = msg['op'].lower()
       
      if operation == 'quit':
         msg['res'] = True
         return msg, True
       
      if self.methods.has_key(operation):
         return self.methods[operation](msg), False
       
      # Default = ping
      return msg, False
    
   # ...................................
   def _buildIndex(self, msg):
      """
      @summary: (Re)builds the species lucene index
      @param msg: The dictionary of parameters (not used, but included 
                     for consistency)
      @return: Status message regarding index build.
      """
      peruser = Scribe(LuceneLogger())
      peruser.openConnections()
      speciesList = peruser.getOccurrenceStats()
      speciesList.sort(compareTitles)
      peruser.closeConnections()

      try:
         self.lmLucene.buildIndex(speciesList)
         return "Successfully Built Index"
      except Exception, e:
         return str(e)
   
   # ...................................
   def _getPid(self, msg):
      """
      @summary: Gets the process id of the listener
      """
      return os.getpid()
   
   # ...................................
   def _searchIndex(self, msg):
      """
      @summary: Queries the Lucene index for species that match the query
      @param msg: A dictionary object of parameters to be used for query
      @return: A lit of hits converted to the desired format
      """
      pass
      query = msg["query"]
      frmt = msg["format"]
      try:
         columns = int(msg["columns"])
      except:
         columns = 1

      try:
         maxCount = int(msg["max"])
         if maxCount <= 0:
            maxCount = None
         if maxCount is None or maxCount == 0:
            maxCount = 24
      except:
         maxCount = 24
      
      results = self.lmLucene.searchIndex(query)
      
      if frmt == "json":
         ret = self._transformForJson(results, columns=columns)
      elif frmt == "newJson":
         ret = self._transformForJsonNew(results)
      else:
         ret = self._transformForAutocomplete(results, maximum=maxCount)
      return ret
   
   # ...................................
   def _transformForAutocomplete(self, results, maximum=24):
      """
      @summary: Transforms the results of a search for autocomplete
      @param results: A list of Lucene search hits
      @param maximum: (optional) Return a maximum of this number of results
      """
      if results is None or len(results) == 0:
         return "No Suggestions\t0\t0"
      else:
         if len(results) < maximum:
            maximum = len(results)
         
         return ''.join(["%s\t%s\t%s\n" % (results[x].get("species"), 
                    results[x].get("occSetId"), 
                    str(results[x].get("numOcc"))) for x in xrange(maximum)])
   
   # ...................................
   def _transformForJson(self, results, columns=1):
      """
      @summary: Transforms the results of a search into a JSON document
      @param results: A list of Lucene search hits
      @param columns: (optional) Return this many columns of results
      @deprecated: This will be replaced in the future with the new version
      """
      jTree = lmJson.JsonObject()
      if results is None:
         return "Search too broad, please enter additional characters"
      elif len(results) > 0:
         if len(results) < columns:
            columns = len(results)
         
         i = 0 if len(results) % columns == 0 else 1
         subListLength = len(results) / columns + i
      
         colsAry = jTree.addArray("columns")
         for y in xrange(columns):
            ary = colsAry.addArray("")
            for x in xrange(y*subListLength, (y+1)*subListLength):
               try:
                  o = ary.addObject("")
                  o.addValue("className", 
                          "%sSpeciesRow" % str(x % 2 == 0 and 'even' or 'odd'))
                  o.addValue("name", results[x].get("species"))
                  o.addValue("numPoints", str(results[x].get("numOcc")))
                  o.addValue("numModels", str(results[x].get("numModels")))
                  o.addValue("occurrenceSet", str(results[x].get("occSetId")))
                  o.addValue("binomial", results[x].get("binomial"))
                  o.addValue("downloadUrl", results[x].get("downloadUrl"))
               except Exception, e:
                  print str(e)
                  #pass
         jTree.addValue("colWidth", "%s%%" % str(100/columns))
         return lmJson.tostring(jTree)
      else:
         return "None of the species currently in the Lifemapper database match"
   
   # ...................................
   def _transformForJsonNew(self, results):
      """
      @summary: Transforms the results of a search into a JSON document
      @param results: A list of Lucene search hits
      @note: This method does not include the extra "column" information and 
                will replace the _transformForJson method
      """
      jTree = lmJson.JsonObject()
      if results is None:
         return "Search too broad, please enter additional characters"
      elif len(results) > 0:
         ary = jTree.addArray("hits")
         for i in results:
            try:
               o = ary.addObject("")
               o.addValue("name", i.get("species"))
               o.addValue("numPoints", str(i.get("numOcc")))
               o.addValue("numModels", str(i.get("numModels")))
               o.addValue("occurrenceSet", str(i.get("occSetId")))
               o.addValue("binomial", i.get("binomial"))
               o.addValue("downloadUrl", i.get("downloadUrl"))
            except:
               pass
         return lmJson.tostring(jTree)
      else:
         return "None of the species currently in the Lifemapper database match"
   
# .............................................................................
def compareTitles(el1, el2):
   return cmp(el1[1].lower(), el2[1].lower())

# ............................................................................
def listen():
   """
   @summary: Starts up the listener service
   """
   log.info("Starting listener")
   tries = 0
   started = False
   waitTime = 3
    
   while not started and tries < LUCENE_MAX_START_TRIES:
      tries = tries + 1
      log.info("Attempt %d to start listener..." % tries)
      try:
         service = LmLuceneListener(LUCENE_LISTEN_ADDRESS)
         started = True
         log.info("Listener started")
      except socket.error, e:
         log.error("Failed to start listener: %s" % str(e))
         log.error("Waiting %d seconds..." % waitTime)
         time.sleep(waitTime)
   if started:
      service.listen()
      log.info("Closed.")
   else:
      return
 
# ............................................................................
def startListener():
   """
   @summary: Starts a subprocess that starts the listener
   """
   try:
      child = subprocess.Popen(SERVICE_COMMAND, shell=False)
      log.info("Listener process started with pid = %s" % str(child.pid))
      return child.pid
   except Exception, e:
      log.error("Exception trying to start listener: %s" % str(e))
   return False

# ............................................................................
def usage():
   helpMsg = """\
   Usage: lmLucene.py [operation]
      Operations:
         build   - Builds the species lucene index
         help    - Prints this help message
         species - Queries species index for hits matching the query string
                   Usage: lmLucene.py species [Query String] [max returned] [format] [number of columns]
                      Query String: The string to look for in the index, if there is a space included, it must be enclosed in quotes
                      Max returned: <= 0 returns all, > 0 returns a maximum of that number
                      Format:
                         autocomplete - Format for Lifemapper yui autocomplete widgets
                         json - Returns json structure of results
                         newJson - Returns json structure of results (without extra column fluff)
                      Number of columns: The number of columns of data (applies to json format)
         start   - Starts the listener
         stop    - Stops the listener
         restart - Restarts the listener
   """
   print helpMsg
   
# ============================================================================
if __name__ == '__main__':
    
   op = ""
   qString = ""
   format = "autocomplete"
   cols = 3
   maxCount = 100
    
   if len(sys.argv) >= 2:
      op = sys.argv[1]
# ............................................
      if op == "build":
         try:
            cli = LmLuceneClient()
            resp = cli.buildIndex()
            print resp
         except Exception, e:
            log.info("Error: %s" % str(e))
# ............................................
      elif op == "help":
         usage()
# ............................................
      elif op == "pid":
         try:
            cli = LmLuceneClient()
            resp = cli.getPid()
            print resp
         except Exception, e:
            log.info("Error: %s" % str(e))
# ............................................
      elif op == "restart":
         try:
            log.info("Attempting to stop listener...")
            cli = LmLuceneClient()
            cli.shutdown()
            log.info("Listener stopped.")
            startListener()
         except Exception, e:
            log.info("Failed to stop listener: %s" % str(e))
# ............................................
      elif op == "species":
         if len(sys.argv) >= 3:
            qString = sys.argv[2]
          
            if len(sys.argv) >= 4:
               maxCount = sys.argv[3]
             
               if len(sys.argv) >= 5:
                  format = sys.argv[4]
                
                  if len(sys.argv) >= 6:
                     cols = int(sys.argv[5])
            try:
               cli = LmLuceneClient()
               resp = cli.querySpecies(qString, maxCount, format, cols)
               print resp
            except Exception, e:
               log.info(str(e))
         else:
            print "Missing arguments for \'species\' operation"
            usage()
# ............................................
      elif op == "start":
         startListener()
# ............................................
      elif op == "!start":
         listen()
         sys.exit()
# ............................................
      elif op == "stop":
         try:
            log.info("Attempting to stop listener...")
            cli = LmLuceneClient()
            cli.shutdown()
            log.info("Listener stopped.")
         except Exception, e:
            log.info("Failed to stop listener")
            log.info(str(e))
# ............................................
      else:
         print "Unknown operation: %s" % op
         usage()
   else:
      print "No operation specified"
      usage()
       
    
