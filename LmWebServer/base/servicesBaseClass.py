"""
@summary: This is a module containing the base classes for the new style 
             Lifemapper services 
@author: CJ Grady
@version: 0.2
@status: alpha
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
from collections import namedtuple

from LmCommon.common.lmAttObject import LmAttList
from LmCommon.common.lmconstants import DEFAULT_POST_USER, HTTPStatus, JobStatus
from LmCommon.common.localconstants import ARCHIVE_USER
 
from LmCommon.common.lmXml import Element, PI, QName, register_namespace, setDefaultNamespace, SubElement, tostring
from LmCommon.common.localconstants import WEBSERVICES_ROOT

from LmServer.base.lmobj import LmHTTPError, LMError
from LmServer.base.utilities import formatTimeHuman

LIST_INTERFACES = ['atom', 'html', 'json', 'list', 'xml']
META_INTERFACES = ['atom', 'html', 'json', 'xml']
OGC_INTERFACES = ['ogc', 'wcs', 'wfs', 'wms', 'wps']

WPS_NS = "http://www.opengis.net/wps/1.0.0"
OWS_NS = "http://www.opengis.net/ows/1.1"
XSI_NS = "http://www.w3.org/2001/XMLSchema-instance"

# =============================================================================
class WebServiceBaseClass(object):
   """
   @summary: Base class for all web service objects
   """
   # ............................................
   def __init__(self, method, conn, userId=ARCHIVE_USER, body=None, vpath=[], 
                         parameters={}, basePath=WEBSERVICES_ROOT, ipAddress=None):
      """
      @summary: Constructor
      @param method: The HTTP method used to call the service [string]
      @param conn: An open database connection object (peruser / scribe)
      @param userId: (optional) The user id associated with this request 
                        [string]
      @param body: (optional) The body (payload) of the HTTP message [string]
      @param vpath: (optional) The url path in list form ex 
                       ['services', 'lm2', 'sdm', 'experiments'] [list]
      @param parameters: (optional) URL parameters for the request 
                            (after the '?') [dictionary]
      """
      self.method = method.lower()
      self.user = userId
      self.body = body
      self.conn = conn
      # Need to ensure path and parameters are lower case
      self.vpath = [str(i).lower() for i in vpath]
      self.parameters = dict(
                       [(k.lower(), parameters[k]) for k in parameters.keys()])
      self.basePath = '%s/%s' % (basePath, self.vpath.pop(0) \
                                                if len(self.vpath) > 0 else "")
      self.ipAddress = ipAddress

   # ............................................
   def processTriggers(self, obj=None):
      """
      @summary: Process triggers found in url parameters, examples are sending 
                   notification emails or posting pml
      """
      pass
   
# =============================================================================
class ServiceGroup(WebServiceBaseClass):
   """
   @summary: Class to group related services
   """
   subServices = []
   # ............................................
   def doAction(self):
      """
      @summary: Performs the action dictated by the inputs
      """
      if len(self.vpath) > 0 and self.vpath[0] not in LIST_INTERFACES:
         for subServ in self.subServices:
            if self.vpath[0] in subServ["names"]:
               serv = subServ["constructor"](self.method, self.conn, 
                                             userId=self.user, body=self.body, 
                                             vpath=self.vpath, 
                                             parameters=self.parameters,
                                             basePath=self.basePath, 
                                             ipAddress=self.ipAddress)
               return serv.doAction()
      else:
         ServiceListItem = namedtuple("Service", 
                                          ['identifier', 'url', 'description'])
         return [ServiceListItem(s["constructor"].identifier, 
                                 '/'.join((self.basePath, 
                                           s["constructor"].identifier)), 
                                 s["constructor"].description) \
                 for s in self.subServices]
         # return named tuple for services
         # note potential error since importing from future
         # wrap exception and return
         #return [s["names"][0] for s in self.subServices]
      # If not found
      raise LmHTTPError(HTTPStatus.NOT_FOUND, msg="Unknown request: %s" % '/'.join(self.vpath))

# =============================================================================
class RestService(WebServiceBaseClass):
   """
   @summary: Base class for RESTful web services
   """
   identifier = None
   version = None
   summary = None
   description = None
   queryParameters = []
   subServices = []
   WebObjectConstructor = None

   # ............................................
   def _passToWebObject(self):
      """
      @summary: Passes the request to the object
      @return: Whatever the object returns
      """
      if self.WebObjectConstructor is not None:
         item = self.WebObjectConstructor(self.method, self.conn, 
                     userId=self.user, body=self.body, vpath=self.vpath, 
                     parameters=self.parameters, ipAddress=self.ipAddress)
         return item.doAction()
      else:
         raise LmHTTPError(HTTPStatus.NOT_IMPLEMENTED, 
                           msg="There is a problem with the service")
   
   # ............................................
   def doAction(self):
      """
      @summary: Performs the action dictated by the inputs
      """
      if self.method == "get":
         if len(self.vpath) == 0 or self.vpath[0] in LIST_INTERFACES:
            if self.parameters.has_key("request") and \
                  self.parameters["request"].lower() == "getcapabilities":
               return self.getCapabilities()
            else:
               return self.list()
         elif self.vpath[0] == "count":
            return self.count()
         elif self.vpath[0] == "help":
            return self.help()
         else:
            for subServ in self.subServices:
               if self.vpath[0] in subServ["names"]:
                  serv = subServ["constructor"](self.method, self.conn, 
                                                userId=self.user, 
                                             body=self.body, vpath=self.vpath, 
                                             parameters=self.parameters,
                                             basePath=self.basePath,
                                             ipAddress=self.ipAddress)
                  return serv.doAction()
            # If not in sub services, fall back to get
            return self.get()
      elif self.method.lower() in ("delete", "post", "put"):
         if len(self.vpath) == 0 or self.vpath[0] in META_INTERFACES:
            # These all call another method but then behave the same way
            if self.method.lower() == 'delete':
               func = self.delete
            elif self.method.lower() == 'post':
               func = self.post
            elif self.method.lower() == 'put':
               func = self.put
            
            obj = func()
            self.processTriggers(obj=obj)
            return obj
         else:
            for subServ in self.subServices:
               if self.vpath[0] in subServ["names"]:
                  serv = subServ["constructor"](self.method, self.conn, 
                                                userId=self.user, 
                                             body=self.body, vpath=self.vpath, 
                                             parameters=self.parameters,
                                             basePath=self.basePath, 
                                             ipAddress=self.ipAddress)
                  return serv.doAction()
            # If not in sub services, fall back to get
            return self.get()
      # If not found
      raise Exception, "Unknown request"

   # ............................................
   def count(self):
      """
      @summary: Counts the number of objects that match the query parameters
      """
      pass
   
   # ............................................
   def delete(self):
      """
      @summary: Deletes the specified object
      """
      return self._passToWebObject()
   
   # ............................................
   def get(self):
      """
      @summary: Gets and individual object from the service
      """
      return self._passToWebObject()
   
   # ............................................
   def help(self):
      """
      @summary: Gets help information about the service
      """
      pass
   
   # ............................................
   def list(self):
      """
      @summary: Lists objects that match the query parameters
      """
      pass
   
   # ............................................
   def getCapabilities(self):
      """
      @summary: Returns a get capabilities document for the service
      """
      processes = []
      for subServ in self.subServices:
         processes.append(subServ["constructor"])

      return wpsGetCapabilities(processes, self.basePath)

   # ............................................
   def ogc(self):
      """
      @summary: Process an OGC request
      """
      if self.parameters.has_key("service"):
         service = self.parameters["service"].lower()
      else:
         service = self.vpath[0].lower()

      if service == "wcs":
         return self.wcs()
      elif service == "wfs":
         return self.wfs()
      elif service == "wms":
         return self.wms()
      elif service == "wps":
         return self.wps()
   
   # ............................................
   def post(self):
      """
      @summary: Posts a new object to the service
      """
      pass

   # ............................................
   def put(self):
      """
      @summary: Updates the object
      """
      return self._passToWebObject()
   
   # ............................................
   def wcs(self):
      """
      @summary: Process WCS requests
      """
      raise LMError("WCS has not been implemented for this service")

   # ............................................
   def wfs(self):
      """
      @summary: Process WFS requests
      """
      raise LMError("WFS has not been implemented for this service")

   # ............................................
   def wms(self):
      """
      @summary: Process WMS requests
      """
      raise LMError("WMS has not been implemented for this service")

   # ............................................
   def wps(self):
      """
      @summary: Process WPS requests
      """
      raise LMError("WPS has not been implemented for this service")

# =============================================================================
class WebObject(WebServiceBaseClass):
   """
   @summary: Base class for individual web service objects
   """
   subObjects = []
   subServices = []
   interfaces = []
   # ............................................
   def __init__(self, method, conn, userId=ARCHIVE_USER, body=None, vpath=[], 
                         parameters={}, basePath=WEBSERVICES_ROOT, ipAddress=None):
      """
      @summary: Constructor
      @see: WebServiceBaseClass
      """
      # Process identifier in case that it is not an integer.
      # This list is assumed to have at least one item in it at time 
      #    of construction
      self.id = self.processId(vpath[0])
      WebServiceBaseClass.__init__(self, method, conn, userId=userId, body=body, 
                                   vpath=vpath, parameters=parameters, 
                                   basePath=basePath, ipAddress=ipAddress)
   
   # ............................................
   def _checkIP(self, ipAddress):
      """
      @summary: This function checks to see if the IP address belongs to a 
                   registered LmCompute machine and can bypass authentication.
      @note: In the future, we could check to see if the machine has permission
                to access private data or not.
      """
      # Get all compute resources
      matchCR = None
      crs = self.conn.getAllComputeResources()
      for cr in crs:
         if cr.matchIP(ipAddress) and (matchCR is None or \
             (matchCR.ipMask is not None and matchCR.ipMask < cr.ipMask)):
            matchCR = cr 
      
      if matchCR is not None:
         return True
      else:
         return False
   
   # ............................................
   def _checkPermission(self, item):
      """
      @summary: Check that the current user has permissions for the object
      """
      if self.method.lower() == 'get':
         return item.user in [ARCHIVE_USER, DEFAULT_POST_USER, 
                              "changeThinking", self.user] or \
                     self._checkIP(self.ipAddress)
      else: # DELETE, PUT
         return item.user in [self.user] or self._checkIP(self.ipAddress)
      
   # ............................................
   def _deleteItem(self, item):
      """
      @summary: This function will delete an item from the Lifemapper system
      @precondition: Permission to delete has been established
      @postcondition: Item will be deleted
      @note: This should be implemented in subclasses
      @return: Some object or notification of success
      """
      raise LmHTTPError(HTTPStatus.NOT_IMPLEMENTED, 
                        msg="DELETE has not been implemented for this service")
   
   # ............................................
   def _getItemIfPermitted(self):
      """
      @summary: This code is common for DELETE, GET, and PUT requests so it has
                   been moved to its own function
      """
      # Attempt to get the object
      item = None
      try:
         item = self._getItem()
      except Exception, e:
         self.conn.log.debug(str(e))

      # Check to see if the object is populated
      if item is None:
         raise LmHTTPError(HTTPStatus.NOT_FOUND, msg="Not Found")
      # Check if the user has permission to execute the request
      if self._checkPermission(item):
         # Return the object if adequate permission
         return item
      else:
         # Raise a 403 error if not
         raise LmHTTPError(HTTPStatus.FORBIDDEN, msg="Permission Denied")
      
   # ............................................
   def _updateItem(self, item):
      """
      @summary: This function will update an item from the Lifemapper system
      @precondition: Permission to delete has been established
      @postcondition: Item will be updated
      @note: This should be implemented in subclasses
      @return: Some object or notification of success
      """
      raise LmHTTPError(HTTPStatus.NOT_IMPLEMENTED, 
                  msg="PUT (update) has not been implemented for this service")
   
   # ............................................
   def delete(self):
      """
      @summary: Attempt to delete the object
      @note: Should be done in the subclasses
      """
      item = self._getItemIfPermitted()
      return self._deleteItem(item)
   
   # ............................................
   def doAction(self):
      """
      @summary: Performs the action dictated by the inputs
      """
#      if (len(self.vpath) == 0 and self.parameters.has_key("request")) \
#            or (len(self.vpath) > 0 and self.vpath[0] in OGC_INTERFACES):
#         return self.ogc()
      if self.method.lower() in ('get', 'post'):
         if len(self.vpath) == 0 or self.vpath[0] in self.interfaces:
            return self.get()
         else:
            for subObj in self.subObjects:
               if self.vpath[0] in subObj["names"]:
                  obj = self.get()
                  return subObj["func"](obj)
            for subServ in self.subServices:
               if self.vpath[0] in subServ["names"]:
                  self.parameters[subServ["idParameter"]] = self.id
                  serv = subServ["constructor"](self.method, self.conn, 
                                                userId=self.user, 
                                                body=self.body, vpath=self.vpath, 
                                                parameters=self.parameters,
                                                basePath=self.basePath,
                                                ipAddress=self.ipAddress)
                  return serv.doAction()
            # If not found
            return self.get()
      elif self.method.lower() == 'delete':
         return self.delete()
      elif self.method.lower() == 'put':
         return self.put()
      raise Exception, "Unknown request"
   
   # ............................................
   def get(self):
      """
      @summary: Tries to get the item
      @raise HTTPError: Raises an HTTP 403 error if the user doesn't have permission
      """
      item = self._getItemIfPermitted()
      return item

   # ............................................
   def ogc(self):
      """
      @summary: Process an OGC request
      """
      if self.parameters.has_key("service"):
         service = self.parameters["service"].lower()
      else:
         service = self.vpath[0].lower()

      if service == "wcs":
         return self.wcs()
      elif service == "wfs":
         return self.wfs()
      elif service == "wms":
         return self.wms()
      elif service == "wps":
         return self.wps()
   
   # ............................................
   def put(self):
      """
      @summary: Attempt to update the object
      @note: Should be done in the subclasses
      """
      item = self._getItemIfPermitted()
      return self._updateItem(item)
   
   # ............................................
   def processId(self, id):
      """
      @summary: Process the id given to the object.  This will probably just
                   be a database id, but it is possible that it could be some
                   controlled vocabulary indicating which item in a list should
                   be returned
      """
      return id
   
   # ............................................
   def wcs(self):
      """
      @summary: Process WCS requests
      """
      raise LMError("WCS has not been implemented for this service")

   # ............................................
   def wfs(self):
      """
      @summary: Process WFS requests
      """
      raise LMError("WFS has not been implemented for this service")

   # ............................................
   def wms(self):
      """
      @summary: Process WMS requests
      """
      raise LMError("WMS has not been implemented for this service")

   # ............................................
   def wps(self):
      """
      @summary: Process WPS requests
      """
      if self.parameters.has_key("request"):
         request = self.parameters["request"].lower()
      else:
         request = ""
      if request == "GetCapabilities".lower():
         return self.getCapabilities()
      elif request == "DescribeProcess".lower():
         ret = wpsDescribeProcess(self.wpsProcesses[self.vpath[0]])
         return str(ret)

# =============================================================================
class OGCService(WebServiceBaseClass):
   """
   @summary: Base class for OGC service calls
   """
   pass

# =============================================================================
class WCSService(OGCService):
   """
   @summary: Base class for OGC WCS (Web Coverage Service) requests
   """
   pass

# =============================================================================
class WFSService(OGCService):
   """
   @summary: Base class for OGC WFS (Web Feature Service) requests
   """
   pass

# =============================================================================
class WMSService(OGCService):
   """
   @summary: Base class for OGC WMS (Web Mapping Service) requests
   """
   pass

# =============================================================================
class WPSService(OGCService):
   """
   @summary: Base class for OGC WPS (Web Processing Service) requests
   """
   identifier = "" # The process identifier.  This will be used to construct 
   #                    it's url and identify the process
   title = "" # The title of the process
   abstract = "" # An abstract about the process
   version = "" # The version of the process
   inputParameters = [ # List of input parameter dictionaries.
                      # Example parameter:
                      #{
                      # "minOccurs" : "0",
                      # "maxOccurs" : "1",
                      # "identifier" : "maxx",
                      # "title" : "Max X",
                      # "reference" : "http://www.w3.org/TR/xmlschema-2/#integer",
                      # "paramType" : "integer",
                      # "defaultValue" : "3573460"
                      #},
                     ]
   outputParameters = [ # List of output parameter dictionaries
                       # Example parameter:
                       #{
                       # "identifier" : "Output1",
                       # "title" : "URL to shapefile",
                       # "reference" : "http://www.w3.org/TR/xmlschema-2/#string",
                       # "paramType" : "string"
                       #},
                      ]

      
   # ............................................
   def doAction(self):
      if self.parameters.has_key("request"):
         request = self.parameters["request"].lower()
      else:
         request = ""
      if request == "GetCapabilities".lower():
         return self.getCapabilities()
      elif request == "DescribeProcess".lower():
         ret = wpsDescribeProcess(self)
         return str(ret)
      elif request == "Execute".lower():
         return self.execute()
      else:
         return self.getStatus(1)
         
   # ...................................
   def execute(self):
      """
      @summary: Execute the process (submit for execution most likely)
      """
      pass
   
   # ...................................
   def getStatus(self, id):
      """
      @summary: Returns a get status document for the process
      """
      pass
   
   # ...................................
   def _executeResponse(self, status, statusString, creationTime, 
                              percentComplete=0, outputs={}):
      """
      @summary: Generates a valid WPS Execute Response for the given input data
      @note: This method exists because the template requires all parameters 
                even if they are not used.  This will stub in unneeded 
                parameters for different request types
      @param id: The id of the running process. (integer)
      @param status: The status of the job (integer)
      @param statusString: Information about the job (string)
      @param creationTime: The time that the job was created in mjd format 
                             (numeric)
      @param percentComplete: (optional) Percent that job is complete for 
                                 running jobs (integer)
      @param outputs: (optional) Dictionary of outputs.  Key should be output 
                         identifier, value should be the value of the output
      @return: A valid WPS Execute Response that can be returned to the user
      @rtype: String (XML)
      """
      process = self.__class__
      getCapabilitiesUrl = "%s?request=GetCapabilities" % \
                                        self.basePath.split(self.identifier)[0]
      statusUrl = "%sstatus" % self.basePath.split(self.identifier)[0]
      status = status
      statusString = statusString
      creationTime = formatTimeHuman(creationTime)
      percentCompleted = percentComplete
      outputs = outputs
      
      ret = wpsExecuteResponse(process, getCapabilitiesUrl, statusUrl, status, 
                               statusString, creationTime, percentCompleted, 
                               outputs)
      return ret
   
   # ............................................
   def getCapabilities(self):
      processList = [self.processTypes[key] \
                            for key in self.processTypes.keys()]
      
      return wpsGetCapabilities(processList, self.basePath)

# =============================================================================
def getQueryParameters(params, parameters):
   """
   @summary: Returns the values of the parameters (processed) from parameters
   @param params: List of parameter dictionaries to attempt to retrieve 
                     [{name: value, process: lambda}]
   @param parameters: Dictionary of parameters {name:value}
   @return: List of parameter values
   @rtype: List
   """
   parameters = dict([(k.lower(), parameters[k]) for k in parameters.keys()])
   ret = []
      
   for param in params:
      if parameters.has_key(param["name"].lower()) \
            and parameters[param["name"].lower()] is not None \
            and len(str(parameters[param["name"].lower()])) > 0:
         try:
            ret.append(param["process"](parameters[param["name"].lower()]))
         except:
            ret.append(None)
      elif param.has_key("default"):
         ret.append(param["default"])
      else:
         ret.append(None)
   return ret
   
# =============================================================================
def buildAttListResponse(items, count, userId, params=[]):
   """
   @summary: Builds a dictionary of list query attributes
   @param items: The list of items in this page of results
   @param count: The total number of items matching the query parameters
   @param userId: The user id related to this request
   @param params: List of parameter tuples [(QueryParameter, value)]
   """
   atts = {
          "itemCount" : count,
          "userId" : userId,
          "queryParameters" : {}
         }
   for param, value in params:
      atts["queryParameters"][param["name"]] = {
                                                "param" : param,
                                                "value" : value
                                               }
      
   return LmAttList(items=items, attrib=atts)

# =============================================================================
def wpsDescribeProcess(process):
   """
   @summary: Returns a string representation of the process description (WPS format)
   """
   pis = []
   pis.append(PI("xml", 'version="1.0"'))
    
   register_namespace('wps', WPS_NS)
   register_namespace('ows', OWS_NS)
   
   el = Element("ProcessDescriptions", 
                attrib={
                     QName(XSI_NS, "schemaLocation") : \
                        "http://www.opengis.net/wps/1.0.0 http://schemas.opengis.net/wps/1.0.0/wpsDescribeProcess_response.xsd",
                     "service" : "WPS",
                     "version" : "1.0.0",
                     "xml:lang" : "eng"},
                namespace=WPS_NS)
   pdEl = SubElement(el, "ProcessDescription", 
                     attrib={QName(WPS_NS, "processVersion") : process.version,
                             "storeSupported" : "True",
                             "statusSupported" : "True"})
   SubElement(pdEl, "Identifier", namespace=OWS_NS, value=process.identifier)
   SubElement(pdEl, "Title", namespace=OWS_NS, value=process.title)
   SubElement(pdEl, "Abstract", namespace=OWS_NS, value=process.abstract)
   
   dataInputsEl = SubElement(pdEl, "DataInputs")
   for param in process.inputParameters:
      inputParameterEl = SubElement(dataInputsEl, "Input", 
                                    attrib={"minOccurs" : param["minOccurs"], 
                                            "maxOccurs" : param["maxOccurs"]})
      SubElement(inputParameterEl, "Identifier", namespace=OWS_NS, value=param["identifier"])
      SubElement(inputParameterEl, "Title", namespace=OWS_NS, value=param["title"])
      
      litDataEl = SubElement(inputParameterEl, "LiteralData")
      SubElement(litDataEl, "DataType", namespace=OWS_NS, attrib={QName(OWS_NS, "reference") : param["reference"]}, value=param["paramType"])
      if param["defaultValue"] is not None:
         SubElement(litDataEl, "DefaultValue", value=param["defaultValue"])
      SubElement(litDataEl, "AnyValue", namespace=OWS_NS)
      
   procOutEl = SubElement(pdEl, "ProcessOutputs")
   for param in process.outputParameters:
      outputEl = SubElement(procOutEl, "Output")
      SubElement(outputEl, "Identifier", namespace=OWS_NS, value=param["identifier"])
      SubElement(outputEl, "Title", namespace=OWS_NS, value=param["title"])
      SubElement(SubElement(outputEl, "LiteralOuput"),
                 "DataType", namespace=OWS_NS, 
                 attrib={QName(OWS_NS, "reference") : param["reference"]}, 
                 value=param["paramType"])
   
   return "%s\n%s" % ('\n'.join([tostring(pi) for pi in pis]), tostring(el))

# =============================================================================
def wpsExecuteResponse(process, getCapabilitiesUrl, statusUrl, status, statusString, creationTime, percentComplete, outputs):
   pis = []
   pis.append(PI("xml", 'version="1.0"'))
    
   register_namespace('wps', WPS_NS)
   register_namespace('ows', OWS_NS)
   
   el = Element("ExecuteResponse", 
                attrib={
                     QName(XSI_NS, "schemaLocation") : \
                        "http://www.opengis.net/wps/1.0.0 http://schemas.opengis.net/wps/1.0.0/wpsExecute_response.xsd",
                     "service" : "WPS",
                     "version" : "1.0.0",
                     "xml:lang" : "eng",
                     "serviceInstance" : getCapabilitiesUrl,
                     "statusLocation" : statusUrl},
                namespace=WPS_NS)

   procEl = SubElement(el, "Process", attrib={
                           QName(WPS_NS, "processVersion") : process.version})
   
   SubElement(procEl, "Identifier", namespace=OWS_NS, value=process.identifier)
   SubElement(procEl, "Title", namespace=OWS_NS, value=process.title)
   SubElement(procEl, "Abstract", namespace=OWS_NS, value=process.abstract)
   
   statusEl = SubElement(el, "Status", namespace=WPS_NS, 
                         attrib={"creationTime" : creationTime})
   if status == JobStatus.INITIALIZE:
      SubElement(statusEl, "ProcessAccepted", namespace=WPS_NS, value=statusString)
   elif status > JobStatus.INITIALIZE and status < JobStatus.COMPLETE:
      SubElement(statusEl, "ProcessStarted", namespace=WPS_NS, 
                 attrib={"percentCompleted" : percentComplete}, 
                 value=statusString)
   elif status == JobStatus.COMPLETE:
      SubElement(statusEl, "ProcessSucceeded", value=statusString, namespace=WPS_NS)
      procOutputsEl = SubElement(statusEl, "ProcessOutputs", namespace=WPS_NS)
      for output in process.outputParameters:
         outputEl = SubElement(procOutputsEl, "Output", namespace=WPS_NS)
         SubElement(outputEl, "Identifier", namespace=OWS_NS, value=output["identifier"])
         SubElement(outputEl, "Title", namespace=OWS_NS, value=output["title"])
         dataEl = SubElement(outputEl, "Data", namespace=WPS_NS)
         SubElement(dataEl, "LiteralData", namespace=WPS_NS, 
                    attrib={"dataType" : output["paramType"], "uom" : "None"}, 
                    value=outputs[output["identifier"]])
   else:
      SubElement(statusEl, "ProcessFailed", namespace=WPS_NS, value=statusString)
   return "%s\n%s" % ('\n'.join([tostring(pi) for pi in pis]), tostring(el))

# =============================================================================
def wpsGetCapabilities(processList, basePath):
   pis = []
   pis.append(PI("xml", 'version="1.0"'))
    
   register_namespace('wps', WPS_NS)
   register_namespace('ows', OWS_NS)
   setDefaultNamespace(OWS_NS)
   
   el = Element("ProcessDescriptions", 
                attrib={
                     QName(XSI_NS, "schemaLocation") : \
                        "http://www.opengis.net/wps/1.0.0 http://schemas.opengis.net/wps/1.0.0/wpsGetCapabilities_response.xsd",
                     "service" : "WPS",
                     "version" : "1.0.0",
                     "xml:lang" : "eng",
                     "updateSequence" : "1"},
                namespace=WPS_NS)
   
   serviceIdEl = SubElement(el, "ServiceIdentification")
   SubElement(serviceIdEl, "Title", value="Lifemapper WPS Services")
   SubElement(serviceIdEl, "Abstract", value="These are the WPS services available from Lifemapper")
   kwsEl = SubElement(serviceIdEl, "Keywords")
   
   keywords = [
               "Lifemapper", 
               "Species Distribution Modeling", 
               "Biogeography", 
               "GRASS", 
               "REST"]
   
   for kw in keywords:
      SubElement(kwsEl, "Keyword", kw)

   SubElement(serviceIdEl, "ServiceType", value="WPS")
   SubElement(serviceIdEl, "ServiceTypeVersion", value="1.0.0")
   SubElement(serviceIdEl, "Fees", value="None")
   SubElement(serviceIdEl, "AccessConstraints", value="None")
   

   serviceProviderEl = SubElement(el, "ServiceProvider")
   SubElement(serviceProviderEl, "ProviderName", value="Lifemapper")
   SubElement(serviceProviderEl, "ProviderSite", attrib={"xlink:href" : WEBSERVICES_ROOT})
  
   contacts = [
               {
                  "name" : "CJ Grady",
                  "position" : "Senior Software Developer",
                  "street" : "1345 Jayhawk Blvd. Room 606",
                  "city" : "Lawrence",
                  "state" : "Kansas",
                  "postalCode" : "66045",
                  "country" : "US",
                  "email" : "cjgrady@ku.edu",
               },
               {
                  "name" : "Jeff Cavner",
                  "position" : "Software Developer",
                  "street" : "1345 Jayhawk Blvd. Room 606",
                  "city" : "Lawrence",
                  "state" : "Kansas",
                  "postalCode" : "66045",
                  "country" : "US",
                  "email" : "jcavner@ku.edu",
               },
               {
                  "name" : "Aimee Stewart",
                  "position" : "Lead Software Developer",
                  "street" : "1345 Jayhawk Blvd. Room 606",
                  "city" : "Lawrence",
                  "state" : "Kansas",
                  "postalCode" : "66045",
                  "country" : "US",
                  "email" : "astewart@ku.edu",
               },
            ]
   
   
   for contact in contacts:
      scEl = SubElement(serviceProviderEl, "ServiceContact")
      SubElement(scEl, "IndividualName", value=contact.name)
      SubElement(scEl, "PositionName", value=contact.position)
      
      addressEl = SubElement(SubElement(scEl, "ContactInfo"),
                             "Address")
      SubElement(addressEl, "DeliveryPoint", value=contact.street)
      SubElement(addressEl, "City", value=contact.city)
      SubElement(addressEl, "State", value=contact.state)
      SubElement(addressEl, "PostalCode", value=contact.postalCode)
      SubElement(addressEl, "Country", value=contact.country)
      SubElement(addressEl, "ElectronicMailAddress", value=contact.email)


   getCapabilitiesUrl = "%s?request=GetCapabilities" % basePath
   describeProcessUrl = "%s?request=DescribeProcess" % basePath
   executeProcessUrl = "%s?request=Execute" % basePath
   wsdlUrl = "%s/wps" % WEBSERVICES_ROOT

   opMetaEl = SubElement(el, "OperationsMetadata")
   getCapEl = SubElement(opMetaEl, "Operation", attrib={"name" : "GetCapabilities"})
   getCapHttpEl = SubElement(SubElement(getCapEl, "DCP"), "HTTP")
   SubElement(getCapHttpEl, "Get", attrib={"xlink:href" : getCapabilitiesUrl})
   SubElement(getCapHttpEl, "Post", attrib={"xlink:href" : getCapabilitiesUrl})

   descProcEl = SubElement(opMetaEl, "Operation", attrib={"name" : "DescribeProcess"})
   descProcHttpEl = SubElement(SubElement(descProcEl, "DCP"), "HTTP")
   SubElement(descProcHttpEl, "Get", attrib={"xlink:href" : describeProcessUrl})
   SubElement(descProcHttpEl, "Post", attrib={"xlink:href" : describeProcessUrl})

   executeEl = SubElement(opMetaEl, "Operation", attrib={"name" : "Execute"})
   executeHttpEl = SubElement(SubElement(executeEl, "DCP"), "HTTP")
   SubElement(executeHttpEl, "Get", attrib={"xlink:href" : executeProcessUrl})
   SubElement(executeHttpEl, "Post", attrib={"xlink:href" : executeProcessUrl})

   

   processOfferingsEl = SubElement(el, "ProcessOfferings", namespace=WPS_NS)
   for process in processList:
      procEl = SubElement(processOfferingsEl, "Process", namespace=WPS_NS, attrib={QName(WPS_NS, "processVersion") : process.version})
      SubElement(procEl, "Identifier", value=process.identifier)
      SubElement(procEl, "Title", value=process.title)
      SubElement(procEl, "Abstract", value=process.abstract)


   langEl = SubElement(el, "Languages", namespace=WPS_NS)
   SubElement(SubElement(langEl, "Default", namespace=WPS_NS),
              "Language", value="eng")
   SubElement(SubElement(langEl, "Supported", namespace=WPS_NS),
              "Language", value="eng")   
   
   
   SubElement(el, "WSDL", namespace=WPS_NS, attrib={"xlink:href" : wsdlUrl})   
   

   return "%s\n%s" % ('\n'.join([tostring(pi) for pi in pis]), tostring(el))

      
