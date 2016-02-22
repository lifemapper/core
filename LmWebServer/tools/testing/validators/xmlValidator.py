"""
@summary: This module contains a class that validates an XML document against
             a Lifemapper-defined schema
@author: CJ Grady
@see: http://www.json.org
"""
from LmWebServer.tools.testing.validators.validatorError import \
                                                       InvalidResponseException

from LmCommon.common.lmXml import deserialize, fromstring
from LmCommon.common.lmAttObject import LmAttObj, LmAttList

# .............................................................................
class XmlValidator(object):
   """
   @summary: Validates XML content against a response schema
   """
   # .........................
   def __init__(self):
      pass
   
   # .........................
   def validate(self, xmlContent, lmSchema):
      """
      @summary: Validates XML content against a response schema
      @param xmlContent: XML content as a string to validate
      @param lmSchema: An object representing a response schema to test against
      @raise InvalidResponseException: Raised if the response does not match
                                          the schema
      @return: True if the response is valid
      """
      xmlObj = deserialize(fromstring(xmlContent))
      
      processFn = self._getProcessFn(lmSchema)
      processFn(xmlObj, lmSchema)

      return True

   # .........................
   def _getProcessFn(self, lmSchema):
      """
      @summary: Gets the processing function for the schema expectation
      """
      if lmSchema.itemType.lower() == 'object':
         return self._processObject
      elif lmSchema.itemType.lower() == 'array':
         return self._processArray
      else:
         return self._checkValidValue
      
   # .........................
   def _processArray(self, xAry, lmSchema):
      """
      @summary: Process a list
      @param xAry: The list object to process
      @param lmSchema: Object defining what this array looks like
      """
      # xAry should be an LmAttList
      if not isinstance(xAry, LmAttList):
         raise InvalidResponseException(
            "Expected an LmAttList object, received %s" % type(xAry))
      
      # Get the sub object type (if exists)
      subObjSchema = lmSchema.SubItem
      if subObjSchema is not None:
         processFn = self._getProcessFn(subObjSchema)
         for i in xAry:
            processFn(i, subObjSchema)
   
   # .........................
   def _processObject(self, xObj, lmSchema):
      """
      @summary: Process an object
      @param xObj: The object to process
      @param lmSchema: Object defining what this object looks like
      """
      # xObj should be an LmAttObj
      if not isinstance(xObj, LmAttObj):
         raise InvalidResponseException(
                      "Expected an LmAttObj object, received %s" % type(xObj))
      
      # Loop through all of the subobjects.  
      try:
         subItems = lmSchema.SubItem
      except KeyError, ke: # Doesn't have any sub items
         print "Key error looking for sub items"
         return
      
      if not isinstance(subItems, list):
         subItems = [subItems]
      
      for si in subItems:
         # See if object has key
         try:
            v = getattr(xObj, si.name)
            self._processValue(v, si)
         except Exception, e:
            try:
               if si.optional.lower() == 'true' or int(si.optional) == 1:
                  continue
            except:
               pass
            print str(e)
            raise InvalidResponseException("Could not find %s" % si.name)
   
   # .........................
   def _processValue(self, xVal, lmSchema):
      # If not a list or dictionary, should just be a simple type
      processFn = self._getProcessFn(lmSchema)
      processFn(xVal, lmSchema)
   
   # .........................
   def _checkValidValue(self, xVal, lmSchema):
      return True
   