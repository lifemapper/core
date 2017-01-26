"""
@summary: This module contains a class that validates a JSON document against
             a Lifemapper-defined schema
@author: CJ Grady
@see: http://www.json.org
"""
import json

from LmWebServer.tools.testing.validators.validatorError import \
                                                       InvalidResponseException

# .............................................................................
class JsonValidator(object):
   """
   @summary: Validates JSON content against a response schema
   """
   # .........................
   def __init__(self):
      pass
   
   # .........................
   def validate(self, jsonContent, lmSchema):
      """
      @summary: Validates JSON content against a response schema
      @param jsonContent: JSON content as a string to validate
      @param lmSchema: An object representing a response schema to test against
      @raise InvalidResponseException: Raised if the response does not match
                                          the schema
      @return: True if the response is valid
      """
      jDoc = json.loads(jsonContent)
      
      processFn = self._getProcessFn(lmSchema)
      processFn(jDoc, lmSchema)

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
   def _processArray(self, jAry, lmSchema):
      """
      @summary: Process a JSON array object
      @param jAry: The JSON array to process
      @param lmSchema: Object defining what this array looks like
      """
      # Check that jAry is, in fact, a list
      if not isinstance(jAry, list):
         raise InvalidResponseException("Expected a list object, received %s" \
                                        % type(jAry))
      
      # Get the sub object type (if exists)
      subObjSchema = lmSchema.SubItem
      if subObjSchema is not None:
         processFn = self._getProcessFn(subObjSchema)
         for i in jAry:
            processFn(i, subObjSchema)
   
   # .........................
   def _processObject(self, jObj, lmSchema):
      """
      @summary: Process a JSON object
      @param jObj: The JSON object to process
      @param lmSchema: Object defining what this JSON object looks like
      """
      # jObj should be a dictionary
      if not isinstance(jObj, dict):
         raise InvalidResponseException(
                      "Expected a dictionary object, received %s" % type(jObj))
      
      # Loop through all of the subobjects.  There should be a key in the 
      #    dictionary for each that we expect
      try:
         subItems = lmSchema.SubItem
      except KeyError, ke: # Doesn't have any sub items
         print "Key error looking for sub items"
         return
      
      if not isinstance(subItems, list):
         subItems = [subItems]
      
      for si in subItems:
         # See if object has key
         if jObj.has_key(si.name):
            self._processValue(jObj[si.name], si)
         else:
            print jObj.keys()
            raise InvalidResponseException("Could not find %s" % si.name)
   
   # .........................
   def _processValue(self, jVal, lmSchema):
      # If not a list or dictionary, should just be a simple type
      processFn = self._getProcessFn(lmSchema)
      processFn(jVal, lmSchema)
   
   # .........................
   def _checkValidValue(self, jVal, lmSchema):
      return True
   