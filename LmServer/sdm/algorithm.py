"""
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
try:
   import cPickle as pickle
except:
   import pickle
import json
from types import StringType, IntType, FloatType, NoneType, DictionaryType

from LmServer.base.lmobj import LMError, LMObject
from LmServer.common.lmconstants import ALGORITHM_DATA

# .........................................................................
class NotImplementedError( LMError ):
   """
   Error thrown when a function is not implemented
   @todo: Can this be removed?
   """
   def __init__(self, currargs, prevargs=None, doTrace=False):
      LMError.__init__(self, currargs, prevargs, doTrace)

# .........................................................................
class InvalidParameterError( LMError ):
   """
   Error thrown when a property does not exist for the algorithm
   """
   def __init__(self, currargs, prevargs=None, doTrace=False):
      LMError.__init__(self, currargs, prevargs, doTrace)

# .........................................................................
class InvalidValueError( LMError ):
   """
   Error thrown when a string value is not a valid choice for the parameter
   """
   def __init__(self, currargs, prevargs=None, doTrace=False):
      LMError.__init__(self, currargs, prevargs, doTrace)

# .........................................................................
class WrongTypeError( LMError ):
   """
   Error thrown when a value is of the wrong type for a parameter
   """
   def __init__(self, currargs, prevargs=None, doTrace=False):
      LMError.__init__(self, currargs, prevargs, doTrace)

# .........................................................................
class OutOfRangeError( LMError ):
   """
   Error thrown when a value is out of range for a parameter
   """
   def __init__(self, currargs, prevargs=None, doTrace=False):
      LMError.__init__(self, currargs, prevargs, doTrace)

   
# .........................................................................
class Algorithm(LMObject):
   """       
   Class to hold algorithm and its parameter values and constraints  
   """
# .............................................................................
# Constructor
# .............................................................................
   def __init__(self, code, metadata={}, parameters={}, name=None):
      """
      @summary Constructor for the algorithm class.  Algorithm should be 
               initialized from the database upon construction.
      @param code: The algorithm code for openModeller
      @param metadata: Dictionary of Algorithm metadata
      @param parameters: Dictionary of Algorithm parameters
      @param name: (optional) The full algorithm name
      """
      self.code = code
      # TODO: update this for Borg
      self.name = name
      if not metadata:
         metadata = {'name': name}
         
      self.algMetadata = {}
      self.loadAlgMetadata(metadata)
      self._initParameters()
      if parameters:
         self._setParameters(parameters)
      
   # ...............................................
   def _initParameters(self):
      self._parameters = {}
      self._parameterConstraints = {}
      for key, constraintDict in ALGORITHM_DATA[self.code]['parameters'].iteritems():
         self._parameterConstraints[key] = constraintDict
         self._parameters[key] = None

   # ...............................................
   def _getParameters(self):
      """
      @summary Return a dictionary in which key = param name and 
               value = param value 
      @return A dictionary of the algorithm's parameters
      """
      return self._parameters

   # ...............................................
   def _setParameters(self, params):
      """
      @summary Set the properties of the algorithm
      @param params: A list of AlgorithmParameter objects for the algorithm 
      """
      if not(isinstance(params, DictionaryType)):
         try:
            params = pickle.loads(params)
         except Exception, e:
            raise LMError('Algorithm Parameters must be a dictionary or a pickled dictionary')
         
      for k, v in params.iteritems():
         self.setParameter(k,v)
      
   ## List of algorithm parameters
   parameters = property(_getParameters, _setParameters)
      
   # .........................................................................
# Public Methods
# .........................................................................
# ...............................................
# ...............................................
   def dumpAlgParameters(self):
      apstr2 = LMObject._dumpMetadata(self, self._parameters)
      return apstr2

# ...............................................
   def dumpAlgMetadata(self):
      return LMObject._dumpMetadata(self, self.algMetadata)
 
# ...............................................
   def loadAlgMetadata(self, newMetadata):
      self.algMetadata = LMObject._loadMetadata(self, newMetadata)

# ...............................................
   def addAlgMetadata(self, newMetadataDict):
      self.algMetadata = LMObject._addMetadata(self, newMetadataDict, 
                                  existingMetadataDict=self.algMetadata)

# ...............................................
   def dumpParametersAsString(self):
      # Use default protocol 0 here, smaller dictionary can be ascii
      apstr = pickle.dumps(self._parameters)
      return apstr

   # ...............................................
   def fillWithDefaults(self):
      self._parameters = {}
      for key, constraints in self._parameterConstraints.iteritems():
         self._parameters[key] = constraints['default']
         
   # ...............................................
   def setParameter(self, name, val):
      """
      @summary If parameterConstraints are present, check to see if a 
               property and value are valid and set the property if they are.
      @param name: The parameter to set
      @param val: The new value for the parameter
      @exception InvalidParameterError: Thrown if the parameter is not valid 
                                       for the algorithm
      @exception InvalidValueError: Thrown if a StringType parameter is set to 
                                    an unknown option
      @exception WrongTypeError: Thrown if the value does not match the expected 
                                 type
      @exception OutOfRangeError: Thrown if a numerical parameter is set outside
                                 the acceptable value range.
      """
      if self._parameterConstraints:
         if self._parameterConstraints.has_key(name):
            constraints = self._parameterConstraints[name]
            if (isinstance(val, NoneType)):
               val = constraints['default']
               
            if constraints['type'] == StringType:
               if (isinstance(val, StringType)):
                  val = val.lower()
                  if not(val in constraints['options']):
                     raise InvalidValueError(['Invalid value %s; Valid options are %s' % 
                                              (val, str(constraints['options']) )])
               else :
                  raise WrongTypeError(['Expected StringType, Received %s - type %s' % 
                                        (str(val), str(type(val))) ])
      
            # Check valid float value
            elif constraints['type'] == FloatType:
               if (isinstance(val, FloatType) or isinstance(val, IntType)): 
                  self._checkNumericValue(val, constraints)
#                elif (isinstance(val, NoneType)):
#                   val = constraints['default']
               else :
                  raise WrongTypeError(['Expected FloatType, Received %s - type %s' % 
                                        (str(val), str(type(val))) ])
                     
            # Check valid int value
            elif constraints['type'] == IntType:
               if (isinstance(val, IntType) or 
                   isinstance(val, FloatType) and (val % 1 == 0) ):
                  self._checkNumericValue(val, constraints)
               else :
                  raise WrongTypeError(['Expected IntType, Received %s - type %s' % 
                                        (str(val), str(type(val))) ])
            # Successfully ran the gauntlet
            self._parameters[name] = val            
         else:
            # If didn't find name and return
            raise InvalidParameterError(['Invalid parameter %s' % str(name)])
      else:
         raise LMError(currargs='Error: parameterConstraints not initialized')
         
   # ...............................................
   def _checkNumericValue(self, val, constraints):
      '''
      @summary: Raises an Exception if value is not within constraints
      '''
      if constraints['min'] is not None:
         if val < constraints['min']:
            raise OutOfRangeError(["Value %s must be greater than %s" % 
                                   (str(val), str(constraints['min']))])
      if constraints['max'] is not None:
         if val > constraints['max']:
            raise OutOfRangeError(["Value %s must be less than %s" % 
                                   (str(val), str(constraints['max']))])

   # ...............................................
   def getParameterValue(self, name):
      """
      @note: the name is case-sensitive
      @summary Return an algorithm parameter value
      @param name: The name of the parameter to return
      @return The algorithm parameter value
      @exception InvalidParameterError: Thrown if the parameter is invalid
      """
      if self._parameters.has_key(name):
         return self._parameters[name]
      # If didn't find name and return
      raise InvalidParameterError(['Unknown parameter %s' % str(name)])
   
   # ...............................................
   def hasParameter(self, name):
      """
      @note: the name is case-sensitive
      @summary Return True if an algorithm parameter is valid
      @param name: The name of the parameter to check 
      @return True/False
      """
      if self._parameters.has_key(name):
         return True
      # If didn't find name and return
      return False

   # ...............................................
   def equals(self, other):
      """
      @summary Compares the current Algorithm to another to determine if they 
               are equal
      @param other: The other Algorithm to compare to
      @return Returns True if the Algorithms are equal, False if not
      """
      if isinstance(other, Algorithm):
         if (self.code == other.code):
            if self._parameters is not None and other._parameters is not None:
               if len(self._parameters) == len(other._parameters):
                  for key, val in self._parameters.iteritems():
                     if self._parameters[key] != other._parameters[key]:
                        return False
                  return True
            elif self._parameters is None and other._parameters is None:
               return True
      return False
