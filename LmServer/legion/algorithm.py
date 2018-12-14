"""
@license: gpl2
@copyright: Copyright (C) 2018, University of Kansas Center for Research

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
from types import StringType, IntType, FloatType, NoneType

from LmBackend.common.lmobj import LMError, LMObject
from LmServer.common.lmconstants import Algorithms

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
    def __init__(self, code, metadata={}, parameters={}, inputs={}, name=None):
        """
        @summary Constructor for the algorithm class.  Algorithm should be 
                    initialized from the database upon construction.
        @param code: The algorithm code for openModeller
        @param metadata: Dictionary of Algorithm metadata
        @param parameters: Dictionary of Algorithm parameters
        @param inputs: Dictionary of Algorithm input data, with keyword and 
                            data object
        @param name: (optional) The full algorithm name
        """
        self.code = code
        if not metadata:
            metadata = {'name': name}
            
        self.algMetadata = {}
        self.loadAlgMetadata(metadata)
        self._initParameters()
        if parameters:
            self._setParameters(parameters)
        self._inputData = {}
        self.setInputs(inputs)
        
    # ...............................................
    def _initParameters(self):
        self._parameters = {}
        self._parameterConstraints = {}
        algparams = Algorithms.get(self.code).parameters
        for key, constraintDict in algparams.iteritems():
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
        if params is not None:
            if type(params) is dict: 
                newParams = params
            else:
                try:
                    newParams = json.loads(params)
                except Exception, e:
                    print('Failed to load JSON object from type {} object {}'
                            .format(type(params), params))

        if type(newParams) is dict: 
            try:
                for k, v in newParams.iteritems():
                    self.setParameter(k,v)
            except Exception, e:
                raise LMError('Failed to load parameter {} with value {}'.format(k, v))
        else:
            raise LMError('Algorithm Parameters must be a dictionary or a JSON-encoded dictionary')
            
    ## List of algorithm parameters
    parameters = property(_getParameters, _setParameters)
        
    # .........................................................................
# Public Methods
# .........................................................................
# ...............................................
    # ...............................................
    def setInputs(self, inputs):
        """
        @summary Set the data inputs of the (masking) algorithm
        @param inputs: A dictionary of Layer objects for the algorithm 
        """
        if inputs is not None:
            if type(inputs) is dict: 
                newInputs = inputs
            else:
                try:
                    newInputs = json.loads(inputs)
                except Exception, e:
                    print('Failed to load JSON object from type {} object {}'
                            .format(type(inputs), inputs))
            self._inputData = newInputs

    # ...............................................
    def setInput(self, key, input):
        """
        @summary Set a data input of the (masking) algorithm
        @param input: A layer name or Layer objects 
        """
        self._inputData[key] = input

    # ...............................................
    def getInputs(self):
        """
        @summary Return a dictionary in which key = param name and 
                    value = layer object
        @return A dictionary of the algorithm's input data
        """
        return self._inputData

# ...............................................
    def dumpAlgParameters(self):
        return LMObject._dumpMetadata(self, self._parameters)

    def loadAlgParameters(self, newMetadata):
        self._parameters = LMObject._loadMetadata(self, newMetadata)

# ...............................................
    def dumpAlgMetadata(self):
        return LMObject._dumpMetadata(self, self.algMetadata)
 
    def loadAlgMetadata(self, newMetadata):
        self.algMetadata = LMObject._loadMetadata(self, newMetadata)

    def addAlgMetadata(self, newMetadataDict):
        self.algMetadata = LMObject._addMetadata(self, newMetadataDict, 
                                             existingMetadataDict=self.algMetadata)

    # ...............................................
    def fillWithDefaults(self):
        self._parameters = {}
        for key, constraints in self._parameterConstraints.iteritems():
            self._parameters[key] = constraints['default']
            
    # ...............................................
    def findParamNameType(self, name):
        """
        @summary Find the correct case-sensitive property name and type for this 
                    algorithm, given a case-insensitive parameter name 
        @param name: The parameter to find, case-insensitive
        @return a correctly-capitalized string for this parameter and type, 
                  None, None if it is not valid for this algorithm
        """
        pname = self._getParamKey(name)
        if pname is not None:
            ptype = self._parameterConstraints[pname]['type']
            return pname, ptype
        return None, None

    # ...............................................
    def _getParamKey(self, name):
        """
        @summary Find the correct case-sensitive property name for this algorithm given a string 
        @param name: The parameter to find
        @return a correctly-capitalized string for this parameter, None if it is 
                  not valid for this algorithm
        """
        if self._parameterConstraints.has_key(name):
            return name
        else:
            for key in self._parameterConstraints.keys():
                if key.lower() == name.lower():
                    return key
            return None

    # ...............................................
    def setParameter(self, name, val):
        """
        @summary If parameterConstraints are present, check to see if a 
                    property and value are valid and set the property if they are.
        @param name: The case-insensitive parameter name to set
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
            paramName = self._getParamKey(name)
            if paramName is not None:
                constraints = self._parameterConstraints[paramName]
                if (isinstance(val, NoneType)):
                    val = constraints['default']
                    
                if constraints['type'] == StringType:
                    if (isinstance(val, StringType)):
                        val = val.lower()
                        try:
                            valOptions = constraints['options']
                        except:
                            pass
                        else:
                            if not(val in valOptions):
                                raise InvalidValueError(['Invalid value {}; Valid options are {}'
                                                                 .format(val, str(valOptions))])
                    else :
                        raise WrongTypeError(['Expected StringType, Received %s - type %s' % 
                                                     (str(val), str(type(val))) ])
        
                # Check valid float value
                elif constraints['type'] == FloatType:
                    if (isinstance(val, FloatType) or isinstance(val, IntType)): 
                        self._checkNumericValue(val, constraints)
#                     elif (isinstance(val, NoneType)):
#                         val = constraints['default']
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
                self._parameters[paramName] = val                
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

    # .............................................
    def getDictionary(self):
        """
        @summary: Get the algorithm as a dictionary
        """
        algoObj = {
            "algorithmCode" : self.code,
            "parameters" : []
        }
            
        for param in self._parameters.keys():
            algoObj["parameters"].append(
                {"name" : param, 
                 "value" : str(self._parameters[param])})
        return algoObj
  