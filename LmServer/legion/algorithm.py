"""Module containing Algorithm class and associated exceptions
"""
import json

from LmBackend.common.lmobj import LMError, LMObject
from LmServer.common.lmconstants import Algorithms


# .........................................................................
class InvalidParameterError(LMError):
    """Error thrown when a property does not exist for the algorithm"""
    def __init__(self, *args, line_num=None, do_trace=False, **kwargs):
        LMError.__init__(
            self, *args, line_num=line_num, do_trace=do_trace, **kwargs)


# .........................................................................
class InvalidValueError(LMError):
    """Error thrown when a value is not a valid choice for the parameter"""
    def __init__(self, *args, line_num=None, do_trace=False, **kwargs):
        LMError.__init__(
            self, *args, line_num=line_num, do_trace=do_trace, **kwargs)


# .........................................................................
class WrongTypeError(LMError):
    """Error thrown when a value is of the wrong type for a parameter."""
    def __init__(self, *args, line_num=None, do_trace=False, **kwargs):
        LMError.__init__(
            self, *args, line_num=line_num, do_trace=do_trace, **kwargs)


# .........................................................................
class OutOfRangeError(LMError):
    """Error thrown when a value is out of range for a parameter."""
    def __init__(self, *args, line_num=None, do_trace=False, **kwargs):
        LMError.__init__(
            self, *args, line_num=line_num, do_trace=do_trace, **kwargs)


# .........................................................................
class Algorithm(LMObject):
    """Class to hold algorithm and its parameter values and constraints."""
    # ................................
    def __init__(self, code, metadata=None, parameters=None, inputs=None,
                 name=None):
        """Constructor

        Args:
            code: The code identifying this algorithm
            metadata: A dictionary of algorithm metadata
            parameters: A dictionary of algorithm parameters
            inputs: A dictionary of algorithm input data
            name: The full name of the algorithm
        """
        self.code = code
        if not metadata:
            metadata = {'name': name}

        self.algorithm_metadata = {}
        self.load_algorithm_metadata(metadata)
        self._parameters = None
        self._init_parameters()
        if parameters:
            self._set_parameters(parameters)
        self._input_data = {}
        self.set_inputs(inputs)

    # ................................
    def _init_parameters(self):
        self._parameters = {}
        self._parameter_constraints = {}
        alg_params = Algorithms.get(self.code).parameters
        for key, constraint_dict in alg_params.items():
            self._parameter_constraints[key] = constraint_dict
            self._parameters[key] = None

    # ................................
    def _get_parameters(self):
        """Return the parameters of this algorithm object."""
        return self._parameters

    # ................................
    def _set_parameters(self, params):
        """Set the properties of the algorithm"""
        if isinstance(params, str):
            params = json.loads(str)
        if isinstance(params, dict):
            for param_name, param_value in params.items():
                self.set_parameter(param_name, param_value)
        else:
            raise LMError('Algorithm parmeters must be dictionary')

    # # List of algorithm parameters
    parameters = property(_get_parameters, _set_parameters)

    # ................................
    def set_inputs(self, inputs):
        """Set the data inputs of the (masking) algorithm

        Args:
            inputs: A dictionary of layer objects for the algorithm
        """
        if inputs is not None:
            if isinstance(inputs, str):
                inputs = json.loads(inputs)
            if isinstance(inputs, dict):
                self._input_data = inputs
            else:
                print('Cannot set inputs.  Must be dictionary')

    # ................................
    def set_input(self, input_key, input_val):
        """Set a data input of the (masking) algorithm"""
        self._input_data[input_key] = input_val

    # ................................
    def get_inputs(self):
        """Return the inputs dictionary of this algorithm object."""
        return self._input_data

    # ................................
    def dump_algorithm_parameters(self):
        """Dump algorithm parameters to a string"""
        return LMObject._dump_metadata(self._parameters)

    # ................................
    def load_algorithm_parameters(self, new_metadata):
        """Load algorithm metadata"""
        self._parameters = LMObject._load_metadata(new_metadata)

    # ................................
    def dump_algorithm_metadata(self):
        """Dump algorithm metadata to a string"""
        return LMObject._dump_metadata(self.algorithm_metadata)

    # ................................
    def load_algorithm_metadata(self, new_metadata):
        """Load algorithm metadata"""
        self.algorithm_metadata = LMObject._load_metadata(new_metadata)

    # ................................
    def add_algorithm_metadata(self, new_metadata_dict):
        """Add algorithm metadata"""
        self.algorithm_metadata = LMObject._add_metadata(
            new_metadata_dict, existing_metadata_dict=self.algorithm_metadata)

    # ................................
    def fill_with_defaults(self):
        """Fill the parameters for this algorithm object with defaults."""
        self._parameters = {}
        for key, constraints in self._parameter_constraints.items():
            self._parameters[key] = constraints['default']

    # ................................
    def find_param_name_type(self, name):
        """Find the correct, case-sensitive, property name."""
        p_name = self._get_param_key(name)
        if p_name is not None:
            p_type = self._parameter_constraints[p_name]['type']
            return p_name, p_type
        return None, None

    # ................................
    def _get_param_key(self, name):
        """Find the correct case-sensitive string for this property.

        Args:
            name: The parameter to find

        Returns:
            str - a correctly-capitalized string for this parameter
            None - If it is not valid for this algorithm
        """
        if name in self._parameter_constraints:
            return name

        for key in list(self._parameter_constraints.keys()):
            if key.lower() == name.lower():
                return key
        return None

    # ................................
    def set_parameter(self, name, val):
        """Attempt to set an algorithm parameter.

        Args:
            name: The case-insensitive parameter name to set
            val: The new value for the parameter

        Raises:
            InvalidParameterError - Thrown if the parameter is not valid for
                the algorithm
            InvalidValueError - Thrown if a str parameter is set to an unknown
                option
            WrongTypeError - Thrown if the value does not match the expected
                type
            OutOfRangeError - Thrown if a numerical parameter is set outside
                the acceptable value range.
        """
        if self._parameter_constraints:
            param_name = self._get_param_key(name)
            if param_name:
                constraints = self._parameter_constraints[param_name]
                if val is None:
                    val = constraints['default']

                if constraints['type'] == str:
                    if isinstance(val, str):
                        val = val.lower()
                        try:
                            val_options = constraints['options']
                        except KeyError:
                            pass
                        else:
                            if val not in val_options:
                                raise InvalidValueError(
                                    'Invalid value {}; valid vals {}'.format(
                                        val, val_options))
                    else:
                        raise WrongTypeError(
                            'Expected str, received {} - type {}'.format(
                                val, type(val)))
                # Check valid float value
                elif constraints['type'] == float:
                    if isinstance(val, (float, int)):
                        self._check_numeric_value(val, constraints)
                    else:
                        raise WrongTypeError(
                            'Expected float, Received {} - type {}'.format(
                                val, type(val)))

                # Check valid int value
                elif constraints['type'] == int:
                    if (isinstance(val, (float, int)) and (val % 1 == 0)):
                        self._check_numeric_value(val, constraints)
                    else:
                        raise WrongTypeError(
                            'Expected int, Received {} - type {}'.format(
                                val, type(val)))
                # Successfully ran the gauntlet
                self._parameters[param_name] = val
            else:
                # If didn't find name and return
                raise InvalidParameterError(
                    'Invalid parameter {}'.format(name))
        else:
            raise LMError('Error: parameterConstraints not initialized')

    # ................................
    @staticmethod
    def _check_numeric_value(val, constraints):
        """Check the value against the constraints for the parameter.

        Raises:
            OutOfRangeError - Raised if the value is out of range
        """
        if constraints['min'] is not None:
            if val < constraints['min']:
                raise OutOfRangeError(
                    'Value {} must be greater than {}'.format(
                        val, constraints['min']))
        if constraints['max'] is not None:
            if val > constraints['max']:
                raise OutOfRangeError(
                    'Value {} must be less than {}'.format(
                        val, constraints['max']))

    # ................................
    def get_parameter_value(self, name):
        """Get the value of a parameter."""
        if name in self._parameters:
            return self._parameters[name]
        # If didn't find name and return
        raise InvalidParameterError('Unknown parameter {}'.format(name))

    # ................................
    def has_parameter(self, name):
        """Return boolean indicating if this algorithm object has the parameter.

        Args:
            name: string to check for existence as a parameter name

        Returns:
            boolean indicating if the name is a parameter for this alogorithm
        """
        if name in self._parameters:
            return True
        # If didn't find name and return
        return False

    # ................................
    def equals(self, other_algorithm):
        """Check to see if this algorithm equals another.

        Args:
            other_algorithm: An algorithm object to compare with.
        """
        if isinstance(other_algorithm, Algorithm):
            return (self.code == other_algorithm.code and
                    self.parameters == other_algorithm.parameters)
        return False

    # ................................
    def get_dictionary(self):
        """Get the algorithm as a dictionary."""
        return {
            'algorithm_code': self.code,
            'parameters': [
                {
                    'name': param,
                    'value': str(self.parameters[param])
                } for param in self.parameters.keys()]
            }
