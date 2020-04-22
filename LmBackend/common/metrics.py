"""Module containing tools for Lifemapper metrics collection

Todo:
    * Consider adding constants for collected metric names
    * Make file name optional or only used in write method
"""
import json
import os

from LmBackend.common.lmobj import LMObject
from LmCommon.common.lmconstants import ENCODING


# .............................................................................
class LmMetricNames:
    """Class containing standard metric names

    Note:
        Other metric names can be used, but these are common metrics
    """
    ALGORITHM_CODE = 'algorithm_code'
    NUMBER_OF_FEATURES = 'num_features'
    OUTPUT_SIZE = 'output_size'
    PROCESS_TYPE = 'process_type'
    RUNNING_TIME = 'running_time'
    SOFTWARE_VERSION = 'software_version'
    STATUS = 'status'


# .............................................................................
class LmMetrics(LMObject):
    """This class is used to track performance metrics.
    """

    # ................................
    def __init__(self, out_file):
        """Metrics constructor

        Args:
            out_file : Write metrics to this location, if None, don't write

        Note:
            This allows you to always collect metrics and only conditionally
                write them.
        """
        self.out_file_name = out_file
        self._metrics = {}

    # ................................
    def add_metric(self, name, value):
        """Add a metric to track

        Args:
            name : The name of the metric to add
            value : The value of the metric
        """
        self._metrics[name] = value

    # ................................
    def get_metric(self, name):
        """Gets the value of a metric if available.

        Args:
            name : The name of the metric to return.

        Returns:
            The value of the metric or None if not found
        """
        if name in self._metrics.keys():
            return self._metrics[name]

        return None

    # ................................
    def get_dir_size(self, dir_name):
        """Get the size of a directory

        Get the size of a directory, presumably for determining output
        footprint.

        Args:
            dir_name : The directory to determine the size of
        """
        total_size = os.path.getsize(dir_name)
        for item in os.listdir(dir_name):
            itempath = os.path.join(dir_name, item)
            if os.path.isfile(itempath):
                total_size += os.path.getsize(itempath)
            elif os.path.isdir(itempath):
                total_size += self.get_dir_size(itempath)
        return total_size

    # ................................
    def get_metrics_dictionary(self):
        """Return a dictionary of metrics.
        """
        return self._metrics

    # ................................
    def write_metrics(self):
        """Write the metrics to the specified file
        """
        if self.out_file_name is not None:
            self.ready_filename(self.out_file_name, overwrite=True)
            with open(self.out_file_name, 'w', encoding=ENCODING) as out_file:
                json.dump(self._metrics, out_file)
