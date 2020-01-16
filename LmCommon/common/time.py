"""Module containing time functions for Lifemapper

Note:
    * Replacement for mx.DateTime calls for Python 3

Todo:
    * Allow both struct_time and datetime?
    * Maybe just use datetime
    * Make sure all attributes match internal data object for time
    * Need to implement math functions for time deltas
    * Less than / greater than / etc
"""
import datetime
import time

MJD_EPOCH_TIME = datetime.datetime(1858, 11, 17, 0, 0, 0, 0)
SECONDS_IN_DAY = 86400

# .............................................................................
class LmTime:
    """Subclass for time for Lifemapper purposes
    """
    # ...........................
    def __init__(self, dtime=None):
        """Constructor that takes an optional time.struct_time object.
        """
        if dtime is None:
            self._time = datetime.datetime.utcnow()
        else:
            self._time = dtime

    # ...........................
    def __float__(self):
        """Returns a floating point value for the LmTime object
        """
        return float(
            '{}{:02d}{:02d}.{:02d}{:02d}{:02d}'.format(
                self._time.tm_year, self._time.tm_mon,
                self._time.tm_mday, self._time.tm_hour, self._time.tm_min,
                self._time.tm_sec))

    # ...........................
    @classmethod
    def from_mjd(cls, mjd_time):
        """Return an LmTime object for the MJD time.
        """
        num_days = int(mjd_time)
        num_seconds = SECONDS_IN_DAY * (mjd_time - num_days)
        return cls(
            dtime=MJD_EPOCH_TIME + datetime.timedelta(
                days=num_days, seconds=num_seconds))

    # ...........................
    @classmethod
    def strptime(cls, time_string, time_format):
        """Return an LmTime object for the time string.
        """
        return cls(dtime=time.strptime(time_string, time_format))

    # ...........................
    @property
    def day(self):
        """Return the day value of the time
        """
        return self._time.day

    # ...........................
    @property
    def mjd(self):
        """Return the modified julian day for the time object

        Return:
            Float - The MJD value for the time
        """
        time_delta = self._time - MJD_EPOCH_TIME
        mjd = time_delta.days + (time_delta.seconds / SECONDS_IN_DAY)
        return mjd

    # ...........................
    @property
    def month(self):
        """Return the month value of the time
        """
        return self._time.month

    # ...........................
    def strftime(self, format_str=None):
        """Format time as specified by format string
        """
        return self._time.strftime(format_str)

    # ...........................
    def tuple(self):
        """Return the time as tuple.
        """
        return tuple(self._time)

    # ...........................
    @property
    def year(self):
        """Return the year value of the time
        """
        return self._time.tm_year

# .............................................................................
def from_timestamp(ticks):
    """Return an LmTime object from timestamp ticks
    """
    return LmTime(dtime=datetime.datetime.fromtimestamp(ticks))

# .............................................................................
def gmt():
    """Return a LmTime object for GMT
    """
    return LmTime(dtime=time.gmtime())

# .............................................................................
def localtime():
    """Return a LmTime object for time.localtime.
    """
    return LmTime(dtime=time.localtime())

# .............................................................................
def time_delta_from_mjd(mjd_value):
    """Get a time delta from an mjd value
    """
    num_seconds = (mjd_value - int(mjd_value)) * SECONDS_IN_DAY
    return datetime.timedelta(days=int(mjd_value), seconds=num_seconds)
