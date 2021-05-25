"""Module containing time functions for Lifemapper

Note:
    * Replacement for mx.DateTime calls for Python 3

TODO:
    * Allow both struct_time and datetime?
    * Maybe just use datetime
    * Make sure all attributes match internal data object for time
    * Need to implement math functions for time deltas
    * Less than / greater than / etc
"""
import datetime as DT


MJD_EPOCH_TIME = DT.datetime(1858, 11, 17, tzinfo=DT.timezone.utc)
SECONDS_IN_DAY = 86400


# .............................................................................
class LmTime:
    """Subclass for time for Lifemapper purposes"""

    # ...........................
    def __init__(self, dtime=None):
        """Constructor that takes an optional time.struct_time object."""
        if dtime is None:
            self._time = DT.datetime.now(DT.timezone.utc)
        elif isinstance(dtime, LmTime):
            self._time = dtime._time
        elif isinstance(dtime, DT.datetime):
            if not dtime.tzinfo:
                dtime.replace(tzinfo=DT.timezone.utc)
            self._time = dtime
        else:
            raise Exception(
                'Optional LmTime argument must be a datetime.datetime object')

    # ...........................
    def __float__(self):
        """Returns a floating point value for the LmTime object"""
        return float(
            '{}{:02d}{:02d}.{:02d}{:02d}{:02d}'.format(
                self._time.tm_year, self._time.tm_mon,
                self._time.tm_mday, self._time.tm_hour, self._time.tm_min,
                self._time.tm_sec))

    # ...........................
    @classmethod
    def from_mjd(cls, mjd_time):
        """Return an LmTime object for the MJD time."""
        num_days = int(mjd_time)
        num_seconds = SECONDS_IN_DAY * (mjd_time - num_days)
        return cls(
            dtime=MJD_EPOCH_TIME + DT.timedelta(
                days=num_days, seconds=num_seconds))

    # ...........................
    @classmethod
    def strptime(cls, time_string, time_format):
        """Return an LmTime object for the time string.

        Args:
            cls: Call with Class
            time_string: string in the format YYYY-MM-DDThh:mm:ss
        Note:
            Year:
                  YYYY (eg 1997)
               Year and month:
                  YYYY-MM (eg 1997-07)
               Complete date:
                  YYYY-MM-DD (eg 1997-07-16)
               Complete date plus hours and minutes:
                  YYYY-MM-DDThh:mmTZD (eg 1997-07-16T19:20+01:00)
               Complete date plus hours, minutes and seconds:
                  YYYY-MM-DDThh:mm:ssTZD (eg 1997-07-16T19:20:30+01:00)
               Complete date plus hours, minutes, seconds and a decimal fraction 
               of a second
                  YYYY-MM-DDThh:mm:ss.sTZD (eg 1997-07-16T19:20:30.45+01:00)
        """
        parts = time_string.split('T')
        dstr = parts[0]
        dparts = dstr.split('-')
        try:
            yr = int(dparts[0])
        except:
            yr = None
        else:
            try:
                mo = int(dparts[1])
            except:
                mo = None
            else:
                try:
                    dy = int(dparts[2])
                except:
                    dy = None
        if None in (yr, mo, dy):
            raise Exception('Year, month and day cannot be parsed from {}'
                            .format(time_string))
        hr = mn = sc = 0
        try:
            tstr = parts[1]
        except:
            pass
        else:
            tparts = tstr.split(':')            
            try:
                hr = int(tparts[0])
            except:
                pass
            else:
                try:
                    mn = int(tparts[1])
                except:
                    pass
                else:
                    try:
                        sc = int(dparts[2])
                    except:
                        pass
        d_time = DT.datetime(yr, mo, dy, hour=hr, minute=mn, second=sc, 
                             tzinfo=DT.timezone.utc)
        return cls(dtime=d_time)

    # ...........................
    @property
    def day(self):
        """Return the day value of the time"""
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
        """Return the month value of the time"""
        return self._time.month

    # ...........................
    def strftime(self, format_str=None):
        """Format time as specified by format string"""
        return self._time.strftime(format_str)

    # ...........................
    def tuple(self):
        """Return the time as tuple."""
        return tuple(self._time)

    # ...........................
    @property
    def year(self):
        """Return the year value of the time"""
        return self._time.year


# .............................................................................
def from_timestamp(ticks):
    """Return an aware LmTime object from timestamp ticks"""
    return LmTime(
        dtime=DT.datetime.fromtimestamp(ticks, tzinfo=DT.timezone.utc))


# .............................................................................
def gmt():
    """Return an aware LmTime object for GMT"""
    return LmTime(DT.datetime.now(DT.timezone.utc))


# .............................................................................
def localtime():
    """Return a naive LmTime object for datetime.localtime.  The object cannot
    be compared to LmTime timezone-aware objects, the default"""
    # returns a naive object
    return LmTime(dtime=DT.datetime.now())


# .............................................................................
def time_delta_from_mjd(mjd_value):
    """Get a time delta from an mjd value"""
    num_seconds = (mjd_value - int(mjd_value)) * SECONDS_IN_DAY
    return DT.timedelta(days=int(mjd_value), seconds=num_seconds)
