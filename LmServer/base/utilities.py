#!/usr/bin/python
# -*- coding: UTF-8 -*-
"""Lifemapper utility functions
"""
import subprocess

from LmCommon.common.lmconstants import (
    ISO_8601_TIME_FORMAT_FULL, ISO_8601_TIME_FORMAT_TRUNCATED, LM_USER,
    YMD_HH_MM_SS, ENCODING)
from LmCommon.common.time import gmt, LmTime


# ...........................................
def format_time_human(d_time=None):
    """Gets the time in human readable format

    Args:
        d_time: Date time in mjd format

    Return:
        String
    """
    if d_time is None:
        d_time = gmt()
    elif d_time == 0:
        return ''
    else:
        d_time = LmTime(dtime=d_time)
    return d_time.strftime('%Y-%m-%d %H:%M:%S')


# ...........................................
def get_mjd_time_from_iso_8601(d_time):
    """Get mjd time from iso 8601"""
    try:
        return LmTime.strptime(d_time, ISO_8601_TIME_FORMAT_FULL).mjd
    except Exception:
        try:
            return LmTime.strptime(d_time, YMD_HH_MM_SS).mjd
        except Exception:
            try:
                return LmTime.strptime(
                    d_time, ISO_8601_TIME_FORMAT_TRUNCATED).mjd
            except Exception:
                return gmt().mjd

# .............................................................................
def _get_current_user():
    """Get the system user running python"""
    cmd = '/usr/bin/whoami'
    info, _ = subprocess.Popen(
        cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE
        ).communicate()
    usr = info.split()[0]
    if isinstance(usr, bytes):
        usr = usr.decode(ENCODING)
    return usr


# .............................
def is_lm_user():
    """Determine if current user is the non-root Lifemapper user"""
    return _get_current_user() == LM_USER


# .............................
def is_root_user():
    """Determine if current user is the root Lifemapper user"""
    return _get_current_user() == 'root'
