"""
@summary: Module containing file creation functions
@license: gpl2
@copyright: Copyright (C) 2019, University of Kansas Center for Research

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
import os
import glob
import sys
import unicodecsv

from LmCommon.common.lmconstants import LMFormat, ENCODING
# ...............................................
def readyFilename(fullfilename, overwrite=False):
    """
    @summary: On existing file, 
                     if overwrite true: delete and return true on success
                                                             raise LmException on failure
                                     false: return false
                 Non-existing file:
                     create parent directories if needed
                     return true if parent directory exists
                     raise Exception if parent directory does not exist
    """
    if fullfilename is None:
        raise Exception('Full filename is None')
    
    if os.path.exists(fullfilename):
        if overwrite:
            success, msg = deleteFile(fullfilename)
            if not success:
                raise Exception('Unable to delete {}'.format(fullfilename))
            else:
                return True
        else:
            return False
    else:
        pth, basename = os.path.split(fullfilename)
        try:
            os.makedirs(pth, 0775)
        except:
            pass
            
        if os.path.isdir(pth):
            return True
        else:
            raise Exception('Failed to create directories {}'.format(pth))

# ...............................................
def deleteFile(fname, deleteDir=False):
    """
    @summary: Delete the file if it exists, delete enclosing directory if 
                 it is now empty, print only warning if fails.  If filename is a 
                 shapefile (ends in '.shp'), delete all other files that comprise
                 the shapefile.
    """
    success = True
    msg = ''
    if fname is None:
        msg = 'Cannot delete file \'None\''
    else:
        pth, basename = os.path.split(fname)
        if fname is not None and os.path.exists(fname):
            base, ext = os.path.splitext(fname)
            if  ext == LMFormat.SHAPE.ext:
                similarFnames = glob.glob(base + '.*')
                try:
                    for simfname in similarFnames:
                        simbase, simext = os.path.splitext(simfname)
                        if simext in LMFormat.SHAPE.getExtensions():
                            os.remove(simfname)
                except Exception, e:
                    success = False
                    msg = 'Failed to remove {}, {}'.format(simfname, str(e))
            else:
                try:
                    os.remove(fname)
                except Exception, e:
                    success = False
                    msg = 'Failed to remove {}, {}'.format(fname, str(e))
            if deleteDir and len(os.listdir(pth)) == 0:
                try:
                    os.removedirs(pth)
                except Exception, e:
                    success = False
                    msg = 'Failed to remove {}, {}'.format(pth, str(e))
    return success, msg

# .............................................................................
def get_unicodecsv_reader(datafile, delimiter):
    '''
    @summary: Get a CSV reader that can handle encoding
    '''
    f = None  
    unicodecsv.field_size_limit(sys.maxsize)
    try:
        f = open(datafile, 'rb')
        reader = unicodecsv.reader(f, delimiter=delimiter, encoding=ENCODING)
    except Exception, e:
        raise Exception('Failed to read or open {}, ({})'
                             .format(datafile, str(e)))
    return reader, f

# .............................................................................
def get_unicodecsv_writer(datafile, delimiter, doAppend=True):
    '''
    @summary: Get a CSV writer that can handle encoding
    '''
    unicodecsv.field_size_limit(sys.maxsize)
    if doAppend:
        mode = 'ab'
    else:
        mode = 'wb'
        
    readyFilename(datafile, overwrite=True)
    try:
        f = open(datafile, mode) 
        writer = unicodecsv.writer(f, delimiter=delimiter, encoding=ENCODING)

    except Exception, e:
        raise Exception('Failed to read or open {}, ({})'
                             .format(datafile, str(e)))
    return writer, f

