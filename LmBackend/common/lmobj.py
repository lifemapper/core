"""Module containing the base Lifemapper object class.
"""
import glob
import inspect
import json
import os
import sys
import traceback

from LmCommon.common.lmconstants import LMFormat


# ............................................................................
class LMObject:
    """Base class for all objects in the Lifemapper project.
    """
    # ..........................
    def getLineno(self):
        return inspect.currentframe().f_back.f_lineno

    # ..........................
    def getModuleName(self):
        return '{}.{}'.format(__name__, self.__class__.__name__)

    # ..........................
    def getLocation(self, lineno=None):
        loc = '{}.{}'.format(__name__, self.__class__.__name__)
        if lineno:
            loc += ' Line {}'.format(lineno)
        return loc

    # ..........................
    @classmethod
    def ready_filename(cls, fullfilename, overwrite=False):
        """Prepare a file location for writing by creating needed parent dirs.

        Args:
            fullfilename (str): The file location to prepare.
            overwrite (bool): If true, deletes existing file.  If false,
                returns False.
        """
        if fullfilename is None:
            raise LMError('Full filename is None')

        if os.path.exists(fullfilename):
            if overwrite:
                success, _ = cls.deleteFile(fullfilename)
                if not success:
                    raise LMError('Unable to delete {}'.format(fullfilename))
                return True
            else:
                print(('File {} exists, overwrite=False'.format(fullfilename)))
                return False
        else:
            pth, _ = os.path.split(fullfilename)

            # If the file path is in cwd we don't need to create directories
            if len(pth) == 0:
                return True

            try:
                os.makedirs(pth, 0o775)
            except IOError:
                pass

            if os.path.isdir(pth):
                return True

            # Else, fail
            raise LMError('Failed to create directories {}'.format(pth))

    # ..........................
    @classmethod
    def deleteFile(cls, fname, deleteDir=False):
        """Delete the file if it exists and parent directory if it is empty.

        Note:
            If file path is a shapefile extension (.shp), delete all other
                files that comprise the shapefile.
        """
        success = True
        msg = ''
        if fname is None:
            msg = 'Cannot delete file \'None\''
        else:
            pth, _ = os.path.split(fname)
            if fname is not None and os.path.exists(fname):
                base, ext = os.path.splitext(fname)
                if ext == LMFormat.SHAPE.ext:
                    similarFnames = glob.glob(base + '.*')
                    try:
                        for simfname in similarFnames:
                            _, simext = os.path.splitext(simfname)
                            if simext in LMFormat.SHAPE.getExtensions():
                                os.remove(simfname)
                    except Exception as e:
                        success = False
                        msg = 'Failed to remove {}, {}'.format(
                            simfname, str(e))
                else:
                    try:
                        os.remove(fname)
                    except Exception as e:
                        success = False
                        msg = 'Failed to remove {}, {}'.format(fname, str(e))
                if deleteDir and len(os.listdir(pth)) == 0:
                    try:
                        os.removedirs(pth)
                    except Exception as e:
                        success = False
                        msg = 'Failed to remove {}, {}'.format(pth, str(e))
        return success, msg

    # ..........................
    def _addMetadata(self, newMetadataDict, existingMetadataDict={}):
        for key, val in newMetadataDict.items():
            try:
                existingVal = existingMetadataDict[key]
            except Exception:
                existingMetadataDict[key] = val
            else:
                # if metadata exists and is ...
                if type(existingVal) is list:
                    # a list, add to it
                    if type(val) is list:
                        newVal = list(set(existingVal.extend(val)))
                        existingMetadataDict[key] = newVal

                    else:
                        newVal = list(set(existingVal.append(val)))
                        existingMetadataDict[key] = newVal
                else:
                    # not a set, replace it
                    existingMetadataDict[key] = val
        return existingMetadataDict

    # ..........................
    def _dumpMetadata(self, metadataDict):
        metadataStr = None
        if metadataDict:
            metadataStr = json.dumps(metadataDict)
        return metadataStr

    # ..........................
    def _loadMetadata(self, newMetadata):
        """
        @note: Adds to dictionary or modifies values for existing keys
        """
        objMetadata = {}
        if newMetadata is not None:
            if isinstance(newMetadata, dict):
                objMetadata = newMetadata
            else:
                try:
                    objMetadata = json.loads(newMetadata)
                except Exception as e:
                    print(
                        'Failed to load JSON from type {} object {}'.format(
                            type(newMetadata), newMetadata))
        return objMetadata


# .............................................................................
class LMError(Exception, LMObject):
    """Base class for exceptions in the lifemapper project.
    """
    # ..........................
    def __init__(self, currargs=None, prevargs=None, lineno=None,
                 doTrace=False, logger=None):
        """
        @todo: Exception will change in Python 3.0: update this.  
                 args will no longer exist, message can be any object
        @summary Constructor for the LMError class
        @param currargs: Current arguments (sequence or single string)
        @param prevargs: (optional) sequence of previous arguments for exception
                                being wrapped by LMError
        """
        super(LMError, self).__init__()
        self.lineno = lineno

        allargs = []
        if doTrace:
            sysinfo = sys.exc_info()
            tb = sysinfo[2]
            if tb is not None:
                tbargs = traceback.format_tb(tb)
            else:
                tbargs = [str(sysinfo)]

            for r in tbargs:
                allargs.append(r)

        if isinstance(currargs, (list, tuple)):
            allargs.extend(currargs)
        elif currargs is not None:
            allargs.append(currargs)

        if isinstance(prevargs, (list, tuple)):
            allargs.extend(prevargs)
        elif prevargs is not None:
            allargs.append(prevargs)
        self.args = tuple(allargs)

    # ..........................
    def __str__(self):
        """
        @summary get the string representation of an LMError
        @return String representation of an LMError
        """
        # Added because the error number was coming through as an integer
        l = [self.getLocation(), self.getTraceback()]
        for x in self.args:
            try:
                sarg = str(x)
            except UnicodeDecodeError as e:
                sarg = 'some unicode arg'
            except Exception as e:
                sarg = 'some other non-string arg ({})'.format(e)
            l.append(sarg)

        return repr('\n'.join(l))

    # ..........................
    def getTraceback(self):
        msg = '\n'
        excType, excValue, thisTraceback = sys.exc_info()
        while thisTraceback :
            framecode = thisTraceback.tb_frame.f_code
            filename = str(framecode.co_filename)
            line_no = str(traceback.tb_lineno(thisTraceback))
            msg += 'Traceback : Line: {}; File: {}\n'.format(line_no, filename)
            thisTraceback = thisTraceback.tb_next
        return msg
