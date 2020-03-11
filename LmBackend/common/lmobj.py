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
    @staticmethod
    def get_line_num():
        """Get the current line number
        """
        return inspect.currentframe().f_back.f_lineno

    # ..........................
    def get_location(self, line_num=None):
        """Get the current location
        """
        loc = '{}.{}'.format(__name__, self.__class__.__name__)
        if line_num:
            loc += ' Line {}'.format(line_num)
        return loc

    # ..........................
    @classmethod
    def ready_filename(cls, full_filename, overwrite=False):
        """Prepare a file location for writing by creating needed parent dirs.

        Args:
            full_filename (str): The file location to prepare.
            overwrite (bool): If true, deletes existing file.  If false,
                returns False.
        """
        if full_filename is None:
            raise LMError('Full filename is None')

        if os.path.exists(full_filename):
            if overwrite:
                success, _ = cls.delete_file(full_filename)
                if not success:
                    raise LMError('Unable to delete {}'.format(full_filename))
                return True

            print(('File {} exists, overwrite=False'.format(full_filename)))
            return False

        pth, _ = os.path.split(full_filename)

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
    def delete_file(cls, file_name, delete_dir=False):
        """Delete the file if it exists and parent directory if it is empty.

        Note:
            If file path is a shapefile extension (.shp), delete all other
                files that comprise the shapefile.
        """
        success = True
        msg = ''
        if file_name is None:
            msg = 'Cannot delete file \'None\''
        else:
            pth, _ = os.path.split(file_name)
            if file_name is not None and os.path.exists(file_name):
                base, ext = os.path.splitext(file_name)
                if ext == LMFormat.SHAPE.ext:
                    similar_file_names = glob.glob(base + '.*')
                    try:
                        for simfname in similar_file_names:
                            _, simext = os.path.splitext(simfname)
                            if simext in LMFormat.SHAPE.get_extensions():
                                os.remove(simfname)
                    except Exception as err:
                        success = False
                        msg = 'Failed to remove {}, {}'.format(
                            simfname, str(err))
                else:
                    try:
                        os.remove(file_name)
                    except Exception as err:
                        success = False
                        msg = 'Failed to remove {}, {}'.format(
                            file_name, str(err))
                if delete_dir and len(os.listdir(pth)) == 0:
                    try:
                        os.removedirs(pth)
                    except Exception as err:
                        success = False
                        msg = 'Failed to remove {}, {}'.format(pth, str(err))
        return success, msg

    # ..........................
    @staticmethod
    def _add_metadata(new_metadata_dict, existing_metadata_dict=None):
        if existing_metadata_dict is None:
            existing_metadata_dict = {}
        for key, val in new_metadata_dict.items():
            try:
                existing_val = existing_metadata_dict[key]
            except Exception:
                existing_metadata_dict[key] = val
            else:
                # if metadata exists and is ...
                if isinstance(existing_val, list):
                    # a list, add to it
                    if isinstance(val, list):
                        new_val = list(set(existing_val.extend(val)))
                        existing_metadata_dict[key] = new_val

                    else:
                        new_val = list(set(existing_val.append(val)))
                        existing_metadata_dict[key] = new_val
                else:
                    # not a set, replace it
                    existing_metadata_dict[key] = val
        return existing_metadata_dict

    # ..........................
    @staticmethod
    def _dump_metadata(metadata_dict):
        metadata_str = None
        if metadata_dict:
            metadata_str = json.dumps(metadata_dict)
        return metadata_str

    # ..........................
    @staticmethod
    def _load_metadata(new_metadata):
        """Read metadata into a dictionary

        Args:
            new_metadata: dictionary or JSON object of metadata

        Returns:
            a dictionary of metadata
        """
        obj_metadata = {}
        if new_metadata is not None:
            if isinstance(new_metadata, dict):
                obj_metadata = new_metadata
            else:
                try:
                    obj_metadata = json.loads(new_metadata)
                except Exception:
                    print(
                        'Failed to load JSON from type {} object {}'.format(
                            type(new_metadata), new_metadata))
        return obj_metadata


# .............................................................................
class LMError(Exception, LMObject):
    """Base class for exceptions in the lifemapper project.
    """

    # ..........................
    def __init__(self, *args, do_trace=False, line_num=None, **kwargs):
        """Constructor for LMError

        Args:
            *args: Any positional agruments sent to this constructor
            do_trace (bool): Should a traceback be attached to the exception
            line_num (int): A line number to attach to this exception
            **kwargs: Any additional keyword arguements sent to the constructor

        Note:
            Assembles all arguments into Exception.args
        """
        LMObject.__init__(self)
        self.previous_exceptions = []
        list_args = []
        for arg in args:
            if isinstance(arg, Exception):
                self.previous_exceptions.append(arg)
            else:
                list_args.append(arg)

        kw_arg_dict = dict(kwargs)
        if line_num:
            kw_arg_dict['Line number'] = line_num
        kw_arg_dict['Location'] = self.get_location(line_num=line_num)
        if do_trace:
            self.traceback = self.get_traceback()
            kw_arg_dict['Traceback'] = self.traceback
        list_args.append(kw_arg_dict)
        self.args = tuple(list_args)
        Exception.__init__(self, self.args)

    # ..........................
    @staticmethod
    def get_traceback():
        """Get the traceback for this exception
        """
        exc_type, exc_val, this_traceback = sys.exc_info()
        return traceback.format_exception(exc_type, exc_val, this_traceback)


# .............................................................................
class JobError(LMError):
    """Exception class for job failures.
    """

    # ..........................
    def __init__(self, code, msg, *args, do_trace=False, line_num=None,
                 **kwargs):
        """Constructor for LMError

        Args:
            code (int): Job error code
            msg (str): An error message
            *args: Any positional agruments sent to this constructor
            do_trace (bool): Should a traceback be attached to the exception
            line_num (int): A line number to attach to this exception
            **kwargs: Any additional keyword arguements sent to the constructor

        Note:
            Assembles all arguments into Exception.args
        """
        LMError.__init__(
            self, code, msg, *args, do_trace=do_trace, line_num=line_num,
            **kwargs)
        self.code = code
        self.msg = msg
