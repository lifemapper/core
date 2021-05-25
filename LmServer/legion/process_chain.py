"""Module containing class for makeflow workflows
"""
import os

from LmBackend.common.cmd import MfRule
from LmCommon.common.lmconstants import LMFormat, ENCODING
from LmCommon.common.time import gmt
from LmServer.base.service_object import ProcessObject
from LmServer.common.data_locator import EarlJr
from LmServer.common.lmconstants import LMFileType


# .........................................................................
class MFChain(ProcessObject):
    """Makeflow chain class."""
    META_CREATED_BY = 'createdBy'
    META_GRIDSET = 'gridsetId'
    META_DESCRIPTION = 'description'
    META_SQUID = 'squid'

    # ................................
    def __init__(self, user_id, dlocation=None, priority=None, metadata=None,
                 status=None, status_mod_time=None, headers=None,
                 mf_chain_id=None):
        """Class used for generating Makeflow document for LM computations.

        Args:
            user_id: Id for the owner of this process
            dlocation: location for Makeflow file
            priority: relative priority for jobs contained within
            metadata: Dictionary of metadata key/values; uses class or
                superclass attribute constants META_* as keys
            headers: Optional list of (header, value) tuples
            mf_chain_id: Database unique identifier
        """
        self.jobs = []
        self.targets = []
        self.headers = []
        if headers is not None:
            self.add_headers(headers)
        self._dlocation = dlocation
        self._user_id = user_id
        self.priority = priority
        self.makeflow_metadata = {}
        self.load_makeflow_metadata(metadata)
        ProcessObject.__init__(
            self, obj_id=mf_chain_id, process_type=None, status=status,
            status_mod_time=status_mod_time)

    # ................................
    def dump_makeflow_metadata(self):
        """Dump metadata to string."""
        return super(MFChain, self)._dump_metadata(self.makeflow_metadata)

    # ................................
    def load_makeflow_metadata(self, new_metadata):
        """Load metadata
        """
        self.makeflow_metadata = super(MFChain, self)._load_metadata(
            new_metadata)

    # ................................
    def add_makeflow_metadata(self, new_metadata_dict):
        """Add to metadata
        """
        self.makeflow_metadata = super(MFChain, self)._add_metadata(
            new_metadata_dict, existing_metadata_dict=self.makeflow_metadata)

    # ................................
    def set_id(self, mf_id):
        """Set the database identifier on the object

        Args:
            mf_id: The database id for the object
        """
        self.obj_id = mf_id
        self.set_dlocation()

    # ................................
    def get_id(self):
        """Return the database identifier for the chain
        """
        return self.obj_id

    # ................................
    def get_relative_directory(self):
        """Return the relative data directory for this makeflow process.

        Note:
            - If the object does not have an ID, this returns None
            - This is to organize a sub-workspace within the Makeflow workspace
                for files used by a single workflow
            - MattDaemon will delete this workspace directory when the
                Makeflow completes.
        """
        base_name = None
        self.set_dlocation()
        if self._dlocation is not None:
            _, base_name = os.path.split(self._dlocation)
            rel_dir, _ = os.path.splitext(base_name)
        return rel_dir

    # ................................
    def create_local_dlocation(self):
        """Create an absolute filepath from object attributes
        """
        dloc = None
        if self.obj_id is not None:
            earl_jr = EarlJr()
            dloc = earl_jr.create_filename(
                LMFileType.MF_DOCUMENT, obj_code=self.obj_id,
                usr=self._user_id)
        return dloc

    # ................................
    def get_dlocation(self):
        """Return the data location of this makeflow chain
        """
        self.set_dlocation()
        return self._dlocation

    # ................................
    def set_dlocation(self, dlocation=None):
        """Set the data location of the makeflow
        """
        if self._dlocation is None:
            if dlocation is None:
                dlocation = self.create_local_dlocation()
            self._dlocation = dlocation

    # ................................
    def clear_dlocation(self):
        """Clear the data location
        """
        self._dlocation = None

    # ................................
    def get_arf_filename(self, arf_dir=None, prefix='mf'):
        """Return the temporary dummy filename for indicating completion.

        Args:
            arf_dir: A directory to put the arf file. Else use relative dir
        """
        # TODO: Update with something more specific
        # earl_jr = EarlJr()
        # pth = earl_jr.create_data_path(self._user_id, LMFileType.MF_DOCUMENT)
        # fname = os.path.join(pth, '{}_{}.arf'.format(prefix, self.obj_id))
        if arf_dir is None:
            arf_dir = self.get_relative_directory()
        fname = os.path.join(arf_dir, 'arf',
                             '{}_{}.arf'.format(prefix, self.obj_id))
        return fname

    # ................................
    def get_triage_filename(self, prefix='potato'):
        """Return filename to contain list of temporary dummy (Arf) files.
        """
        # TODO: Do this a different way.  Unfortunately we have to handle this
        #    differently
        if prefix == 'mashedPotato':
            fname = os.path.join(
                prefix, '{}_{}{}'.format(
                    prefix, self.obj_id, LMFormat.TXT.ext))
        else:
            # TODO: Update
            earl_jr = EarlJr()
            pth = earl_jr.create_data_path(
                self._user_id, LMFileType.MF_DOCUMENT)
            fname = os.path.join(
                pth, '{}_{}{}'.format(prefix, self.obj_id, LMFormat.TXT.ext))
        return fname

    # ................................
    def get_triage_output_name(self, prefix='mashed'):
        """Return the triage output name."""
        return os.path.join(
            prefix, '{}_{}{}'.format(prefix, self.obj_id, LMFormat.TXT.ext))

    # ................................
    def get_user_id(self):
        """Return the User id."""
        return self._user_id

    # ................................
    def set_user_id(self, usr):
        """Set the user id on the object."""
        self._user_id = usr

    # ................................
    def _add_job_command(self, outputs, cmd, dependencies=None, comment=''):
        """Add a job command to the document.

        Args:
            outputs: A list of output files created by this job
            cmd: The command to execute
            dependencies: A list of dependencies (files that must exist before
                this job can run
        """
        if dependencies is None:
            dependencies = []
        job = "# {comment}\n{outputs}: {dependencies}\n\t{cmd}\n".format(
            outputs=' '.join(outputs), cmd=cmd, comment=comment,
            dependencies=' '.join(dependencies))
        self.jobs.append(job)
        # Add the new targets to self.targets
        # NOTE: Uncomment this version if removing absolute paths causes
        #    problems
        # self.targets.extend(outputs)
        for target in outputs:
            if not os.path.isabs(target):
                self.targets.append(target)

    # ................................
    def add_commands(self, rule_list):
        """Add a list of commands to the Makeflow document.

        Args:
            rule_list: A list of MfRule objects
        """
        # Check if this is just a single tuple, if so, make it a list
        if isinstance(rule_list, MfRule):
            rule_list = [rule_list]

        # For each tuple in the list
        for rule in rule_list:
            # If dependency is not absolute path
            deps = [
                d for d in rule.dependencies
                if d is not None and not os.path.isabs(d)]
            targets = rule.targets
            cmd = rule.command
            comment = rule.comment

            # Check to see if these targets are already defined by creating a
            #    new list of targets that are not in self.targets
            new_targets = [t for t in targets if t not in self.targets]

            # If there are targets that have not been defined before
            if len(new_targets) > 0:
                self._add_job_command(
                    new_targets, cmd, dependencies=deps, comment=comment)

    # ................................
    def add_headers(self, headers):
        """Add headers to the document.

        Args:
            headers: A list of (header, value) tuples
        """
        if isinstance(headers, tuple):
            headers = [headers]
        self.headers.extend(headers)

    # ................................
    def write(self, filename=None):
        """Write the document to the specified location.

        Args:
            filename: The file location to write this document

        Raises:
            ValueError: If no jobs exist to be computed (list is right type,
                empty is bad value)
            IOError: if there is a problem writing to a location
        """
        if not self.jobs:
            raise ValueError("No jobs to be computed, fail for empty document")
        if filename is None:
            filename = self.get_dlocation()
        self.ready_filename(filename, overwrite=True)
        with open(filename, 'w', encoding=ENCODING) as out_file:
            for header, value in self.headers:
                out_file.write(
                    '{header}={value}\n'.format(header=header, value=value))
            for job in self.jobs:
                # These have built-in newlines
                out_file.write(job)

    # ................................
    def update_status(self, status, mod_time=gmt().mjd):
        """Update the status of the workflow."""
        ProcessObject.update_status(self, status, mod_time)
