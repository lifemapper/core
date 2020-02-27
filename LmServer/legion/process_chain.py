"""
"""
import os

from LmBackend.common.cmd import MfRule
from LmCommon.common.lmconstants import LMFormat
from LmCommon.common.time import gmt
from LmServer.base.service_object import ProcessObject
from LmServer.common.data_locator import EarlJr
from LmServer.common.lmconstants import LMFileType


# .........................................................................
class MFChain(ProcessObject):
    # .............................................................................
    META_CREATED_BY = 'createdBy'
    META_GRIDSET = 'gridsetId'
    META_DESCRIPTION = 'description'
    META_SQUID = 'squid'

    # .............................................................................
    def __init__(self, user_id, dlocation=None, priority=None, metadata=None,
                 status=None, status_mod_time=None, headers=None,
                 mf_chain_id=None):
        """
        @summary Class used to generate a Makeflow document with Lifemapper 
                 computational jobs
        @copydoc LmServer.base.service_object.ProcessObject::__init__()
        @param user_id: Id for the owner of this process
        @param dlocation: location for Makeflow file
        @param priority: relative priority for jobs contained within
        @param metadata: Dictionary of metadata key/values; uses class or 
                         superclass attribute constants META_* as keys
        @param headers: Optional list of (header, value) tuples
        @param mfChainId: Database unique identifier
        """
        self.jobs = []
        self.targets = []
        self.headers = []
        if headers is not None:
            self.addHeaders(headers)
        self._dlocation = dlocation
        self._user_id = user_id
        self.priority = priority
        self.mfMetadata = {}
        self.loadMfMetadata(metadata)
        ProcessObject.__init__(self, obj_id=mf_chain_id, process_type=None,
                               status=status, status_mod_time=status_mod_time)

    # ...............................................
    def dumpMfMetadata(self):
        return super(MFChain, self)._dump_metadata(self.mfMetadata)

    # ...............................................
    def loadMfMetadata(self, newMetadata):
        self.mfMetadata = super(MFChain, self)._load_metadata(newMetadata)

    # ...............................................
    def addMfMetadata(self, newMetadataDict):
        self.mfMetadata = super(MFChain, self)._add_metadata(newMetadataDict,
                                   existingMetadataDict=self.mtxColMetadata)

    # ...............................................
    def set_id(self, mfid):
        """
        @summary: Sets the database id on the object, and sets the 
                  dlocation of the file if it is None.
        @param mfid: The database id for the object
        """
        self.objId = mfid
        self.set_dlocation()

    # ...............................................
    def get_id(self):
        """
        @summary Returns the database id from the object table
        @return integer database id of the object
        """
        return self.objId

    # .............................................................................
    # Superclass methods overridden
    # # .............................................................................
    # ...............................................

    # ...............................................
    def getRelativeDirectory(self):
        """
        @summary: Return the relative directory for data associated with this 
                  Makeflow process
        @note: If the object does not have an ID, this returns None
        @note: This is to organize a sub-workspace within the Makeflow workspace 
               for files used by a single workflow
        @note: MattDaemon will delete this workspace directory when the
            Makeflow completes.
        """
        basename = None
        self.set_dlocation()
        if self._dlocation is not None:
            _, basename = os.path.split(self._dlocation)
            reldir, _ = os.path.splitext(basename)
        return reldir

    def create_local_dlocation(self):
        """
        @summary: Create an absolute filepath from object attributes
        @note: If the object does not have an ID, this returns None
        """
        dloc = None
        if self.objId is not None:
            earl_jr = EarlJr()
            dloc = earl_jr.create_filename(LMFileType.MF_DOCUMENT,
                                              objCode=self.objId,
                                              usr=self._user_id)
        return dloc

    def get_dlocation(self):
        self.set_dlocation()
        return self._dlocation

    def set_dlocation(self, dlocation=None):
        """
        @note: Does NOT override existing dlocation, use clear_dlocation for that
        """
        if self._dlocation is None:
            if dlocation is None:
                dlocation = self.create_local_dlocation()
            self._dlocation = dlocation

    def clear_dlocation(self):
        self._dlocation = None

    # ...............................................
    def getArfFilename(self, arfDir=None, prefix='mf'):
        """
        @summary: Return temporary dummy filename written to indicate completion  
                  of this MFChain.
        @param arfDir: A directory to put the arf file. Else use relative dir
        """
        # TODO: Update with something more specific
        # earl_jr = EarlJr()
        # pth = earl_jr.create_data_path(self._user_id, LMFileType.MF_DOCUMENT)
        # fname = os.path.join(pth, '{}_{}.arf'.format(prefix, self.objId))
        if arfDir is None:
            arfDir = self.getRelativeDirectory()
        fname = os.path.join(arfDir, 'arf',
                             '{}_{}.arf'.format(prefix, self.objId))
        return fname

    # ...............................................
    def getTriageFilename(self, prefix='potato'):
        """
        @summary: Return filename to contain list of temporary dummy (Arf) files.
                  This file is used as input for triage to jettison failures
                  from inputs to another MF.
        """
        # TODO: Do this a different way.  Unfortunately we have to handle this differently
        if prefix == 'mashedPotato':
            fname = os.path.join(prefix, '{}_{}{}'.format(prefix, self.objId, LMFormat.TXT.ext))
        else:
            # TODO: Update
            earl_jr = EarlJr()
            pth = earl_jr.create_data_path(self._user_id, LMFileType.MF_DOCUMENT)
            fname = os.path.join(pth, '{}_{}{}'.format
                              (prefix, self.objId, LMFormat.TXT.ext))
        return fname

    # ...............................................
    def getTriageOutputname(self, prefix='mashed'):
        """
        @summary: Return filename to contain list of temporary dummy (Arf) files.
                  This file is used as input for triage to jettison failures
                  from inputs to another MF.
        """
        # TODO: Update
        # earl_jr = EarlJr()
        # pth = earl_jr.create_data_path(self._user_id, LMFileType.MF_DOCUMENT)
        # fname = os.path.join(pth, '{}_{}{}'.format
        #                     (prefix, self.objId, LMFormat.TXT.ext))
        fname = os.path.join(prefix, '{}_{}{}'.format(prefix, self.objId, LMFormat.TXT.ext))
        return fname

    # ...............................................
    def get_user_id(self):
        """
        @summary Gets the User id
        @return The User id
        """
        return self._user_id

    def setUserId(self, usr):
        """
        @summary: Sets the user id on the object
        @param usr: The user id for the object
        """
        self._user_id = usr

    # ...........................
    def _addJobCommand(self, outputs, cmd, dependencies=[], comment=''):
        """
        @summary: Adds a job command to the document
        @param outputs: A list of output files created by this job
        @param cmd: The command to execute
        @param dependencies: A list of dependencies (files that must exist before 
                                this job can run
        """
        job = "# {comment}\n{outputs}: {dependencies}\n\t{cmd}\n".format(
           outputs=' '.join(outputs),
           cmd=cmd, comment=comment,
           dependencies=' '.join(dependencies))
        self.jobs.append(job)
        # Add the new targets to self.targets
        # NOTE: Uncomment this version if removing absolute paths causes problems
        # self.targets.extend(outputs)
        for target in outputs:
            if not os.path.isabs(target):
                self.targets.append(target)

    # ...........................
    def addCommands(self, ruleList):
        """
        @summary: Adds a list of commands to the Makeflow document
        @param ruleList: A list of MfRule objects
        """
        # Check if this is just a single tuple, if so, make it a list
        if isinstance(ruleList, MfRule):
            ruleList = [ruleList]

        # For each tuple in the list
        for rule in ruleList:
            # If dependency is not absolute path
            deps = [d for d in rule.dependencies if d is not None and not os.path.isabs(d)]
            targets = rule.targets
            cmd = rule.command
            comment = rule.comment

            # Check to see if these targets are already defined by creating a new
            #    list of targets that are not in self.targets
            newTargets = [t for t in targets if t not in self.targets]

            # If there are targets that have not been defined before
            if len(newTargets) > 0:
                self._addJobCommand(
                    newTargets, cmd, dependencies=deps, comment=comment)

    # ...........................
    def addHeaders(self, headers):
        """
        @summary: Adds headers to the document
        @param headers: A list of (header, value) tuples
        """
        if isinstance(headers, tuple):
            headers = [headers]
        self.headers.extend(headers)

    # ...........................
    def write(self, filename=None):
        """
        @summary: Write the document to the specified location
        @param filename: The file location to write this document
        @raise ValueError: If no jobs exist to be computed (list is right type, 
                              empty is bad value)
        @note: May fail with IOError if there is a problem writing to a location
        """
        if not self.jobs:
            raise ValueError("No jobs to be computed, fail for empty document")
        if filename is None:
            filename = self.get_dlocation()
        self.ready_filename(filename, overwrite=True)
        with open(filename, 'w') as outF:
            for header, value in self.headers:
                outF.write("{header}={value}\n".format(header=header, value=value))
            for job in self.jobs:
                # These have built-in newlines
                outF.write(job)

    # ...............................................
    def update_status(self, status, mod_time=gmt().mjd):
        """
        @copydoc LmServer.base.service_object.ProcessObject::update_status()
        """
        ProcessObject.update_status(self, status, mod_time)
