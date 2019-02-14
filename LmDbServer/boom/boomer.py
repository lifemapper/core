"""Module containing boomer class

Note:
    Aimee: We need to add collate to the same workflow as the SDMs. To do this,
        we need to keep track of the single species output files (just the
        stockpile files probably) and send those as inputs to the collator. The
        other issue is that we should probably only enable collate if the
        boomer creates a single makeflow, otherwise we cannot be sure that it
        will run after all of the sdms are created.
"""
import argparse
import logging
import mx.DateTime as dt
import os, sys, time
import signal

from LmBackend.common.lmobj import LMError, LMObject

from LmCommon.common.lmconstants import JobStatus, LM_USER

from LmServer.base.utilities import isLMUser
from LmServer.common.datalocator import EarlJr
from LmServer.common.lmconstants import (
    DEFAULT_NUM_PERMUTATIONS, DEFAULT_RANDOM_GROUP_SIZE, LMFileType,
    PUBLIC_ARCHIVE_NAME) 
from LmServer.common.localconstants import PUBLIC_USER 
from LmServer.common.log import ScriptLogger
from LmServer.db.borgscribe import BorgScribe
from LmServer.legion.processchain import MFChain
from LmServer.tools.cwalken import ChristopherWalken
from LmDbServer.boom.boom_collate import BoomCollate

# Only relevant for "archive" public data, all user workflows will put all
# spuds into a single makeflow, along with multi-species commands to follow SDMs 
SPUD_LIMIT = 200

# .............................................................................
class Boomer(LMObject):
    """
    Class to iterate with a ChristopherWalken through a sequence of species data,    
    creating individual species (Spud) commands into one or more "Bushel" 
    MFChains. 
    * If working on the Public Archive, a huge, constantly updated dataset,  
      Bushel MFChains are limited to SPUD_LIMIT number of species commands -
      the full Bushel is rotated and a new Bushel is created to hold the next
      set of commands.
    * In all other multi-species workflows, all spuds are put into the same 
      bushel. If the daemon is interrupted, it will write out the current  
      MFChain, and pick up where it left off with a new MFChains for 
      unprocessed species data.
    @todo: Next instance of boom.Walker will create new MFChains, but add data
    to the existing Global PAM matrices.  
    """
    # .............................
    def __init__(self, configFname, successFname, log=None):      
        self.name = self.__class__.__name__.lower()
        # Logfile
        if log is None:
            secs = time.time()
            timestamp = "{}".format(time.strftime("%Y%m%d-%H%M", time.localtime(secs)))
            logname = '{}.{}'.format(self.name, timestamp)
            log = ScriptLogger(logname, level=logging.INFO)
        self.log = log
        
        self.configFname = configFname
        self._successFname = successFname
        self.assemblePams = None
        # Send Database connection
        self._scribe = BorgScribe(self.log)
        # iterator tool for species
        self.christopher = None

#         # Dictionary of {scenCode: (potatoChain, triagePotatoFile)}
#         self.potatoes = None

        # MFChain for lots of spuds 
        self.potatoBushel = None
        self.squidNames = None
        # Stop indicator
        self.keepWalken = False
        
        signal.signal(signal.SIGTERM, self._receiveSignal) # Stop signal

    # .............................
    def _receiveSignal(self, sigNum, stack):
        """
        @summary: Handler used to receive signals
        @param sigNum: The signal received
        @param stack: The stack at the time of signal
        """
        if sigNum in (signal.SIGTERM, signal.SIGKILL):
            self.close()
        else:
            message = "Unknown signal: %s" % sigNum
            self.log.error(message)

    # .............................
    def initializeMe(self):
        """
        @summary: Creates objects (ChristopherWalken for walking the species
                  and MFChain objects for workflow computation requests.
        """
        # Send Database connection
        try:
            success = self._scribe.openConnections()
        except Exception, e:
            raise LMError(currargs='Exception opening database', prevargs=e.args)
        else:
            if not success:
                raise LMError(currargs='Failed to open database')
            else:
                self.log.info('{} opened databases'.format(self.name))
              
        try:
            self.christopher = ChristopherWalken(self.configFname,
                                                 scribe=self._scribe)
            self.christopher.initializeMe()
        except Exception, e:
            raise LMError(currargs='Failed to initialize Chris with config {} ({})'
                         .format(self.configFname, e))
        try:
            self.gridset = self.christopher.boomGridset
            self.gridsetId = self.christopher.boomGridset.getId()
        except:
            self.log.warning('Exception getting christopher.boomGridset id!!')
        if self.gridsetId is None:
            self.log.warning('Missing christopher.boomGridset id!!')
        
        self.assemblePams = self.christopher.assemblePams
        self.priority = self.christopher.priority
        # Start where we left off 
        self.christopher.moveToStart()
        self.log.debug('Starting Chris at location {} ... '
                       .format(self.christopher.currRecnum))
        self.keepWalken = True
        
        self.squidNames = []
        self.pav_index_filenames = []
        # master MF chain
        self.masterPotatoHead = None
        self.potatoBushel = None
        self.rotatePotatoes()
         
    # .............................
    def processOneSpecies(self):
        try:
            self.log.info('Next species ...')
            # Get Spud rules (single-species SDM) and dict of {scencode: pavFilename}
            workdir = self.potatoBushel.getRelativeDirectory()
            squid, spudRules, idx_success_filename = self.christopher.startWalken(
                workdir)
            if idx_success_filename is not None:
                self.pav_index_filenames.append(idx_success_filename)
            
            # TODO: Track squids
            if squid is not None:
                self.squidNames.append(squid)
            
            self.keepWalken = not self.christopher.complete
            # TODO: Master process for occurrence only? SDM only? 
            if spudRules:
                self.log.debug('Processing spud for potatoes') 
                self.potatoBushel.addCommands(spudRules)
                # TODO: Don't write triage file, but don't delete code
                #if potatoInputs:
                #   for scencode, (pc, triagePotatoFile) in self.potatoes.iteritems():
                #      pavFname = potatoInputs[scencode]
                #      triagePotatoFile.write('{}: {}\n'.format(squid, pavFname))
                #   self.log.info('Wrote spud squid to {} triage files'
                #                 .format(len(potatoInputs)))
                #if len(self.spudArfFnames) >= SPUD_LIMIT:
                if not self.assemblePams and len(self.squidNames) >= SPUD_LIMIT:
                    self.rotatePotatoes()
            self.log.info('-----------------')
        except Exception, e:
            self.log.debug('Exception {} on spud, closing ...'.format(str(e)))
            self.close()
            raise e

    # .............................
    def _writeBushel(self):
        """
        Todo:
            * Aimee: Add call to boom_collate here probably.  We should
                probably only call it if there is only one bushel as everything
                will need to be done before the multi-species computations and
                run.  I suppose we could punt and add the rules on complete if
                the config says to do so, but that will create incorrect
                results if this is for a rolling pam.  Maybe that isn't a big
                deal...
            
            Code should be something like this:
            
            collate_rules = _get_multispecies_rules(self, gridset,
                self.potatoBushel.getRelativeDirectory(), do_pam_stats, do_mcpa,
                num_permutations=DEFAULT_NUM_PERMUTATIONS,
                group_size=DEFAULT_RANDOM_GROUP_SIZE,
                sdm_dependencies=self.pav_index_filenames, log=None)
            # Add rules to bushel workflow
            self.potatoBushel.addCommands(collate_rules)
        """
        # Write all spud commands in existing bushel MFChain
        if self.potatoBushel:
            if self.potatoBushel.jobs:
                if self.assemblePams and not self.christopher.complete:
                    # Add multispecies rules requested in boom config file
                    collate_rules = self._get_multispecies_rules(
                        self.christopher.compute_pam_stats, 
                        self.christopher.compute_mcpa,
                        num_permutations=self.christopher.num_permutations,
                        group_size=DEFAULT_RANDOM_GROUP_SIZE,
                        sdm_dependencies=self.pav_index_filenames, log=None)

                    # Add rules to bushel workflow
                    self.potatoBushel.addCommands(collate_rules)
                    
                self.potatoBushel.write()
                self.potatoBushel.updateStatus(JobStatus.INITIALIZE)
                self._scribe.updateObject(self.potatoBushel)
                self.log.info('   Wrote potatoBushel {} ({} spuds)'
                              .format(self.potatoBushel.objId, len(self.squidNames)))
            else:
                self.log.info('   No commands in potatoBushel {}'.format(
                    self.potatoBushel.objId))
        else:
            self.log.info('   No existing potatoBushel')


    # .............................
    def rotatePotatoes(self):
        """
        
        """
        if self.potatoBushel:
            self._writeBushel()
        
        # Create new bushel IFF assemblePAMs is False 
        #   and there are more species to process
        if not self.christopher.complete:
            self.potatoBushel = self._createBushelMakeflow()
            if self.christopher.assemblePams:
                self.log.info('Create new potatoes')
                self.squidNames = []
            
    # .............................
    def close(self):
        self.keepWalken = False
        self.log.info('Closing boomer ...')
        # Stop walken the archive and saveNextStart
        self.christopher.stopWalken()
        self.rotatePotatoes()

    # .............................
    def restartWalken(self):
        if self.christopher.complete() and self.christopher.moreDataToProcess():
            # Rename old file
            oldfname = self.christopher.weaponOfChoice.occParser.dataFname
            ts = dt.localtime().tuple()
            timestamp = '{}{:02d}{:02d}-{:02d}{:02d}'.format(ts[0], ts[1], ts[2], ts[3], ts[4])
            newfname = oldfname + '.' + timestamp
            try:
                os.rename(oldfname, newfname)
            except Exception, e:
                self.log.error('Failed to rename {} to {}'.format(oldfname, newfname))
            # Restart with next file
            self.initializeMe()

    # ...............................................
    def _createBushelMakeflow(self):
        meta = {MFChain.META_CREATED_BY: self.name,
                MFChain.META_DESCRIPTION: 'Bushel for User {}, Archive {}'
                    .format(self.christopher.userId, self.christopher.archiveName),
                MFChain.META_GRIDSET: self.gridsetId 
        }
        newMFC = MFChain(self.christopher.userId, priority=self.priority, 
                         metadata=meta, status=JobStatus.GENERAL, 
                         statusModTime=dt.gmt().mjd)
        mfChain = self._scribe.insertMFChain(newMFC, self.gridsetId)
        return mfChain

    # ...............................................
    def _createMasterPotatoHeadMakeflow(self):
        meta = {MFChain.META_CREATED_BY: self.name,
                MFChain.META_DESCRIPTION: 'MasterPotatoHead for User {}, Archive {}'
                    .format(self.christopher.userId, self.christopher.archiveName),
                MFChain.META_GRIDSET: self.gridsetId 
        }
        newMFC = MFChain(self.christopher.userId, priority=self.priority, 
                         metadata=meta, status=JobStatus.GENERAL, 
                         statusModTime=dt.gmt().mjd)
        mfChain = self._scribe.insertMFChain(newMFC, self.gridsetId)
        return mfChain

    # ...............................................
    def _get_multispecies_rules(self, do_mcpa,
                                num_permutations=DEFAULT_NUM_PERMUTATIONS,
                                group_size=DEFAULT_RANDOM_GROUP_SIZE,
                                sdm_dependencies=None, log=None):
        """Get rules for multi-species computations

        Args:
            do_pam_stats (:obj: `bool`): Should PAM stats be created
            do_mcpa (:obj: `bool`): Should MCPA be computed
            num_permutations (:obj: `int`) : The number of randomizations that
                should be performed for the multi-species computations
            group_size (:obj: `int`) : The number of randomized runs to perform
                in each group of permutations
            sdm_dependencies (:obj: `list of str`) : A list of file names that
                must be created before these rules can run.  They should be
                created by the same workflow
            log (:obj: `Logger`) : An optional log object to use

        Note:
            This only makes sense after all SDMs are created for a gridset

        Return:
            A list of compute rules for the gridset
        """
        work_dir = self.potatoBushel.getRelativeDirectory()
        if sdm_dependencies is None:
            sdm_dependencies = []
        bc = BoomCollate(self.gridset, dependencies=sdm_dependencies, 
                         do_pam_stats=self.assemblePams, 
                         do_mcpa=do_mcpa, 
                         num_permutations=num_permutations,
                         random_group_size=group_size, 
                         work_dir=work_dir, log=log)
        rules = bc.get_collate_rules()
        
        return rules

    # ...............................................
    def writeSuccessFile(self, message):
        self.readyFilename(self._successFname, overwrite=True)
        try:
            f = open(self._successFname, 'w')
            f.write(message)
        except:
            raise
        finally:
            f.close()

    # .............................
    def processAllSpecies(self):
        print('processAll with configFname = {}'.format(self.configFname))
        count = 0
        while self.keepWalken:
            self.processOneSpecies()
            count += 1
        if not self.keepWalken:
            self.close()
        self.writeSuccessFile('Boomer finished walken {} species'.format(count))



# .............................................................................
if __name__ == "__main__":
    if not isLMUser():
        print("Run this script as `{}`".format(LM_USER))
        sys.exit(2)
    earl = EarlJr()
    defaultConfigFile = earl.createFilename(LMFileType.BOOM_CONFIG, 
                                            objCode=PUBLIC_ARCHIVE_NAME, 
                                            usr=PUBLIC_USER)
    parser = argparse.ArgumentParser(
             description=('Populate a Lifemapper archive with metadata ' +
                          'for single- or multi-species computations ' + 
                          'specific to the configured input data or the ' +
                          'data package named.'))
    parser.add_argument('--config_file', default=defaultConfigFile,
             help=('Configuration file for the archive, gridset, and grid ' +
                   'to be created from these data.'))
    parser.add_argument('--success_file', default=None,
             help=('Filename to be written on successful completion of script.'))
    
    args = parser.parse_args()
    configFname = args.config_file
    successFname = args.success_file
    if not os.path.exists(configFname):
        raise Exception('Configuration file {} does not exist'.format(configFname))
    if successFname is None:
        boombasename, _ = os.path.splitext(configFname)
        successFname = boombasename + '.success'
    
    secs = time.time()
    timestamp = "{}".format(time.strftime("%Y%m%d-%H%M", time.localtime(secs)))
    
    scriptname = os.path.splitext(os.path.basename(__file__))[0]
    logname = '{}.{}'.format(scriptname, timestamp)
    logger = ScriptLogger(logname, level=logging.INFO)
    boomer = Boomer(configFname, successFname, log=logger)
    boomer.initializeMe()
    boomer.processAllSpecies()
   
"""
$PYTHON LmDbServer/boom/boom.py --help



# ##########################################################################

# ##########################################################################

       
"""
