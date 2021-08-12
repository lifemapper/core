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
import os
import sys
import time
import signal

from LmBackend.common.lmobj import LMError, LMObject
from LmCommon.common.lmconstants import (JobStatus, LM_USER, ENCODING)
import LmCommon.common.time as lt
from LmDbServer.boom.boom_collate import BoomCollate
from LmServer.base.utilities import is_lm_user
from LmServer.common.data_locator import EarlJr
from LmServer.common.lmconstants import (
    DEFAULT_RANDOM_GROUP_SIZE, LMFileType, PUBLIC_ARCHIVE_NAME)
from LmServer.common.localconstants import PUBLIC_USER
from LmServer.common.log import ScriptLogger
from LmServer.db.borg_scribe import BorgScribe
from LmServer.legion.process_chain import MFChain
from LmServer.tools.cwalken import ChristopherWalken

# Only relevant for "archive" public data, all user workflows will put all
#    spuds into a single makeflow, along with multi-species commands to follow
#    SDMs
SPUD_LIMIT = 5000

# .............................................................................
class Boomer(LMObject):
    """Iterate with ChristopherWalken through a sequence of species data

    Class to iterate with a ChristopherWalken through a sequence of species
    data, creating individual species (Spud) commands into one or more "Bushel"
    MFChains.

    Note:
        - If working on the Public Archive, a huge, constantly updated dataset,
            Bushel MFChains are limited to SPUD_LIMIT number of species
            commands - the full Bushel is rotated and a new Bushel is created
            to hold the next set of commands.
        - In all other multi-species workflows, all spuds are put into the same
            bushel. If the daemon is interrupted, it will write out the current
            MFChain, and pick up where it left off with a new MFChains for
            unprocessed species data.

    Todo:
        Next instance of boom.Walker will create new MFChains, but add data to
            the existing Global PAM matrices.
    """

    # .............................
    def __init__(self, config_fname, success_fname, log=None):
        self.name = self.__class__.__name__.lower()
        # Logfile
        if log is None:
            secs = time.time()
            timestamp = "{}".format(
                time.strftime("%Y%m%d-%H%M", time.localtime(secs)))
            logname = '{}.{}'.format(self.name, timestamp)
            log = ScriptLogger(logname, level=logging.INFO)
        self.log = log

        self.config_fname = config_fname
        self._success_fname = success_fname

        self.do_pam_stats = None
        self.do_mcpa = None
        # Database connection
        self._scribe = BorgScribe(self.log)
        # iterator tool for species
        self.christopher = None

        self.gridset = None
        self.gridset_id = None
        self.priority = None
        self.pav_index_filenames = []
        self.master_potato_head = None

        # MFChain for lots of spuds
        self.potato_bushel = None
        self.squid_names = None
        # Stop indicator
        self.keep_walken = False

        signal.signal(signal.SIGTERM, self._receive_signal)  # Stop signal

    # .............................
    def _receive_signal(self, sig_num, stack):
        """Handler used to receive signals

        Args:
            sig_num: The signal received
            stack: The stack at the time of signal
        """
        if sig_num in (signal.SIGTERM, signal.SIGKILL):
            self.close()
        else:
            message = 'Unknown signal: {}'.format(sig_num)
            self.log.error(message)

    # .............................
    def initialize_me(self):
        """
        Initializes attributes and objects for workflow computation requests.
        
        Note: 
            Christopher Walken reads the config file, so pull needed config from there
        """
        try:
            success = self._scribe.open_connections()
        except Exception as e:
            raise LMError('Exception opening database', e)
        if not success:
            raise LMError('Failed to open database')
        self.log.info('{} opened databases'.format(self.name))

        try:
            self.christopher = ChristopherWalken(
                self.config_fname, scribe=self._scribe)
            self.christopher.initialize_me()
        except Exception as e:
            raise LMError(
                'Failed to initialize Chris with config {} ({})'.format(
                    self.config_fname, e))
        try:
            self.gridset = self.christopher.boom_gridset
            self.gridset_id = self.christopher.boom_gridset.get_id()
        except Exception:
            self.log.warning('Exception getting christopher.boom_gridset id!!')
        if self.gridset_id is None:
            self.log.warning('Missing christopher.boom_gridset id!!')

        self.do_pam_stats = self.christopher.compute_pam_stats
        self.do_mcpa = self.christopher.compute_mcpa
        self.priority = self.christopher.priority
        
        # Start where we left off
        self.christopher.move_to_start()
        self.log.debug(
            'Starting Chris at location {} ... '.format(
                self.christopher.curr_rec_num))
        self.keep_walken = True

        self.pav_index_filenames = []
        # master MF chain
        self.master_potato_head = None
        self.log.info('Create first potato')
        self.potato_bushel = self._create_bushel_makeflow()
        self.squid_names = []

    # .............................
    def process_one_species(self):
        """Process one species occurrence set."""
        try:
            self.log.info('Next species ...')
            # Get Spud rules (single-species SDM) and dict of
            #    {scencode: pavFilename}
            workdir = self.potato_bushel.get_relative_directory()
            (squid, spud_rules, idx_success_filename
             ) = self.christopher.start_walken(workdir)
            if idx_success_filename is not None:
                self.pav_index_filenames.append(idx_success_filename)

            # TODO: Track squids
            if squid is not None:
                self.squid_names.append(squid)

            self.keep_walken = not self.christopher.complete
            # TODO: Master process for occurrence only? SDM only?
            if spud_rules:
                self.log.debug('Processing spud for potatoes')
                self.potato_bushel.add_commands(spud_rules)
                # TODO: Don't write triage file, but don't delete code
                if not self.do_pam_stats and len(
                        self.squid_names) >= SPUD_LIMIT:
                    self.rotate_potatoes()
            self.log.info('-----------------')
        except Exception as e:
            self.log.debug('Exception {} on spud, closing ...'.format(str(e)))
            self.close()
            raise e

    # .............................
    def _write_bushel(self):
        # Write all spud commands in existing bushel MFChain
        if self.potato_bushel:
            if self.potato_bushel.jobs:
                # Only collate if do_pam_stats and finished with all SDMs
                if self.do_pam_stats and self.christopher.complete:
                    # Add multispecies rules requested in boom config file
                    collate_rules = self._get_multispecies_rules()

                    # Add rules to bushel workflow
                    self.potato_bushel.add_commands(collate_rules)

                self.potato_bushel.write()
                self.potato_bushel.update_status(JobStatus.INITIALIZE)
                self._scribe.update_object(self.potato_bushel)
                self.log.info(
                    '   Wrote potato_bushel {} ({} spuds)'.format(
                        self.potato_bushel.obj_id, len(self.squid_names)))
            else:
                self.log.info(
                    '   No commands in potato_bushel {}'.format(
                        self.potato_bushel.obj_id))
        else:
            self.log.info('   No existing potato_bushel')

    # .............................
    def rotate_potatoes(self):
        """Rotate potatoes to start on next set of species."""
        if self.potato_bushel:
            self._write_bushel()

        # Create new bushel IFF do_pam_stats is False, i.e. Rolling PAM,
        #   and there are more species to process
        if not self.christopher.complete and not self.do_pam_stats:
            self.potato_bushel = self._create_bushel_makeflow()
            self.log.info('Create new potato')
            self.squid_names = []

    # .............................
    def close(self):
        """Close connections and stop."""
        self.keep_walken = False
        self.log.info('Closing boomer ...')
        # Stop walken the archive and saveNextStart
        self.christopher.stop_walken()
        self.rotate_potatoes()

    # .............................
    def restart_walken(self):
        """Restart species processing."""
        if self.christopher.complete() and\
                self.christopher.more_data_to_process():
            # Rename old file
            oldfname = self.christopher.weapon_of_choice.occ_parser.csv_fname
            ts = lt.localtime().tuple()
            timestamp = '{}{:02d}{:02d}-{:02d}{:02d}'.format(
                ts[0], ts[1], ts[2], ts[3], ts[4])
            newfname = oldfname + '.' + timestamp
            try:
                os.rename(oldfname, newfname)
            except Exception as e:
                self.log.error('Failed to rename {} to {} ({})'.format(
                    oldfname, newfname, e))
            # Restart with next file
            self.initialize_me()

    # ...............................................
    def _create_bushel_makeflow(self):
        meta = {
            MFChain.META_CREATED_BY: self.name,
            MFChain.META_DESCRIPTION:
                'Bushel for User {}, Archive {}'.format(
                    self.christopher.user_id, self.christopher.archive_name),
            MFChain.META_GRIDSET: self.gridset_id
            }
        new_mfc = MFChain(
            self.christopher.user_id, priority=self.priority, metadata=meta,
            status=JobStatus.GENERAL, status_mod_time=lt.gmt().mjd)
        return self._scribe.insert_mf_chain(new_mfc, self.gridset_id)

    # ...............................................
    def _get_multispecies_rules(self):
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
        work_dir = self.potato_bushel.get_relative_directory()
        collator = BoomCollate(
            self.gridset, dependencies=self.pav_index_filenames,
            do_pam_stats=self.do_pam_stats, do_mcpa=self.do_mcpa,
            num_permutations=self.christopher.num_permutations,
            random_group_size=DEFAULT_RANDOM_GROUP_SIZE,
            work_dir=work_dir, log=self.log)
        rules = collator.get_collate_rules()

        return rules

    # ...............................................
    def write_success_file(self, message):
        """Write out the success file."""
        self.ready_filename(self._success_fname, overwrite=True)
        try:
            with open(self._success_fname, 'w', encoding=ENCODING) as out_file:
                out_file.write(message)
        except IOError as io_err:
            raise LMError('Failed to write success file', io_err)

    # .............................
    def process_all_species(self):
        """Read and process all species, while checking for stop signal."""
        print(('processAll with config_fname = {}'.format(self.config_fname)))
        count = 0
        while self.keep_walken:
            self.process_one_species()
            count += 1
        if not self.keep_walken:
            self.close()
        self.write_success_file(
            'Boomer finished walken {} species'.format(count))


# .............................................................................
def main():
    """Main method for script."""
    if not is_lm_user():
        print(("Run this script as `{}`".format(LM_USER)))
        sys.exit(2)
    earl = EarlJr()
    default_config_file = earl.create_filename(
        LMFileType.BOOM_CONFIG, obj_code=PUBLIC_ARCHIVE_NAME, usr=PUBLIC_USER)
    parser = argparse.ArgumentParser(
        description=(
            'Populate a Lifemapper archive with metadata for single- or '
            'multi-species computations specific to the configured input data '
            'or the data package named.'))
    parser.add_argument(
        '--config_file', default=default_config_file,
        help=('Configuration file for the archive, gridset, and grid '
              'to be created from these data.'))
    parser.add_argument(
        '--success_file', default=None,
        help=('Filename to be written on successful completion of script.'))

    args = parser.parse_args()
    config_fname = args.config_file
    success_fname = args.success_file
    if not os.path.exists(config_fname):
        raise LMError(
            'Configuration file {} does not exist'.format(config_fname))
    if success_fname is None:
        boombasename, _ = os.path.splitext(config_fname)
        success_fname = boombasename + '.success'

    secs = time.time()
    timestamp = "{}".format(time.strftime("%Y%m%d-%H%M", time.localtime(secs)))

    scriptname = os.path.splitext(os.path.basename(__file__))[0]
    logname = '{}.{}'.format(scriptname, timestamp)
    logger = ScriptLogger(logname, level=logging.INFO)
    boomer = Boomer(config_fname, success_fname, log=logger)
    boomer.initialize_me()
    boomer.process_all_species()


# .............................................................................
if __name__ == '__main__':
    main()

"""
"""
