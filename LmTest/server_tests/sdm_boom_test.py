"""Tests for SDM BOOM jobs initiated by backend."""
from copy import deepcopy
import json
import os
from random import randint, random

import lmtest.base.test_base as test_base

from LmCommon.common.ready_file import ready_filename
from LmCommon.common.lmconstants import JobStatus

from LmDbServer.boom.init_workflow import BOOMFiller

from LmServer.common.lmconstants import ARCHIVE_PATH, TEMP_PATH
from LmServer.common.localconstants import APP_PATH
from LmServer.common.log import ScriptLogger
from LmServer.db.borg_scribe import BorgScribe

from LmTest.validate.raster_validator import validate_raster_file
from LmTest.validate.vector_validator import validate_vector_file


# .....................................................................................
class BoomJobSubmissionTest(test_base.LmTest):
    """Test of job submission for SDM BOOM."""

    # .............................
    def __init__(
        self,
        user_id,
        config,
        wait_timeout,
        delay_time=0,
        delay_interval=86400,  # One day
    ):
        """Construct the simulated submission test."""
        self._next_run_params = {
            'user_id': user_id,
            'config': deepcopy(config),
            'wait_timeout': wait_timeout,
            'delay_time': delay_interval,
            'delay_interval': delay_interval
        }
        test_base.LmTest.__init__(self, delay_time=delay_time)
        self.wait_timeout = wait_timeout
        self.boom_config = config
        # Create a random value used for filenames
        rand_val = randint(0, 99999)
        self.user_dir = os.path.join(ARCHIVE_PATH, user_id)
        self._replace_lookup = {
            'TEST_USER': user_id,
            'ARCHIVE_NAME': 'Auto_test-{}'.format(rand_val),
            'OCCURRENCE_FILENAME': 'Auto_test_occ-{}'.format(rand_val),
        }
        self.test_name = 'SDM BOOM Job test (user: {}, archive: {})'.format(
            user_id, self._replace_lookup['ARCHIVE_NAME']
        )
        self.config_filename = os.path.join(
            TEMP_PATH, '{}.ini'.format(self._replace_lookup['ARCHIVE_NAME'])
        )

    # .............................
    def __repr__(self):
        """Return a string representation of this instance."""
        return self.test_name

    # .............................
    def _generate_random_occurrences(self, num_species, min_points, max_points):
        """Generate random points for this test.

        Args:
            num_species (int): Number of species to include in this run.
            min_points (int): Minimum number of points per species.
            max_points (int): Maximum number of points per species.
        """
        csv_filename = os.path.join(
            self.user_dir, '{}.csv'.format(self._replace_lookup['OCCURRENCE_FILENAME'])
        )
        json_filename = os.path.join(
            self.user_dir, '{}.json'.format(self._replace_lookup['OCCURRENCE_FILENAME'])
        )
        ready_filename(csv_filename, overwrite=True)
        with open(csv_filename, mode='wt') as out_file:
            out_file.write('Species,Longitude,Latitude\n')
            for i in range(num_species):
                for _ in range(randint(min_points, max_points)):
                    out_file.write(
                        '{},{},{}\n'.format(
                            'Species {}'.format(i),
                            360.0 * random() - 180.0,
                            180.0 * random() - 90.0,
                        )
                    )
        point_meta = {
            '0': {'name': 'Species', 'role': 'taxaName', 'type': 'string'},
            '1': {'name': 'Longitude', 'role': 'longitude', 'type': 'real'},
            '2': {'name': 'Latitude', 'role': 'latitude', 'type': 'real'},
        }
        ready_filename(json_filename, overwrite=True)
        with open(json_filename, mode='wt') as json_file:
            json.dump(point_meta, json_file)

    # .............................
    def _generate_config_file(self):
        """Generate a SDM BOOM job configuration file."""
        ready_filename(self.config_filename, overwrite=True)
        with open(self.config_filename, mode='wt') as config_file:
            for key in self.boom_config.keys():
                # Write the section header
                config_file.write('[{}]\n'.format(key))
                # Write the parameters
                for k, val in self.boom_config[key].items():
                    config_file.write('{} = {}\n'.format(k, self._replace_val(val)))
                # Write a blank line
                config_file.write('\n')

    # .............................
    def _replace_val(self, value):
        """Fill in any templated strings in value.

        Args:
            value (str): A string potentially containing a template value.
        """
        parts = str(value).split('$')
        # Replace odd values with lookup replace values.
        for i in range(1, len(parts), 2):
            parts[i] = self._replace_lookup[parts[i]]
        return ''.join(parts)

    # .............................
    def run_test(self):
        """Run the test."""
        num_species = 10
        min_points = 200
        max_points = 1000
        try:
            # Create point file
            self._generate_random_occurrences(num_species, min_points, max_points)
            # Create config file
            self._generate_config_file()
            # init workflow
            filler = BOOMFiller(
                self.config_filename, logname=self._replace_lookup['ARCHIVE_NAME']
            )
            # Gridset
            gridset = filler.init_boom()
            gridset_id = gridset.get_id()
            # Add new tests
            self.add_new_test(BoomWaitTest(gridset_id, self.wait_timeout))
            self.add_new_test(BoomJobSubmissionTest(**self._next_run_params))
        except Exception as err:
            raise test_base.LmTestFailure(
                'Failed to submit test job: {}'.format(err)
            ) from err


# .............................................................................
class BoomWaitTest(test_base.LmTest):
    """Waiting test for a gridset computations to complete."""

    # .............................
    def __init__(self, gridset_id, wait_timeout, delay_time=0, delay_interval=120):
        """Construct the instance."""
        test_base.LmTest.__init__(self, delay_time=delay_time)
        self.gridset_id = gridset_id
        self.wait_timeout = wait_timeout
        self.test_name = 'Waiting test for gridset id: {}'.format(self.gridset_id)
        self.delay_interval = delay_interval

    # .............................
    def __repr__(self):
        """Return a string representation of this instance."""
        return self.test_name

    # .............................
    def run_test(self):
        """Run the test."""
        # Check if gridset is finished
        scribe = BorgScribe(
            ScriptLogger('Auto_test_gridset_{}'.format(self.gridset_id))
        )
        scribe.open_connections()
        gridset_summary = scribe.summarize_mf_chains_for_gridset(self.gridset_id)
        scribe.close_connections()
        # Check if complete
        waiting = False
        for status, count in gridset_summary:
            # Check if any are waiting or running
            if status < JobStatus.COMPLETE and count > 0:
                waiting = True
            # Check if errors
            if status > JobStatus.COMPLETE and count > 0:
                raise test_base.LmTestFailure(
                    'Some makeflows failed for gridset {}'.format(self.gridset_id)
                )
        # If still waiting, check that we should
        if waiting:
            if self.wait_timeout < 0:
                raise test_base.LmTestFailure(
                    'Wait timeout reached for gridset {}'.format(self.gridset_id)
                )
            # Still time? Add new test
            self.add_new_test(
                BoomWaitTest(
                    self.gridset_id,
                    self.wait_timeout - self.delay_interval,
                    delay_time=self.delay_interval,
                    delay_interval=self.delay_interval,
                )
            )
        else:
            # Finished? Validate it
            self.add_new_test(BoomValidateTest(self.gridset_id))


# .............................................................................
class BoomValidateTest(test_base.LmTest):
    """Gridset validation test."""

    # .............................
    def __init__(self, gridset_id, delay_time=0, delay_interval=60):
        """Construct the instance."""
        test_base.LmTest.__init__(self, delay_time=delay_time)
        self.gridset_id = gridset_id
        self.test_name = 'Gridset {} validation test'.format(self.gridset_id)

    # .............................
    def __repr__(self):
        """Return a string representation of this instance."""
        return self.test_name

    # .............................
    def run_test(self):
        """Run the test."""
        scribe = BorgScribe(
            ScriptLogger('Auto_test_gridset_{}'.format(self.gridset_id))
        )
        scribe.open_connections()
        occs = scribe.list_occurrence_sets(
            0, 1000, gridset_id=self.gridset_id, atom=False
        )
        prjs = scribe.list_sdm_projects(0, 1000, gridset_id=self.gridset_id, atom=False)
        scribe.close_connections()
        for occ in occs:
            # Fail if unknown error status, known errors okay
            if occ.status == JobStatus.GENERAL_ERROR:
                raise test_base.LmTestFailure(
                    'Unknown error for occurrence set {} from gridset {}'.format(
                        occ.get_id(), self.gridset_id
                    )
                )
            # Fail if status < COMPLETE
            if occ.status < JobStatus.COMPLETE:
                raise test_base.LmTestFailure(
                    'Occurrence set {} did not complete for gridset {}'.format(
                        occ.get_id(), self.gridset_id
                    )
                )
            # Validate if occurrence set is complete
            if occ.status == JobStatus.COMPLETE:
                valid, msg = validate_vector_file(occ.get_dlocation())
                if not valid:
                    raise test_base.LmTestFailure(
                        'Occurrence set {} for gridset {} is not valid: {}'.format(
                            occ.get_id(),
                            self.gridset_id,
                            msg,
                        )
                    )

        for prj in prjs:
            # Fail if unknown error status, known errors okay
            if prj.status == JobStatus.GENERAL_ERROR:
                raise test_base.LmTestFailure(
                    'Unknown error for projection {} from gridset {}'.format(
                        prj.get_id(), self.gridset_id
                    )
                )
            # Fail if status < COMPLETE
            if prj.status < JobStatus.COMPLETE:
                raise test_base.LmTestFailure(
                    'Projection {} did not complete for gridset {}'.format(
                        prj.get_id(), self.gridset_id
                    )
                )
            # Validate if projection is complete
            if prj.status == JobStatus.COMPLETE:
                valid, msg = validate_raster_file(prj.get_dlocation())
                if not valid:
                    raise test_base.LmTestFailure(
                        'Projection {} for gridset {} is not valid: {}'.format(
                            prj.get_id(),
                            self.gridset_id,
                            msg,
                        )
                    )

