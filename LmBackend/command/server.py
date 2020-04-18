"""This module contains command objects for server processes

Server commands are commands that run tools only found in the LmServer roll.
By default, these commands have their get_makeflow_rule function set to use
local=True to tell Makeflow that the command should not be distributed to a
compute resource that is not the front end.

Todo:
    * Clean up obsolete commands
    * Get command method could probably be moved to super class and just use
        args and opt_args for each subclass
"""
import os
import time

from LmBackend.command.base import _LmCommand
from LmBackend.common.lmconstants import (
    DB_SERVER_SCRIPTS_DIR, SERVER_SCRIPTS_DIR)


# .............................................................................
class _LmServerCommand(_LmCommand):
    """Subclass for server commands

    The _LmServerCommand class is an intermediate class that all server command
    classes should inherit from.
    """
    relative_directory = SERVER_SCRIPTS_DIR

    # ................................
    def get_makeflow_rule(self, local=True):
        """Get a MfRule object for this command

        Args:
            local: Should this be a local command in a Makeflow DAG

        Note:
            This differs from the superclass because the default local is True
        """
        return super(_LmServerCommand, self).get_makeflow_rule(local=local)


# .............................................................................
class _LmDbServerCommand(_LmServerCommand):
    """Subclass for database server commands

    The _LmDbServerCommand class is an intermediate class that all database
    server command classes should inherit from.
    """
    relative_directory = DB_SERVER_SCRIPTS_DIR


# .............................................................................
class AssemblePamFromSolrQueryCommand(_LmServerCommand):
    """Assembles PAM data from a Solr query"""
    script_name = 'assemble_pam_from_solr.py'

    # ................................
    def __init__(self, pam_id, pam_filename, success_filename,
                 dependency_files=None):
        """Construct the command

        Args:
            pam_id (:obj: `int`): The database identifier of the PAM to
                assemble
            success_filename (:obj: `str`): A file location to write an
                indication of success
            dependency_files (:obj: `list`): An optional list of dependency
                files that should exist before running this command
        """
        _LmServerCommand.__init__(self)

        self.args = '{} {}'.format(pam_id, success_filename)
        self.outputs.append(success_filename)
        self.outputs.append(pam_filename)

        if dependency_files is not None:
            if isinstance(dependency_files, list):
                self.inputs.extend(dependency_files)
            else:
                self.inputs.append(dependency_files)


# .............................................................................
class CatalogTaxonomyCommand(_LmDbServerCommand):
    """Create taxonomy workflows

    This command will create makeflows to catalog boom archive inputs, create
    GRIMs, create an archive ini file, and run the Boomer Daemon to walk
    through the inputs
    """
    script_name = 'catalog_taxonomy.py'

    # ................................
    def __init__(self, source_name, taxon_data_filename,
                 taxon_success_filename, source_url=None, delimiter='\t'):
        """Construct the command object

        Args:
            source_name: The taxonomic authority (locally unique)
                name/identifier for the data
            taxon_filename: The file location of the taxonomy csv file
            source_url: The unique URL for the taxonomic authority
            delimiter: Delimiter for the data file
        """
        _LmDbServerCommand.__init__(self)

        # scen_package_meta may be full pathname or in ENV_DATA_PATH dir
        if not os.path.exists(taxon_data_filename):
            raise Exception(
                'Missing Taxonomy data file {}'.format(taxon_data_filename))

        data_basename, _ = os.path.splitext(
            os.path.basename(taxon_data_filename))
        # file ends up in LOG_PATH
        secs = time.time()
        timestamp = "{}".format(
            time.strftime("%Y%m%d-%H%M", time.localtime(secs)))
        logname = '{}.{}.{}'.format(
            self.script_basename, data_basename, timestamp)

        # Optional script args, required here
        self.opt_args = ' --taxon_source_name="{}"'.format(source_name)
        self.opt_args += ' --taxon_data_filename={}'.format(
            taxon_data_filename)
        self.opt_args += ' --success_filename={}'.format(
            taxon_success_filename)
        self.opt_args += ' --log_name={}'.format(logname)

        # Optional args
        if source_url:
            self.opt_args += ' --taxon_source_url={}'.format(source_url)
        if delimiter != '\t':
            self.opt_args += ' --delimiter={}'.format(delimiter)

        self.outputs.append(taxon_success_filename)
#         # Logfile is created by script in LOG_DIR
#         logfilename = '{}{}'.format(logname, LMFormat.LOG.ext)
#         self.outputs.append(logfilename)


# .............................................................................
class EncodeBioGeoHypothesesCommand(_LmServerCommand):
    """Encode biogeographic hypotheses"""
    
    script_name = 'encode_biogeo_hypotheses.py'

    # ................................
    def __init__(self, user_id, gridset_name, success_file):
        """Construct the command object

        Args:
            user_id: User for the gridset
            gridset_name: The unique gridset name
        """
        _LmServerCommand.__init__(self)

        # file ends up in LOG_PATH
        secs = time.time()
        timestamp = '{}'.format(
            time.strftime('%Y%m%d-%H%M', time.localtime(secs)))
        logname = '{}.{}'.format(self.script_basename, timestamp)

        # Required args
        self.args = '{} {} {}'.format(user_id, gridset_name, success_file)
        # Optional arg, we also want for output
        self.opt_args += ' --logname={}'.format(logname)

        self.outputs.append(success_file)


# .............................................................................
class IndexPAVCommand(_LmServerCommand):
    """Post PAV information to a solr index"""
    script_name = 'index_pavs.py'

    # ................................
    def __init__(self, pav_file_name, pav_id, proj_id, pam_id,
                 pav_idx_file_name):
        """Construct the command object

        Args:
            pav_file_name: The file location of the PAV matrix
            pav_id: The database id of the PAV
            proj_id: The database id of the projection used to build the PAV
            pam_id: The database id of the PAM that the PAV belongs to
            pav_idx_file_name: The file location to write the POST data
        """
        _LmServerCommand.__init__(self)
        self.inputs.append(pav_file_name)
        self.outputs.append(pav_idx_file_name)

        self.args = '{} {} {} {} {}'.format(
            pav_file_name, pav_id, proj_id, pam_id, pav_idx_file_name)


# .............................................................................
class TouchFileCommand(_LmServerCommand):
    """Touches a file, creating necessary directories."""
    script_name = 'touch_file.py'

    # ................................
    def __init__(self, filename):
        """Construct the command object

        Args:
            filename: The file location to touch
        """
        _LmServerCommand.__init__(self)
        self.outputs.append(filename)
        self.args = filename


# .............................................................................
class MultiIndexPAVCommand(_LmServerCommand):
    """Indexes multiple PAV objects in one call"""
    script_name = 'index_pavs.py'

    # ..............................
    def __init__(self, pavs_filename, post_doc_filename):
        """Construct the command object

        Args:
            pavs_filename : A JSON file with PAV information
            post_doc_filename : A file location to write the post document
        """
        _LmServerCommand.__init__(self)
        self.inputs.append(pavs_filename)
        self.outputs.append(post_doc_filename)
        self.args = '{} {}'.format(pavs_filename, post_doc_filename)


# .............................................................................
class MultiStockpileCommand(_LmServerCommand):
    """Stockpiles multiple objects in one call"""
    script_name = 'multi_stockpile.py'

    # ..............................
    def __init__(self, stockpile_filename, success_filename,
                 pav_filename=None):
        """Construct the command object

        Args:
            stockpile_filename : A JSON file with stockpile information
            success_filename : A file location to write success or failure
        """
        _LmServerCommand.__init__(self)
        self.inputs.append(stockpile_filename)
        self.outputs.append(success_filename)
        self.args = '{} {}'.format(stockpile_filename, success_filename)
        if pav_filename is not None:
            self.inputs.append(pav_filename)
            self.opt_args += ' -p {}'.format(pav_filename)


# .............................................................................
class ShootSnippetsCommand(_LmServerCommand):
    """Shoots snippets into an index"""
    script_name = 'shoot_snippets.py'

    # ................................
    def __init__(self, occ_id, operation, post_file_name, o2ident=None,
                 url=None, who=None, agent=None, why=None):
        """Construct the command object

        Args:
            occ_id: The occurrence set id to generate snippets for
            operation: The operation performed (see SnippetOperations)
            post_file_name: The file location to store the data posted
            o2ident: An identifier for an optional target object
            url: A URL associated with this action
            who: Who initiated this action
            agent: The agent used to initiate this action
            why: Why this action was initiated
        """
        _LmServerCommand.__init__(self)
        self.outputs.append(post_file_name)

        self.args = '{} {} {}'.format(occ_id, operation, post_file_name)
        if o2ident is not None:
            self.opt_args += ' -o2ident {}'.format(o2ident)

        if url is not None:
            self.opt_args += ' -url {}'.format(url)

        if who is not None:
            self.opt_args += ' -who {}'.format(who)

        if agent is not None:
            self.opt_args += ' -agent {}'.format(agent)

        if why is not None:
            self.opt_args += ' -why {}'.format(why)


# .............................................................................
class SquidAndLabelTreeCommand(_LmServerCommand):
    """Add SQUIDs and node labels to tree"""
    script_name = 'add_squids_to_tree.py'

    # ................................
    def __init__(self, tree_id, user_id, success_filename):
        """Construct the command object

        Args:
            tree_id: The database id of the tree in the database
            user_id: The user id, used for generating squids
            success_filename : The file location where success should be
                indicated
        """
        _LmServerCommand.__init__(self)
        self.outputs.append(success_filename)

        self.args = '{} {} {}'.format(tree_id, user_id, success_filename)


# .............................................................................
class StockpileCommand(_LmServerCommand):
    """Stockpile the outputs of a process"""
    script_name = 'stockpile.py'

    # ................................
    def __init__(self, proc_type, object_id, success_file_name,
                 obj_output_file_names, status=None, status_file_name=None,
                 metadata_file_name=None):
        """Construct the command object

        Args:
            proc_type: The process type of the outputs
            object_id: The id of the object
            success_file_name: The file location of the output file to create
                if the process is successful
            obj_output_file_names: A list of object files to test
            status: An optional object status (currently not used) to update
                the database with
            status_file_name: The location of a file that contains status
                information for the object
            metadata_file_name: The file location of metadata about this object

        Todo:
            use status in stockpile script
            Is this used now?  Or just multi-stockpile
        """
        _LmServerCommand.__init__(self)

        self.args = '{} {} {}'.format(proc_type, object_id, success_file_name)
        self.outputs.append(success_file_name)

        if isinstance(obj_output_file_names, list):
            self.inputs.extend(obj_output_file_names)
            self.args += ' {}'.format(' '.join(obj_output_file_names))
        else:
            self.inputs.append(obj_output_file_names)
            self.args += ' {}'.format(obj_output_file_names)

        if status is not None:
            self.opt_args += ' -s {}'.format(status)

        if status_file_name is not None:
            self.opt_args += ' -f {}'.format(status_file_name)
            self.inputs.append(status_file_name)

        if metadata_file_name is not None:
            self.inputs.append(metadata_file_name)
            self.opt_args += ' -m {}'.format(metadata_file_name)
