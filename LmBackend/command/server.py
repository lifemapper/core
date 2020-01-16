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

from LmCommon.common.lmconstants import LMFormat


# .............................................................................
class _LmServerCommand(_LmCommand):
    """A command subclass for server commands

    The _LmServerCommand class is an intermediate class that all server command
    classes should inherit from.
    """
    relDir = SERVER_SCRIPTS_DIR
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
    """A command subclass for database server commands

    The _LmDbServerCommand class is an intermediate class that all database
    server command classes should inherit from.
    """
    relDir = DB_SERVER_SCRIPTS_DIR


# .............................................................................
class AddBioGeoAndTreeCommand(_LmServerCommand):
    """Command to add biogeographic hypotheses to a gridset
    """
    scriptName = 'addBioGeoAndTree.py'

    # ................................
    def __init__(self, gridsetId, hypothesesFilenames, treeFilename=None,
                 treeName=None, eventField=None):
        """Construct the command object

        Args:
            gridsetId: The database id of the gridset to add to
            hypothesesFilenames: A list of file locations of hypothesis
                shapefiles
            treeFilename: The file location of the JSON tree to add to the
                gridset
            treeName: If a tree is provided, this is the name of the tree
            eventField: The name of the event field in the hypotheses
                shapefiles
        """
        _LmServerCommand.__init__(self)

        self.args = str(gridsetId)
        if isinstance(hypothesesFilenames, list):
            self.inputs.extend(hypothesesFilenames)
            self.args += ' {}'.format(' '.join(hypothesesFilenames))
        else:
            self.inputs.append(hypothesesFilenames)
            self.args += ' {}'.format(hypothesesFilenames)

        if treeFilename is not None:
            self.opt_args += ' -t {}'.format(treeFilename)
            self.inputs.append(treeFilename)

        if treeName is not None:
            self.opt_args += ' -tn {}'.format(treeName)

        if eventField is not None:
            self.opt_args += ' -e {}'.format(eventField)


# .............................................................................
class AssemblePamFromSolrQueryCommand(_LmServerCommand):
    """Command to assemble PAM data from a Solr query
    """
    scriptName = 'assemble_pam_from_solr.py'

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
class CatalogScenarioPackageCommand(_LmDbServerCommand):
    """This command will catalog a scenario package
    """
    scriptName = 'catalogScenPkg.py'

    # ................................
    def __init__(self, package_metadata_filename, user_id, user_email=None):
        """Construct the command object

        Args:
            package_metadata_filename: The file location of the metadata file
                for the scenario package to be cataloged in the database
            user_id: The user id to use for this package
            user_email: The user email for this package
        """
        _LmDbServerCommand.__init__(self)

        # scen_package_meta may be full pathname or in ENV_DATA_PATH dir
        if not os.path.exists(package_metadata_filename):
            raise Exception(
                'Missing Scenario Package metadata file {}'.format(
                    package_metadata_filename))
        else:
            spBasename, _ = os.path.splitext(
                os.path.basename(package_metadata_filename)) 
            # file ends up in LOG_PATH
            secs = time.time()
            timestamp = "{}".format(
                time.strftime("%Y%m%d-%H%M", time.localtime(secs)))
            logname = '{}.{}.{}.{}'.format(
                self.script_basename, spBasename, user_id, timestamp)
            # Logfile is created by script in LOG_DIR
            logfilename = '{}{}'.format(logname, LMFormat.LOG.ext)

        # Required args
        self.args = '{} {}'.format(package_metadata_filename, user_id)
        # Optional arg, we also want for output 
        self.opt_args += ' --logname={}'.format(logname)
        # Optional arg, if user is not there, add with dummy email if not provided
        if user_email is not None:
            self.opt_args += ' --user_email={}'.format(user_email)

        self.outputs.append(logfilename)


# .............................................................................
class CatalogTaxonomyCommand(_LmDbServerCommand):
    """Command to create taxonomy workflows

    This command will create makeflows to catalog boom archive inputs, create
    GRIMs, create an archive ini file, and run the Boomer Daemon to walk
    through the inputs
    """
    scriptName = 'catalogTaxonomy.py'

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
        else:
            dataBasename, _ = os.path.splitext(
                os.path.basename(taxon_data_filename))
            # file ends up in LOG_PATH
            secs = time.time()
            timestamp = "{}".format(
                time.strftime("%Y%m%d-%H%M", time.localtime(secs)))
            logname = '{}.{}.{}'.format(
                self.script_basename, dataBasename, timestamp)

        # Optional script args, required here
        self.opt_args = ' --taxon_source_name="{}"'.format(source_name)
        self.opt_args += ' --taxon_data_filename={}'.format(
            taxon_data_filename)
        self.opt_args += ' --success_filename={}'.format(
            taxon_success_filename)
        self.opt_args += ' --logname={}'.format(logname)

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
    """Command to encode biogeographic hypotheses
    """
    scriptName = 'encodeBioGeoHypotheses.py'

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
        timestamp = "{}".format(time.strftime("%Y%m%d-%H%M", time.localtime(secs)))
        logname = '{}.{}'.format(self.script_basename, timestamp)

        # Required args
        self.args = '{} {} {}'.format(user_id, gridset_name, success_file)
        # Optional arg, we also want for output 
        self.opt_args += ' --logname={}'.format(logname)

        self.outputs.append(success_file)
#         # Logfile is created by script in LOG_DIR
#         logfilename = '{}{}'.format(logname, LMFormat.LOG.ext)
#         self.outputs.append(logfilename)


# .............................................................................
class CreateBlankMaskTiffCommand(_LmServerCommand):
    """This command will create a mask Tiff file of all ones
    """
    scriptName = 'create_blank_mask.py'

    # ................................
    def __init__(self, inRasterFilename, outRasterFilename):
        """Construct the command object

        Args:
            inRasterFilename: The input raster file to use
            outRasterFilename: The file location to write the output raster
        """
        _LmServerCommand.__init__(self)

        self.args = '{} {}'.format(inRasterFilename, outRasterFilename)
        self.outputs.append(outRasterFilename)


# .............................................................................
class CreateConvexHullShapefileCommand(_LmServerCommand):
    """Command to create a shapefile of the convex hull of the occurrence set
    """
    scriptName = 'create_convex_hull_shapefile.py'

    # ................................
    def __init__(self, occId, outFilename, bufferDistance=None):
        """Construct the command object

        Args:
            occId: The database id of the occurrence set to use
            outFilename: The file location to write the shapefile
            bufferDistance: A buffer, in map units, to include with the convex hull
        """
        _LmServerCommand.__init__(self)
        self.args = '{} {}'.format(occId, outFilename)
        if bufferDistance is not None:
            self.opt_args += ' -b {}'.format(bufferDistance)


# .............................................................................
class CreateMaskTiffCommand(_LmServerCommand):
    """This command will create a mask Tiff file

    Todo:
        Probably should rename this to be more specific
    """
    scriptName = 'create_mask_tiff.py'

    # ................................
    def __init__(self, inRasterFilename, pointsFilename, outRasterFilename):
        """Construct the command object

        Args:
            inRasterFilename: The input raster file to use
            pointsFilename: The path to the points shapefile to use
            outRasterFilename: The file location to write the output raster
        """
        _LmServerCommand.__init__(self)

        self.args = '{} {} {}'.format(inRasterFilename, pointsFilename, 
                                                outRasterFilename)
        #self.inputs.extend([inRasterFilename, pointsFilename])
        self.outputs.append(outRasterFilename)

# .............................................................................
class IndexPAVCommand(_LmServerCommand):
    """This command will post PAV information to a solr index
    """
    scriptName = 'indexPAV.py'

    # ................................
    def __init__(self, pavFilename, pavId, projId, pamId, pavIdxFilename):
        """Construct the command object

        Args:
            pavFilename: The file location of the PAV matrix
            pavId: The database id of the PAV
            projId: The database id of the projection used to build the PAV
            pamId: The database id of the PAM that the PAV belongs to
            pavIdxFilename: The file location to write the POST data
        """
        _LmServerCommand.__init__(self)
        self.inputs.append(pavFilename)
        self.outputs.append(pavIdxFilename)

        self.args = '{} {} {} {} {}'.format(
            pavFilename, pavId, projId, pamId, pavIdxFilename)


# .............................................................................
class LmTouchCommand(_LmServerCommand):
    """This command will touch a file, creating necessary directories  
    """
    scriptName = 'lmTouch.py'

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
    """This command indexes multiple PAV objects in one call
    """
    scriptName = 'index_pavs.py'

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
    """This command stockpiles multiple objects in one call
    """
    scriptName = 'multi_stockpile.py'

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
    """This command will shoot snippets into an index
    """
    scriptName = 'shootSnippets.py'

    # ................................
    def __init__(self, occSetId, operation, postFilename, o2ident=None, 
                             url=None, who=None, agent=None, why=None):
        """Construct the command object

        Args:
            occSetId: The occurrence set id to generate snippets for
            operation: The operation performed (see SnippetOperations)
            postFilename: The file location to store the data posted
            o2ident: An identifier for an optional target object
            url: A URL associated with this action
            who: Who initiated this action
            agent: The agent used to initiate this action
            why: Why this action was initiated
        """
        _LmServerCommand.__init__(self)
        self.outputs.append(postFilename)

        self.args = '{} {} {}'.format(occSetId, operation, postFilename)
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
    """Add SQUIDs and node labels to tree
    """
    scriptName = 'add_squids_to_tree.py'

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
    """This command will stockpile the outputs of a process
    """
    scriptName = 'stockpile.py'

    # ................................
    def __init__(self, pType, objectId, successFilename, objOutputFilenames, 
                 status=None, statusFilename=None, metadataFilename=None):
        """Construct the command object

        Args:
            pType: The process type of the outputs
            objectId: The id of the object
            successFilename: The file location of the output file to create if
                the process is successful
            objOutputFilenames: A list of object files to test
            status: An optional object status (currently not used) to update
                the database with
            statusFilename: The location of a file that contains status
                information for the object
            metadataFilename: The file location of metadata about this object

        Todo:
            use status in stockpile script
        """
        _LmServerCommand.__init__(self)

        self.args = '{} {} {}'.format(pType, objectId, successFilename)
        self.outputs.append(successFilename)

        if isinstance(objOutputFilenames, list):
            self.inputs.extend(objOutputFilenames)
            self.args += ' {}'.format(' '.join(objOutputFilenames))
        else:
            self.inputs.append(objOutputFilenames)
            self.args += ' {}'.format(objOutputFilenames)

        if status is not None:
            self.opt_args += ' -s {}'.format(status)

        if statusFilename is not None:
            self.opt_args += ' -f {}'.format(statusFilename)
            self.inputs.append(statusFilename)

        if metadataFilename is not None:
            self.inputs.append(metadataFilename)
            self.opt_args += ' -m {}'.format(metadataFilename)
