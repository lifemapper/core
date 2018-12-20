"""
@summary: This module contains command objects for server processes
@author: CJ Grady
@license: gpl2
@copyright: Copyright (C) 2018, University of Kansas Center for Research

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
import time

from LmBackend.command.base import _LmCommand
from LmBackend.common.lmconstants import (CMD_PYBIN, DB_SERVER_SCRIPTS_DIR, 
                                                        SERVER_SCRIPTS_DIR)
from LmCommon.common.lmconstants import LMFormat

# .............................................................................
class _LmServerCommand(_LmCommand):
    """
    @summary: The _LmServerCommand class is an intermediate class that all 
                     server command classes should inherit from.
    """
    relDir = SERVER_SCRIPTS_DIR
    # ................................
    def getMakeflowRule(self, local=True):
        """
        @summary: Get a MfRule object for this command
        @param local: Should this be a local command in a Makeflow DAG
        @note: This differs from the superclass because the default local is True
        """
        return super(_LmServerCommand, self).getMakeflowRule(local=local)

# .............................................................................
class _LmDbServerCommand(_LmCommand):
    """
    @summary: The _LmServerCommand class is an intermediate class that all 
                     server command classes should inherit from.
    """
    relDir = DB_SERVER_SCRIPTS_DIR
    # ................................
    def getMakeflowRule(self, local=True):
        """
        @summary: Get a MfRule object for this command
        @param local: Should this be a local command in a Makeflow DAG
        @note: This differs from the superclass because the default local is True
        """
        return super(_LmDbServerCommand, self).getMakeflowRule(local=local)

# .............................................................................
class AddBioGeoAndTreeCommand(_LmServerCommand):
    """
    @summary: This command will add biogeographic hypotheses and a tree to a 
                     grid set
    """
    scriptName = 'addBioGeoAndTree.py'

    # ................................
    def __init__(self, gridsetId, hypothesesFilenames, treeFilename=None, 
                             treeName=None, eventField=None):
        """
        @summary: Construct the command object
        @param gridsetId: The database id of the gridset to add to
        @param hypothesesFilenames: A list of file locations of hypothesis 
                                                 shapefiles
        @param treeFilename: The file location of the JSON tree to add to the 
                                        gridset
        @param treeName: If a tree is provided, this is the name of the tree
        @param eventField: The name of the event field in the hypotheses 
                                     shapefiles
        """
        _LmServerCommand.__init__(self)
        
        self.args = [str(gridsetId)]
        if isinstance(hypothesesFilenames, list):
            self.inputs.extend(hypothesesFilenames)
            self.args.extend(hypothesesFilenames)
        else:
            self.inputs.append(hypothesesFilenames)
            self.args.append(hypothesesFilenames)

        self.optArgs = ''
        if treeFilename is not None:
            self.optArgs += ' -t {}'.format(treeFilename)
            self.inputs.append(treeFilename)
        
        if treeName is not None:
            self.optArgs += ' -tn {}'.format(treeName)
            
        if eventField is not None:
            self.optArgs += ' -e {}'.format(eventField)

    # ................................
    def getCommand(self):
        """
        @summary: Get the raw command to run on the system
        """
        return '{} {} {} {}'.format(CMD_PYBIN, self.getScript(), 
                self.optArgs, ' '.join(self.args))

# .............................................................................
class CatalogScenarioPackageCommand(_LmDbServerCommand):
    """
    @summary: This command will catalog a scenario package
    """
    scriptName = 'catalogScenPkg.py'

    # ................................
    def __init__(self, package_metadata_filename, user_id, user_email=None):
        """
        @summary: Construct the command object
        @param package_metadata_filename: The file location of the metadata file 
                     for the scenario package to be cataloged in the database
        @param user_id: The user id to use for this package
        @param user_email: The user email for this package
        """
        _LmDbServerCommand.__init__(self)
        
        # scen_package_meta may be full pathname or in ENV_DATA_PATH dir
        if not os.path.exists(package_metadata_filename):
            raise Exception('Missing Scenario Package metadata file {}'.format(package_metadata_filename))
        else:
            spBasename, _ = os.path.splitext(os.path.basename(package_metadata_filename)) 
            # file ends up in LOG_PATH
            secs = time.time()
            timestamp = "{}".format(time.strftime("%Y%m%d-%H%M", time.localtime(secs)))
            logname = '{}.{}.{}.{}'.format(self.scriptBasename, spBasename, user_id, timestamp)
            # Logfile is created by script in LOG_DIR
            logfilename = '{}{}'.format(logname, LMFormat.LOG.ext)
            
        # Required args
        self.args = '{} {}'.format(package_metadata_filename, user_id)
        # Optional arg, we also want for output 
        self.args += ' --logname={}'.format(logname)
        # Optional arg, if user is not there, add with dummy email if not provided
        if user_email is not None:
            self.args += ' --user_email={}'.format(user_email)
            
        self.outputs.append(logfilename)
            
    # ................................
    def getCommand(self):
        """
        @summary: Get the command
        """
        return '{} {} {}'.format(CMD_PYBIN, self.getScript(), self.args)

# .............................................................................
class CatalogBoomCommand(_LmDbServerCommand):
    """
    @summary: This command will create makeflows to:
                    * catalog boom archive inputs,
                    * catalog ScenarioPackage if necessary
                    * create GRIMs, 
                    * create an archive ini file, and 
                    * start the Boomer to walk through inputs
    """
    scriptName = 'initBoomJob.py'

    # ................................
    def __init__(self, config_filename, init_makeflow=False):
        """
        @summary: Construct the command object
        @param config_filename: The file location of the ini file 
                 with parameters for a boom/gridset
        """
        _LmDbServerCommand.__init__(self)
        
        # scen_package_meta may be full pathname or in ENV_DATA_PATH dir
        if not os.path.exists(config_filename):
            raise Exception('Missing Boom configuration file {}'.format(config_filename))
        else:
            boomBasename, _ = os.path.splitext(os.path.basename(config_filename)) 
            # file ends up in LOG_PATH
            secs = time.time()
            timestamp = "{}".format(time.strftime("%Y%m%d-%H%M", time.localtime(secs)))
            logname = '{}.{}.{}'.format(self.scriptBasename, boomBasename, timestamp)
            # Logfile is created by script in LOG_DIR
            logfilename = '{}{}'.format(logname, LMFormat.LOG.ext)
            
        # Required args
        self.args = config_filename
        # Optional arg, we also want for output 
        self.args += ' --logname={}'.format(logname)
        # Optional arg, defaults to False
        if init_makeflow:
            self.args += ' --init_makeflow=True'
            
        self.outputs.append(logfilename)
            
    # ................................
    def getCommand(self):
        """
        @summary: Get the command
        """
        return '{} {} {}'.format(CMD_PYBIN, self.getScript(), self.args)

# .............................................................................
class CatalogTaxonomyCommand(_LmDbServerCommand):
    """
    @summary: This command will create makeflows to catalog boom archive inputs,
                 create GRIMs, create an archive ini file, and run the Boomer Daemon
                 to walk through the inputs
    """
    scriptName = 'catalogTaxonomy.py'

    # ................................
    def __init__(self, source_name, taxon_data_filename, taxon_success_filename,
                     source_url=None, delimiter='\t'):
        """
        @summary: Construct the command object
        @param source_name: The taxonomic authority (locally unique) name/identifier 
                                  for the data 
        @param taxon_filename: The file location of the taxonomy csv file 
        @param source_url: The unique URL for the taxonomic authority
        @param delimiter: Delimiter for the data file
        """
        _LmDbServerCommand.__init__(self)
        
        # scen_package_meta may be full pathname or in ENV_DATA_PATH dir
        if not os.path.exists(taxon_data_filename):
            raise Exception('Missing Taxonomy data file {}'.format(taxon_data_filename))
        else:
            dataBasename, _ = os.path.splitext(os.path.basename(taxon_data_filename)) 
            # file ends up in LOG_PATH
            secs = time.time()
            timestamp = "{}".format(time.strftime("%Y%m%d-%H%M", time.localtime(secs)))
            logname = '{}.{}.{}'.format(self.scriptBasename, dataBasename, timestamp)
            
        # Optional script args, required here
        self.args =  ' --taxon_source_name=\'{}\''.format(source_name)
        self.args += ' --taxon_data_filename={}'.format(taxon_data_filename)
        self.args += ' --success_filename={}'.format(taxon_success_filename)      
        self.args += ' --logname={}'.format(logname)

        # Optional args
        if source_url:
            self.args += ' --taxon_source_url={}'.format(source_url)
        if delimiter != '\t': 
            self.args += ' --delimiter={}'.format(delimiter)
            
        self.outputs.append(taxon_success_filename)
#         # Logfile is created by script in LOG_DIR
#         logfilename = '{}{}'.format(logname, LMFormat.LOG.ext)
#         self.outputs.append(logfilename)
            
    # ................................
    def getCommand(self):
        """
        @summary: Get the command
        """
        return '{} {} {}'.format(CMD_PYBIN, self.getScript(), self.args)
    
# .............................................................................
class EncodeTreeCommand(_LmServerCommand):
    """
    @summary: This command will create a makeflow to encode a tree with 
                 species identifiers.
    """
    scriptName = 'encodeTree.py'

    # ................................
    def __init__(self, user_id, tree_name, success_file):
        """
        @summary: Construct the command object
        @param user_id: User for the tree and gridset
        @param tree_name: The unique tree name
        """
        _LmServerCommand.__init__(self)
        
        # file ends up in LOG_PATH
        secs = time.time()
        timestamp = "{}".format(time.strftime("%Y%m%d-%H%M", time.localtime(secs)))
        log_name = '{}.{}'.format(self.scriptBasename, timestamp)
            
        # Required args
        self.args = '{} {} {}'.format(user_id, tree_name, success_file)
        # Optional arg, we also want for output 
        self.args += ' --logname={}'.format(log_name)

        self.outputs.append(success_file)
#         # Logfile is created by script in LOG_DIR
#         log_file = '{}{}'.format(log_name, LMFormat.LOG.ext)
#         self.outputs.append(log_file)
            
    # ................................
    def getCommand(self):
        """
        @summary: Get the command
        """
        return '{} {} {}'.format(CMD_PYBIN, self.getScript(), self.args)


# .............................................................................
class EncodeBioGeoHypothesesCommand(_LmServerCommand):
    """
    @summary: This command will create a makeflow to encode a tree with 
                 species identifiers.
    """
    scriptName = 'encodeBioGeoHypotheses.py'

    # ................................
    def __init__(self, user_id, gridset_name, success_file):
        """
        @summary: Construct the command object
        @param user_id: User for the gridset
        @param gridset_name: The unique gridset name
        """
        _LmServerCommand.__init__(self)
        
        # file ends up in LOG_PATH
        secs = time.time()
        timestamp = "{}".format(time.strftime("%Y%m%d-%H%M", time.localtime(secs)))
        logname = '{}.{}'.format(self.scriptBasename, timestamp)
            
        # Required args
        self.args = '{} {} {}'.format(user_id, gridset_name, success_file)
        # Optional arg, we also want for output 
        self.args += ' --logname={}'.format(logname)

        self.outputs.append(success_file)
#         # Logfile is created by script in LOG_DIR
#         logfilename = '{}{}'.format(logname, LMFormat.LOG.ext)
#         self.outputs.append(logfilename)
            
    # ................................
    def getCommand(self):
        """
        @summary: Get the command
        """
        return '{} {} {}'.format(CMD_PYBIN, self.getScript(), self.args)



# .............................................................................
class CreateBlankMaskTiffCommand(_LmServerCommand):
    """
    @summary: This command will create a mask Tiff file of all ones
    """
    scriptName = 'create_blank_mask.py'

    # ................................
    def __init__(self, inRasterFilename, outRasterFilename):
        """
        @summary: Construct the command object
        @param inRasterFilename: The input raster file to use
        @param outRasterFilename: The file location to write the output raster
        """
        _LmServerCommand.__init__(self)
        
        self.args = '{} {}'.format(inRasterFilename, outRasterFilename)
        self.outputs.append(outRasterFilename)
            
    # ................................
    def getCommand(self):
        """
        @summary: Get the concatenate matrices command
        """
        return '{} {} {}'.format(CMD_PYBIN, self.getScript(), self.args)

# .............................................................................
class CreateConvexHullShapefileCommand(_LmServerCommand):
    """
    @summary: This command will write a shapefile containing a feature with the 
                     convex hull of the occurrence set
    """
    scriptName = 'create_convex_hull_shapefile.py'

    # ................................
    def __init__(self, occId, outFilename, bufferDistance=None):
        """
        @summary: Construct the command object
        @param occId: The database id of the occurrence set to use
        @param outFilename: The file location to write the shapefile
        @param bufferDistance: A buffer, in map units, to include with the convex hull
        """
        _LmServerCommand.__init__(self)
        self.args = '{} {}'.format(occId, outFilename)
        self.optArgs = ''
        if bufferDistance is not None:
            self.optArgs += ' -b {}'.format(bufferDistance)
        
    # ................................
    def getCommand(self):
        """
        @summary: Get the raw command to run on the system
        """
        return '{} {} {} {}'.format(CMD_PYBIN, self.getScript(), 
                self.optArgs, self.args)

# .............................................................................
class CreateMaskTiffCommand(_LmServerCommand):
    """
    @summary: This command will create a mask Tiff file
    @todo: Probably should rename this to be more specific
    """
    scriptName = 'create_mask_tiff.py'

    # ................................
    def __init__(self, inRasterFilename, pointsFilename, outRasterFilename):
        """
        @summary: Construct the command object
        @param inRasterFilename: The input raster file to use
        @param pointsFilename: The path to the points shapefile to use
        @param outRasterFilename: The file location to write the output raster
        """
        _LmServerCommand.__init__(self)
        
        self.args = '{} {} {}'.format(inRasterFilename, pointsFilename, 
                                                outRasterFilename)
        #self.inputs.extend([inRasterFilename, pointsFilename])
        self.outputs.append(outRasterFilename)
            
    # ................................
    def getCommand(self):
        """
        @summary: Get the concatenate matrices command
        """
        return '{} {} {}'.format(CMD_PYBIN, self.getScript(), self.args)

# .............................................................................
class IndexPAVCommand(_LmServerCommand):
    """
    @summary: This command will post PAV information to a solr index
    """
    scriptName = 'indexPAV.py'

    # ................................
    def __init__(self, pavFilename, pavId, projId, pamId, pavIdxFilename):
        """
        @summary: Construct the command object
        @param pavFilename: The file location of the PAV matrix
        @param pavId: The database id of the PAV
        @param projId: The database id of the projection used to build the PAV
        @param pamId: The database id of the PAM that the PAV belongs to
        @param pavIdxFilename: The file location to write the POST data
        """
        _LmServerCommand.__init__(self)
        self.inputs.append(pavFilename)
        self.outputs.append(pavIdxFilename)
        
        self.args = [pavFilename, str(pavId), str(projId), 
                         str(pamId), pavIdxFilename]

    # ................................
    def getCommand(self):
        """
        @summary: Get the raw command to run on the system
        """
        return '{} {} {}'.format(CMD_PYBIN, self.getScript(), ' '.join(self.args))

# .............................................................................
class LmTouchCommand(_LmServerCommand):
    """
    @summary: This command will touch a file, creating necessary directories  
    """
    scriptName = 'lmTouch.py'

    # ................................
    def __init__(self, filename):
        """
        @summary: Construct the command object
        @param filename: The file location to touch
        """
        _LmServerCommand.__init__(self)
        self.outputs.append(filename)
        self.filename = filename

    # ................................
    def getCommand(self):
        """
        @summary: Get the raw command to run on the system
        """
        return '{} {} {}'.format(CMD_PYBIN, self.getScript(), self.filename)

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

    # ..............................
    def getCommand(self):
        """Get the raw command to run on the system
        """
        return '{} {} {}'.format(CMD_PYBIN, self.getScript(), self.args)
    
# .............................................................................
class MultiStockpileCommand(_LmServerCommand):
    """This command stockpiles multiple objects in one call
    """
    scriptName = 'multi_stockpile.py'

    # ..............................
    def __init__(self, stockpile_filename, success_filename):
        """Construct the command object

        Args:
            stockpile_filename : A JSON file with stockpile information
            success_filename : A file location to write success or failure
        """
        _LmServerCommand.__init__(self)
        self.inputs.append(stockpile_filename)
        self.outputs.append(success_filename)
        self.args = '{} {}'.format(stockpile_filename, success_filename)

    # ..............................
    def getCommand(self):
        """Get the raw command to run on the system
        """
        return '{} {} {}'.format(CMD_PYBIN, self.getScript(), self.args)

# .............................................................................
class ShootSnippetsCommand(_LmServerCommand):
    """
    @summary: This command will shoot snippets into an index
    """
    scriptName = 'shootSnippets.py'

    # ................................
    def __init__(self, occSetId, operation, postFilename, o2ident=None, 
                             url=None, who=None, agent=None, why=None):
        """
        @summary: Construct the command object
        @param occSetId: The occurrence set id to generate snippets for
        @param operation: The operation performed (see SnippetOperations)
        @param postFilename: The file location to store the data posted
        @param o2ident: An identifier for an optional target object
        @param url: A URL associated with this action
        @param who: Who initiated this action
        @param agent: The agent used to initiate this action
        @param why: Why this action was initiated
        """
        _LmServerCommand.__init__(self)
        self.outputs.append(postFilename)
        
        self.args = [str(occSetId), operation, postFilename]
        self.optArgs = ''
        if o2ident is not None:
            self.optArgs += ' -o2ident {}'.format(o2ident)
        
        if url is not None:
            self.optArgs += ' -url {}'.format(url)
            
        if who is not None:
            self.optArgs += ' -who {}'.format(who)
            
        if agent is not None:
            self.optArgs += ' -agent {}'.format(agent)
            
        if why is not None:
            self.optArgs += ' -why {}'.format(why)

    # ................................
    def getCommand(self):
        """
        @summary: Get the raw command to run on the system
        """
        return '{} {} {} {}'.format(CMD_PYBIN, self.getScript(), 
                self.optArgs, ' '.join(self.args))

# .............................................................................
class SquidIncCommand(_LmServerCommand):
    """
    @summary: This command will add squids to a tree
    """
    scriptName = 'squid_inc.py'

    # ................................
    def __init__(self, treeFilename, userId, outTreeFilename):
        """
        @summary: Construct the command object
        @param treeFilename: The file location of the original tree
        @param userId: The user id, used for generating squids
        @param outTreeFilename: The file location of the resulting tree
        """
        _LmServerCommand.__init__(self)
        self.inputs.append(treeFilename)
        self.outputs.append(outTreeFilename)
        
        self.args = [treeFilename, userId, outTreeFilename]

    # ................................
    def getCommand(self):
        """
        @summary: Get the raw command to run on the system
        """
        return '{} {} {}'.format(CMD_PYBIN, self.getScript(), ' '.join(self.args))

# .............................................................................
class StockpileCommand(_LmServerCommand):
    """
    @summary: This command will stockpile the outputs of a process
    """
    scriptName = 'stockpile.py'

    # ................................
    def __init__(self, pType, objectId, successFilename, objOutputFilenames, 
                     status=None, statusFilename=None, metadataFilename=None):
        """
        @summary: Construct the command object
        @param pType: The process type of the outputs
        @param objectId: The id of the object
        @param successFilename: The file location of the output file to create if 
                                            the process is successful
        @param objOutputFilenames: A list of object files to test
        @param status: An optional object status to update the database with
        @param statusFilename: The location of a file that contains status 
                                          information for the object
        @param metadataFilename: The file location of metadata about this object
        """
        _LmServerCommand.__init__(self)
        
        self.args = [str(pType), str(objectId), successFilename]
        self.outputs.append(successFilename)
        
        if isinstance(objOutputFilenames, list):
            self.args.extend(objOutputFilenames)
            self.inputs.extend(objOutputFilenames)
        else:
            self.inputs.append(objOutputFilenames)
            self.args.append(objOutputFilenames)
        
        self.optArgs = ''
        if status is not None:
            self.optArgs += ' -s {}'.format(status)
            
        if statusFilename is not None:
            self.optArgs += ' -f {}'.format(statusFilename)
            self.inputs.append(statusFilename)
            
        if metadataFilename is not None:
            self.inputs.append(metadataFilename)
            self.optArgs += ' -m {}'.format(metadataFilename)

    # ................................
    def getCommand(self):
        """
        @summary: Get the raw command to run on the system
        """
        return '{} {} {} {}'.format(CMD_PYBIN, self.getScript(), 
                self.optArgs, ' '.join(self.args))

# .............................................................................
class TriageCommand(_LmServerCommand):
    """
    @summary: This command will determine which files referenced in the input
                     file exist and will output a file containing those references
    """
    scriptName = 'triage.py'

    # ................................
    def __init__(self, inFilename, outFilename):
        """
        @summary: Construct the command object
        @param inFilename: The file location of a file containing a list of 
                                     potential target filenames
        @param outFilename: The file location to write the output file indicating
                                      which of the potential targets actually exist
        """
        _LmServerCommand.__init__(self)
        self.inputs.append(inFilename)
        self.outputs.append(outFilename)
        
        self.args = [inFilename, outFilename]

    # ................................
    def getCommand(self):
        """
        @summary: Get the raw command to run on the system
        """
        return '{} {} {}'.format(CMD_PYBIN, self.getScript(), ' '.join(self.args))

