"""
"""
import os

from LmBackend.common.lmobj import LMError, LMObject

from LmCommon.common.lmconstants import (LMFormat, DEFAULT_POST_USER, 
                                         DEFAULT_EPSG, DEFAULT_MAPUNITS) 
from LmCommon.common.time import gmt

from LmDbServer.common.lmconstants import TNCMetadata, TAXONOMIC_SOURCE

from LmServer.common.lmconstants import Algorithms
from LmServer.common.lmconstants import (ENV_DATA_PATH, DEFAULT_EMAIL_POSTFIX)
from LmServer.common.lmuser import LMUser
from LmServer.common.localconstants import PUBLIC_USER
from LmServer.common.log import ScriptLogger
from LmServer.base.layer2 import Vector
from LmServer.base.serviceobject2 import ServiceObject
from LmServer.db.borgscribe import BorgScribe
from LmServer.legion.algorithm import Algorithm

# .............................................................................
class Defcat(LMObject):
    """
    @summary 
    Class to: 
      1) populate a Lifemapper database with scenario package for a BOOM archive
    """
    # .............................................................................
    # Constructor
    # .............................................................................
    def __init__(self, logname):
        """
        @summary Constructor for BOOMFiller class.
        """
        super(Defcat, self).__init__()
        self.name = self.__class__.__name__.lower()
              
        # Get database
        try:
            self.scribe = self._getDb(logname)
        except: 
            raise
        self.open()
       
    # ...............................................
    def open(self):
        success = self.scribe.openConnections()
        if not success: 
            raise LMError('Failed to open database')
    
    # ...............................................
    def close(self):
        self.scribe.closeConnections()
    
    # ...............................................
    @property
    def logFilename(self):
        try:
            fname = self.scribe.log.baseFilename
        except:
            fname = None
        return fname
          
    # ...............................................
    def _getDb(self, logname):
        import logging
        logger = ScriptLogger(logname, level=logging.INFO)
        # DB connection
        scribe = BorgScribe(logger)
        return scribe
    
    
    # ...............................................
    def addUsers(self):
        """
        @summary Adds or finds PUBLIC_USER, DEFAULT_POST_USER and USER arguments 
              in the database
        """
        currtime = gmt().mjd
        defUsers = [PUBLIC_USER, DEFAULT_POST_USER]
        ids = []
        
        for usr in defUsers:
            email = '{}{}'.format(usr, DEFAULT_EMAIL_POSTFIX)
            # Nothing changes if these are already present
            lmuser = LMUser(usr, email, email, mod_time=currtime)
            
            self.scribe.log.info('  Find or insert user {} ...'.format(usr))
            # If exists, found by unique Id or unique Email, update object and return existing
            thisUser = self.scribe.findOrInsertUser(lmuser)
            ids.append(thisUser.userid)
        return ids
    
    # ...............................................
    def addTaxonomicSources(self):
        """
        @summary Adds Taxonomic Sources to the database from the TAXONOMIC_SOURCE dictionary
        """
        # Insert all taxonomic sources for now
        for name, taxInfo in TAXONOMIC_SOURCE.items():
            taxSourceId = self.scribe.findOrInsertTaxonSource(taxInfo['name'],
                                                              taxInfo['url'])
        
    # .............................
    def addTNCEcoregions(self):
        meta = {Vector.META_IS_CATEGORICAL: TNCMetadata.isCategorical, 
                ServiceObject.META_TITLE: TNCMetadata.title, 
                ServiceObject.META_AUTHOR: TNCMetadata.author, 
                ServiceObject.META_DESCRIPTION: TNCMetadata.description,
                ServiceObject.META_KEYWORDS: TNCMetadata.keywords,
                ServiceObject.META_CITATION: TNCMetadata.citation,
                }
        dloc = os.path.join(ENV_DATA_PATH, 
                            TNCMetadata.filename + LMFormat.getDefaultOGR().ext)
        ecoregions = Vector(TNCMetadata.title, PUBLIC_USER, DEFAULT_EPSG, 
                            ident=None, dlocation=dloc, 
                            metadata=meta, dataFormat=LMFormat.getDefaultOGR().driver, 
                            ogrType=TNCMetadata.ogrType,
                            valAttribute=TNCMetadata.valAttribute, 
                            mapunits=DEFAULT_MAPUNITS, bbox=TNCMetadata.bbox,
                            mod_time=gmt().mjd)
        updatedEcoregions = self.scribe.findOrInsertLayer(ecoregions)
        return updatedEcoregions
    
    # ...............................................
    def addAlgorithms(self):
        """
        @summary Adds algorithms to the database from the algorithm dictionary
        """
        algs = []
        for alginfo in Algorithms.implemented():
            meta = {'name': alginfo.name, 
                    'isDiscreteOutput': alginfo.isDiscreteOutput,
                    'outputFormat': alginfo.outputFormat,
                    'acceptsCategoricalMaps': alginfo.acceptsCategoricalMaps}
            alg = Algorithm(alginfo.code, metadata=meta)
            self.scribe.log.info('  Insert algorithm {} ...'.format(alginfo.code))
            algid = self.scribe.findOrInsertAlgorithm(alg)
            algs.append(algid)
        
    # ...............................................
    def addDefaults(self):
        """
        @summary Inserts or locates PUBLIC_USER, DEFAULT_POST_USER, 
                 TAXONOMIC_SOURCE, ALGORITHMS, and TNC_ECOREGIONS in the database
        """
        # Insert PUBLIC_USER, DEFAULT_POST_USER 
        self.scribe.log.info('  Insert public and default users ...')
        self.addUsers()
        
        # Insert all taxonomic sources used by LM
        self.scribe.log.info('  Insert taxonomic authority metadata ...')
        self.addTaxonomicSources()
        
        # Insert all algorithms 
        self.scribe.log.info('  Insert Algorithms ...')
        self.addAlgorithms()
        
        # Insert all algorithms 
        self.scribe.log.info('  Insert TNC Ecoregions ...')
        self.addTNCEcoregions()


   
# ...............................................
if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(
             description=("""Populate a Lifemapper archive with metadata 
                             for default data and parameters """))
    # Optional
    parser.add_argument('--logname', type=str, default=None,
             help=('Basename of the logfile, without extension'))
    
    args = parser.parse_args()
    logname = args.logname
    
    if logname is None:
        import time
        scriptname, _ = os.path.splitext(os.path.basename(__file__))
        secs = time.time()
        timestamp = "{}".format(time.strftime("%Y%m%d-%H%M", time.localtime(secs)))
        logname = '{}.{}'.format(scriptname, timestamp)
    
    print(('Running {} with logbasename: {}'
          .format(scriptname, logname)))
    
    defcat = Defcat(logname)
    defcat.addDefaults()

"""
from LmDbServer.tools.catalogDefaults import *

logname = '/state/partition1/lmscratch/log/test_catalogDefaults.log'

import logging
logger = ScriptLogger(logname, level=logging.INFO)

########################
from LmServer.db.borgscribe import *

########################

# scribe = BorgScribe(logger)
# scribe.openConnections()

########################
# defcat = Defcat(logname)

########################
# defcat.addDefaults()
########################

"""
