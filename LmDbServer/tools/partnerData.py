"""Module containing functions for API Queries
"""
try:
    from osgeo.ogr import OFTInteger, OFTReal, OFTString, OFTBinary
except:
    OFTInteger = 0 
    OFTReal = 2 
    OFTString = 4
    OFTBinary = 8

import json
import os
import sys
import urllib.request, urllib.error, urllib.parse

from LmBackend.common.lmobj import LMError

from LmCommon.common.api_query import GbifAPI
from LmCommon.common.lmconstants import PhyloTreeKeys
from LmCommon.common.readyfile import get_unicodecsv_writer
from LmCommon.common.time import gmt

from LmDbServer.common.lmconstants import (TAXONOMIC_SOURCE, SpeciesDatasource)

from LmServer.base.taxon import ScientificName
from LmServer.db.borgscribe import BorgScribe
from LmServer.common.log import ScriptLogger
from LmServer.legion.tree import Tree

DEV_SERVER = 'http://141.211.236.35:10999'
INDUCED_SUBTREE_BASE_URL = '{}/induced_subtree'.format(DEV_SERVER)
OTTIDS_FROM_GBIFIDS_URL = '{}/ottids_from_gbifids'.format(DEV_SERVER)
GBIF_MISSING_KEY = GbifAPI.GBIF_MISSING_KEY

# .............................................................................
class Partners(object):
    OTT_MISSING_KEY = 'unmatched_ott_ids'
    OTT_TREE_KEY = 'newick'
    OTT_TREE_FORMAT = 'newick'

# .............................................................................
class LABEL_FORMAT(object):
    """
    @TODO: pull this from rpm of `ot_service_wrapper`
    @summary: This class represents label format constants that can be used 
              when calling the induced subtree function
    """
    NAME = 'name'
    ID = 'id'
    NAME_AND_ID = 'name_and_id'

# .............................................................................
def get_ottids_from_gbifids(gbif_ids):
    """
    @summary: Calls the Open Tree 'ottids_from_gbifids' service to retrieve a 
              mapping dictionary from the Open Tree service where each key is 
              one of the provided GBIF identifiers and the value is the 
              corresponding OpenTree id.
    @note: Any GBIF ID that was not found will have a value of None
    @param gbif_ids: A list of GBIF identifiers.  They will be converted to
                       integers in the request.
    @return: a dictionary with each key is an 'ACCEPTED' TaxonId from the GBIF 
             Backbone Taxonomy and the value is the corresponding OpenTree id.
    """
    if not(isinstance(gbif_ids, list)):
        gbif_ids = [gbif_ids]
    # Ids need to be integers
    processed_ids = [int(gid) for gid in gbif_ids]
      
    request_body = {
      "gbif_ids" : processed_ids
    }
   
    headers = {
     'Content-Type' : 'application/json'
    }
    req = urllib.request.Request(OTTIDS_FROM_GBIFIDS_URL, 
                          data=json.dumps(request_body), headers=headers)
   
    resp = json.load(urllib.request.urlopen(req))
    unmatchedIds = resp[GBIF_MISSING_KEY]
   
    id_map = resp["gbif_ott_id_map"]
   
    for gid in unmatchedIds:
        id_map[gid] = None
   
    return id_map

# .............................................................................
def induced_subtree(ott_ids, label_format=LABEL_FORMAT.NAME):
    """
    @summary: Calls the Open Tree 'induced_subtree' service to retrieve a tree,
                 in Newick format, containing the nodes represented by the 
                 provided Open Tree IDs
    @param ott_ids: A list of Open Tree IDs.  These will be converted to into
                       integers
    @param label_format: The label string format to use when creating the tree
                            on the server 
    """
    # Ids need to be integers
    processed_ids = [int(ottid) for ottid in ott_ids]
    request_body = {
       "ott_ids" : processed_ids,
       "label_format" : label_format
    }
    
    headers = {
       'Content-Type' : 'application/json'
    }
    req = urllib.request.Request(INDUCED_SUBTREE_BASE_URL, 
                          data=json.dumps(request_body), headers=headers)
    
    resp_str = urllib.request.urlopen(req).read()
    return json.loads(resp_str)

# .............................................................................
class PartnerQuery(object):
    """
    Class to query iDigBio for species data and OTOL for phylogenetic trees 
    using 'ACCEPTED' TaxonIDs from the GBIF Backbone Taxonomy
    """
    def __init__(self, logger=None):
        """
        @summary Constructor for the PartnerQuery class
        """
        self.name = self.__class__.__name__.lower()
        if logger is None:
            logger = ScriptLogger(self.name)
        self.log = logger
        self.encoding = 'utf-8'
        self.delimiter = '\t'

    # .............................................................................
    def _convertType(self, ogrtype):
        if ogrtype == OFTInteger:
            return 'int'
        elif ogrtype == OFTString:
            return 'str'
        elif ogrtype == OFTReal:
            return 'float'
        else:
            raise LMError('Unknown field type {}'.format(ogrtype))
            
    # ...............................................
    def _getInsertSciNameForGBIFSpeciesKey(self, scribe, gbifSrcId, taxonKey):
        """
        Returns an existing or newly inserted ScientificName
        """
        sciName = scribe.getTaxon(taxonSourceId=gbifSrcId, 
                                  taxonKey=taxonKey)
        if sciName is None:
            # Use API to get and insert species name 
            try:
                (rankStr, scinameStr, canonicalStr, acceptedKey, acceptedStr, 
                 nubKey, taxStatus, kingdomStr, phylumStr, classStr, orderStr, 
                 familyStr, genusStr, speciesStr, genusKey, speciesKey, 
                 loglines) = GbifAPI.getTaxonomy(taxonKey)
            except Exception as e:
                self.log.info('Failed lookup for key {}, ({})'.format(
                                                      taxonKey, e))
            else:
                # if no species key, this is not a species
                if rankStr in ('SPECIES', 'GENUS') and taxStatus == 'ACCEPTED':
                    currtime = gmt().mjd
                    sname = ScientificName(scinameStr, 
                                  rank=rankStr, 
                                  canonicalName=canonicalStr,
                                  kingdom=kingdomStr, phylum=phylumStr, 
                                  txClass=classStr, txOrder=orderStr, 
                                  family=familyStr, genus=genusStr, 
                                  mod_time=currtime, 
                                  taxonomySourceId=gbifSrcId, 
                                  taxonomySourceKey=taxonKey, 
                                  taxonomySourceGenusKey=genusKey, 
                                  taxonomySourceSpeciesKey=speciesKey)
                    try:
                        sciName = scribe.findOrInsertTaxon(sciName=sname)
                        self.log.info('Inserted sciName for taxonKey {}, {}'
                                      .format(taxonKey, sciName.scientificName))
                    except Exception as e:
                        if not isinstance(e, LMError):
                            e = LMError(
                                'Failed on taxonKey {}'.format(taxonKey), e,
                                line_num=self.get_line_num())
                            raise e
                else:
                    self.log.info('taxonKey {} is not an accepted genus or species'
                                  .format(taxonKey))
        return sciName
            
    # .............................................................................
    def _lookupGBIFForOTT(self, gbifott, ottlabel):
        ottLabelPrefix = 'ott'
        ottid = ottlabel[len(ottLabelPrefix):]
        matches = []
        for g, o in gbifott.items():
            if str(o) == ottid:
                matches.append(g)
        return matches
                 
        
    # .............................................................................
    """
    @note: relabels OTT tree with Lifemapper squids
    """
    def _relabelOttTree(self, scribe, otree, gbifott):
        taxSrc = scribe.getTaxonSource(tsName=
                    TAXONOMIC_SOURCE[SpeciesDatasource.GBIF]['name'])
        gbifSrcId = taxSrc.taxonomysourceid   
           
        squidDict = {}
        for ottlabel in otree.getLabels():
            gbifids = self._lookupGBIFForOTT(gbifott, ottlabel)
            if len(gbifids) == 0:
                print(('No gbifids for OTT {}'.format(ottlabel)))                
            else:
                squidDict[ottlabel] = []
                for gid in gbifids:
                    sno = self._getInsertSciNameForGBIFSpeciesKey(scribe, gbifSrcId, gid)
                    if sno:
                        squidDict[ottlabel].append(sno.squid)
                if len(gbifids) == 1:
                    squidDict[ottlabel] = sno.squid
                    self.log.warning('Multiple matches (gbifids {}) for OTT {}'
                          .format(gbifids, ottlabel))
        
        otree.annotateTree(PhyloTreeKeys.SQUID, squidDict)
        print("Adding interior node labels to tree")
        otree.addNodeLabels()

    # .............................................................................
    """
    @note: relabels OTT tree with GBIF TaxonKeys
    """
    def relabelOTTTree2GbifName(self, otree, gbifott, keys_names):
        ottgbifDict = {}
        for ottlabel in otree.getLabels():
            gbifids = self._lookupGBIFForOTT(gbifott, ottlabel)
            if len(gbifids) == 0:
                print(('No gbifids for OTT {}'.format(ottlabel)))                
            else:
                if len(gbifids) == 1:
                    gid = gbifids[0]
                    canonical = keys_names[gid]
                    ottgbifDict[ottlabel] = canonical
                else:
                    ottgbifDict[ottlabel] = gbifids
                    self.log.warning('Multiple matches (gbifids {}) for OTT {}'
                          .format(gbifids, ottlabel))
        
        otree.annotateTree('label', ottgbifDict)
        print("Adding interior node labels to tree")
        otree.addNodeLabels()

    # .............................................................................
    def _getOptVal(self, retdict, fld):
        # Top Match
        try:
            val = retdict[fld]
        except:
            val = ''
        return val

    # .............................................................................
    def _writeNameMatches(self, origname, goodnames, writer):
        # Top Match
        rec = [origname]
        gudname = goodnames[0]
        for fld in GbifAPI.NameMatchFieldnames:
            try:
                rec.append(gudname[fld])
            except:
                rec.append('')
        writer.writerow(rec)

        canonical = self._getOptVal(gudname, 'canonicalName')
        speciesKey1 = self._getOptVal(gudname, 'speciesKey')    
        print(('origname {}, canonical {}, speciesKey {}'.format(origname, 
                                                        canonical, speciesKey1)))
        
        # Alternate matches
        alternatives = goodnames[1:]
        for altname in alternatives:
            rec = [origname]
            for fld in GbifAPI.NameMatchFieldnames:
                try:
                    rec.append(altname[fld])
                except:
                    rec.append('')
            writer.writerow(rec)
            
            canonical = self._getOptVal(altname, 'canonicalName')
            speciesKey = self._getOptVal(altname, 'speciesKey')    
            print(('origname {}, canonical {}, speciesKey {}'.format(origname, 
                                                            canonical, speciesKey)))
        # Return only top match
        return speciesKey1, canonical
    
    # .............................................................................
    def readGBIFTaxonIds(self, gbifidFname):
        taxon_ids = []
        name_to_gbif_ids = {}
        try:
            f = open(gbifidFname, 'r') 
            csvreader = unicodecsv.reader(f, delimiter=self.delimiter, 
                                          encoding=self.encoding)        
        except Exception as e:
            raise Exception('Failed to read or open {}, ({})'
                            .format(gbifidFname, str(e)))
        header = next(csvreader)
        line = next(csvreader)
        currname = None
        while line is not None:
            try:
                thisname = line[header.index('providedName')]
                thistaxonid = line[header.index('speciesKey')]
                thiscanonical = line[header.index('canonicalName')]
                thisscore = line[header.index('confidence')]
            except KeyError as e:
                self.log.error('Failed on line {} finding key {}'.format(line, str(e)))
            except Exception as e:
                self.log.error('Failed on line {}, {}'.format(line, str(e)))                
            else:
                # If starting a new set of matches, save last winner and reset
                if currname != thisname:
                    # Set default winner values on first line
                    if currname is None:
                        currname = thisname
                        toptaxonid = thistaxonid
                        topcanonical = thiscanonical
                        topscore = thisscore
                    else:
                        # Save winner from last name
                        taxon_ids.append(toptaxonid)
                        name_to_gbif_ids[currname] = (toptaxonid, topcanonical)
                        self.log.info('Found id {} for name {}, score {}'
                                      .format(toptaxonid, currname, topscore))
                        # Reset current values
                        currname = thisname
                        toptaxonid = thistaxonid
                        topcanonical = thiscanonical
                        topscore = thisscore
                        
                # Test this match score against winner, save if new winner
                if thisscore > topscore:
                    toptaxonid = thistaxonid
                    topcanonical = thiscanonical
                    topscore = thisscore
                    self.log.info('   New winner id {} for name {}, score {}'
                                  .format(toptaxonid, currname, topscore))
            

            # Get next one
            try:
                line = next(csvreader)
            except OverflowError as e:
                self.log.debug( 'Overflow on line {}, ({}))'
                                .format(csvreader.line_num, str(e)))
            except StopIteration:
                self.log.debug('EOF after line {}'.format(csvreader.line_num))
                line = None
            except Exception as e:
                self.log.warning('Bad record {}'.format(e))
        
        # Save winner from final name
        taxon_ids.append(toptaxonid)
        name_to_gbif_ids[currname] = (toptaxonid, topcanonical)
        self.log.info('Found final id {} for name {}, score {}'
                      .format(toptaxonid, currname, topscore))
        
        return name_to_gbif_ids
              
    # .............................................................................
    def assembleGBIFTaxonIds(self, names, outfname):
        unmatched_names = []
        name_to_gbif_ids = {}
        if not(isinstance(names, list)):
            names = [names]
           
        if os.path.exists(outfname):
            print(('Deleting existing file {} ...'.format(outfname)))
            os.remove(outfname)
           
#         writer, f = self._getCSVWriter(outfname, doAppend=False)
        writer, f = get_unicodecsv_writer(outfname, delimiter='\t', 
                                          doAppend=False)
        header = ['providedName']
        header.extend(GbifAPI.NameMatchFieldnames)
        writer.writerow(header)
        
        for origname in names:
            goodnames = GbifAPI.getAcceptedNames(origname)
            if len(goodnames) == 0:
                unmatched_names.append(origname)
            else:
                top_id_match, canonical = self._writeNameMatches(origname, 
                                                            goodnames, writer)
                name_to_gbif_ids [origname] = (top_id_match, canonical)
                
        return unmatched_names, name_to_gbif_ids
          
    # .............................................................................
    def assembleOTOLData(self, gbifTaxonIds, dataname):
        tree = None
        gbif_to_ott = get_ottids_from_gbifids(gbifTaxonIds)
        ottids = list(gbif_to_ott.values())
        output = induced_subtree(ottids)
                
        try:
            ott_unmatched_gbif_ids = output[Partners.OTT_MISSING_KEY]
        except:
            ott_unmatched_gbif_ids = []
            
        try:
            otree = output[Partners.OTT_TREE_KEY]
        except:
            raise LMError('Failed to retrieve OTT tree')
        else:
            tree = Tree(dataname, data=otree, schema=Partners.OTT_TREE_FORMAT)
        
#         updatedtree = self.encodeOTTTreeToGBIF(otree, gbifOTT)
    
        return tree, gbif_to_ott, ott_unmatched_gbif_ids

    # .............................................................................
    def encodeOTTTreeToGBIF(self, otree, gbifott, scribe=None):
        labeledTree = None
        if scribe is None:
            scribe = BorgScribe(self.log)
            try:    
                scribe.openConnections()
                labeledTree = self._relabelOttTree(scribe, otree, gbifott)
            except Exception as e:
                raise LMError('Failed to relabel or update tree ({})'.format(e))
            finally:
                scribe.closeConnections()
        else:
            try:    
                labeledTree = self._relabelOttTree(scribe, otree, gbifott)
            except Exception as e:
                raise LMError('Failed to relabel or update tree ({})'.format(e))

        return labeledTree 
  
# .............................................................................
# .............................................................................
if __name__ == '__main__':
    dataname = '/tmp/testIdigbioData'
    gbifidFname = dataname + '.gids'
    ptFname = dataname + '.csv'
    metaFname = dataname + '.json'
    names = ['Methanococcoides burtonii', 'Methanogenium frigidum', 
             'Hexarthra fennica', 'Hexarthra longicornicula', 
             'Hexarthra intermedia', 'Hexarthra mira', 'Horaella thomassoni', 
             'Filinia longiseta', 'Filinia opoliensis', 'Filinia novaezealandiae', 
             'Filinia terminalis', 'Trochosphaera aequatorialis', 
             'Ptygura linguata', 'Ptygura barbata', 'Ptygura crystallina', 
             'Ptygura libera', 'Floscularia janus', 'Floscularia conifera', 
             'Floscularia ringens', 'Sinantherina semibullata']
    
    iquery = PartnerQuery()

    # ............................
    # Get GBIF ACCEPTED TaxonIDs and canonical name for user-provided names
    if os.path.exists(gbifidFname):
        name_to_gbif_ids = iquery.readGBIFTaxonIds(gbifidFname)
    else:
        unmatched_names, name_to_gbif_ids = iquery.assembleGBIFTaxonIds(names, gbifidFname)
    user_gbif_ids = [match[0] for match in list(name_to_gbif_ids.values())]
    # ............................
    # Get iDigBio point data for TaxonIDs
    if os.path.exists(ptFname) and os.path.exists(metaFname):
        # Reads keys as integers
        gbifid_counts = iquery.readIdigbioData(ptFname, metaFname)
    else:
        gbifid_counts, idig_unmatched_gbif_ids = iquery.assembleIdigbioData(user_gbif_ids, ptFname, metaFname)   
                     
    idig_gbif_ids = list(gbifid_counts.keys())
    
    # ............................
    # Get OpenTree tree and map for OTT Ids to GBIF TaxonIDs
    otree, gbif_to_ott, ott_unmatched_gbif_ids = iquery.assembleOTOLData(idig_gbif_ids, dataname)
    # Update Tree with TaxonIDs
    updatedTree = iquery.encodeOTTTreeToGBIF(otree, gbif_to_ott)
    
    print ('Now what?')

         
"""
from LmDbServer.tools.partnerData import *

from LmBackend.common.lmobj import LMError
try:
   from osgeo.ogr import OFTInteger, OFTReal, OFTString, OFTBinary
except:
   OFTInteger = 0 
   OFTReal = 2 
   OFTString = 4
   OFTBinary = 8

import idigbio
import json
import os
import sys
import unicodecsv
import urllib2

from LmBackend.common.lmobj import LMError
from LmCommon.common.api_query import GbifAPI
from LmCommon.common.lmconstants import PhyloTreeKeys, GBIF
from LmCommon.common.occparse import OccDataParser

from LmDbServer.common.lmconstants import (TAXONOMIC_SOURCE, SpeciesDatasource)

from LmServer.base.taxon import ScientificName
from LmServer.db.borgscribe import BorgScribe
from LmServer.common.log import ScriptLogger
from LmServer.legion.tree import Tree
from LmCommon.common.api_query import IdigbioAPI

from LmDbServer.tools.partnerData import PartnerQuery

outfname = 'nmmst_sp_gids.txt'
names = []
pq = PartnerQuery()

names = ['Acrocystis nana', 'Bangia atropurpurea', 'Boergesenia forbesii', 
'Boodlea composita', 'Bostrychia tenella', 'Brachytrichia quoyi', 
'Caulerpa peltata', 'Caulerpa prolifera', 'Centroceras clavultum', 
'Chaetomorpha spiralis', 'Champia parvula', 'Chnoospora minima', 
'Chondracanthus intermedius', 'Cladophora herpestica', 'colpomenia sinuosa', 
'Corallina pilulifera', 'Dasya sessilis', 'Dictyosphaeria cavernosa', 
'Dictyota sp.', 'Enteromorpba clatbrata', 'Gelidiella acerosa', 
'Gracilaria coronopifolia', 'Grateloupia filicina', 'Hincksia breviarticulatus', 
'Hincksia mitchellae', 'Hypnea spinella', 'Marginosporum aberrans', 
'Microdictyon nigrescens', 'Monostroma nitidum', 
'non-articulate corallina alga', 'Peyssonnelia conchicola', 'Porphyra crispata', 
'Prionitis ramosissima', 'Ulthrix flaccida', 'Ulva conglobata', 
'Ulva intestinales', 'Ulva lactuca', 'Ulva prolifera', 'Valoniopsis pachynema', 
'Yamadaella cenomyce']




unmatched_names, name_to_gbif_id = pq.assembleGBIFTaxonIds(names, outfname)



DEV_SERVER = 'http://141.211.236.35:10999'
INDUCED_SUBTREE_BASE_URL = '{}/induced_subtree'.format(DEV_SERVER)
OTTIDS_FROM_GBIFIDS_URL = '{}/ottids_from_gbifids'.format(DEV_SERVER)
GBIF_MISSING_KEY = GbifAPI.GBIF_MISSING_KEY


logger = ScriptLogger('partnerData.test')
delimiter = '\t'

dataname = '/tmp/testIdigbioData'
taxon_id_file = dataname + '.gids'
point_output_file = dataname + '.csv'
meta_output_file = dataname + '.json'

# TODO: Find names with data
names = ['Methanococcoides burtonii', 'Methanogenium frigidum', 
         'Hexarthra fennica', 'Hexarthra longicornicula', 
         'Hexarthra intermedia', 'Hexarthra mira', 'Horaella thomassoni', 
         'Filinia longiseta', 'Filinia opoliensis', 'Filinia novaezealandiae', 
         'Filinia terminalis', 'Trochosphaera aequatorialis', 
         'Ptygura linguata', 'Ptygura barbata', 'Ptygura crystallina', 
         'Ptygura libera', 'Floscularia janus', 'Floscularia conifera', 
         'Floscularia ringens', 'Sinantherina semibullata']


names = ['Prenolepis imparis']


iquery = PartnerQuery(logger=logger)

# ............................
# Get GBIF ACCEPTED TaxonIDs and canonical name for user-provided names
# if os.path.exists(gbifidFname):
#     name_to_gbif_ids = iquery.readGBIFTaxonIds(gbifidFname)
# else:
unmatched_names, name_to_gbif_ids = iquery.assembleGBIFTaxonIds(names, taxon_id_file) 
taxon_ids = [match[0] for match in name_to_gbif_ids.values()]

# ............................
# Get iDigBio point data for TaxonIDs
# if os.path.exists(ptFname) and os.path.exists(metaFname):
#     # Reads keys as integers
#     gbifid_counts = iquery.readIdigbioData(point_output_file, meta_output_file)
# else:
idigAPI = IdigbioAPI()
summary = idigAPI.assembleIdigbioData(taxon_ids, point_output_file, meta_output_file, missing_id_file=None)                                          
print('Missing: {}'.format(summary[GBIF_MISSING_KEY])
                 
idig_gbif_ids = gbifid_counts.keys()

# ............................
# Get OpenTree tree and map for OTT Ids to GBIF TaxonIDs
otree, gbif_to_ott, ott_unmatched_gbif_ids = iquery.assembleOTOLData(idig_gbif_ids, dataname)
# Update Tree with TaxonIDs
updatedTree = iquery.encodeOTTTreeToGBIF(otree, gbif_to_ott)




"""