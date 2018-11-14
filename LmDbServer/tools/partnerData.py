"""
@summary: Module containing functions for API Queries
@status: beta

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
try:
    from osgeo.ogr import OFTInteger, OFTReal, OFTString, OFTBinary
except:
    OFTInteger = 0 
    OFTReal = 2 
    OFTString = 4
    OFTBinary = 8

import idigbio
import json
import mx.DateTime
import os
import sys
import unicodecsv
import urllib2

from LmBackend.common.lmobj import LMError
from LmCommon.common.apiquery import GbifAPI
from LmCommon.common.lmconstants import PhyloTreeKeys
from LmCommon.common.occparse import OccDataParser

from LmDbServer.common.lmconstants import (TAXONOMIC_SOURCE, SpeciesDatasource)

from LmServer.base.taxon import ScientificName
from LmServer.db.borgscribe import BorgScribe
from LmServer.common.log import ScriptLogger
from LmServer.legion.tree import Tree

DEV_SERVER = 'http://141.211.236.35:10999'
INDUCED_SUBTREE_BASE_URL = '{}/induced_subtree'.format(DEV_SERVER)
OTTIDS_FROM_GBIFIDS_URL = '{}/ottids_from_gbifids'.format(DEV_SERVER)

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
    req = urllib2.Request(OTTIDS_FROM_GBIFIDS_URL, 
                          data=json.dumps(request_body), headers=headers)
   
    resp = json.load(urllib2.urlopen(req))
    unmatchedIds = resp['unmatched_gbif_ids']
   
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
    req = urllib2.Request(INDUCED_SUBTREE_BASE_URL, 
                          data=json.dumps(request_body), headers=headers)
    
    resp_str = urllib2.urlopen(req).read()
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
        unicodecsv.field_size_limit(sys.maxsize)
        self.encoding = 'utf-8'
        self.delimiter = '\t'

    # .............................................................................
    def _getCSVWriter(self, datafile, doAppend=True):
        '''
        @summary: Get a CSV writer that can handle encoding
        '''
        unicodecsv.field_size_limit(sys.maxsize)
        if doAppend:
            mode = 'ab'
        else:
            mode = 'wb'
           
        try:
            f = open(datafile, mode) 
            writer = unicodecsv.writer(f, delimiter=self.delimiter, 
                                      encoding=self.encoding)
        
        except Exception, e:
            raise Exception('Failed to read or open {}, ({})'
                            .format(datafile, str(e)))
        return writer, f

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
   
    # .............................................................................
    def _writeIdigbioMetadata(self, origFldnames, metaFname):
        newMeta = {}
        for colIdx in range(len(origFldnames)):
            fldname = origFldnames[colIdx]
            
            valdict = {'name': fldname , 
                       'type': 'str'}
            if fldname == 'uuid':
                valdict['role'] = OccDataParser.FIELD_ROLE_IDENTIFIER
            elif fldname == 'taxonid':
                valdict['role'] = OccDataParser.FIELD_ROLE_GROUPBY
            elif fldname == 'geopoint':
                valdict['role'] = OccDataParser.FIELD_ROLE_GEOPOINT
            elif fldname == 'canonicalname':            
                valdict['role'] = OccDataParser.FIELD_ROLE_TAXANAME
            elif fldname == 'dec_long':            
                valdict['role'] = OccDataParser.FIELD_ROLE_LONGITUDE
            elif fldname == 'dec_lat':            
                valdict['role'] = OccDataParser.FIELD_ROLE_LATITUDE
            newMeta[str(colIdx)] = valdict
            
        with open(metaFname, 'w') as outf:
            json.dump(newMeta, outf)
        return newMeta
   
    # .............................................................................
    def _getIdigbioFields(self, gbifTaxonId):
        """
        @param gbifTaxonIds: one GBIF TaxonId or a list
        """
        fldnames = None
        api = idigbio.json()
        recordQuery = {'taxonid':str(gbifTaxonId), 
                       'geopoint': {'type': 'exists'}}
        try:
            output = api.search_records(rq=recordQuery, limit=1, offset=0)
        except:
            print 'Failed on {}'.format(gbifTaxonId)
        else:
            items = output['items']
            print('  Retrieved 1 record for metadata')
            if len(items) == 1:
                itm = items[0]
                fldnames = itm['indexTerms'].keys()
                # add dec_long and dec_lat to records
                fldnames.extend(['dec_lat', 'dec_long'])
                fldnames.sort()
        return fldnames
   
    # .............................................................................
    def _getIdigbioRecords(self, gbifTaxonId, fields, writer):
        """
        @param gbifTaxonIds: one GBIF TaxonId or a list
        """
        api = idigbio.json()
        limit = 100
        offset = 0
        currcount = 0
        total = 0
        recordQuery = {'taxonid':str(gbifTaxonId), 
                       'geopoint': {'type': 'exists'}}
        while offset <= total:
            try:
                output = api.search_records(rq=recordQuery,
                                            limit=limit, offset=offset)
            except:
                print 'Failed on {}'.format(gbifTaxonId)
            else:
                total = output['itemCount']
                items = output['items']
                currcount += len(items)
                print("  Retrieved {} records, {} records starting at {}"
                      .format(len(items), limit, offset))
                for itm in items:
                    itmdata = itm['indexTerms']
                    vals = []
                    for fldname in fields:
                        # Pull long, lat from geopoint
                        if fldname == 'dec_long':
                            try:
                                vals.append(itmdata['geopoint']['lon'])
                            except:
                                vals.append('')
                        elif fldname == 'dec_lat':
                            try:
                                vals.append(itmdata['geopoint']['lat'])
                            except:
                                vals.append('')
                        # or just append verbatim
                        else:
                            try:
                                vals.append(itmdata[fldname])
                            except:
                                vals.append('')
                    
                    writer.writerow(vals)
                offset += limit
        print('Retrieved {} of {} reported records for {}'.format(currcount, total, gbifTaxonId))
        return currcount
            
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
            except Exception, e:
                self.log.info('Failed lookup for key {}, ({})'.format(
                                                      taxonKey, e))
            else:
                # if no species key, this is not a species
                if rankStr in ('SPECIES', 'GENUS') and taxStatus == 'ACCEPTED':
                    currtime = mx.DateTime.gmt().mjd
                    sname = ScientificName(scinameStr, 
                                  rank=rankStr, 
                                  canonicalName=canonicalStr,
                                  kingdom=kingdomStr, phylum=phylumStr, 
                                  txClass=classStr, txOrder=orderStr, 
                                  family=familyStr, genus=genusStr, 
                                  modTime=currtime, 
                                  taxonomySourceId=gbifSrcId, 
                                  taxonomySourceKey=taxonKey, 
                                  taxonomySourceGenusKey=genusKey, 
                                  taxonomySourceSpeciesKey=speciesKey)
                    try:
                        sciName = scribe.findOrInsertTaxon(sciName=sname)
                        self.log.info('Inserted sciName for taxonKey {}, {}'
                                      .format(taxonKey, sciName.scientificName))
                    except Exception, e:
                        if not isinstance(e, LMError):
                            e = LMError(currargs='Failed on taxonKey {}'
                                        .format(taxonKey), 
                                        prevargs=e.args, lineno=self.getLineno())
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
        for g, o in gbifott.iteritems():
            if str(o) == ottid:
                matches.append(g)
        return matches
                 
        
    # .............................................................................
    def _relabelOttTree(self, scribe, otree, gbifott):
        taxSrc = scribe.getTaxonSource(tsName=
                    TAXONOMIC_SOURCE[SpeciesDatasource.GBIF]['name'])
        gbifSrcId = taxSrc.taxonomysourceid   
           
        squidDict = {}
        for ottlabel in otree.getLabels():
            gbifids = self._lookupGBIFForOTT(gbifott, ottlabel)
            if len(gbifids) == 0:
                print('No gbifids for OTT {}'.format(ottlabel))                
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
        print "Adding interior node labels to tree"
        otree.addNodeLabels()

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
        canonical = gudname['canonicalName']
        print('origname {}, canonical {}, speciesKey, usageKey :  {}, {}'
              .format(origname, canonical, gudname['speciesKey'], \
                      gudname['usageKey']))
        writer.writerow(rec)
        
        # Alternate matches
        alternatives = goodnames[1:]
        for gudname in alternatives:
            rec = [origname]
            for fld in GbifAPI.NameMatchFieldnames:
                try:
                    rec.append(gudname[fld])
                except:
                    rec.append('')
            writer.writerow(rec)
            print('origname {}, canonical {}, speciesKey, usageKey :  {}, {}'
                  .format(origname, canonical, gudname['speciesKey'], 
                          gudname['usageKey']))
        # Return only top match
        return goodnames[0]['speciesKey'], canonical
    
    # .............................................................................
    def readGBIFTaxonIds(self, gbifidFname):
        taxon_ids = []
        name_to_gbif_ids = {}
        try:
            f = open(gbifidFname, 'r') 
            csvreader = unicodecsv.reader(f, delimiter=self.delimiter, 
                                          encoding=self.encoding)        
        except Exception, e:
            raise Exception('Failed to read or open {}, ({})'
                            .format(gbifidFname, str(e)))
        header = csvreader.next()
        line = csvreader.next()
        currname = None
        while line is not None:
            try:
                thisname = line[header.index('providedName')]
                thistaxonid = line[header.index('speciesKey')]
                thiscanonical = line[header.index('canonicalName')]
                thisscore = line[header.index('confidence')]
            except KeyError, e:
                self.log.error('Failed on line {} finding key {}'.format(line, str(e)))
            except Exception, e:
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
                line = csvreader.next()
            except OverflowError, e:
                self.log.debug( 'Overflow on line {}, ({}))'
                                .format(csvreader.line_num, str(e)))
            except StopIteration:
                self.log.debug('EOF after line {}'.format(csvreader.line_num))
                line = None
            except Exception, e:
                self.log.warning('Bad record {}'.format(e))
        
        # Save winner from final name
        taxon_ids.append(toptaxonid)
        name_to_gbif_ids[currname] = (toptaxonid, topcanonical)
        self.log.info('Found final id {} for name {}, score {}'
                      .format(toptaxonid, currname, topscore))
        
        return name_to_gbif_ids
              
    # .............................................................................
    """
    nm = 'Sphagnum capillifolium var. capillifolium'
    """
    def assembleGBIFTaxonIds(self, names, gbifidFname):
        unmatched_names = []
        name_to_gbif_ids = {}
        if not(isinstance(names, list)):
            names = [names]
           
        if os.path.exists(gbifidFname):
            print('Deleting existing file {} ...'.format(gbifidFname))
            os.remove(gbifidFname)
           
        writer, f = self._getCSVWriter(gbifidFname, doAppend=False)
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
    def assembleIdigbioData(self, gbifTaxonIds, ptFname, metaFname): 
        unmatched_gbif_ids = []     
        if not(isinstance(gbifTaxonIds, list)):
            gbifTaxonIds = [gbifTaxonIds]
           
        for fname in (ptFname, metaFname):
            if os.path.exists(fname):
                print('Deleting existing file {} ...'.format(fname))
                os.remove(fname)
           
        summary = {'unmatched_gbif_ids': []}
        writer, f = self._getCSVWriter(ptFname, doAppend=False)
        
        # Keep trying in case no records are available
        tryidx = 0
        origFldnames = self._getIdigbioFields(gbifTaxonIds[tryidx])
        while not origFldnames and tryidx < len(gbifTaxonIds):
            tryidx += 1
            origFldnames = self._getIdigbioFields(gbifTaxonIds[tryidx])
        if not origFldnames:
            raise LMError('Unable to pull data from iDigBio')
        
        # write header, but also put column indices in metadata
        writer.writerow(origFldnames)
        meta = self._writeIdigbioMetadata(origFldnames, metaFname)
        
        # get/write data
        for gid in gbifTaxonIds:
            ptCount = self._getIdigbioRecords(gid, origFldnames, writer)
            if ptCount > 0:
                summary[gid] = ptCount
            else:
                unmatched_gbif_ids.append(gid)
        return summary
   
    # .............................................................................
    def readIdigbioData(self, ptFname, metaFname):
        gbifid_counts = {}
        if not(os.path.exists(ptFname)):
            print ('Point data {} does not exist'.format(ptFname))
        elif not(os.path.exists(metaFname)):
            print ('Metadata {} does not exist'.format(metaFname))
        else:
            occParser = OccDataParser(self.log, ptFname, metaFname, 
                                      delimiter=self.delimiter,
                                      pullChunks=True)
            occParser.initializeMe()  
            # returns dict with key = taxonid, val = (name, count)
            summary = occParser.readAllChunks()
            for taxid, (name, count) in summary.iteritems():
                gbifid_counts[taxid] = count
        return gbifid_counts
          
    # .............................................................................
    def assembleOTOLData(self, gbifTaxonIds, dataname):
        tree = None
        gbif_to_ott = get_ottids_from_gbifids(gbifTaxonIds)
        ottids = gbif_to_ott.values()
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
            except Exception, e:
                raise LMError('Failed to relabel or update tree ({})'.format(e))
            finally:
                scribe.closeConnections()
        else:
            try:    
                labeledTree = self._relabelOttTree(scribe, otree, gbifott)
            except Exception, e:
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
    user_gbif_ids = [match[0] for match in name_to_gbif_ids.values()]
    # ............................
    # Get iDigBio point data for TaxonIDs
    if os.path.exists(ptFname) and os.path.exists(metaFname):
        # Reads keys as integers
        gbifid_counts = iquery.readIdigbioData(ptFname, metaFname)
    else:
        gbifid_counts, idig_unmatched_gbif_ids = iquery.assembleIdigbioData(user_gbif_ids, ptFname, metaFname)   
                     
    idig_gbif_ids = gbifid_counts.keys()
    
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
from LmCommon.common.apiquery import GbifAPI
from LmCommon.common.lmconstants import PhyloTreeKeys, GBIF
from LmCommon.common.occparse import OccDataParser

from LmDbServer.common.lmconstants import (TAXONOMIC_SOURCE, SpeciesDatasource)

from LmServer.base.taxon import ScientificName
from LmServer.db.borgscribe import BorgScribe
from LmServer.common.log import ScriptLogger
from LmServer.legion.tree import Tree

DEV_SERVER = 'http://141.211.236.35:10999'
INDUCED_SUBTREE_BASE_URL = '{}/induced_subtree'.format(DEV_SERVER)
OTTIDS_FROM_GBIFIDS_URL = '{}/ottids_from_gbifids'.format(DEV_SERVER)


logger = ScriptLogger('partnerData.test')
delimiter = '\t'

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

iquery = PartnerQuery(logger=logger)

# ............................
# Get GBIF ACCEPTED TaxonIDs and canonical name for user-provided names
if os.path.exists(gbifidFname):
    name_to_gbif_ids = iquery.readGBIFTaxonIds(gbifidFname)
else:
    unmatched_names, name_to_gbif_ids = iquery.assembleGBIFTaxonIds(names, gbifidFname)
user_gbif_ids = [match[0] for match in name_to_gbif_ids.values()]
# ............................
# Get iDigBio point data for TaxonIDs
if os.path.exists(ptFname) and os.path.exists(metaFname):
    # Reads keys as integers
    gbifid_counts = iquery.readIdigbioData(ptFname, metaFname)
else:
    gbifid_counts, idig_unmatched_gbif_ids = iquery.assembleIdigbioData(user_gbif_ids, ptFname, metaFname)   
                 
idig_gbif_ids = gbifid_counts.keys()

# ............................
# Get OpenTree tree and map for OTT Ids to GBIF TaxonIDs
otree, gbif_to_ott, ott_unmatched_gbif_ids = iquery.assembleOTOLData(idig_gbif_ids, dataname)
# Update Tree with TaxonIDs
updatedTree = iquery.encodeOTTTreeToGBIF(otree, gbif_to_ott)




"""