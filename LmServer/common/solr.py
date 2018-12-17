"""This module wraps interactions with Solr
"""
from ast import literal_eval
from mx.DateTime import DateTimeFromMJD
import urllib2

from LmServer.common.lmconstants import (
     SnippetFields, SOLR_ARCHIVE_COLLECTION, SOLR_FIELDS, SOLR_SERVER, 
     SOLR_SNIPPET_COLLECTION, SOLR_TAXONOMY_COLLECTION, SOLR_TAXONOMY_FIELDS)
from LmServer.common.localconstants import PUBLIC_USER
import json

# .............................................................................
def buildSolrDocument(docPairs):
    """
    @summary: Builds a document for a Solr POST from the key, value pairs
    @param docPairs: A list of lists of [field name, value] pairs -- 
                              [[(field name, value)]]
    """
    if not docPairs:
        raise Exception, "Must provide at least one pair for Solr POST"
    
    # We want to allow multiple documents.  Make sure that field pairs is a list
    #     of lists of tuples
    elif not isinstance(docPairs[0][0], (list, tuple)):
        docPairs = [docPairs]
    
    docLines = ['<add>']
    for fieldPairs in docPairs:
        docLines.append('    <doc>')
        for fName, fVal in fieldPairs:
            # Only add the field if the value is not None
            if fVal is not None: 
                docLines.append('        <field name="{}">{}</field>'.format(fName, 
                                                             str(fVal).replace('&', '&amp;')))
        docLines.append('    </doc>')
    docLines.append('</add>')
    return '\n'.join(docLines)

# .............................................................................
def postSolrDocument(collection, docFilename):
    """
    @summary: Posts a document to a Solr index
    @param collection: The name of the Solr core (index) to add this document to
    @param docFilename: The file location of the document to post
    """
    return _post(collection, docFilename, 
                     headers={'Content-Type': 'text/xml'})
    
# .............................................................................
def _post(collection, docFilename, headers=None):
    """
    @summary: Post a document to a Solr index
    """
    if not headers:
        headers = {}
    url = '{}{}/update?commit=true'.format(SOLR_SERVER, collection)
    
    with open(docFilename) as inF:
        data = inF.read()
    
    req = urllib2.Request(url, data=data, headers=headers)
    return urllib2.urlopen(req).read()
    
# .............................................................................
def _query(collection, qParams=None, fqParams=None,
                otherParams='wt=python&indent=true'):
    """
    @summary: Perform a query on a Solr index
    @param collection: The Solr collection (index / core) to query
    @param qParams: Parameters to include in the query section of the Solr query
    @param fqParams: Parameters to include in the filter section of the query
    @param otherParams: Other parameters to pass to Solr
    """
    queryParts = []
    if qParams:
        qParts = []
        for k, v in qParams:
            if v is not None:
                if isinstance(v, list):
                    if len(v) > 1:
                        qParts.append('{}:({})'.format(k, '+OR+'.join(v)))
                    else:
                        qParts.append('{}:{}'.format(k, v[0]))
                else:
                    qParts.append('{}:{}'.format(k, v))
        # If we have at least one query parameter
        if qParts:
            queryParts.append('q={}'.format('+AND+'.join(qParts)))
    
    if fqParams:
        fqParts = []
        for k, v in fqParams:
            if v is not None:
                if isinstance(v, list):
                    if len(v) > 1:
                        fqParts.append('{}:({})'.format(k, ' '.join(v)))
                    else:
                        fqParts.append('{}:{}'.format(k, v[0]))
                else:
                    fqParts.append('{}:{}'.format(k, v))
        # If we have at least one filter parameter
        if fqParts:
            queryParts.append('fq={}'.format('+AND+'.join(fqParts)))
    
    if otherParams is not None:
        queryParts.append(otherParams)
    
    url = '{}{}/select?{}'.format(SOLR_SERVER, collection, '&'.join(queryParts))
    res = urllib2.urlopen(url)
    resp = res.read()
    
    return resp

# .............................................................................
def add_taxa_to_taxonomy_index(sciname_objects):
    """Create a solr document and post it for the provided objects
    """
    doc_pairs = []
    for sno in sciname_objects:
        doc_pairs.append([
                   [SOLR_TAXONOMY_FIELDS.CANONICAL_NAME, sno.canonicalName],
                   [SOLR_TAXONOMY_FIELDS.SCIENTIFIC_NAME, sno.scientificName],
                   [SOLR_TAXONOMY_FIELDS.SQUID, sno.squid],
                   [SOLR_TAXONOMY_FIELDS.TAXON_CLASS, sno.txClass],
                   [SOLR_TAXONOMY_FIELDS.TAXON_FAMILY, sno.family],
                   [SOLR_TAXONOMY_FIELDS.TAXON_GENUS, sno.genus],
                   [SOLR_TAXONOMY_FIELDS.TAXON_KEY, sno.sourceTaxonKey],
                   [SOLR_TAXONOMY_FIELDS.TAXON_KINGDOM, sno.kingdom],
                   [SOLR_TAXONOMY_FIELDS.TAXON_ORDER, sno.txOrder],
                   [SOLR_TAXONOMY_FIELDS.TAXON_PHYLUM, sno.phylum],
                   [SOLR_TAXONOMY_FIELDS.USER_ID, sno.getUserId()],
                   [SOLR_TAXONOMY_FIELDS.TAXONOMY_SOURCE_ID,
                    sno.taxonomySourceId()],
                   [SOLR_TAXONOMY_FIELDS.ID, sno.getId()]
        ])
    post_doc = buildSolrDocument(doc_pairs)
    # Note: This is somewhat redundant.
    # TODO: Modify _post to accept a string or file like object as well
    url = '{}{}/update?commit=true'.format(SOLR_SERVER, 
                                           SOLR_TAXONOMY_COLLECTION)
    req = urllib2.Request(url, data=post_doc, 
                          headers={'Content-Type' : 'text/xml'})
    return urllib2.urlopen(req).read()
    
# .............................................................................
def add_taxa_to_taxonomy_index_dicts(taxon_dicts):
    """Create a solr document and post it for the provided objects
    
    Note:
        Should have the following keys
            taxonid,
            taxonomysourceid,
            userid,
            taxonomykey,
            squid,
            kingdom,
            phylum,
            tx_class,
            tx_order,
            family,
            genus,
            canonical,
            sciname
    """
    doc_pairs = []
    for taxon_info in taxon_dicts:
        doc_pairs.append([
            [SOLR_TAXONOMY_FIELDS.ID, taxon_info['taxonid']],
            [SOLR_TAXONOMY_FIELDS.TAXONOMY_SOURCE_ID,
             taxon_info['taxonomysourceid']],
            [SOLR_TAXONOMY_FIELDS.USER_ID, taxon_info['userid']],
            [SOLR_TAXONOMY_FIELDS.TAXON_KEY, taxon_info['taxonomykey']],
            [SOLR_TAXONOMY_FIELDS.SQUID, taxon_info['squid']],
            [SOLR_TAXONOMY_FIELDS.TAXON_KINGDOM, taxon_info['kingdom']],
            [SOLR_TAXONOMY_FIELDS.TAXON_PHYLUM, taxon_info['phylum']],
            [SOLR_TAXONOMY_FIELDS.TAXON_CLASS, taxon_info['tx_class']],
            [SOLR_TAXONOMY_FIELDS.TAXON_ORDER, taxon_info['tx_order']],
            [SOLR_TAXONOMY_FIELDS.TAXON_FAMILY, taxon_info['family']],
            [SOLR_TAXONOMY_FIELDS.TAXON_GENUS, taxon_info['genus']],
            [SOLR_TAXONOMY_FIELDS.CANONICAL_NAME, taxon_info['canonical']],
            [SOLR_TAXONOMY_FIELDS.SCIENTIFIC_NAME, taxon_info['sciname']]
        ])
    post_doc = buildSolrDocument(doc_pairs)
    # Note: This is somewhat redundant.
    # TODO: Modify _post to accept a string or file like object as well
    url = '{}{}/update?commit=true'.format(SOLR_SERVER, 
                                           SOLR_TAXONOMY_COLLECTION)
    req = urllib2.Request(url, data=post_doc, 
                          headers={'Content-Type' : 'text/xml'})
    return urllib2.urlopen(req).read()

# .............................................................................
def delete_from_archive_index(gridset_id=None, pav_id=None, sdmproject_id=None,
                              occ_id=None, squid=None, user_id=None):
    """Deletes records from the archive index

    Args:
        gridset_id (:obj:`int`, optional): The database identifier of a gridset
            that you want to remove the PAVs of from the Solr index.
        pav_id (:obj:`int`, optional): The database identifier for a single PAV
            to remove from the Solr index.
        sdmproject_id (:obj:`int`, optional): The database identifier of a SDM
            projection to remove all corresponding PAVs from the Solr index.
        occ_id (:obj:`int`, optional): The database identifier of an occurrence
            set to remove all resulting PAVs from the index.
        squid (:obj:`str`, optional): Remove all PAVs from the index that have
            this species squid.
        user_id (:obj:`str`, optional): Remove all PAVs from the index for this
            user.

    Note:
        * Must provide at least one input parameter so as to not accidentally
            delete everything in the index
    """
    if gridset_id is None and pav_id is None and sdmproject_id is None and \
            occ_id is None and squid is None and user_id is None:
        raise Exception('Must provide at least one query parameter')
    query_parts = [
        (SOLR_FIELDS.GRIDSET_ID, gridset_id),
        (SOLR_FIELDS.ID, pav_id),
        (SOLR_FIELDS.PROJ_ID, sdmproject_id),
        (SOLR_FIELDS.OCCURRENCE_ID, occ_id),
        (SOLR_FIELDS.SQUID, squid),
        (SOLR_FIELDS.USER_ID, user_id)
    ]
    query = ' AND '.join(
        ['{}:{}'.format(f, v) for (f, v) in query_parts if v is not None])
    
    url = '{}{}/update?commit=true'.format(
        SOLR_SERVER, SOLR_ARCHIVE_COLLECTION)
    doc = {
        "delete" : {
            "query" : query
        }
    }

    req = urllib2.Request(
        url, data=json.dumps(doc),
        headers={'Content-Type' : 'application/json'})
    return urllib2.urlopen(req).read()

# .............................................................................
def facetArchiveOnGridset(userId=None):
    """
    @summary: Query the PAV archive Solr index to get the gridsets contained
        and the number of matches for each
    @todo: Consider integrating this with regular solr query
    """
    qParams = [
        (SOLR_FIELDS.USER_ID, userId),
        ('*', '*')
    ]
    
    otherParams = '&facet=true&facet.field={}&wt=python&indent=true'.format(
        SOLR_FIELDS.GRIDSET_ID)
    
    rDict = literal_eval(_query(SOLR_ARCHIVE_COLLECTION, qParams=qParams,
                                         otherParams=otherParams))
    
    return rDict['facet_counts']['facet_fields'][SOLR_FIELDS.GRIDSET_ID]

# .............................................................................
def queryArchiveIndex(algorithmCode=None, bbox=None, displayName=None,
                      gridSetId=None, modelScenarioCode=None, pointMax=None,
                      pointMin=None, projectionScenarioCode=None, squid=None,
                      taxKingdom=None, taxPhylum=None, taxClass=None,
                      taxOrder=None, taxFamily=None, taxGenus=None,
                      taxSpecies=None, userId=None):
    """
    @summary: Query the PAV archive Solr index
    
    """
    qParams = [
        (SOLR_FIELDS.ALGORITHM_CODE, algorithmCode),
        (SOLR_FIELDS.DISPLAY_NAME, displayName),
        (SOLR_FIELDS.GRIDSET_ID, gridSetId),
        (SOLR_FIELDS.USER_ID, userId),
        (SOLR_FIELDS.MODEL_SCENARIO_CODE, modelScenarioCode),
        (SOLR_FIELDS.PROJ_SCENARIO_CODE, projectionScenarioCode),
        (SOLR_FIELDS.SQUID, squid),
        (SOLR_FIELDS.TAXON_KINGDOM, taxKingdom),
        (SOLR_FIELDS.TAXON_PHYLUM, taxPhylum),
        (SOLR_FIELDS.TAXON_CLASS, taxClass),
        (SOLR_FIELDS.TAXON_ORDER, taxOrder),
        (SOLR_FIELDS.TAXON_FAMILY, taxFamily),
        (SOLR_FIELDS.TAXON_GENUS, taxGenus),
        (SOLR_FIELDS.TAXON_SPECIES, taxSpecies),
    ]
    
    if pointMax is not None or pointMin is not None:
        pmax = pointMax
        pmin = pointMin
        
        if pointMax is None:
            pmax = '*'
        
        if pointMin is None:
            pmin = '*'
            
        qParams.append((SOLR_FIELDS.POINT_COUNT, '%5B{}%20TO%20{}%5D'.format(
                            pmin, pmax)))
    
    fqParams = []
    if bbox is not None:
        minx, miny, maxx, maxy = bbox.split(',')
        fqParams.append((SOLR_FIELDS.PRESENCE,
                         '%5B{},{}%20{},{}%5D'.format(miny, minx, maxy, maxx)))
    
    rDict = literal_eval(_query(SOLR_ARCHIVE_COLLECTION, qParams=qParams, 
                                         fqParams=fqParams))
    return rDict['response']['docs']

# .............................................................................
def querySnippetIndex(ident1=None, provider=None, collection=None,
                      catalogNumber=None, operation=None, afterTime=None,
                      beforeTime=None, ident2=None, url=None, who=None,
                      agent=None, why=None):
    """
    @summary: Query the snippet Solr index
    @param ident1: An identifier for the primary object (probably occurrence 
                            point)
    @param provider: The occurrence point provider
    @param collection: The collection the point belongs to
    @param catalogNumber: The catalog number of the occurrence point
    @param operation: A LmServer.common.lmconstants.SnippetOperations
    @param afterTime: Return hits after this time (MJD format)
    @param beforeTime: Return hits before this time (MJD format)
    @param ident2: A identifier for the secondary object (occurrence set or 
                            projection)
    @param url: A url for the resulting object
    @param who: Who initiated the action
    @param agent: The agent that initiated the action
    @param why: Why the action was initiated
    """
    qParams = [
        (SnippetFields.AGENT, agent),
        (SnippetFields.CATALOG_NUMBER, catalogNumber),
        (SnippetFields.COLLECTION, collection),
        (SnippetFields.IDENT_1, ident1),
        (SnippetFields.IDENT_2, ident2),
        (SnippetFields.OPERATION, operation),
        (SnippetFields.PROVIDER, provider),
        (SnippetFields.URL, url),
        (SnippetFields.WHO, who),
        (SnippetFields.WHY, why)
    ]
    
    fqParams = []
    if afterTime is not None or beforeTime is not None:
        if afterTime is not None:
            aTime = DateTimeFromMJD(afterTime).strftime('%Y-%m-%dT%H:%M:%SZ')
        else:
            aTime = '*'

        if beforeTime is not None:
            bTime = DateTimeFromMJD(beforeTime).strftime('%Y-%m-%dT%H:%M:%SZ')
        else:
            bTime = '*'
    
        fqParams.append((SnippetFields.OP_TIME, 
                         '%5B{}%20TO%20{}%5D'.format(aTime, bTime)))
    
    
    rDict = literal_eval(_query(SOLR_SNIPPET_COLLECTION, qParams=qParams, 
                                         fqParams=fqParams))
    return rDict['response']['docs']

# .............................................................................
def query_taxonomy_index(taxon_kingdom=None, taxon_phylum=None, 
                         taxon_class=None, taxon_order=None, taxon_family=None, 
                         taxon_genus=None, taxon_key=None, 
                         scientific_name=None, canonical_name=None, squid=None, 
                         user_id=None):
    """Query the Taxonomy index
    
    Args:
        taxon_kingdom: Search for matches in this kingdom
        taxon_phylum:
        taxon_class:
        taxon_family:
        taxon_genus:
        taxon_key:
        scientific_name:
        canonical_name:
        squid:
        user_id:
    """
    q_params = [
        (SOLR_TAXONOMY_FIELDS.CANONICAL_NAME, canonical_name),
        (SOLR_TAXONOMY_FIELDS.SCIENTIFIC_NAME, scientific_name),
        (SOLR_TAXONOMY_FIELDS.SQUID, squid),
        (SOLR_TAXONOMY_FIELDS.TAXON_CLASS, taxon_class),
        (SOLR_TAXONOMY_FIELDS.TAXON_FAMILY, taxon_family),
        (SOLR_TAXONOMY_FIELDS.TAXON_GENUS, taxon_genus),
        (SOLR_TAXONOMY_FIELDS.TAXON_KEY, taxon_key),
        (SOLR_TAXONOMY_FIELDS.TAXON_KINGDOM, taxon_kingdom),
        (SOLR_TAXONOMY_FIELDS.TAXON_ORDER, taxon_order),
        (SOLR_TAXONOMY_FIELDS.TAXON_PHYLUM, taxon_phylum),
        (SOLR_TAXONOMY_FIELDS.USER_ID, user_id)
        ]

    rDict = literal_eval(_query(SOLR_TAXONOMY_COLLECTION, qParams=q_params))
    return rDict['response']['docs']

