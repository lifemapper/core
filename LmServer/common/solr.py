"""This module wraps interactions with Solr
"""
import json
from urllib.error import URLError
import urllib.request

from LmBackend.common.lmobj import LMError
from LmCommon.common.lmconstants import ENCODING
from LmCommon.common.time import LmTime
from LmServer.common.lmconstants import (
    SnippetFields, SOLR_ARCHIVE_COLLECTION, SOLR_FIELDS, SOLR_SERVER,
    SOLR_SNIPPET_COLLECTION, SOLR_TAXONOMY_COLLECTION, SOLR_TAXONOMY_FIELDS)
from LmServer.common.log import SolrLogger


# .............................................................................
def build_solr_document(doc_pairs):
    """Build a document for a Solr POST from the key, value pairs.

    Args:
        doc_pairs: A list of lists of [field name, value] pairs --
            [[(field name, value)]]

    Returns:
        a bytes object, suitable for posting to a solr/HTTP service

    Note:
        When writing the results to a file (encoded), solr_doc must first
        be decoded
    """
    if not doc_pairs:
        raise Exception("Must provide at least one pair for Solr POST")

    # We want to allow multiple documents.  Make sure that field pairs is a
    #     list of lists of tuples
    if not isinstance(doc_pairs[0][0], (list, tuple)):
        doc_pairs = [doc_pairs]

    doc_lines = ['<add>']
    for field_pairs in doc_pairs:
        doc_lines.append('    <doc>')
        for f_name, f_val in field_pairs:
            # Only add the field if the value is not None
            if f_val is not None:
                doc_lines.append(
                    '        <field name="{}">{}</field>'.format(
                        f_name, str(f_val).replace('&', '&amp;')))
        doc_lines.append('    </doc>')
    doc_lines.append('</add>')
    tmpstr = '\n'.join(doc_lines)
    solr_doc = tmpstr.encode(encoding=ENCODING)
    return solr_doc


# .............................................................................
def post_solr_document(collection, doc_filename):
    """Post a document to a Solr index.

    Args:
        collection: The name of the Solr core (index) to add this document to
        doc_filename: The file location of the document to post
    """
    return _post(
        collection, doc_filename, headers={'Content-Type': 'text/xml'})


# .............................................................................
def _post(collection, doc_filename, headers=None):
    """Post a document to a Solr index."""
    if not headers:
        headers = {}
    url = '{}{}/update?commit=true'.format(SOLR_SERVER, collection)

    with open(doc_filename, 'r', encoding=ENCODING) as in_file:
        data_str = in_file.read()
    # urllib requires byte data for post
    data_bytes = data_str.encode(encoding=ENCODING)
    req = urllib.request.Request(url, data=data_bytes, headers=headers)
    return urllib.request.urlopen(req).read()


# .............................................................................
def _query(collection, q_params=None, fq_params=None,
           other_params='wt=json&indent=true'):
    """Perform a query on a Solr index.

    Args:
        collection: The Solr collection (index / core) to query
        q_params: Parameters to include in the query section of the Solr query
        fq_params: Parameters to include in the filter section of the query
        other_params: Other parameters to pass to Solr
    """
    log = SolrLogger()
    query_parts = []
    if q_params:
        q_parts = []
        for k, val in q_params:
            if val is not None:
                if isinstance(val, list):
                    if len(val) > 1:
                        q_parts.append('{}:({})'.format(k, '+OR+'.join(val)))
                    else:
                        q_parts.append('{}:{}'.format(k, val[0]))
                else:
                    q_parts.append('{}:{}'.format(k, val).replace(' ', '+'))
        # If we have at least one query parameter
        if q_parts:
            query_parts.append('q={}'.format('+AND+'.join(q_parts)))

    if fq_params:
        fq_parts = []
        for k, val in fq_params:
            if val is not None:
                if isinstance(val, list):
                    if len(val) > 1:
                        fq_parts.append('{}:({})'.format(k, ' '.join(val)))
                    else:
                        fq_parts.append('{}:{}'.format(k, val[0]))
                else:
                    fq_parts.append('{}:{}'.format(k, val))
        # If we have at least one filter parameter
        if fq_parts:
            query_parts.append('fq={}'.format('+AND+'.join(fq_parts)))

    if len(query_parts) == 0:
        query_parts.append('q=*:*')

    if other_params is not None:
        query_parts.append(other_params)

    url = '{}{}/select?{}'.format(
        SOLR_SERVER, collection, '&'.join(query_parts))
    try:
        res = urllib.request.urlopen(url)
    except URLError as err:
        log.error('URLError on urlopen for {}: {}'.format(url, str(err)), err)
        raise
    except Exception as err:
        log.error('Exception on urlopen for {}: {}'.format(url, str(err)), err)
        raise

    # retcode = res.getcode()
    return json.load(res)


# .............................................................................
def raw_query(collection, query_string):
    """Perform a raw solr query and return the unprocessed results."""
    url = '{}{}/select?{}'.format(SOLR_SERVER, collection, query_string)
    res = urllib.request.urlopen(url)
    return res.read()

# .............................................................................
def _get_record_from_scientificname(sciname):
    rec = [
        [SOLR_TAXONOMY_FIELDS.TAXON_RANK, sciname.rank],
        [SOLR_TAXONOMY_FIELDS.CANONICAL_NAME, sciname.canonical_name],
        [SOLR_TAXONOMY_FIELDS.SCIENTIFIC_NAME, sciname.scientific_name],
        [SOLR_TAXONOMY_FIELDS.SQUID, sciname.squid],
        [SOLR_TAXONOMY_FIELDS.TAXON_CLASS, sciname.class_],
        [SOLR_TAXONOMY_FIELDS.TAXON_FAMILY, sciname.family],
        [SOLR_TAXONOMY_FIELDS.TAXON_GENUS, sciname.genus],
        [SOLR_TAXONOMY_FIELDS.TAXON_KEY, sciname.source_taxon_key],
        [SOLR_TAXONOMY_FIELDS.TAXON_KINGDOM, sciname.kingdom],
        [SOLR_TAXONOMY_FIELDS.TAXON_ORDER, sciname.order_],
        [SOLR_TAXONOMY_FIELDS.TAXON_PHYLUM, sciname.phylum],
        [SOLR_TAXONOMY_FIELDS.TAXONOMY_SOURCE_ID, sciname.taxonomy_source_id],
        [SOLR_TAXONOMY_FIELDS.ID, sciname.get_id()] ]
    return rec

# .............................................................................
def _get_record_from_taxon_csv(taxon_info):
    rec = [
        [SOLR_TAXONOMY_FIELDS.ID, taxon_info['taxonid']],
        [SOLR_TAXONOMY_FIELDS.TAXONOMY_SOURCE_ID, taxon_info['taxonomysourceid']],
        [SOLR_TAXONOMY_FIELDS.TAXON_KEY, taxon_info['taxonomykey']],
        [SOLR_TAXONOMY_FIELDS.SQUID, taxon_info['squid']],
        [SOLR_TAXONOMY_FIELDS.TAXON_KINGDOM, taxon_info['kingdom']],
        [SOLR_TAXONOMY_FIELDS.TAXON_PHYLUM, taxon_info['phylum']],
        [SOLR_TAXONOMY_FIELDS.TAXON_CLASS, taxon_info['tx_class']],
        [SOLR_TAXONOMY_FIELDS.TAXON_ORDER, taxon_info['tx_order']],
        [SOLR_TAXONOMY_FIELDS.TAXON_FAMILY, taxon_info['family']],
        [SOLR_TAXONOMY_FIELDS.TAXON_GENUS, taxon_info['genus']],
        [SOLR_TAXONOMY_FIELDS.TAXON_RANK, taxon_info['rank']],
        [SOLR_TAXONOMY_FIELDS.CANONICAL_NAME, taxon_info['canonical']],
        [SOLR_TAXONOMY_FIELDS.SCIENTIFIC_NAME, taxon_info['sciname']] ]
    return rec

# .............................................................................
def add_taxa_to_taxonomy_index(sciname_objects):
    """Create a solr document and post it for the provided objects."""
    doc_pairs = []
    for sno in sciname_objects:
        rec = _get_record_from_scientificname(sno)
        doc_pairs.append(rec)
    post_doc = build_solr_document(doc_pairs)
    # Note: This is somewhat redundant.
    # TODO: Modify _post to accept a string or file like object as well
    url = '{}{}/update?commit=true'.format(
        SOLR_SERVER, SOLR_TAXONOMY_COLLECTION)
    req = urllib.request.Request(
        url, data=post_doc, headers={'Content-Type': 'text/xml'})
    response = urllib.request.urlopen(req)
    return response.read()


# .............................................................................
def add_taxa_to_taxonomy_index_dicts(taxon_dicts):
    """Create a solr document and post it for the provided objects.

    Note:
        Should be able to post directly from a CSV with header matching solr fields
        
    TODO: Implement writing to CSV and posting directly from CSV
    """
    doc_pairs = []
    for taxon_info in taxon_dicts:
        rec = _get_record_from_taxon_csv(taxon_info)
        doc_pairs.append(rec)
    post_doc = build_solr_document(doc_pairs)
    # Note: This is somewhat redundant.
    # TODO: Modify _post to accept a string or file like object as well
    url = '{}{}/update?commit=true'.format(
        SOLR_SERVER, SOLR_TAXONOMY_COLLECTION)
    req = urllib.request.Request(
        url, data=post_doc, headers={'Content-Type': 'text/xml'})
    return urllib.request.urlopen(req).read()

# .............................................................................
def add_taxa_to_taxonomy_from_csv(taxon_filename):
    """
    Post a CSV solr document to the taxonomy index

    Note:
        CSV must have a header containing solr fieldnames 
    """
    with open(taxon_filename, 'rb') as in_file:
        post_data = in_file.read()
    url = '{}{}/update?commit=true'.format(
        SOLR_SERVER, SOLR_TAXONOMY_COLLECTION)
    req = urllib.request.Request(
        url, data=post_data, headers={'Content-Type': 'application/csv'})
    return urllib.request.urlopen(req).read()


# .............................................................................
def delete_from_archive_index(gridset_id=None, pav_id=None, sdmproject_id=None,
                              occ_id=None, squid=None, user_id=None):
    """Delete records from the archive index.

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
    if all(item is None for item in [
            gridset_id, pav_id, sdmproject_id, occ_id, squid, user_id]):
        raise Exception('Must provide at least one query parameter')
    if isinstance(pav_id, (list, tuple)):
        pav_id = '({})'.format(' OR '.join([str(p) for p in pav_id]))
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
        "delete": {
            "query": query
        }
    }

    data_str = json.dumps(doc).encode(encoding=ENCODING)
    req = urllib.request.Request(
        url, data=data_str, headers={'Content-Type': 'application/json'})
    return urllib.request.urlopen(req).read()


# .............................................................................
def facet_archive_on_gridset(user_id=None):
    """Query the global pam index and get the number of matches for gridsets.

    TODO:
        Consider integrating this with regular solr query
    """
    q_params = [
        (SOLR_FIELDS.USER_ID, user_id),
        ('*', '*')
    ]

    other_params = '&facet=true&facet.field={}&wt=python&indent=true'.format(
        SOLR_FIELDS.GRIDSET_ID)
    try:
        r_dict = _query(
            SOLR_ARCHIVE_COLLECTION, q_params=q_params,
            other_params=other_params)
    except Exception as err:
        raise LMError(err)
    else:
        sdata = r_dict['facet_counts']['facet_fields'][SOLR_FIELDS.GRIDSET_ID]
        return sdata


# .............................................................................
def query_archive_index(algorithm_code=None, bbox=None, display_name=None,
                        gridset_id=None, model_scenario_code=None,
                        point_max=None, point_min=None,
                        projection_scenario_code=None, squid=None,
                        tax_kingdom=None, tax_phylum=None, tax_class=None,
                        tax_order=None, tax_family=None, tax_genus=None,
                        tax_species=None, user_id=None, pam_id=None):
    """Query the PAV archive Solr index."""
    q_params = [
        (SOLR_FIELDS.ALGORITHM_CODE, algorithm_code),
        (SOLR_FIELDS.DISPLAY_NAME, display_name),
        (SOLR_FIELDS.GRIDSET_ID, gridset_id),
        (SOLR_FIELDS.USER_ID, user_id),
        (SOLR_FIELDS.MODEL_SCENARIO_CODE, model_scenario_code),
        (SOLR_FIELDS.PROJ_SCENARIO_CODE, projection_scenario_code),
        (SOLR_FIELDS.SQUID, squid),
        (SOLR_FIELDS.TAXON_KINGDOM, tax_kingdom),
        (SOLR_FIELDS.TAXON_PHYLUM, tax_phylum),
        (SOLR_FIELDS.TAXON_CLASS, tax_class),
        (SOLR_FIELDS.TAXON_ORDER, tax_order),
        (SOLR_FIELDS.TAXON_FAMILY, tax_family),
        (SOLR_FIELDS.TAXON_GENUS, tax_genus),
        (SOLR_FIELDS.TAXON_SPECIES, tax_species),
        (SOLR_FIELDS.PAM_ID, pam_id)
    ]

    if point_max is not None or point_min is not None:
        pmax = point_max if point_max is not None else '*'
        pmin = point_min if point_min is not None else '*'

        q_params.append((
            SOLR_FIELDS.POINT_COUNT, '%5B{}%20TO%20{}%5D'.format(pmin, pmax)))

    fq_params = []
    if bbox is not None:
        minx, miny, maxx, maxy = bbox.split(',')
        fq_params.append(
            (SOLR_FIELDS.PRESENCE, '%5B{},{}%20{},{}%5D'.format(
                miny, minx, maxy, maxx)))

    try:
        r_dict = _query(
            SOLR_ARCHIVE_COLLECTION, q_params=q_params, fq_params=fq_params)
    except Exception as err:
        raise LMError(err)

    return r_dict['response']['docs']


# .............................................................................
def query_snippet_index(ident1=None, provider=None, collection=None,
                        catalog_number=None, operation=None, after_time=None,
                        before_time=None, ident2=None, url=None, who=None,
                        agent=None, why=None):
    """Query the snippet Solr index.

    Args:
        ident1: An identifier for the primary object (probably occurrence
            point)
        provider: The occurrence point provider
        collection: The collection the point belongs to
        catalog_number: The catalog number of the occurrence point
        operation: A LmServer.common.lmconstants.SnippetOperations
        after_time: Return hits after this time (MJD format)
        before_time: Return hits before this time (MJD format)
        ident2: A identifier for the secondary object (occurrence set or
            projection)
        url: A url for the resulting object
        who: Who initiated the action
        agent: The agent that initiated the action
        why: Why the action was initiated
    """
    q_params = [
        (SnippetFields.AGENT, agent),
        (SnippetFields.CATALOG_NUMBER, catalog_number),
        (SnippetFields.COLLECTION, collection),
        (SnippetFields.IDENT_1, ident1),
        (SnippetFields.IDENT_2, ident2),
        (SnippetFields.OPERATION, operation),
        (SnippetFields.PROVIDER, provider),
        (SnippetFields.URL, url),
        (SnippetFields.WHO, who),
        (SnippetFields.WHY, why)
    ]

    fq_params = []
    if after_time is not None or before_time is not None:
        if after_time is not None:
            a_time = LmTime.from_mjd(after_time).strftime('%Y-%m-%dT%H:%M:%SZ')
        else:
            a_time = '*'

        if before_time is not None:
            b_time = LmTime.from_mjd(
                before_time).strftime('%Y-%m-%dT%H:%M:%SZ')
        else:
            b_time = '*'

        fq_params.append(
            (SnippetFields.OP_TIME,
             '%5B{}%20TO%20{}%5D'.format(a_time, b_time)))

    try:
        r_dict = _query(
            SOLR_SNIPPET_COLLECTION, q_params=q_params, fq_params=fq_params)
    except Exception as err:
        raise LMError(err)

    return r_dict['response']['docs']


# .............................................................................
def query_taxonomy_index(
        taxon_kingdom=None, taxon_phylum=None, taxon_class=None, taxon_order=None, taxon_family=None,
        taxon_genus=None, taxon_key=None, scientific_name=None, canonical_name=None, squid=None,
        taxon_rank=None):
    """Query the Taxonomy index.

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
        taxon_rank:
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
        (SOLR_TAXONOMY_FIELDS.TAXON_RANK, taxon_rank)
        ]

    try:
        r_dict = _query(SOLR_TAXONOMY_COLLECTION, q_params=q_params)
    except Exception as err:
        raise LMError(err)

    return r_dict['response']['docs']


"""
Post:
/opt/solr/bin/post -c spcoco /state/partition1/git/t-rex/data/solrtest/occurrence.solr.csv

Query:
curl http://localhost:8983/solr/taxonomy/select?q=occurrence_guid:47d04f7e-73fa-4cc7-b50a-89eeefdcd162
curl http://localhost:8983/solr/taxonomy/select?q=*:*
"""
