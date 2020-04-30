"""Module containing snippet shooter
"""
import os
from random import randint

from LmBackend.common.lmobj import LMError, LMObject
from LmCommon.common.lmconstants import ENCODING
from LmCommon.common.time import gmt, LmTime
from LmServer.common.lmconstants import (
    SnippetFields, SOLR_SERVER, SOLR_SNIPPET_COLLECTION, UPLOAD_PATH)
from LmServer.common.solr import build_solr_document, post_solr_document


# .............................................................................
class SnippetShooter(LMObject):
    """Creates snippets and "shoots" them to an awaiting snippet server"""

    # ............................
    def __init__(self, snippet_server=SOLR_SERVER,
                 snippet_collection=SOLR_SNIPPET_COLLECTION):
        """Constructor

        Args:
            snippet_server: A snippet server that will accept these snippets.
            snippet_collection: A solr collection for these snippets
        """
        self.server = snippet_server
        self.collection = snippet_collection

        self.snippets = []

    # ............................
    def add_snippets(self, obj1, operation, op_time=None, obj2ident=None,
                     url=None, who=None, agent=None, why=None):
        """Add snippets for posting to the snippet shooter's list

        Args:
            obj1: Required, this will start out as only occurrence sets but
                should be expanded later to include other object types
            operation: See LmServer.common.lmconstants.SnippetOperations
                for available operations
            op_time: (optional) MJD time that this operation took place
            obj2ident: (optional) Identifier of the secondary object
            url: (optional) A URL related to this snippet
            who: (optional) A string representing who initiated this action
            agent: (optional) The agent that this action was initiated through
                examples could be LmCompute, web client, or similar
            why: (optional) Why this action was initiated (archive, user
                request, etc)
        """
        try:
            obj1.get_scientific_name()
        except Exception as err:
            raise LMError(
                'Do no know how to create snippets for: {}'.format(
                    str(obj1.__class__)), err)

        if len(obj1.features) == 0:
            # Try to read the data if no features
            obj1.readData(doReadData=True)

        if len(obj1.features) == 0:
            raise LMError(
                'Occurrence set must have features to create snippets for')

        if op_time is None:
            op_time = gmt().mjd
        op_time_str = LmTime.from_mjd(op_time).strftime('%Y-%m-%dT%H:%M:%SZ')

        for feat in obj1.features:
            try:
                cat_num = feat.catnum
                provider = feat.provider
                col = feat.coll_code
                ident = '{}:{}:{}'.format(provider, col, cat_num)
                op_id = '{}:{}:{}'.format(ident, operation, op_time_str)
                self.snippets.append([
                    (SnippetFields.AGENT, agent),
                    (SnippetFields.CATALOG_NUMBER, cat_num),
                    (SnippetFields.COLLECTION, col),
                    (SnippetFields.ID, op_id),
                    (SnippetFields.IDENT_1, ident),
                    (SnippetFields.IDENT_2, obj2ident),
                    (SnippetFields.OP_TIME, op_time_str),
                    (SnippetFields.OPERATION, operation),
                    (SnippetFields.PROVIDER, provider),
                    (SnippetFields.URL, url),
                    (SnippetFields.WHO, who),
                    (SnippetFields.WHY, why),
                ])
            except Exception:
                # If we don't know what to create a snippet for, skip
                pass

    # ............................
    def shoot_snippets(self, solr_post_filename=None):
        """Shoot the snippets to the snippet collection.

        Args:
            solr_post_filename: If provided, write out the Solr post document
                here
        """
        # Build the Solr document, returning bytes object
        solr_post_bytes = build_solr_document(self.snippets)

        delete_post_filename = False
        # Write to temp file
        if solr_post_filename is None:
            # TODO: Fill in
            solr_post_filename = os.path.join(
                UPLOAD_PATH, 'snippetPost-{}'.format(randint(0, 10000)))
            delete_post_filename = True

        # Encode to string before writing to file
        # Write the (bytes) post document as an encoded string
        solr_post_str =  solr_post_bytes.decode(encoding=ENCODING)
        with open(solr_post_filename, 'w', encoding=ENCODING) as out_f:
            out_f.write(solr_post_str)

        # Shoot snippets
        if len(self.snippets) > 0:
            post_solr_document(self.collection, solr_post_filename)

        if delete_post_filename:
            os.remove(solr_post_filename)

        # Reset snippet list
        self.snippets = []
