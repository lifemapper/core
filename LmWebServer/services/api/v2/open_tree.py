"""This module provides a wrapper around OpenTree's induce subtree service

Todo:
    * Use opentree wrapper code
    * Catch service errors from OpenTree
"""
import hashlib
import json
import os

import cherrypy
from ot_service_wrapper import get_ottids_from_gbifids, induced_subtree

from LmCommon.common.lmconstants import HTTPStatus, ENCODING
from LmCommon.common.ready_file import ready_filename
from LmCommon.common.time import gmt
from LmDbServer.tools.partner_data import Partners
from LmServer.common.lmconstants import (
    ARCHIVE_PATH, NONTREE_GBIF_IDS_KEY, TREE_DATA_KEY, TREE_FORMAT_KEY,
    TREE_NAME_KEY, UNMATCHED_GBIF_IDS_KEY)
from LmWebServer.services.api.v2.base import LmService
from LmWebServer.services.cp_tools.lm_format import lm_formatter


# .............................................................................
@cherrypy.expose
class OpenTreeService(LmService):
    """Open Tree wrapper service for retrieving trees from GBIF Ids
    """

    # ................................
    @lm_formatter
    def POST(self):
        """Gets an Open Tree tree for a list of GBIF taxon ids
        """
        taxon_ids_obj = json.load(cherrypy.request.body)

        if not isinstance(taxon_ids_obj, list):
            raise cherrypy.HTTPError(
                HTTPStatus.BAD_REQUEST,
                'Taxon IDs must be provided as a JSON list')

        try:
            # Get OpenTree ids for GBIF ids
            gbif_to_ott = get_ottids_from_gbifids(taxon_ids_obj)
            # Get the unmatched GBIF IDs
            unmatched_gbif_ids = [
                k for k in list(gbif_to_ott.keys()) if gbif_to_ott[k] is None]
            # Create a reverse lookup for OTT to GBIF IDs
            ott_to_gbif = {v: k for (k, v) in gbif_to_ott.items()}
            # Get the ids and drop Nones
            ott_ids = [
                oid for oid in list(gbif_to_ott.values()) if oid is not None]
            if len(ott_ids) <= 1:
                raise cherrypy.HTTPError(
                    HTTPStatus.BAD_REQUEST,
                    'Need more than one open tree ID to create a tree')
            # Get the tree from Open Tree
            output = induced_subtree(ott_ids)
            tree_data = output[Partners.OTT_TREE_KEY]
            # Get the list of GBIF IDs that matched to OTT IDs but were not in
            #    tree
            nontree_ids = [
                int(ott_to_gbif[ott]) for ott in output[
                    Partners.OTT_MISSING_KEY]]
        except cherrypy.HTTPError:
            raise
        except Exception as e:
            raise cherrypy.HTTPError(
                HTTPStatus.SERVICE_UNAVAILABLE,
                'We are having trouble connecting to Open Tree: {}'.format(
                    str(e)))

        # Determine a name for the tree, use user id, 16 characters of hashed
        #    tree data, and mjd
        tree_name = '{}-{}-{}.tre'.format(
            self.get_user_id(), hashlib.md5(tree_data).hexdigest()[:16],
            gmt().mjd)
        # Write the tree
        out_filename = os.path.join(self._get_user_dir(), tree_name)
        if not os.path.exists(out_filename):
            ready_filename(out_filename)
            with open(out_filename, 'w', encoding=ENCODING) as out_f:
                out_f.write(tree_data)
        else:
            raise cherrypy.HTTPError(
                HTTPStatus.CONFLICT,
                'Tree with this name already exists in the user space')

        resp = {
            NONTREE_GBIF_IDS_KEY: nontree_ids,
            TREE_DATA_KEY: tree_data,
            TREE_FORMAT_KEY: Partners.OTT_TREE_FORMAT,  # Newick
            TREE_NAME_KEY: tree_name,
            UNMATCHED_GBIF_IDS_KEY: unmatched_gbif_ids,
        }

        return resp

    # ................................
    def _get_user_dir(self):
        """Get the user's workspace directory for tree upload

        Todo:
            * Move this function to base class and generalize
        """
        return os.path.join(
            ARCHIVE_PATH, self.get_user_id(), 'uploads', 'tree')
