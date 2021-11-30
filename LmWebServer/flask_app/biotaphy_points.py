"""This module provides a wrapper around iDigBio's occurrence service to get an occurrence count for GBIF taxon keys"""
import os
import random
import werkzeug.exceptions as WEXC

from LmCommon.common.api_query import IdigbioAPI, GbifAPI
from LmCommon.common.lmconstants import DEFAULT_POST_USER
from LmServer.common.data_locator import EarlJr
from LmServer.common.lmconstants import LMFileType, FileFix
from LmServer.common.localconstants import PUBLIC_USER
from LmWebServer.flask_app.base import LmService
from LmWebServer.services.cp_tools.lm_format import lm_formatter


# .............................................................................
class IDigBioOccurrenceService(LmService):
    """iDigBio occurrence data service"""

    # ................................
    def _get_data_targets(self):
        earl = EarlJr()
        user_id = self.get_user_id()
        if user_id == PUBLIC_USER:
            user_id = DEFAULT_POST_USER
        # All results are temp files
        out_dir = earl.create_data_path(user_id, LMFileType.TMP_JSON)
        if not os.path.exists(out_dir):
            os.makedirs(out_dir)
        basename = earl.create_basename(
            LMFileType.OCCURRENCE_RAW_FILE, obj_code=random.randint(0, 100000))

        point_output_file = os.path.join(
            out_dir,
            basename + FileFix.EXTENSION[LMFileType.OCCURRENCE_RAW_FILE])
        meta_output_file = os.path.join(
            out_dir,
            basename + FileFix.EXTENSION[LMFileType.OCCURRENCE_META_FILE])
        return point_output_file, meta_output_file

    # ................................
    @lm_formatter
    def get_occurrence_counts_for_taxonids(self, taxonids_obj):
        """Queries iDigBio for the number of occurrence points for the provided GBIF taxon keys
        
        Args:
            taxonids_obj: a JSON list of GBIF taxon_keys to count iDigBio occurrences for.
        """

        if not isinstance(taxonids_obj, list):
            return WEXC.BadRequest('GBIF Taxon IDs must be a JSON list')

        _point_output_file, _meta_output_file = self._get_data_targets()
        idig_api = IdigbioAPI()
        response = []
        try:
            # queryIdigbioData gets and returns counts
            summary = idig_api.query_idigbio_data(taxonids_obj)

        except Exception as e:
            self.log.error('Could not get iDigBio points for GBIF taxon IDs: {}'.format(e))
            
        else:
            for key, val in summary.items():
                if key != GbifAPI.GBIF_MISSING_KEY:
                    response.append({GbifAPI.TAXON_ID_KEY: key, IdigbioAPI.OCCURRENCE_COUNT_KEY: val})
        
        return response
