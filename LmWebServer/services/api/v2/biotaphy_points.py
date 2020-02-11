#! /usr/bin/env python
# -*- coding: utf-8 -*-
"""This module provides a wrapper around GBIF's names service
"""
import json
import os
import random

from LmCommon.common.api_query import IdigbioAPI, GbifAPI
from LmCommon.common.lmconstants import HTTPStatus, DEFAULT_POST_USER
from LmServer.common.datalocator import EarlJr
from LmServer.common.lmconstants import LMFileType, FileFix
from LmServer.common.localconstants import PUBLIC_USER
from LmWebServer.services.api.v2.base import LmService
from LmWebServer.services.cp_tools.lm_format import lm_formatter
import cherrypy


# .............................................................................
@cherrypy.expose
class IDigBioOccurrenceService(LmService):
    """iDigBio occurrence data service
    """
    # ................................
    def _get_data_targets(self):
        """
        """
        earl = EarlJr()
        user_id = self.get_user_id()
        if user_id == PUBLIC_USER:
            user_id = DEFAULT_POST_USER
        # All results are temp files
        out_dir = earl.createDataPath(user_id, LMFileType.TMP_JSON)
        if not os.path.exists(out_dir):
            os.makedirs(out_dir)
        basename = earl.createBasename(
            LMFileType.OCCURRENCE_RAW_FILE, objCode=random.randint(0, 100000))

        point_output_file = os.path.join(
            out_dir,
            basename + FileFix.EXTENSION[LMFileType.OCCURRENCE_RAW_FILE])
        meta_output_file = os.path.join(
            out_dir,
            basename + FileFix.EXTENSION[LMFileType.OCCURRENCE_META_FILE])
        return point_output_file, meta_output_file

    # ................................
    @lm_formatter
    def POST(self):
        """Queries GBIF for accepted names matching the provided list of names
        """
        taxon_ids = json.load(cherrypy.request.body)
        if not isinstance(taxon_ids, list):
            raise cherrypy.HTTPError(
                HTTPStatus.BAD_REQUEST,
                'GBIF taxon ids must be provided as a JSON list')

        point_output_file, meta_output_file = self._get_data_targets()
        ret = []
        idig_api = IdigbioAPI()
        try:
            # queryIdigbioData gets and returns counts
            summary = idig_api.queryIdigbioData(taxon_ids)
        except Exception as e:
            self.log.error(
                'Could not get iDigBio points for GBIF taxon IDs: {}'.format(
                    str(e)))
        else:
            for key, val in summary.items():
                if key != GbifAPI.GBIF_MISSING_KEY:
                    ret.append({GbifAPI.TAXON_ID_KEY: key,
                                IdigbioAPI.OCCURRENCE_COUNT_KEY: val})
        return ret
