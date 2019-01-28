#! /usr/bin/env python
# -*- coding: utf-8 -*-
"""This module provides a wrapper around GBIF's names service
"""
import cherrypy
import json
import os
import random

from LmCommon.common.apiquery import IdigbioAPI, GbifAPI
from LmCommon.common.lmconstants import HTTPStatus, DEFAULT_POST_USER
from LmWebServer.services.api.v2.base import LmService
from LmWebServer.services.cpTools.lmFormat import lmFormatter
from LmServer.common.datalocator import EarlJr
from LmServer.common.localconstants import PUBLIC_USER
from LmServer.common.lmconstants import LMFileType, FileFix

# .............................................................................
@cherrypy.expose
class IDigBioOccurrenceService(LmService):
    """
    """
    # ................................
    def _get_data_targets(self):
        """
        """
        earl = EarlJr()
        userId = self.getUserId()
        if userId == PUBLIC_USER:
            userId = DEFAULT_POST_USER
        # All results are temp files
        out_dir = earl.createDataPath(userId, LMFileType.TMP_JSON)
        if not os.path.exists(out_dir):
            os.makedirs(out_dir)
        basename = earl.createBasename(LMFileType.OCCURRENCE_RAW_FILE, 
                                       objCode=random.randint(0, 100000))
        
        point_output_file = os.path.join(out_dir, basename +
                            FileFix.EXTENSION(LMFileType.OCCURRENCE_RAW_FILE))
        meta_output_file =  os.path.join(out_dir, basename +
                            FileFix.EXTENSION(LMFileType.OCCURRENCE_META_FILE))
        return point_output_file, meta_output_file
            
    # ................................
    @lmFormatter
    def POST(self):
        """Queries GBIF for accepted names matching the provided list of names
        """
        json_obj = json.load(cherrypy.request.body)
        if not isinstance(json_obj, list):
            raise cherrypy.HTTPError(
                HTTPStatus.BAD_REQUEST,
                'GBIF taxon ids must be provided as a JSON list')

        point_output_file, meta_output_file = self._get_data_targets()
        taxon_ids = []
        for elt in json_obj:
            taxon_ids.append(elt[0])
        
            ret = []
            try:
                summary = IdigbioAPI.assembleIdigbioData(taxon_ids, 
                                        point_output_file, meta_output_file, 
                                        missing_id_file=None)
            except Exception as e:
                self.log.error(
                    'Could not get occurrence points from iDigBio for GBIF taxon IDs: {}'
                    .format(str(e)))
            else:
                for key, val in summary.iteritems():
                    if key != GbifAPI.GBIF_MISSING_KEY:
                        ret.append({GbifAPI.TAXON_ID_KEY : key,
                                    IdigbioAPI.OCCURRENCE_COUNT_KEY : val})
            return ret

"""        
curl 'http://notyeti-191.lifemapper.org/api/v2/biotaphypoints' \
     -H 'Accept: application/json' \
     -H 'Origin: null' -H 'User-Agent: Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Ubuntu Chromium/71.0.3578.98 Chrome/71.0.3578.98 Safari/537.36' \
     -H 'DNT: 1' \
     -H 'Content-Type: application/json' \
     --data-binary '[7263110,9022303,7907188,7263052,3189849,9019916,3768081,7262927,3767750]' \
     --compressed
     
import json
import os
import random

from LmCommon.common.apiquery import IdigbioAPI, GbifAPI
from LmCommon.common.lmconstants import HTTPStatus, DEFAULT_POST_USER
from LmWebServer.services.api.v2.base import LmService
from LmWebServer.services.cpTools.lmFormat import lmFormatter
from LmServer.common.datalocator import EarlJr
from LmServer.common.localconstants import PUBLIC_USER
from LmServer.common.lmconstants import LMFileType, FileFix

body = 

json_obj = json.load(body)
if not isinstance(json_obj, list):
    raise cherrypy.HTTPError(
        HTTPStatus.BAD_REQUEST,
        'GBIF taxon ids must be provided as a JSON list')

earl = EarlJr()
userId = self.getUserId()
if userId == PUBLIC_USER:
    userId = DEFAULT_POST_USER
# All results are temp files
out_dir = earl.createDataPath(userId, LMFileType.TMP_JSON)
if not os.path.exists(out_dir):
    os.makedirs(out_dir)
basename = earl.createBasename(LMFileType.OCCURRENCE_RAW_FILE, 
                               objCode=random.randint(0, 100000))

point_output_file = os.path.join(out_dir, basename +
                    FileFix.EXTENSION(LMFileType.OCCURRENCE_RAW_FILE))
meta_output_file =  os.path.join(out_dir, basename +
                    FileFix.EXTENSION(LMFileType.OCCURRENCE_META_FILE))


point_output_file, meta_output_file = self._get_data_targets()
taxon_ids = []
for elt in json_obj:
    taxon_ids.append(elt[0])

    ret = []
    try:
        summary = IdigbioAPI.assembleIdigbioData(taxon_ids, 
                                point_output_file, meta_output_file, 
                                missing_id_file=None)
    except Exception as e:
        self.log.error(
            'Could not get occurrence points from iDigBio for GBIF taxon IDs: {}'
            .format(str(e)))
    else:
        for key, val in summary.iteritems():
            if key != GbifAPI.GBIF_MISSING_KEY:
                ret.append({GbifAPI.TAXON_ID_KEY : key,
                            IdigbioAPI.OCCURRENCE_COUNT_KEY : val})
    return ret


"""