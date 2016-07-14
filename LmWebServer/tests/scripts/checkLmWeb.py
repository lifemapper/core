"""
@summary: Test script that will check Lifemapper web pages to make sure that 
             the website is responding properly
@note: Could be run as a cron job for automation
@author: CJ Grady

@license: gpl2
@copyright: Copyright (C) 2015, University of Kansas Center for Research

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
import urllib2

from LmCommon.common.lmconstants import JobStatus

from LmServer.common.localconstants import TROUBLESHOOTERS, WEBSERVICES_ROOT
from LmServer.common.log import ScriptLogger
from LmServer.db.scribe import Scribe
from LmServer.notifications.email import EmailNotifier

MAP_PARAMETERS = "height=200&width=400&request=GetMap&service=WMS&bbox=-180.0,-90.0,180.0,90.0&srs=epsg:4326&format=image/png&color=ffff00&version=1.1.0&styles="

# ..............................................................................
def assembleUrls(logger, urls):
   INTERFACES = {
      "exp" : ['atom', 'eml', 'html', 'json', 'kml', 'model', 'status', 'xml'],
      "lyr" : ['ascii', 'atom', 'eml', 'html', 'json', 'kml', 'raw', 'tiff', 'xml'],
      "occ" : ['atom', 'csv', 'eml', 'html', 'json', 'kml', 'shapefile', 'xml'],
      "prj" : ['ascii', 'atom', 'eml', 'html', 'json', 'kml', 'raw', 'status', 'tiff', 'xml'],
      "scn" : ['atom', 'eml', 'html', 'json', 'xml']
   }   
   BASEURLS = {}
   for key in INTERFACES:
      BASEURLS[key] = None
   peruser = Scribe(logger)
   peruser.openConnections()
      
   # experiment
   exps = peruser.listModels(0, 1, status=JobStatus.COMPLETE)
   if len(exps) > 0:
      BASEURLS['exp'] = "{}/services/sdm/experiments/{}".format(WEBSERVICES_ROOT, exps[0].id)
   
   # layer
   lyrs = peruser.listLayers(0, 1)
   if len(lyrs) > 0:
      BASEURLS['lyr'] = "{}/services/sdm/layers/{}".format(WEBSERVICES_ROOT, lyrs[0].id)
   
   # occurrence set - Complete
   occs = peruser.listOccurrenceSets(0, 10, minOccurrenceCount=50,status=JobStatus.COMPLETE)
   if len(occs) > 0:
      BASEURLS['occ'] = "{}/services/sdm/occurrences/{}".format(WEBSERVICES_ROOT, occs[0].id)
      # Add map urls
      occMap = "{}/services/sdm/occurrences/{}/ogc?{}&layers=occ_{}".format(
               WEBSERVICES_ROOT, occs[0].id, MAP_PARAMETERS, occs[0].id)
      occBGMap = "{}/services/sdm/occurrences/{}/ogc?{}&layers=bmng,occ_{}".format(
               WEBSERVICES_ROOT, occs[0].id, MAP_PARAMETERS, occs[0].id)
      urls.append(occMap)
      urls.append(occBGMap)
      
   # projection
   prjs = peruser.listProjections(0, 1, status=JobStatus.COMPLETE)
   if len(prjs) > 0:
      BASEURLS['prj'] = "{}/services/sdm/projections/{}".format(WEBSERVICES_ROOT, prjs[0].id)
      
      # Add map urls
      prjMap = "{}/services/sdm/projections/{}/ogc?{}&layers=prj_{}".format(
               WEBSERVICES_ROOT, prjs[0].id, MAP_PARAMETERS, prjs[0].id)
      prjBGMap = "{}/services/sdm/projections/{}/ogc?{}&layers=bmng,prj_{}".format(
               WEBSERVICES_ROOT, prjs[0].id, MAP_PARAMETERS, prjs[0].id)
      urls.append(prjMap)
      urls.append(prjBGMap)

   # scenario
   scens = peruser.listScenarios(0, 1)
   if len(scens) > 0:
      BASEURLS['scn'] = "{}/services/sdm/scenarios/{}".format(WEBSERVICES_ROOT, scens[0].id)
   
   for key, url in BASEURLS.iteritems():
      if url is not None:
         for iface in INTERFACES[key]:
            urls.append("{}/{}".format(url, iface))
   
   # type code
   peruser.closeConnections()
   return urls

# ..............................................................................
def checkUrl(logger, url):
   try:
      logger.debug("Url: " + url)
      req = urllib2.Request(url)
      resp = urllib2.urlopen(req)
      return resp.code
   except urllib2.HTTPError, e:
      return e.code

# ..............................................................................
def reportFailure(mesgs):
   notifier = EmailNotifier()
   notifier.sendMessage(TROUBLESHOOTERS, "Lifemapper failing urls", 
                        '\n'.join(mesgs))

# ..............................................................................
# # debug
# WEBSERVICES_ROOT = 'http://lifemapper.org'

urls = [
        # Main site
        WEBSERVICES_ROOT, 
        # Check listing services
        "{}/services/".format(WEBSERVICES_ROOT),
        "{}/services/sdm/".format(WEBSERVICES_ROOT),
        "{}/services/sdm/experiments".format(WEBSERVICES_ROOT),
        "{}/services/sdm/layers".format(WEBSERVICES_ROOT),
        "{}/services/sdm/projections".format(WEBSERVICES_ROOT),
        "{}/services/sdm/scenarios".format(WEBSERVICES_ROOT),
        "{}/services/rad/".format(WEBSERVICES_ROOT),
        "{}/services/rad/experiments".format(WEBSERVICES_ROOT),
        "{}/services/rad/layers".format(WEBSERVICES_ROOT),
        # Hint services
        "{0}/hint/species/ace?maxReturned=1000&format=json".format(WEBSERVICES_ROOT),
        ]

ctBBox = '-5702214.17,-3732756.48,4073244.21,4704460.00'
ctParamStr = 'request=GetMap&service=WMS&version=1.1.0&bbox=-5702214.17,-3732756.48,4073244.21,4704460.00&srs=EPSG:2163&format=image/png&HEIGHT=600&WIDTH=600'            
ctNSPrefix = '{0}/ogc?map=anc_ctrange&{1}&styles=green'.format(WEBSERVICES_ROOT, ctParamStr)
ctPrjPrefix = '{0}/ogc?map=usr_changeThinking_2163&{1}&styles='.format(WEBSERVICES_ROOT, ctParamStr)
ctBckPrefix = '{0}/ogc?map=anc_ctbackground&{1}&styles='.format(WEBSERVICES_ROOT, ctParamStr)
ctClmPrefix = '{0}/ogc?map=anc_ctclim&{1}&TRANSPARENT=true&styles='.format(WEBSERVICES_ROOT, ctParamStr)
ctRadPrefix = '{0}/ogc?map=anc_ctRAD&{1}&styles='.format(WEBSERVICES_ROOT, ctParamStr)
kuUrls = [
           # Species page
           "{}/species/".format(WEBSERVICES_ROOT),
           # Specify
           "{0}/ogc?MAP=anc_nasalocal.map&layers=bmnglowres&height=200&width=400&request=GetMap&service=WMS&bbox=-180.0,-90.0,180.0,90.0&srs=epsg:4326&format=image/gif&version=1.1.0&styles=".format(WEBSERVICES_ROOT)
           # Change Thinking
           #"{0}&layers=prj_1699113".format(ctPrjPrefix),
           #"{0}&layers=prj_1699186".format(ctPrjPrefix),
           #"{0}&layers=prj_1858424".format(ctPrjPrefix),
           
           # CT Background
           #"{0}&layers=Landcover&TRANSPARENT=true".format(ctBckPrefix),
           #"{0}&layers=Cities&TRANSPARENT=false".format(ctBckPrefix),
           #"{0}&layers=Landcover&TRANSPARENT=false".format(ctBckPrefix),
           #"{0}&layers=Biome&TRANSPARENT=false".format(ctBckPrefix),
           #"{0}&layers=terrestrial_water&TRANSPARENT=false".format(ctBckPrefix),
           
           # CT Climate layers
           #"{0}&layers=five_degree_ann_temp".format(ctClmPrefix),
           #"{0}&layers=fifty_cm_ann_precip".format(ctClmPrefix),
           #"{0}&layers=ann_temp_B1_2070_2099".format(ctClmPrefix),
           
           # CT Projection Layers
           #"{0}&layers=prj_1699133".format(ctPrjPrefix),
           #"{0}&layers=prj_1699057".format(ctPrjPrefix),
           #"{0}&layers=prj_1699156".format(ctPrjPrefix),
           #"{0}&layers=prj_1698897".format(ctPrjPrefix),
           #"{0}&layers=prj_1698912".format(ctPrjPrefix),
           #"{0}&layers=prj_1698961".format(ctPrjPrefix),
           #"{0}&layers=prj_1699346".format(ctPrjPrefix),
           
           # CT RAD Layers
           #"{0}&layers=passeriforme_richness_worldclim".format(ctRadPrefix),
           #"{0}&layers=amphibia_richness_worldclim".format(ctRadPrefix),
           #"{0}&layers=butterfly_richness_worldclim".format(ctRadPrefix),
           
           # CT NatureServe
           #"{0}&layers=southern_red_backed_vole".format(ctNSPrefix),
           #"{0}&layers=southern_bog_lemming".format(ctNSPrefix),
           #"{0}&layers=southern_short_tailed_shrew".format(ctNSPrefix),
           #"{0}&layers=Elliots_short_tail_shrew".format(ctNSPrefix),
           #"{0}&layers=plains_garter_snake".format(ctNSPrefix),
           #"{0}&layers=common_garter_snake".format(ctNSPrefix),
           #"{0}&layers=northern_cardinal".format(ctNSPrefix),
           #"{0}&layers=northern_leopard_frog".format(ctNSPrefix),
           #"{0}&layers=red_tailed_hawk".format(ctNSPrefix),
           #"{0}&layers=American_crow".format(ctNSPrefix),           
       ]

# ..............................................................................
# ..............................................................................
if __name__ == '__main__':
   logger = ScriptLogger('checkLmWeb')
   
   if WEBSERVICES_ROOT.find('svc.lifemapper.org') >= 0:
      urls.extend(kuUrls)
   urls = assembleUrls(logger, urls)
   
   mesgs = []
   for url in urls:
      code = checkUrl(logger, url)
      if code not in [200, 503]:
         msg = " returned HTTP code: {}".format(code)
         logger.debug(msg)
         mesgs.append(url + msg)
   if len(mesgs) > 0:
      reportFailure(mesgs)
      
