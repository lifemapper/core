"""
@summary: Tests all ChangeThinking maps and metadata
@author: Aimee Stewart
@contact: astewart@ku.edu
@status: alpha

@license: gpl2
@copyright: Copyright (C) 2014, University of Kansas Center for Research

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
from urllib2 import urlopen, URLError

from LmServer.common.datalocator import EarlJr
from LmServer.common.lmconstants import CT_USER
from LmServer.common.log import ThreadLogger
from LmServer.db.scribe import Scribe

from LmBackend.notifications.email import EmailNotifier

# ...............................................
def createTestMap(lyr):
   height, width = lyr.getHeightWidthByBBox(limitWidth=500)
   srs = 'epsg:%d' % lyr.epsgcode
   lyrurl = earl.constructLMMapRequest(lyr.mapPrefix, width, height, lyr.bbox,
                                       srs=lyr.SRS)
   return lyrurl

# ...............................................   
def testUrl(url):
   try:
      response = urlopen(url)
      results = response.read()
   except URLError, e:
      log.error('Fail: %s' % url)
      if hasattr(e, 'reason'):
         log.error('   Failed to reach a server; reason: %s' % e.reason)
      elif hasattr(e, 'code'):
         log.error('   Server unable to fulfill request; code: %s' % e.code)
   else:
      info = response.info()
      if info.maintype == 'image':
         log.info('Pass!  Content: %s; url: %s' % (info.type, url))
      else:
         log.info('Fail??  Content: %s; url: %s' % (info.type, url))

# ...............................................   
occcount = 0
prjcount = 0
earl = EarlJr()
log = ThreadLogger('testCTMaps')
scribe = Scribe(log)
scribe.openConnections()

occsets = scribe.getOccurrenceSetsForUser(CT_USER)

for occ in occsets:
   occurl = createTestMap(occ)
   testUrl(occurl)
   occcount += 1
   projs = scribe.getProjectionsForOccurrenceSet(occ)
   for prj in projs:
      prjurl = createTestMap(prj)
      testUrl(prjurl)
      prjcount += 1

scribe.closeConnections()
