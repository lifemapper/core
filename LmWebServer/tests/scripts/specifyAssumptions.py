"""
@summary: Tests that the assumptions made by the Specify client are valid
@author: CJ Grady

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
import json
import urllib2

from LmServer.common.localconstants import WEBSERVICES_ROOT
from LmServer.common.log import ConsoleLogger
from LmServer.db.scribe import Scribe
from LmServer.notifications.email import EmailNotifier

# ............................................................................. 
def testSearch():
   """
   @summary: Tests that the results returned from a search URL can be processed
                by Specify
   
   @note: Expects a tag 'columns' to be a list of lists and each item contained 
             within should have 'name', 'numPoints', and 'occurrenceSet'
   """
   url = '%s/hint/species/ursus?format=json&columns=1&maxreturned=1000' % WEBSERVICES_ROOT
   try:
      j = json.load(urllib2.urlopen(url))
      name = j['columns'][0][0]['name']
      numPoints =  j['columns'][0][0]['numPoints']
      occId = j['columns'][0][0]['occurrenceSet']
      return None
   except Exception, e:
      return 'Search failed: %s.  URL: %s' % (str(e), url)

# ............................................................................. 
def testOccurrenceSet():
   """
   @summary: Tests that an occurrence set response contains parameters that
                Specify expects
   @note: Expects 'feature' to be a list and each item should have 'lat' and 
             'lon'
   """
   try:
      url = None
      peruser = Scribe(ConsoleLogger())
      peruser.openConnections()
      occId = peruser.listOccurrenceSets(0, 1, minOccurrenceCount=10)[0].id
      
      url = "%s/services/sdm/occurrences/%s/json?format=specify&fillPoints=true" % (WEBSERVICES_ROOT, occId)
      
      #occ = peruser.getOccurrenceSet(occId)
      peruser.closeConnections()
      #url = '%s/json' % occ.metadataUrl
      j = json.load(urllib2.urlopen(url))
      features = j['feature']
      failures = 0
      for feat in features:
         try:
            lat = feat['lat']
            lon = feat['lon']
         except:
            failures = failures + 1
      if failures >= j['queryCount']:
         return "Too many points failed.  Url: %s" % url
      else:
         return None
   except Exception, e:
      return 'Occurrence set failed: %s, URL: %s' % (str(e), url)

# .............................................................................
if __name__ == '__main__':
   failures = []
   searchResult = testSearch()
   if searchResult is not None:
      failures.append(searchResult)
   occResult = testOccurrenceSet()
   if occResult is not None:
      failures.append(occResult)
   if len(failures) > 0:
      notifier = EmailNotifier()
      notifier.sendMessage(['cjgrady@ku.edu'], "Specify assumptions failing", '\n'.join(failures))
   
