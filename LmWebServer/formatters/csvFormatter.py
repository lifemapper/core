"""
@summary: Module containing CSV Formatter class and helping functions
@author: CJ Grady
@version: 1.0
@status: beta
@note: Part of the Factory pattern
@see: Formatter
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
import csv
from StringIO import StringIO

from LmCommon.common.localconstants import ENCODING
from LmServer.base.lmobj import LMError
from LmServer.rad.radbucket import RADBucket
from LmServer.rad.pamvim import PamSum
from LmServer.sdm.occlayer import OccurrenceLayer

from LmWebServer.formatters.formatter import Formatter, FormatterResponse

# .............................................................................
class CsvFormatter(Formatter):
   """
   @summary: Formatter class for CSV output
   """
   # ..................................
   def format(self):
      """
      @summary: Formats the object
      @return: A response containing the content and metadata of the format 
                  operation
      @rtype: FormatterResponse
      """
      if isinstance(self.obj, OccurrenceLayer):
         ret = _getCSVForFeatures(self.obj.features).getvalue()
      elif isinstance(self.obj, RADBucket):
         pam = self.obj.getFullPAM()
         pam.readData()
         
         params = dict([(k.lower(), self.parameters[k]) for k in self.parameters.keys()])
         if params.has_key('addheaders') and bool(str(params['addheaders'])):
            headers = True
         else:
            headers = False

         csvStringIO = StringIO()
         
         if headers:
            columnHeaders = ['siteid', 'centerX', 'centerY']
            keys = self.obj.getLayersPresent().keys()
            keys.sort()
            
            for k in keys:
               columnHeaders.append("sp_%s" % k)
         
            csvStringIO.write(', '.join(columnHeaders))
            csvStringIO.write('\n')


         if headers:
            sg = self.obj.shapegrid
            sg.readData()
            allFeats = sg.features
            
            def compareBySiteId(a, b):
               if a.siteid < b.siteid:
                  return -1
               elif a.siteid > b.siteid:
                  return 1
               else: # equal
                  return 0
            
            allFeats.sort(cmp=compareBySiteId)
            
            centroids = []
            for feat in allFeats:
               centroids.append((feat.siteid, feat.centerX, feat.centerY))
         
         cI = 0

         for row in pam.data:
            if headers:
               csvStringIO.write('%s, %s, %s,' % (centroids[cI]))
               cI += 1
            csvStringIO.write(', '.join([str(int(col)) for col in row]))
            csvStringIO.write('\n')
         
         csvStringIO.seek(0)
         ret = csvStringIO.getvalue()
         
         
      elif isinstance(self.obj, PamSum):
         from LmServer.common.log import LmPublicLogger
         from LmServer.db.peruser import Peruser
         peruser = Peruser(LmPublicLogger())
         peruser.openConnections()
         exp = peruser.getRADExperimentWithOneBucket(self.obj.user, 
                        self.obj._bucketId, fillIndices=True, fillLayers=False, 
                        fillRandoms=False)
         bucket = exp.bucketList[0]
         
         if int(self.obj.randomMethod) in [0, 1]:
            self.obj.readPAM(pamFilename=self.obj.pamDLocation)
            pam = self.obj.getPam()
         elif int(self.obj.randomMethod) == 2:
            self.obj.readSplotchPAM()
            pam = self.obj.getSplotchPAM()
         #else:
            
         peruser.closeConnections()
         
         params = dict([(k.lower(), self.parameters[k]) for k in self.parameters.keys()])
         if params.has_key('addheaders') and bool(str(params['addheaders'])):
            headers = True
         else:
            headers = False
         
         layerIndices = exp.orgLayerset.getLayerIndices()
         layersPresent = bucket.getLayersPresent()
         sitesPresent = bucket.getSitesPresent()
         
         columnHeaders = ['siteId', 'x', 'y']
         keys = layerIndices.keys()
         keys.sort()
         
         for k in keys:
            if layersPresent[k]:
               # These should be matrix ids
               columnHeaders.append("sp_%s" % k)

         csvStringIO = StringIO()
         if headers:
            csvStringIO.write(', '.join(columnHeaders))
            csvStringIO.write('\n')

         sg = bucket.shapegrid
         sg.readData()
         allFeats = sg.features
         #allFeats = sg.getFeatures()
         def compareBySiteId(a, b):
            if a.siteid < b.siteid:
               return -1
            elif a.siteid > b.siteid:
               return 1
            else: # equal
               return 0
            
         allFeats.sort(cmp=compareBySiteId)
         
         centroids = []
         for feat in allFeats:
            if sitesPresent[feat.siteid]:
               centroids.append((feat.siteid, feat.centerX, feat.centerY))
         
         cI = 0
         
         for row in pam.data:
            if headers:
               csvStringIO.write('%s, %s, %s,' % (centroids[cI]))
               cI += 1
                                 
            csvStringIO.write(', '.join([str(int(col)) for col in row]))
            csvStringIO.write('\n')
         
         csvStringIO.seek(0)
         ret = csvStringIO.getvalue()
      else:
         raise LMError("Not sure how to format %s to CSV" % str(self.obj.__class__))
      try:
         name = self.obj.serviceType[:-1]
      except:
         name = "items"
      
      ct = "text/csv"
      fn = "%s%s.csv" % (name, self.obj.getId())
      headers = {"Content-Disposition" : 'attachment; filename="%s"' % fn}
      
      return FormatterResponse(ret, contentType=ct, filename=fn, 
                                                         otherHeaders=headers)

# .............................................................................
def _displayValueOrDefault(value, default=""):
   if value is None:
      return default
   else:
      try:
         return unicode(str(value), ENCODING).encode(ENCODING)
      except:
         try:
            return str(value)
         except:
            try:
               return value.encode(ENCODING)
            except:
               return default

# .............................................................................
def _getCSVForFeatures(features):
   """
   @summary: Creates a CSV stream from a list of feature dictionaries
   """
   outStream = StringIO()
   writer = csv.writer(outStream, dialect='excel')
   keys = sorted(features[0].getAttributes().keys())
   writer.writerow(keys)
   for feat in features:
      atts = feat.getAttributes()
      writer.writerow([_displayValueOrDefault(atts[k]) for k in keys])
   return outStream
