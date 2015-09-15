"""
@summary: Module containing statistics formatter and helper functions
@author: CJ Grady
@version: 1.0
@status: alpha
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
import numpy
from types import ListType

from LmWebServer.formatters.formatter import Formatter, FormatterResponse

# .............................................................................
class StatisticsFormatter(Formatter):
   """
   @summary: Formatter class for statistics output
   """
   # ..................................
   def format(self):
      """
      @summary: Formats the object
      @return: A response containing the content and metadata of the format 
                  operation
      @rtype: FormatterResponse
      """
      if self.parameters.has_key("statistic"):
         stat = self.parameters["statistic"].lower()
      else:
         stat = "keys"
         
      psSum = self.obj.getSum()
      
      
      # Add tree stats
      
      
         
      stats = {
         "speciesrichness" : {
            "statType" : "site",
            "value" : psSum["sites"]["speciesRichness-perSite"]
         },
         "meanproportionalrangesize" : {
            "statType" : "site",
            "value" : psSum["sites"]["MeanProportionalRangeSize"]
         },
         "proportionalspeciesdiversity" : {
            "statType" : "site",
            "value" : psSum["sites"]['ProportionalSpeciesDiversity']
         },
         "localityrangesize" : {
            "statType" : "site",
            "value" : psSum['sites']['Per-siteRangeSizeofaLocality']
         },
         "speciesrangesize" : {
            "statType" : "species",
            "value" : psSum["species"]['RangeSize-perSpecies']
         },
         "meanproportionalspeciesdiversity" : {
            "statType" : "species", 
            "value" : psSum["species"]['MeanProportionalSpeciesDiversity']
         },
         "proportionalrangesize" : {
            "statType" : "species", 
            "value" : psSum["species"]['ProportionalRangeSize']
         },
         "rangerichness" : {
            "statType" : "species", 
            "value" : psSum["species"]['Range-richnessofaSpecies']
         },
         "whittakersbeta" : {
            "statType" : "diversity", 
            "value" : psSum["diversity"]['WhittakersBeta']
         },
         "ladditivebeta" : {
            "statType" : "diversity", 
            "value" : psSum["diversity"]['LAdditiveBeta']
         },
         "legendrebeta" : {
            "statType" : "diversity", 
            "value" : psSum["diversity"]['LegendreBeta']
         },
         "sigmaspecies" : {
            "statType" : "species", 
            "value" : psSum["matrices"]['SigmaSpecies']
         },
         "sigmasites" : {
            "statType" : "site", 
            "value" : psSum["matrices"]['SigmaSites']
         },
         "compositioncovariance" : {
            "statType" : "site", 
            "value" : psSum["Schluter"]['Sites-CompositionCovariance']
         },
         "rangecovariance" : {
            "statType" : "species", 
            "value" : psSum["Schluter"]['Species-RangesCovariance']
         }
      }

      # Tree stats
      try:
         stats["mntd"] = {
                  "statType" : "site",
                  "value" : psSum['sites']['MNTD']
         }
         stats["pearsonsoftdandsitesshared"] = {
                  "statType" : "site",
                  "value" : psSum['sites']['PearsonsOfTDandSitesShared']
         }
         stats["averagetaxondistance"] = {
                  "statType" : "site",
                  "value" : psSum['sites']['AverageTaxonDistance']
         }
      except: # Old experiment
         pass


      if stat == "keys" :
         ret = [k for k in stats.keys() if stats[k]["value"] is not None]
         resp = ' '.join(ret)
      elif stat == "specieskeys" :
         ret = [k for k in stats.keys() if stats[k]["statType"] == "species" \
                   and stats[k]["value"] is not None]
         resp = ' '.join(ret)
      elif stat == "siteskeys" : 
         ret = [k for k in stats.keys() if stats[k]["statType"] == "site" \
                   and stats[k]["value"] is not None]
         resp = ' '.join(ret)
      elif stat == "diversitykeys" : 
         ret = [k for k in stats.keys() if stats[k]["statType"] == "diversity" \
                   and stats[k]["value"] is not None]
         resp = ' '.join(ret)
      else:
         resp = ""
         ret = stats[stat]["value"]
         if isinstance(ret, numpy.ndarray):
            ret = ret.tolist()
         
         if isinstance(ret, ListType):
            for i in ret:
               if isinstance(i, numpy.ndarray):
                  i = i.tolist()
               
               if isinstance(i, ListType):
                  resp = resp + "\n%s" % ' '.join(i)
               else:
                  resp = resp + "%s " % str(i)
         else:
            resp = resp + "%s " % str(ret)
            
      ct = "text/plain"
      fn = "%s.txt" % stat
      
      return FormatterResponse(resp, contentType=ct, filename=fn)
