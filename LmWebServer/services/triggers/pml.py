"""
@summary: Module containing class to interact with PML from UTEP
@author: CJ Grady
@version: 1.0
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
from common.formatters.pmlFormatter import PmlFormatter
from common.notifications.pml import PMLClient

# .............................................................................
class PML(object):
   """
   @summary: Sends pml documents to UTEP
   """
   # ...................................
   def __init__(self):
      """
      @summary: Constructor
      """
      self.cl = PMLClient()
   
   # ...................................
   def postSDMExperiment(self, exp):
      """
      @summary: Sends a postExperiment metadata document to UTEP
      @param exp: An SDM Experiment object
      """
      project = "%sSdmExps" % exp.user
      resource = "sdmExp%s" % exp.id
      data = str(PmlFormatter(exp).format())
      resp = self.cl.pushPML(project, resource, data)
      return resp
   
   # ...................................
   def postSDMLayer(self, lyr):
      """
      @summary: Sends a postLayer metadata document to UTEP
      @param lyr: An SDM climate scenario layer
      """
      project = "%sSdmLayers" % lyr.user
      resource = "sdmLyr%s" % lyr.id
      data = str(PmlFormatter(lyr).format())
      resp = self.cl.pushPML(project, resource, data)
      return resp
   
   # ...................................
   def postSDMOccurrenceSet(self, occ):
      """
      @summary: Sends a postOccurrenceSet metadata document to UTEP
      @param occ: An SDM occurrence set
      """
      project = "%sSdmOccs" % occ.user
      resource = "sdmOcc%s" % occ.id
      data = str(PmlFormatter(occ).format())
      resp = self.cl.pushPML(project, resource, data)
      return resp

   # ...................................
   def postSDMScenario(self, scn):
      """
      @summary: Sends a postScenario metadata document to UTEP
      @param scn: An SDM climate scenario
      """
      project = "%sSdmScns" % scn.user
      resource = "sdmScn%s" % scn.id
      data = str(PmlFormatter(scn).format())
      resp = self.cl.pushPML(project, resource, data)
      return resp
  
   