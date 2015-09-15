"""
@summary Module that contains the _Experiment class
@author Aimee Stewart
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
from LmServer.base.experiment import _Experiment
from LmServer.base.lmobj import LMError
from LmServer.common.lmconstants import LMServiceType, LMServiceModule
from LmServer.sdm.sdmmodel import SDMModel
from LmServer.sdm.sdmprojection import SDMProjection

# .............................................................................
class SDMExperiment(_Experiment):
   """
   The Experiment class contains all of the information 
   for a single model and all the projections from that model.
   """
# .............................................................................
# Constructor
# .............................................................................
   def __init__(self, model, projections):
      """
      @summary Constructor for the Experiment class
      @param model: The model which binds all these objects
      @param projections: The list of projections created from the model
      """
      self.model = model
      self.projections = projections
      _Experiment.__init__(self, model.getId(), model.getUserId(), 
                           model.epsgcode, model.createTime, model.modTime,
                           description=model.description, 
                           serviceType=LMServiceType.SDM_EXPERIMENTS, 
                           moduleType=LMServiceModule.SDM)
            
# .............................................................................
# Public methods
# .............................................................................
   def getId(self):
      """
      @summary Returns the database id from the object table
      @return integer database id of the object
      @note: Overrides ServiceObject.getId
      """
      return self.model.getId()
   
   def setId(self, id):
      """
      @summary: Sets the modelId on the model, all its projections, and the 
                experiment.
      @param id: The database id for the model
      @note: Overrides ServiceObject.setId
      """
      self.model.setId(id)
      for proj in self.projections:
         proj.getModel().setId(id)
      self._experimentPath = self.model.getAbsolutePath()
      _Experiment.setId(self, id)

   
# ...............................................
   def getUserId(self):
      """
      @summary Gets the User id
      @return The User id
      @note: Overrides ServiceObject.getUserId
      """
      return self.model.getUserId()

   def setUserId(self, id):
      """
      @summary: Sets the user id on the object
      @param id: The user id for the object
      @note: Overrides ServiceObject.setUserId
      """
      raise LMError('setUserId not allowed on Experiment, set on Model')
   
# ...............................................
   def getAbsolutePath(self):
      """
      @summary Gets the absolute path to the species data
      @return Path to species points
      """
      return self.model.occurrenceSet.getAbsolutePath()

# ...............................................
   def _getAlgorithm(self):
      """
      @summary Gets the User id
      @return The User id
      @note: Overrides ServiceObject.getUserId
      """
      return self.model.getAlgorithm()

# ...............................................
   def rollback(self, currtime):
      """
      @summary: Rollback processing
      @param currtime: Time of status/stage modfication

      """
      self.model.rollback(currtime)
      for prj in self.projections:
         prj.rollback()

# .............................................................................
# Properties
# .............................................................................
   algorithm = property(_getAlgorithm)

# ...............................................
   @staticmethod
   def initFromParts(occset, mdlScen, prjScens, alg, userId, initStatus, 
                     priority, mdlMask=None, prjMask=None, email=None):
      model = SDMModel(priority, occset, mdlScen, alg, mask=mdlMask, 
                       userId=userId, status=initStatus, email=email)
      projs = []
      
      #TODO: CJG - 03/21/2013 - talk to Aimee about this and at least switch to constants
#       if alg.code == 'ATT_MAXENT':
#          prjDataFormat = 'AAIGrid'
#       else:
#          prjDataFormat = 'GTiff'
      
      for pscen in prjScens:
         prj = SDMProjection(model, pscen, mask=prjMask, status=initStatus, 
                             userId=userId)
         projs.append(prj)
         
      exp = SDMExperiment(model, projs)
      return exp
         
# .............................................................................
# Private methods
# .............................................................................

   def _getBBox(self):
      """
      @summary: Returns the largest area that can be projected from this model.
      @return:  bounding box object
      """
      return self.model.bbox
   
   def _getStatusModTime(self):
      """
      @summary Gets the latest modification time of the model or any of the 
               projections
      @return the latest status modification time
      """
      latestModTime = self.model.statusModTime
      for p in self.projections:
         if p.statusModTime > latestModTime:
            latestModTime = p.statusModTime
      return latestModTime

# .............................................................................
# Read-0nly Properties
# .............................................................................
   bbox = property(_getBBox)
   statusModTime = property(_getStatusModTime)
