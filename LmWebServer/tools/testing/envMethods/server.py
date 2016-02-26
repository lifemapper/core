"""
@summary: This module contains a class that relies on LmServer code and the 
             scribe / peruser to fill in values for URLs
"""
from LmCommon.common.lmconstants import JobStatus
from LmServer.common.localconstants import ARCHIVE_USER, WEBSERVICES_ROOT
from LmServer.common.log import LmServerLogger
from LmServer.db.scribe import Scribe
from LmWebServer.tools.testing.envMethods.base import LmEnv

# .............................................................................
class LmServerEnv(LmEnv):
   """
   @summary: LmEnv subclass that relies on the Lifemapper core code and the 
                scribe to determine what values to replace strings with
   """
   # .......................
   def __init__(self, server=WEBSERVICES_ROOT, userId=None):
      self.log = LmServerLogger("serverEnv")
      self.scribe = Scribe(self.log)
      self.scribe.openConnections()
      self.values = {
         "#SERVER#" : server,
      }
      self._getSdmValues()
      if userId is not None:
         self._getSdmValues(userId=userId)
         self._getRadValues(userId)
   
   # .......................
   def getReplacementValue(self, valKey):
      """
      @summary: Get the replacement value for the key
      @param valKey: The key to get the value of
      """
      return self.values[valKey]
   
   # .......................
   def _getSdmValues(self, userId=ARCHIVE_USER):
      if userId == ARCHIVE_USER:
         prefix = "PUB"
         forUser = "public"
      else:
         prefix = "USR"
         forUser = "user"
         
      try:
         self.values['#%s_SDM_EXP_ID#' % prefix] = str(
               self.scribe.listModels(0, 1, userId=userId, 
                                              status=JobStatus.COMPLETE)[0].id)
      except Exception, e:
         self.log.debug(
                    "Could not populate %s SDM experiment id: %s" % (forUser, 
                                                                     str(e)))

      try:
         self.values['#%s_SDM_LYR_ID#' % prefix] = str(
               self.scribe.listLayers(0, 1, userId=userId)[0].id)
      except Exception, e:
         self.log.debug("Could not populate %s SDM layer id: %s" % (forUser, 
                                                                    str(e)))

      try:
         self.values['#%s_SDM_OCC_ID#' % prefix] = str(
               self.scribe.listOccurrenceSets(0, 1, minOccurrenceCount=30, 
                                              userId=userId)[0].id)
      except Exception, e:
         self.log.debug("Could not populate %s SDM occurrence set id: %s" % (
                                                              forUser, str(e)))

      try:
         self.values['#%s_SDM_PRJ_ID#' % prefix] = str(
               self.scribe.listProjections(0, 1, userId=userId, 
                                           status=JobStatus.COMPLETE)[0].id)
      except Exception, e:
         self.log.debug("Could not populate %s SDM projection id: %s" % (
                                                              forUser, str(e)))
      try:
         self.values['#%s_SDM_SCN_ID#' % prefix] = str(
               self.scribe.listScenarios(0, 1, userId=userId)[0].id)
      except Exception, e:
         self.log.debug("Could not populate %s SDM scenario id: %s" % (forUser, 
                                                                       str(e)))
      try:
         self.values['#%s_SDM_TC_ID#' % prefix] = str(
               self.scribe.listLayerTypeCodes(0, 1, userId=userId)[0].id)
      except Exception, e:
         self.log.debug("Could not populate %s SDM type code id: %s" % (
                                                              forUser, str(e)))
         
   # .......................
   def _getRadValues(self, userId):
      # Get full buckets
      buckets = self.scribe.listRADBuckets(0, 10, userId, atom=False)
      
      expId = bktId = psId = None
      
      # Loop through buckets until we find one with a completed pam sum
      for bkt in buckets:
         expId = str(bkt.experimentId)
         bktId = str(bkt.getId())
         # If it has a pam sum (that is completed)
         if bkt.pamSum is not None and bkt.pamSum.status == JobStatus.COMPLETE:
            psId = str(bkt.pamSum.getId())
            break
      
      self.values['#USR_RAD_EXP_ID#'] = expId
      self.values['#USR_RAD_BKT_ID#'] = bktId
      self.values['#USR_RAD_PS_ID#'] = psId
