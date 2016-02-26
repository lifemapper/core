"""
@summary: This module contains the LmEnv base class used to retrieve string
             replacements from the environment.  This class just defines what 
             the interface should look like.
"""
# .............................................................................
class LmEnv(object):
   """
   @summary: The class should be used to get string replacement values for 
                Lifemapper URLs
   """
   # .......................
   def __init__(self):
      pass
   
   # .......................
   def getReplacementValue(self, valKey):
      """
      @summary: Get the replacement value for the key
      @param valKey: The key to get the value of
      """
      pass
   
   