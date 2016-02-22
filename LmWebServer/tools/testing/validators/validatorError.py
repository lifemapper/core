"""
@summary: This module contains an exception class used by the validators
"""
# .............................................................................
class InvalidResponseException(Exception):
   """
   @summary: This exception indicates that the response does not match what the 
                schema expects
   """
   # ...............................
   def __init__(self, msg):
      Exception.__init__(self)
      self.msg = msg
   
   # ...............................
   def __repr__(self):
      return "%s %s" % (self.__class__, unicode(self))
      
   # ...............................
   def __str__(self):
      return unicode(self)
   
   # ...............................
   def __unicode__(self):
      return self.msg   

