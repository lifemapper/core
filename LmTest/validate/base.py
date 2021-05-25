"""This module contains an exception class used by the validators
"""


# .............................................................................
class InvalidResponseException(Exception):
    """This exception indicates that the file did not validate
    """

    # ...............................
    def __init__(self, msg):
        Exception.__init__(self)
        self.msg = msg

    # ...............................
    def __repr__(self):
        return "%s %s" % (self.__class__, str(self))

    # ...............................
    def __str__(self):
        return str(self)

    # ...............................
    def __unicode__(self):
        return self.msg
