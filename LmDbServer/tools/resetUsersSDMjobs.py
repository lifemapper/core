"""
@summary: This script will reset all of the SDM models and projections for a 
             user
@author: CJ Grady
@version: 1.0
"""
import argparse

from LmServer.common.log import ConsoleLogger
from LmServer.db.scribe import Scribe

MAX_NUMBER = 1000 # Only reset this many as a max, add paging later if wanted
PRIORITY = 10 # New job priority

# .............................................................................
if __name__ == "__main__":
   parser = argparse.ArgumentParser(
                description="Script to reset all of a user's SDM models and projections")
   parser.add_argument("userId", metavar="UserId", type=str, nargs="+",
                        help="These are the users to reset jobs for")

   args = parser.parse_args()

   userIds = args.userId

   # Build a scribe object
   scribe = Scribe(ConsoleLogger())

   scribe.openConnections()

   for userId in userIds:

      print "Resetting jobs for:", userId

      print "   Resetting models..."
      # Only does the first 1000, add paging later if wanted
      mdlAtoms = scribe.listModels(0, MAX_NUMBER, userId=userId)

      for mdlAtom in mdlAtoms:
         scribe.reinitSDMModel(scribe.getModel(mdlAtom.id), PRIORITY)

      print "   Resetting projections..."

      prjAtoms = scribe.listProjections(0, MAX_NUMBER, userId=userId)

      for prjAtom in prjAtoms:
         scribe.reinitSDMProjection(scribe.getProjectionById(prjAtom.id), PRIORITY)

   scribe.closeConnections()

