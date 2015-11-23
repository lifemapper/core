"""
@summary: This script will fill in a projection job request using the ruleset 
             from a model
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
import sys

REPLACE_STRING = "REPLACE-THIS"
# .............................................................................
if __name__ == "__main__":
   partPrjFn = sys.argv[1]
   rulesetFn = sys.argv[2]
   outPrjFn = sys.argv[3]
   # Get partial projection request filename
   # Get model ruleset file
   # Open both
   # Replace val in request
   # Assumes maxent for now
   with open(partPrjFn) as partPrjF:
      partPrj = partPrjF.read()

   with open(rulesetFn) as rulesetF:
      ruleset = rulesetF.read()
      
   with open(outPrjFn, 'w') as outF:
      outF.write(partPrj.replace(REPLACE_STRING, ruleset))
      