"""
@summary: This script aggregates and normalizes f values output from MCPA 
             randomizations

"""
import argparse
import numpy as np

# Jeff: Fix this import to import the correct functions
from Pedro_Analysis.MCPA.MCPA import sumProbabilities

# ............................................................................
if __name__ == "__main__":
   desc="""\
This script aggregates and normalizes f values output from MCPA \
randomizations.  It can aggregate either probability semi-partial \
correlations or R squared probablities, but only one at a time.  \
Normalization is an optional step and divides the sum by the provided value.\
"""

   parser = argparse.ArgumentParser(description=desc)
   # Inputs
   parser.add_argument('-d', help="Divide the sum by this number (optional)")
   parser.add_argument('fVal', nargs='+', help="Numpy array for an f value")
   # Outputs
   parser.add_argument('-o', dest='outFn', help="Write the aggregated output to this file")

   args = parser.parse_args()

   if args.d is not None:
      d = float(args.d)
   else:
      d = None
      
   mtxs = []
   for fn in args.fVal:
      mtxs.append(np.load(fn))
   
   # Jeff: Change name as needed
   newVal = sumProbabilities(mtxs, divisor=d)

   np.save(args.outFn, newVal)

