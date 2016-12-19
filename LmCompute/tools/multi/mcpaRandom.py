"""
@summary: This script performs MCPA randomizations
"""
import argparse
import numpy as np

# Jeff: Fix this import to import the correct functions
from Pedro_Analysis.MCPA.MCPA import calculateMCPA, appendENVtoBG

# .............................................................................
if __name__ == "__main__":
   parser = argparse.ArgumentParser(description="This script performs MCPA randomizations")
   # Inputs
   parser.add_argument("fPartialMtxFn", help="The file path for the f matrix for partial correlations")
   parser.add_argument("fGlobalMtxFn", help="The file path for the f global matrix")
   parser.add_argument("incidenceMtxFn", help="Incidence matrix for the analysis (PAM) file name")
   parser.add_argument("phyloEncodingFn", help="Encoded phylogenetic matrix file location")
   parser.add_argument("envFn", help="Environment predictor matrix file location")
   parser.add_argument('-b', dest='bio', help="Biogeographic predictor matrix file location")
   parser.add_argument('-n', help="The number of randomizations to perform")
   parser.add_argument('-c', help="The number of concurrent randomizations to perform (number of available cores or less)")
   parser.add_argument('-d', help="Divide the sums by this")
   # Outputs
   parser.add_argument("probSemiPartialCorFn", help="Where to store the aggregated semi-partial probabilities produced from the randomizations")
   parser.add_argument("rSqProbFn", help="Where to store the aggregated R squared probabilities")
   
   args = parser.parse_args()
   
   i = np.load(args.incidenceMtxFn)
   p = np.load(args.phyloEncodingFn)
   e = np.load(args.envFn)
   predictor = e
   if args.bio is not None:
      b = np.load(args.bio)
      # Jeff: Change as needed
      predictor = appendENVtoBG(b,e)


   fPartial = np.load(args.fPartialMtxFn)
   fGlobal = np.load(args.fGlobalMtxFn)

   if args.n is None:
      n = 1
   else:
      n = int(args.n)
      
   if args.c is None:
      c = 1
   else:
      c = int(args.c)
      
   if args.d is None:
      d = None
   else:
      d = float(args.d)

   # Jeff: Correct this as needed
   # needs to sum n outputs  to 2 npy in module and mcpaRandomize calls fn()(list of numpy arr.) 
   probSemiPartialCor, rSqProb = calculateMCPA(i, p, predictor, FGlobal=fGlobal, FSemiPartial=fPartial, numPermute=n, numConcurrent=c, divisor=d)
   
   # Write outputs to file system
   np.save(args.probSemiPartialCorFn, probSemiPartialCor)


   np.save(args.rSqProbFn, rSqProb)
