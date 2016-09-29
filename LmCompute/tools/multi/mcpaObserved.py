"""
@summary: This script calculates MCPA values for observed data
"""
import argparse
import numpy as np

# Jeff: Fix this import to import the correct functions
from MCPA import calculateMCPA, appendENVtoBG

# .............................................................................
if __name__ == "__main__":
   parser = argparse.ArgumentParser(description="This script calculates MCPA for observed data")
   # Inputs
   parser.add_argument("incidenceMtxFn", help="Incidence matrix for the analysis (PAM) file name")
   parser.add_argument("phyloEncodingFn", help="Encoded phylogenetic matrix file location")
   parser.add_argument("envFn", help="Environment predictor matrix file location")
   parser.add_argument('-b', dest='bio', help="Biogeographic predictor matrix file location")
   # Outputs
   parser.add_argument("partCorMtxFn", help="Where the partial correlation matrix should be stored")
   parser.add_argument("r2vector", help="The file path to store the r squared vector")
   parser.add_argument("fPartialMtxFn", help="The file path for the f matrix for partial correlations")
   parser.add_argument("fGlobalMtxFn", help="The file path for the f global matrix")
   
   args = parser.parse_args()
   
   i = np.load(args.incidenceMtxFn)
   p = np.load(args.phyloEncodingFn)
   e = np.load(args.envFn)
   predictor = e
   if args.bio is not None:
      b = np.load(args.bio)
      # Jeff: Change as needed
      predictor = appendENVtoBG(b, e)

   # Jeff: Correct as needed
   partCorMtx, rSqVect, fPartial, fGlobal = calculateMCPA(i, p, predictor)

   # Write outputs
   np.save(args.partCorMtxFn, partCorMtx)
   np.save(args.r2vector, rSqVect)
   np.save(args.fPartialMtxFn, fPartial)
   np.save(args.fGlobalMtxFn, fGlobal)
