"""
@summary: This script corrects p-values for summed and divided f values
"""

import argparse
import numpy as np

# Jeff fix import
from Pedro_Analysis.MCPA.MCPA import correctPValue

# ............................................................................
if __name__ == "__main__":
   desc = """\
This script takes a summed f value as an argument and does a p-value \
correction."""

   parser = argparse.ArgumentParser(description=desc)

   # Inputs
   parser.add_argument('PValues', help="P-value matrix")

   # Outputs
   parser.add_argument('correctedP', help="The output location for the corrected p-value")

   args = parser.parse_args()
   
   arr = np.load(args.PValues)
   
   # Jeff: Fix as necessary
   corrected = correctPValue(arr)
   
   np.save(args.correctedP, corrected)
   
