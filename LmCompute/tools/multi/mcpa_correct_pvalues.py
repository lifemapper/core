#!/bin/bash
"""
@summary: This script calculates corrected P-values for F-Global or 
             F-Semi-partial
@author: CJ Grady
@version: 4.0.0
@status: beta
@license: gpl2
@copyright: Copyright (C) 2018, University of Kansas Center for Research

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
import argparse

from LmCommon.common.matrix import Matrix
from LmCommon.statistics.pValueCorrection import correctPValues
from LmCompute.plugins.multi.mcpa.mcpa import get_p_values

# .............................................................................
if __name__ == "__main__":
    parser = argparse.ArgumentParser(
                   description="This script calculates and corrects P-Values")

    parser.add_argument('observed_filename',
                        help='File location of observed values to test')
    parser.add_argument('p_values_filename',
                        help='File location to store the P-Values')
    parser.add_argument('bh_values_filename', 
            help='File location to store the Benjamini-Hochberg output matrix')
    parser.add_argument('f_value_filename', nargs='+', 
                        help='A file of F-values or a stack of F-Values')
   
    args = parser.parse_args()
   
    # Load the matrices
    test_values = []
    num_values = 0
   
    for f_val in args.f_value_filename:
        test_mtx = Matrix.load(f_val)
      
        # Add the values to the test values list
        test_values.append(test_mtx)
      
        # Add to the number of values
        if test_mtx.data.ndim == 3: # Stack of values
            num_values += test_mtx.data.shape[2]
        else:
            num_values += 1
   
    obs_vals = Matrix.load(args.observed_filename)
    p_values = get_p_values(obs_vals, test_values,
                            num_permutations=num_values)
   
    bh_values = correctPValues(p_values)
   
    with open(args.p_values_filename, 'w') as p_val_f:
        p_values.save(p_val_f)
      
    with open(args.bh_values_filename, 'w') as bh_val_f:
        bh_values.save(bh_val_f)

   
