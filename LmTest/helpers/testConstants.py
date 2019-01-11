"""
@summary: This module contains test constants for the LmCommon package
@author: CJ Grady
@version: 1.0
@status: alpha

@license: gpl2
@copyright: Copyright (C) 2019, University of Kansas Center for Research

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
import os

# TODO: Find a better mechanism for this.
#         1. This is fragile.  Any path change will break it
#         2. This is not flexible.  It would be nice if we could specify a
#               testing data directory

TEST_DATA_PATH = os.path.join(os.path.dirname(__file__), '..', 'data')

# Input data
BIO_GEO_HYPOTHESES_PATH = os.path.join(TEST_DATA_PATH, 'bioGeoHypotheses')
SHAPEGRIDS_PATH = os.path.join(TEST_DATA_PATH, 'shapegrids')
TREES_PATH = os.path.join(TEST_DATA_PATH, 'trees')

# Test outputs
OUTPUT_BIO_GEO_ENCODINGS_PATH = os.path.join(TEST_DATA_PATH, 'outputs', 
                                        'bioGeoEncodings')
OUTPUT_PHYLO_ENCODINGS_PATH = os.path.join(TEST_DATA_PATH, 'outputs', 
                                      'phyloEncodings')
