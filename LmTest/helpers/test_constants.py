"""This module contains test constants for the LmCommon package
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
OUTPUT_BIO_GEO_ENCODINGS_PATH = os.path.join(
    TEST_DATA_PATH, 'outputs', 'bioGeoEncodings')
OUTPUT_PHYLO_ENCODINGS_PATH = os.path.join(
    TEST_DATA_PATH, 'outputs', 'phyloEncodings')
