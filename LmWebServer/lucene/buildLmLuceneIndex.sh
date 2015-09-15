#!/bin/bash

# @author: CJ Grady
# @lastModified: 2014/03/6 16:19
# @summary: This script will rebuild the Lifemapper species lucene index

# Set environment in case it is not already set
SHELL=/usr/local/bin/bash
PATH=/bin:/usr/bin:/usr/local/bin

# Nadya: this should read the configured APP_DIR
# Have to export PYTHONPATH or the modules will not be found
export PYTHONPATH=/share/apps/lm2

# Build the index
# Note that the output of this could be piped to a file to check for success
#   or you can check the last modified time of the files in: 
#   /var/lib/lm2/luceneIndex/
python /share/apps/lm2/LmWebServer/lucene/lmLucene.py build