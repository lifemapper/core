#!/bin/bash

# @author: CJ Grady
# @lastModified: 2015/09/25 09:49
# @summary: This script will rebuild the Lifemapper Archive Solr index

# Set environment in case it is not already set
SHELL=/usr/local/bin/bash
PATH=/bin:/usr/bin:/usr/local/bin

# Nadya: this should read the configured APP_DIR
# Have to export PYTHONPATH or the modules will not be found
export PYTHONPATH=/opt/lifemapper/

# Build the index
# Note that the output of this could be piped to a file to check for success
#   or you can check the last modified time of the files in: 
#   /var/solr/data/cores/lmArchive
python /opt/lifemapper/LmWebServer/solr/buildIndex.py
