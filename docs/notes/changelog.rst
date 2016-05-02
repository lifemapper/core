############################################################
Changes made to current version that need to be incorporated
############################################################

Java Environment Variable
#########################
* The default memory options for Java are inadequate to actually start java, add
    the following environment variable
    
    JAVA_TOOL_OPTIONS=-Xmx512m
    

Cron job changes
################

* Remove the buildLuceneIndex cron job
* Add arguments to build solr index call.  Add -t 4 to line 18 in buildLmSolrIndex

Modifications to **/opt/lifemapper/bin/buildLmSolrIndex**
.. code-block:: 

   #!/bin/bash

   # @author: CJ Grady
   # @summary: This script will rebuild the Lifemapper Archive Solr index
   
   # Set environment in case it is not already set
   SHELL=/usr/local/bin/bash
   PATH=/bin:/usr/bin:/usr/local/bin
   . /etc/profile.d/lifemapper.sh
   
   # Have to export PYTHONPATH or the modules will not be found
   export PYTHONPATH=/opt/lifemapper
   
   # Build the index
   # Note that the output of this could be piped to a file to check for success
   #   or you can check the last modified time of the files in:
   #   /var/solr/data/cores/lmArchive
   /opt/python/bin/python2.7 /opt/lifemapper/LmWebServer/solr/buildIndex.py -t 4


Layer Changes
#############
   Add constant 'CONVERT_TOOL' to 'LmCompute - plugins - maxent' in the compute 
   config file.  It should be set to 'density.Convert'
   
   Add subdirectory 'retrieved' under LmCompute layers directory.  By default,
   at: /share/lm/data/layers/retrieved  - this directory will contain downloaded
   and converted layers
