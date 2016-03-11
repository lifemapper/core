############################################################
Changes made to current version that need to be incorporated
############################################################



Cron job changes
################

* Remove the buildLuceneIndex cron job
* Add arguments to build solr index call.  Add -t 4 to line 18 in buildLmSolrIndex

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

    
