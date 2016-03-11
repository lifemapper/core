############################################################
Changes made to current version that need to be incorporated
############################################################



Cron job changes
################

* Remove the buildLuceneIndex cron job
* Add arguments to build solr index call.  Add -t 4 to line 18 in buildLmSolrIndex

code-block::

   /opt/python/bin/python2.7 /opt/lifemapper/LmWebServer/solr/buildIndex.py -t 4




-------------------


