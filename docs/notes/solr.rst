This page will contain information for working with solr

## How to remove data older than a certain time
You can delete large numbers of records by sending a query to the update endpoint.  For example, to delete 
documents in the lmArchive core that have projections older than 7 days, do this:

  $ curl 'http://localhost:8983/solr/lmArchive/update?commit=true' --data '<delete><query>sdmProjModTime:[* TO NOW-7DAY]</query></delete>' -H 'Content-TYpe: text/xml'
  
