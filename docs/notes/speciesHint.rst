########
Overview
########

This page contains notes from the conversion of the Lifemapper Species Hint 
Service from a Lucene backend to Solr.  For this initial conversion, we will
create a new index with only the information required by the species hint 
service.  This information is nearly all included in the archive index though
and we can combine these in the future.

The species hint service takes the following parameters:
 * query - a string of characters to match for the binomial name
 * format - the format to return the results in
 * columns - the number of columns for a json response (obsolete)
 * maxreturned - the maximum number of results to return
 * seeall - this flag says to return all results

The Lucene index contained the following fields:
 * species - the accepted name with author
 * occSetId - the occurrence set id
 * speciesSearch - a string used to search the index
 * numOcc - the number of occurrence points in the occurrence set
 * numModels - the number of models built from this occurrence set
 * binomial - the binomial name of the species
 * downloadUrl - a url where the data can be downloaded
 
 
Notes
##### 
 * Binomial can probably be found from a filter