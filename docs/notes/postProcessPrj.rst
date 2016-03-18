###########################
Post-processing Projections
###########################

Suggestion
**********

Add an optional post-processing step on projection computation to create the PAV for the Global PAM as well as create spatial index for the archive browser

Benefits
********

#. We won't need a separate job for the intersection
#. The Solr index would be updated on projection completion, so it will be more up-to-date
#. PAV available when projection is available

Considerations
**************

#. Which projections will we create PAVs for?
   * All?
   * Current scenario only?
#. What should the intersection parameters be?
   * Static?
   * Use the input data to determine?

Workflow
********

#. A job is requested from the job server
   #. If it meets the criteria, a post processing step is specified to create a PAV and spatial index information
#. Projection is computed
#. If post processing step is present
  #. Intersect the projection against the Global PAM shapegrid.  Use parameters 
     specified in the job request
  #. Process the intersected projection (PAV) and create WKT entries for the present cells
  #. Add the PAV and WKT files to the projection package
#. POST the results of the projection computation to the job server
#. The job server writes the projection information
#. The job server creates a document to be added to the archive Solr instance
#. If a WKT file is present, add spatial information to the index document
#. The index document is posted to the Solr index
#. If a PAV file is present, write it to the file system and do whatever else we decide for Global PAM

Sample Job Request
******************
::

  <job>
   <postProcessing>
      <intersect>
         <minPresence>25</minPresence>
         <maxPresence>100</maxPresence>
         <percentPresence>15</percentPresence>
         <shapegrid>http://yeti.lifemapper.org/services/rad/shapegrids/123/shapefile
      </intersect>
      <spatiallyIndex>True</spatiallyIndex>
      <post>
         <jobServer>http://yeti.lifemapper.org/jobs</jobServer>\n
      </post>
   </postProcessing>
   <processType>120</processType>
   <jobId>4791</jobId>
   <parentUrl>http://yeti.lifemapper.org/services/sdm/models/380</parentUrl>
   <url>http://yeti.lifemapper.org/services/sdm/projections/1868</url>
   <userId>DermotYeti</userId>
   <lambdas>
      <![CDATA[layer0, 0.0, -94.0, 376.0
               layer1, 0.0, 0.0, 3076.0
               layer10, 0.0, -538.0, 257.0
               layer11, 0.0, 55.0, 724.0
               layer12, 0.0, 112.0, 22527.0
               layer13, 0.0, -57.0, 488.0

      <intersect>
         <minPresence>25</minPresence>
         <maxPresence>100</maxPresence>
         <percentPresence>15</percentPresence>
         <shapegrid>http://yeti.lifemapper.org/services/rad/shapegrids/123/shapefile
      </intersect>

      <spatiallyIndex>True</spatiallyIndex>

**spatiallyIndex** tells LmCompute to create a spatial index WKT to be returned 
with the package

**intersect** section indicates that an intersection should be performed with 
those parameters


Strategy
********

#. Create a second index for spatial queries that is created as projections are completed
#. Create a non-spatial index that is created via cron job
#. When we are confident that the new index is working correctly, merge them