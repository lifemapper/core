-- ----------------------------------------------------------------------------
-- file:   createBorg.sql
-- author: Aimee Stewart
-- ----------------------------------------------------------------------------
-- ----------------------------------------------------------------------------
\c template1 admin

-- ----------------------------------------------------------------------------
CREATE DATABASE mal ENCODING='UTF8'
                    LC_COLLATE='en_US.UTF-8'
                    LC_CTYPE='en_US.UTF-8'
                    TEMPLATE=template1;
\c mal
-- ----------------------------------------------------------------------------

-- Note: LM_SCHEMA = 'lm3' is in LM.common.lmconstants
CREATE SCHEMA lm3;
ALTER DATABASE "mal" SET search_path=lm3,public;
GRANT USAGE ON SCHEMA lm3 TO reader, writer;
    
-- ----------------------------------------------------------------------------
-- -------------------------------
create table lm3.LMUser
(
   userId varchar(20) UNIQUE PRIMARY KEY,
   firstname varchar(50),
   lastname varchar(50),
   institution text,
   address1 text,
   address2 text,
   address3 text,
   phone varchar(20),
   email varchar(64) UNIQUE NOT NULL,
   dateLastModified double precision, 
   password varchar(32)
);

-- -------------------------------
create table lm3.ComputeResource
(
   computeResourceId serial UNIQUE PRIMARY KEY,
   name varchar(32) NOT NULL,
   ipaddress varchar(16) UNIQUE NOT NULL,
   ipmask varchar(2),
   fqdn varchar(100),
   
   -- Contact info will be in LMUser table
   userId varchar(20) NOT NULL REFERENCES lm3.LMUser,
   
   datecreated double precision,
   datelastmodified double precision,
   lastheartbeat double precision,
   UNIQUE (name, userId),
   UNIQUE (ipaddress, ipmask)
);

-- -------------------------------
create table lm3.LMJob
(
   lmJobId serial UNIQUE PRIMARY KEY,
   jobFamily int NOT NULL,
   referenceType int NOT NULL,
   referenceId int NOT NULL,

   computeResourceId int REFERENCES lm3.ComputeResource,
   priority int,
   progress int,
      
   -- status/stage of current processing
   status int,
   statusmodtime double precision,
   stage int,
   stagemodtime double precision,
   
   -- Email notification
   doNotify boolean,

   -- Compute resource filters
   reqData int,
   reqSoftware int,

   datecreated double precision,
   lastheartbeat double precision,
   retryCount int,
   
   UNIQUE (jobFamily, referenceType, referenceId, reqSoftware)
);

-- -------------------------------
create table lm3.JobChain
(
   jobchainId serial UNIQUE PRIMARY KEY,
   userid varchar(20) NOT NULL REFERENCES lm3.LMUser ON DELETE CASCADE,
   dlocation text,
   priority int,
   progress int,
   status int,
   statusmodtime double precision,
   datecreated double precision
);


-- -------------------------------
create table lm3.TaxonomySource
(
   taxonomySourceId serial UNIQUE PRIMARY KEY,
   url text,
   datasetIdentifier text UNIQUE,
   dateCreated double precision,
   dateLastModified double precision
);

-- -------------------------------
-- ** MAL ScientificName
create table lm3.Taxon
(
	-- ** MAL scientificNameId
   taxonId serial UNIQUE PRIMARY KEY,
   taxonomySourceId int REFERENCES lm3.TaxonomySource,
   taxonomyKey int,
   kingdom text,
   phylum text,
   tx_class  text,
   tx_order text,
   family  text,
   genus text,
   rank varchar(20),
   canonical text,
   sciname text,
   genuskey int,
   specieskey int,
   keyHierarchy text,
   lastcount int,
   datecreated double precision,
   datelastmodified double precision,
   UNIQUE (taxonomySourceId, taxonomyKey)
);
CREATE INDEX idx_lower_canonical on lm3.Taxon(lower(canonical));
CREATE INDEX idx_lower_sciname on lm3.Taxon(lower(sciname));
CREATE INDEX idx_lower_genus on lm3.Taxon(lower(genus));

-- -------------------------------
-- 'query' column is the query on the source to get these occurrence
-- records.  That could (probably will be) a query on the LM Point Bucket (PBJ 
-- database) using a stored procedure or could be a WFS or REST query to a web
-- service.
create table lm3.OccurrenceSet
(
   occurrenceSetId serial UNIQUE PRIMARY KEY,
   verify varchar(64),
   squid varchar(64),
   userId varchar(20) NOT NULL REFERENCES lm3.LMUser ON DELETE CASCADE,
   fromGbif boolean,
   displayName text,
      
   -- ** new (prev scientificNameId)
   taxonId int REFERENCES lm3.Taxon,
   primaryEnv int,
   metadataUrl text UNIQUE,
   dlocation text,
   queryCount int,
   dateLastModified double precision,
   dateLastChecked double precision,
   bbox varchar(60),
   epsgcode integer,
   status integer,
   statusmodtime double precision,
   rawDlocation text
);
Select AddGeometryColumn('lm3', 'occurrenceset', 'geom', 4326, 'POLYGON', 2);
ALTER TABLE lm3.OccurrenceSet ADD CONSTRAINT geometry_valid_check CHECK (st_isvalid(geom));
ALTER TABLE lm3.OccurrenceSet ADD CONSTRAINT enforce_srid_geom CHECK (st_srid(geom) = 4326);
ALTER TABLE lm3.OccurrenceSet ADD CONSTRAINT enforce_dims_geom CHECK (st_ndims(geom) = 2);
CREATE INDEX spidx_occset ON lm3.OccurrenceSet USING GIST ( geom );

Select AddGeometryColumn('lm3', 'occurrenceset', 'geompts', 4326, 'MULTIPOINT', 2);
-- CREATE INDEX spidx_occset_pts ON lm3.OccurrenceSet USING GIST ( geompts );
ALTER TABLE lm3.OccurrenceSet ADD CONSTRAINT geometry_pts_valid_check CHECK (st_isvalid(geompts));
ALTER TABLE lm3.OccurrenceSet ADD CONSTRAINT enforce_srid_geompts CHECK (st_srid(geompts) = 4326);
ALTER TABLE lm3.OccurrenceSet ADD CONSTRAINT enforce_dims_geompts CHECK (st_ndims(geompts) = 2);

CREATE INDEX idx_lower_displayName on lm3.OccurrenceSet(lower(displayName));
CREATE INDEX idx_pattern_lower_displayname on lm3.OccurrenceSet  (lower(displayname) varchar_pattern_ops );
CREATE INDEX idx_queryCount ON lm3.OccurrenceSet(queryCount);
CREATE INDEX idx_min_queryCount ON lm3.OccurrenceSet((queryCount >= 50));
CREATE INDEX idx_occLastModified ON lm3.OccurrenceSet(dateLastModified);
CREATE INDEX idx_occLastChecked ON lm3.OccurrenceSet(dateLastChecked);
CREATE INDEX idx_occUser ON lm3.OccurrenceSet(userId);
CREATE INDEX idx_occStatus ON lm3.OccurrenceSet(status);
CREATE INDEX idx_occSquid on lm3.OccurrenceSet(squid);


-- -------------------------------
create table lm3.Algorithm
(
   algorithmId serial UNIQUE PRIMARY KEY,
   algorithmCode varchar(30) UNIQUE,
   name varchar(60),
   dateLastModified double precision
);

-- -------------------------------
 create table lm3.Scenario
 (
    scenarioId serial UNIQUE PRIMARY KEY,
    scenarioCode varchar(30),
    metadataUrl text UNIQUE,
    dlocation text,
    title text,
    author text,
    description text,
    startDate double precision,
    endDate double precision,
    units varchar(20),
    resolution double precision,
    bbox varchar(60),
    dateLastModified double precision,
    userid varchar(20),
    epsgcode int,
    UNIQUE (scenarioCode, userid)
 );
 Select AddGeometryColumn('lm3', 'scenario', 'geom', 4326, 'POLYGON', 2);
 ALTER TABLE lm3.Scenario ADD CONSTRAINT geometry_valid_check CHECK (st_isvalid(geom));
 ALTER TABLE lm3.Scenario ADD CONSTRAINT enforce_srid_geom CHECK (st_srid(geom) = 4326);
 ALTER TABLE lm3.Scenario ADD CONSTRAINT enforce_dims_geom CHECK (st_ndims(geom) = 2);
 CREATE INDEX spidx_scenario ON lm3.Scenario USING GIST ( geom );

-- -------------------------------
create table lm3.LayerType
(
   layerTypeId serial UNIQUE PRIMARY KEY,
   code varchar(30),
   title text,
   userid varchar(20) NOT NULL REFERENCES lm3.LMUser ON DELETE CASCADE,
   description text,
   dateLastModified double precision
);
ALTER TABLE lm3.LayerType ADD CONSTRAINT unique_layertype UNIQUE (userid, code);

-- -------------------------------
-- Note: Enforce unique userid/name pairs (in code) for display layers only
create table lm3.Layer
(
   layerId serial UNIQUE PRIMARY KEY,
   verify varchar(64),
   squid varchar(64),
   userid varchar(20) NOT NULL REFERENCES lm3.LMUser ON DELETE CASCADE,
   -- ** new
   taxonId int REFERENCES lm3.Taxon,
   -- ** layername in RAD
   name text,
   title text,
   -- ** not in RAD
   author text,
   description text,
   dlocation text,
   metadataUrl text UNIQUE,
   metalocation text,
   -- ** layerurl in RAD
   gdalType int,
   ogrType int,
   -- ** not in RAD
   isCategorical boolean,
   -- GDAL/OGR codes indicating driver to use when writing files
   dataFormat varchar(32),
   epsgcode int,
   mapunits varchar(20),
   resolution double precision,
   startDate double precision,
   endDate double precision,
   -- ** not in MAL
   datecreated double precision,
   dateLastModified double precision,
   bbox varchar(60),
   -- removed from MAL: thumbnail bytea,
   -- Used for classification on pixel or featAttribute
   -- 'pixel' if raster data
   valAttribute varchar(20),
   nodataVal double precision,
   minVal double precision,
   maxVal double precision,
   valUnits varchar(60),
   -- Used to match layers between SDM scenarios
   layerTypeId int REFERENCES lm3.LayerType
);
 Select AddGeometryColumn('lm3', 'layer', 'geom', 4326, 'POLYGON', 2);
 ALTER TABLE lm3.Layer ADD CONSTRAINT geometry_valid_check CHECK (st_isvalid(geom));
 ALTER TABLE lm3.layer ADD CONSTRAINT enforce_srid_geom CHECK (st_srid(geom) = 4326);
 ALTER TABLE lm3.layer ADD CONSTRAINT enforce_dims_geom CHECK (st_ndims(geom) = 2);
 CREATE INDEX spidx_layer ON lm3.Layer USING GIST ( geom );
 CREATE INDEX idx_lyrSquid on lm3.Layer(squid);
 CREATE INDEX idx_lyrVerify on lm3.Layer(verify);

-- -------------------------------
create table lm3.ScenarioLayers
(
   scenarioId int REFERENCES lm3.Scenario MATCH FULL ON DELETE CASCADE,
   layerId int REFERENCES lm3.Layer MATCH FULL ON DELETE CASCADE,
   PRIMARY KEY (scenarioId, layerId)
);

-- -------------------------------
create table lm3.Keyword
(
   keywordId serial UNIQUE PRIMARY KEY,
   keyword text UNIQUE
);

-- -------------------------------
create table lm3.LayerTypeKeyword
(
   layerTypeId int REFERENCES lm3.LayerType MATCH FULL ON DELETE CASCADE,
   keywordId int REFERENCES lm3.Keyword MATCH FULL ON DELETE CASCADE,
   PRIMARY KEY (layerTypeId, keywordId)
);

-- -------------------------------
create table lm3.ScenarioKeywords
(
   scenarioId int REFERENCES lm3.Scenario MATCH FULL ON DELETE CASCADE,
   keywordId int REFERENCES lm3.Keyword MATCH FULL ON DELETE CASCADE,
   PRIMARY KEY (scenarioId, keywordId)
);

-- -------------------------------
-- The 'algorithmParams' column is a pickled (protocol=0) dictionary of 
-- algorithm parameters with the 
   -- key = case sensitive parameter name (previously omkey)
   -- value = parameter value for this model 

create table lm3.Model 
(
   modelId serial UNIQUE PRIMARY KEY,
   userId varchar(20) REFERENCES lm3.LMUser ON DELETE CASCADE,
   name text NOT NULL,
   description text,
   occurrenceSetId int REFERENCES lm3.OccurrenceSet ON DELETE CASCADE,
   scenarioCode varchar(30),
   scenarioId int REFERENCES lm3.Scenario ON DELETE CASCADE,
   maskId int REFERENCES lm3.Layer,
   createTime double precision,
   status int,
   statusModTime double precision,
   priority int,
   dlocation text,
   qc varchar(20),
   jobId int,
   email varchar(64), 
   algorithmParams text,
   algorithmCode varchar(30) NOT NULL REFERENCES lm3.Algorithm(algorithmCode),
   computeResourceId int REFERENCES lm3.ComputeResource
);
CREATE INDEX idx_mdlLastModified ON lm3.Model(statusModTime);
CREATE INDEX idx_modelUser ON lm3.Model(userId);
CREATE INDEX idx_mdlStatus ON lm3.Model(status);

-- -------------------------------
-- Holds projection of a ruleset on to a set of environmental layers
-- 
create table lm3.Projection
(
   projectionId serial UNIQUE PRIMARY KEY,
   verify varchar(64),
   squid varchar(64),
   metadataUrl text UNIQUE,
   -- ** NEW
   metalocation text,
   -- ** new
   taxonId int REFERENCES lm3.Taxon,
   modelId int REFERENCES lm3.Model ON DELETE CASCADE,
   scenarioCode varchar(30),
   scenarioId int REFERENCES lm3.Scenario ON DELETE CASCADE,
   maskId int REFERENCES lm3.Layer,
   createTime double precision,
   status int,
   statusModTime double precision,
   priority int,
   units varchar(20),
   resolution double precision,
   epsgcode int,
   bbox varchar(60),
   dlocation text,
   dataType int,
   jobId int,
   computeResourceId int REFERENCES lm3.ComputeResource
);  
Select AddGeometryColumn('lm3', 'projection', 'geom', 4326, 'POLYGON', 2);
ALTER TABLE lm3.Projection ADD CONSTRAINT geometry_valid_check CHECK (st_isvalid(geom));
ALTER TABLE lm3.Projection ADD CONSTRAINT enforce_srid_geom CHECK (st_srid(geom) = 4326);
ALTER TABLE lm3.Projection ADD CONSTRAINT enforce_dims_geom CHECK (st_ndims(geom) = 2);

CREATE INDEX spidx_projection ON lm3.Projection USING GIST ( geom );
CREATE INDEX idx_projLastModified ON lm3.Projection(statusModTime);
CREATE INDEX idx_prjStatus ON lm3.Projection(status);
CREATE INDEX idx_prjSquid on lm3.Projection(squid);

-- -------------------------------
create table lm3.ShapeGrid
(
   shapeGridId serial UNIQUE PRIMARY KEY,
   layerId int NOT NULL REFERENCES lm3.Layer ON DELETE CASCADE,
   cellsides int,
   cellsize double precision,
   vsize int,
   idAttribute varchar(20),
   xAttribute varchar(20),
   yAttribute varchar(20),
   -- status of current processing stage (build only)
   status int,
   statusmodtime double precision,
   computeResourceId int REFERENCES lm3.ComputeResource
);

-- -------------------------------
create table lm3.AncillaryValue
(
   ancillaryValueId  serial UNIQUE PRIMARY KEY,
   userId varchar(20) NOT NULL,
   -- Name of the field containing the value for calculations
   nameValue varchar(20),
   weightedMean boolean,
   largestClass boolean,
   minPercent int,
   nameFilter varchar(20),
   valueFilter varchar(20),
   UNIQUE (userId, nameValue, weightedMean, largestClass, minPercent),
   CHECK (minPercent >= 0 AND minPercent <= 100)
);

-- -------------------------------
create table lm3.PresenceAbsence
(
   presenceAbsenceId  serial UNIQUE PRIMARY KEY,
   userId varchar(20) NOT NULL,
   -- Fieldname for filtering records, applicable only to multi-species files 
   nameFilter varchar(20),
   -- Value for matching records when filtering
   valueFilter varchar(50),
   -- Name of the field containing the value for presence
   namePresence varchar(20),
   minPresence double precision,
   maxPresence double precision,
   percentPresence int,
   -- Name of the field containing the value for absence
   nameAbsence varchar(20),
   minAbsence double precision,
   maxAbsence double precision,
   percentAbsence int, 
   UNIQUE(userId, namePresence, minPresence, maxPresence, percentPresence, 
                  nameAbsence, minAbsence, maxAbsence, percentAbsence),
   CHECK (percentPresence >= 0 AND percentPresence <= 100),
   CHECK (percentAbsence >= 0 AND percentAbsence <= 100)
);

-- ----------------------------------------------------------------------------

GRANT SELECT ON TABLE 
lm3.lmuser, 
lm3.scenario, lm3.scenario_scenarioid_seq,
lm3.keyword, lm3.keyword_keywordid_seq, 
lm3.layer, lm3.layer_layerid_seq, 
lm3.layertype, lm3.layertype_layertypeid_seq,
lm3.layertypekeyword, lm3.scenariokeywords, lm3.scenariolayers,
lm3.occurrenceset, lm3.occurrenceset_occurrencesetid_seq, 
lm3.model, lm3.model_modelid_seq, 
lm3.algorithm, lm3.algorithm_algorithmid_seq,
lm3.projection, lm3.projection_projectionid_seq,
lm3.computeresource, lm3.computeresource_computeresourceid_seq,
lm3.lmjob, lm3.lmjob_lmjobid_seq,
lm3.lmjobchain, lm3.lmjobchain_lmjobchainid_seq,
lm3.taxonomysource, lm3.taxonomysource_taxonomysourceid_seq,
lm3.taxon, lm3.taxon_taxonid_seq
TO GROUP reader;

GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE 
lm3.lmuser, 
lm3.algorithm, 
lm3.scenario,
lm3.keyword,
lm3.layer, lm3.layertype,
lm3.layertypekeyword, lm3.scenariokeywords, lm3.scenariolayers,
lm3.occurrenceset, 
lm3.model,  
lm3.projection,
lm3.computeresource, 
lm3.lmjob,
lm3.experiment,
lm3.taxonomysource,
lm3.taxon
TO GROUP writer;

GRANT SELECT, UPDATE ON TABLE 
lm3.occurrenceset_occurrencesetid_seq,
lm3.algorithm_algorithmid_seq,
lm3.keyword_keywordid_seq,
lm3.scenario_scenarioid_seq,
lm3.layer_layerid_seq,
lm3.layertype_layertypeid_seq,
lm3.model_modelid_seq,
lm3.projection_projectionid_seq,
lm3.statistics_statisticsid_seq,
lm3.computeresource_computeresourceid_seq,
lm3.lmjob_lmjobid_seq,
lm3.lmjobchain_lmjobchainid_seq,
lm3.experiment_experimentid_seq,
lm3.taxonomysource_taxonomysourceid_seq,
lm3.taxon_taxonid_seq
TO GROUP writer;

-- ----------------------------------------------------------------------------
-- From http://www.databasesoup.com/2012/09/freezing-your-tuples-off-part-1.html
-- "give me the top 20 tables over 1GB, sorted by the age of their oldest XID".

-- SELECT relname, age(relfrozenxid) as xid_age, 
--   pg_size_pretty(pg_table_size(oid)) as table_size
-- FROM pg_class 
-- WHERE relkind = 'r' and pg_table_size(oid) > 1073741824
-- ORDER BY age(relfrozenxid) DESC LIMIT 20;
-- ----------------------------------------------------------------------------
