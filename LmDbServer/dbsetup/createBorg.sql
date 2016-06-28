-- ----------------------------------------------------------------------------
-- file:   createBorg.sql
-- author: Aimee Stewart
-- ----------------------------------------------------------------------------
-- ----------------------------------------------------------------------------
\c template1 admin

-- ----------------------------------------------------------------------------
CREATE DATABASE borg ENCODING='UTF8'
                    LC_COLLATE='en_US.UTF-8'
                    LC_CTYPE='en_US.UTF-8'
                    TEMPLATE=template1;
\c borg
-- ----------------------------------------------------------------------------

-- Note: LM_SCHEMA = 'lm' is in LM.common.lmconstants
CREATE SCHEMA lm_v3;
ALTER DATABASE "borg" SET search_path=lm_v3,public;
GRANT USAGE ON SCHEMA lm_v3 TO reader, writer;
    
-- ----------------------------------------------------------------------------
-- -------------------------------
create table lm_v3.LMUser
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
   modtime double precision, 
   password varchar(32)
);

-- -------------------------------
create table lm_v3.ComputeResource
(
   computeResourceId serial UNIQUE PRIMARY KEY,
   name varchar(32) NOT NULL,
   ipaddress varchar(16) UNIQUE NOT NULL,
   ipmask varchar(2),
   fqdn varchar(100),
   userId varchar(20) NOT NULL REFERENCES lm_v3.LMUser,
   modtime double precision,
   lastheartbeat double precision,
   UNIQUE (name, userId),
   UNIQUE (ipaddress, ipmask)
);

-- -------------------------------
create table lm_v3.JobChain
(
   jobchainId serial UNIQUE PRIMARY KEY,
   userid varchar(20) NOT NULL REFERENCES lm_v3.LMUser ON DELETE CASCADE,
   dlocation text,
   priority int,
   progress int,
   status int,
   statusmodtime double precision
);


-- -------------------------------
create table lm_v3.TaxonomySource
(
   taxonomySourceId serial UNIQUE PRIMARY KEY,
   url text UNIQUE,
   datasetIdentifier text UNIQUE,
   modTime double precision
);

-- -------------------------------
-- aka ** MAL ScientificName
create table lm_v3.Taxon
(
	-- ** MAL scientificNameId
   taxonId serial UNIQUE PRIMARY KEY,
   taxonomySourceId int REFERENCES lm_v3.TaxonomySource,
   taxonomyKey int,
   squid varchar(64),
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
   modTime double precision,
   UNIQUE (taxonomySourceId, taxonomyKey)
);
CREATE INDEX idx_lower_canonical on lm_v3.Taxon(lower(canonical));
CREATE INDEX idx_lower_sciname on lm_v3.Taxon(lower(sciname));
CREATE INDEX idx_lower_genus on lm_v3.Taxon(lower(genus));

-- -------------------------------
create table lm_v3.Keyword
(
   keywordId serial UNIQUE PRIMARY KEY,
   keyword text UNIQUE
);

-- -------------------------------
create table lm_v3.LayerType
(
   layerTypeId serial UNIQUE PRIMARY KEY,
   userid varchar(20) NOT NULL REFERENCES lm_v3.LMUser ON DELETE CASCADE,
   code varchar(30),
   title text,
   description text,
   modTime double precision
);
ALTER TABLE lm_v3.LayerType ADD CONSTRAINT unique_layertype UNIQUE (userid, code);

-- -------------------------------
create table lm_v3.LayerTypeKeyword
(
   layerTypeId int REFERENCES lm_v3.LayerType MATCH FULL ON DELETE CASCADE,
   keywordId int REFERENCES lm_v3.Keyword MATCH FULL ON DELETE CASCADE,
   PRIMARY KEY (layerTypeId, keywordId)
);

-- -------------------------------
-- Note: Enforce unique userid/name pairs (in code) for display layers only
create table lm_v3.Layer
(
   layerId serial UNIQUE PRIMARY KEY,
   userid varchar(20) NOT NULL REFERENCES lm_v3.LMUser ON DELETE CASCADE,
   taxonId int REFERENCES lm_v3.Taxon,
   verify varchar(64),
   squid varchar(64),
   name text,
   title text,
   author text,
   description text,
   dlocation text,
   metadataUrl text UNIQUE,
   metalocation text,
   gdalType int,
   ogrType int,
   isCategorical boolean,
   -- GDAL/OGR codes indicating driver to use when writing files
   dataFormat varchar(32),
   epsgcode int,
   mapunits varchar(20),
   resolution double precision,
   startDate double precision,
   endDate double precision,
   modTime double precision,
   bbox varchar(60),
   valAttribute varchar(20),
   nodataVal double precision,
   minVal double precision,
   maxVal double precision,
   valUnits varchar(60),
   layerTypeId int REFERENCES lm_v3.LayerType,
   UNIQUE (userid, name, epsgcode)
);
 Select AddGeometryColumn('lm_v3', 'layer', 'geom', 4326, 'POLYGON', 2);
 ALTER TABLE lm_v3.Layer ADD CONSTRAINT geometry_valid_check CHECK (st_isvalid(geom));
 ALTER TABLE lm_v3.layer ADD CONSTRAINT enforce_srid_geom CHECK (st_srid(geom) = 4326);
 ALTER TABLE lm_v3.layer ADD CONSTRAINT enforce_dims_geom CHECK (st_ndims(geom) = 2);
 ALTER TABLE lm_v3.layer ADD CONSTRAINT unique_usr_name CHECK (st_ndims(geom) = 2);
 CREATE INDEX spidx_layer ON lm_v3.Layer USING GIST ( geom );
 CREATE INDEX idx_lyrSquid on lm_v3.Layer(squid);
 CREATE INDEX idx_lyrVerify on lm_v3.Layer(verify);

-- -------------------------------
 create table lm_v3.Scenario
 (
    scenarioId serial UNIQUE PRIMARY KEY,
    userid  varchar(20) NOT NULL REFERENCES lm_v3.LMUser ON DELETE CASCADE,
    scenarioCode varchar(30),
    metadataUrl text UNIQUE,
    title text,
    author text,
    description text,
    startDate double precision,
    endDate double precision,
    units varchar(20),
    resolution double precision,
    epsgcode int,
    bbox varchar(60),
    modTime double precision,
    UNIQUE (scenarioCode, userid)
 );
 Select AddGeometryColumn('lm_v3', 'scenario', 'geom', 4326, 'POLYGON', 2);
 ALTER TABLE lm_v3.Scenario ADD CONSTRAINT geometry_valid_check CHECK (st_isvalid(geom));
 ALTER TABLE lm_v3.Scenario ADD CONSTRAINT enforce_srid_geom CHECK (st_srid(geom) = 4326);
 ALTER TABLE lm_v3.Scenario ADD CONSTRAINT enforce_dims_geom CHECK (st_ndims(geom) = 2);
 CREATE INDEX spidx_scenario ON lm_v3.Scenario USING GIST ( geom );


-- -------------------------------
create table lm_v3.ScenarioLayers
(
   scenarioId int REFERENCES lm_v3.Scenario MATCH FULL ON DELETE CASCADE,
   layerId int REFERENCES lm_v3.Layer MATCH FULL ON DELETE CASCADE,
   PRIMARY KEY (scenarioId, layerId)
);


-- -------------------------------
create table lm_v3.ScenarioKeywords
(
   scenarioId int REFERENCES lm_v3.Scenario MATCH FULL ON DELETE CASCADE,
   keywordId int REFERENCES lm_v3.Keyword MATCH FULL ON DELETE CASCADE,
   PRIMARY KEY (scenarioId, keywordId)
);

-- -------------------------------
create table lm_v3.OccurrenceSet
(
   occurrenceSetId serial UNIQUE PRIMARY KEY,
   verify varchar(64),
   squid varchar(64),
   userId varchar(20) NOT NULL REFERENCES lm_v3.LMUser ON DELETE CASCADE,
   displayName text,
   taxonId int REFERENCES lm_v3.Taxon,
   primaryEnv int,
   metadataUrl text UNIQUE,
   dlocation text,
   rawDlocation text,
   queryCount int,
   bbox varchar(60),
   epsgcode integer,
   status integer,
   statusmodtime double precision
);
Select AddGeometryColumn('lm_v3', 'occurrenceset', 'geom', 4326, 'POLYGON', 2);
ALTER TABLE lm_v3.OccurrenceSet ADD CONSTRAINT geometry_valid_check CHECK (st_isvalid(geom));
ALTER TABLE lm_v3.OccurrenceSet ADD CONSTRAINT enforce_srid_geom CHECK (st_srid(geom) = 4326);
ALTER TABLE lm_v3.OccurrenceSet ADD CONSTRAINT enforce_dims_geom CHECK (st_ndims(geom) = 2);
CREATE INDEX spidx_occset ON lm_v3.OccurrenceSet USING GIST ( geom );

Select AddGeometryColumn('lm_v3', 'occurrenceset', 'geompts', 4326, 'MULTIPOINT', 2);
-- CREATE INDEX spidx_occset_pts ON lm_v3.OccurrenceSet USING GIST ( geompts );
ALTER TABLE lm_v3.OccurrenceSet ADD CONSTRAINT geometry_pts_valid_check CHECK (st_isvalid(geompts));
ALTER TABLE lm_v3.OccurrenceSet ADD CONSTRAINT enforce_srid_geompts CHECK (st_srid(geompts) = 4326);
ALTER TABLE lm_v3.OccurrenceSet ADD CONSTRAINT enforce_dims_geompts CHECK (st_ndims(geompts) = 2);

CREATE INDEX idx_lower_displayName on lm_v3.OccurrenceSet(lower(displayName));
CREATE INDEX idx_pattern_lower_displayname on lm_v3.OccurrenceSet  (lower(displayname) varchar_pattern_ops );
CREATE INDEX idx_queryCount ON lm_v3.OccurrenceSet(queryCount);
CREATE INDEX idx_min_queryCount ON lm_v3.OccurrenceSet((queryCount >= 50));
CREATE INDEX idx_occUser ON lm_v3.OccurrenceSet(userId);
CREATE INDEX idx_occStatus ON lm_v3.OccurrenceSet(status);
CREATE INDEX idx_occSquid on lm_v3.OccurrenceSet(squid);


-- -------------------------------
create table lm_v3.Algorithm
(
   algorithmCode varchar(30) UNIQUE PRIMARY KEY,
   name varchar(60),
   modTime double precision
);


-- -------------------------------
-- The 'algorithmParams' column is algorithm parameters in JSON format, with the 
   -- key = case sensitive parameter name (previously omkey)
   -- value = parameter value for this model 
-- ** Note change from pickled, protocol 0, to JSON 
create table lm_v3.SDMModel 
(
   sdmmodelid serial UNIQUE PRIMARY KEY,
   userId varchar(20) REFERENCES lm_v3.LMUser ON DELETE CASCADE,
   name text NOT NULL,
   description text,
   occurrenceSetId int REFERENCES lm_v3.OccurrenceSet ON DELETE CASCADE,
   scenarioId int REFERENCES lm_v3.Scenario ON DELETE CASCADE,
   scenarioCode varchar(30),
   maskId int REFERENCES lm_v3.Layer,
   status int,
   statusModTime double precision,
   priority int,
   dlocation text,
   email varchar(64), 
   algorithmParams text,
   algorithmCode varchar(30) NOT NULL REFERENCES lm_v3.Algorithm(algorithmCode)
);
CREATE INDEX idx_mdlLastModified ON lm_v3.SDMModel(statusModTime);
CREATE INDEX idx_modelUser ON lm_v3.SDMModel(userId);
CREATE INDEX idx_mdlStatus ON lm_v3.SDMModel(status);

-- -------------------------------
-- Holds projection of a ruleset on to a set of environmental layers
-- 
create table lm_v3.SDMProjection
(
   sdmprojectionId serial UNIQUE PRIMARY KEY,
   verify varchar(64),
   squid varchar(64),
   metadataUrl text UNIQUE,
   metalocation text,
   taxonId int REFERENCES lm_v3.Taxon,
   sdmmodelid int REFERENCES lm_v3.SDMModel ON DELETE CASCADE,
   scenarioCode varchar(30),
   scenarioId int REFERENCES lm_v3.Scenario ON DELETE CASCADE,
   maskId int REFERENCES lm_v3.Layer,
   status int,
   statusModTime double precision,
   units varchar(20),
   resolution double precision,
   epsgcode int,
   bbox varchar(60),
   dlocation text,
   dataType int
);  
Select AddGeometryColumn('lm_v3', 'sdmprojection', 'geom', 4326, 'POLYGON', 2);
ALTER TABLE lm_v3.SDMProjection ADD CONSTRAINT geometry_valid_check CHECK (st_isvalid(geom));
ALTER TABLE lm_v3.SDMProjection ADD CONSTRAINT enforce_srid_geom CHECK (st_srid(geom) = 4326);
ALTER TABLE lm_v3.SDMProjection ADD CONSTRAINT enforce_dims_geom CHECK (st_ndims(geom) = 2);

CREATE INDEX spidx_sdmprojection ON lm_v3.SDMProjection USING GIST ( geom );
CREATE INDEX idx_prjLastModified ON lm_v3.SDMProjection(statusModTime);
CREATE INDEX idx_prjStatus ON lm_v3.SDMProjection(status);
CREATE INDEX idx_prjSquid on lm_v3.SDMProjection(squid);

-- -------------------------------
create table lm_v3.ShapeGrid
(
   shapeGridId serial UNIQUE PRIMARY KEY,
   layerId int NOT NULL REFERENCES lm_v3.Layer ON DELETE CASCADE,
   cellsides int,
   cellsize double precision,
   vsize int,
   idAttribute varchar(20),
   xAttribute varchar(20),
   yAttribute varchar(20),
   status int,
   statusmodtime double precision
);

-- -------------------------------
create table lm_v3.AncillaryValue
(
   ancillaryValueId  serial UNIQUE PRIMARY KEY,
   userId varchar(20) REFERENCES lm_v3.LMUser ON DELETE CASCADE,
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
create table lm_v3.PresenceAbsence
(
   presenceAbsenceId  serial UNIQUE PRIMARY KEY,
   userId varchar(20) REFERENCES lm_v3.LMUser ON DELETE CASCADE,
   -- Fieldname/value for filtering records, applicable only to multi-species files 
   nameFilter varchar(20),
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

-- -------------------------------
create table lm_v3.Boom
(
   boomId serial UNIQUE PRIMARY KEY,
   userId varchar(20) NOT NULL REFERENCES lm_v3.LMUser ON DELETE CASCADE,
   name varchar(100) NOT NULL,
   attrMatrixDlocation varchar(256),
   attrTreeDlocation varchar(256),
   epsgcode int,
   description text,
   modTime double precision,
   UNIQUE (userId, name)
);

-- -------------------------------
create table lm_v3.PAM
(
   pamId serial UNIQUE PRIMARY KEY,
   boomId int NOT NULL REFERENCES lm_v3.Boom ON DELETE CASCADE,
   shapeGridId int NOT NULL REFERENCES lm_v3.ShapeGrid,
   slIndicesDlocation varchar(256),   
   -- Uncompressed
   pamDlocation varchar(256),
   status int,
   statusmodtime double precision
);

-- -------------------------------
create table lm_v3.GRIM
(
   grimId serial UNIQUE PRIMARY KEY,
   boomId int NOT NULL REFERENCES lm_v3.Boom ON DELETE CASCADE,
   shapeGridId int NOT NULL REFERENCES lm_v3.ShapeGrid,
   slIndicesDlocation varchar(256),   
   -- Uncompressed 
   grimDlocation varchar(256),
   status int,
   statusmodtime double precision
);

-- -------------------------------
create table lm_v3.BoomPALayer
(
   boomPALayerId  serial UNIQUE PRIMARY KEY,
   boomId int NOT NULL REFERENCES lm_v3.Boom ON DELETE CASCADE,
   layerId int NOT NULL REFERENCES lm_v3.Layer ON DELETE CASCADE,
   presenceAbsenceId int NOT NULL REFERENCES lm_v3.PresenceAbsence ON DELETE CASCADE,
   name varchar(20),
   -- initialized as -1
   matrixIdx int NOT NULL,
   UNIQUE (boomId, layerId, presenceAbsenceId),
   UNIQUE (boomId, matrixIdx)
);

-- -------------------------------
create table lm_v3.BoomAncLayer
(
   boomAncLayerId  serial UNIQUE PRIMARY KEY,
   boomId int NOT NULL REFERENCES lm_v3.Boom ON DELETE CASCADE,
   layerId int NOT NULL REFERENCES lm_v3.Layer ON DELETE CASCADE,
   ancillaryValueId int NOT NULL REFERENCES lm_v3.AncillaryValue ON DELETE CASCADE,
   name varchar(20),
   -- initialized as -1
   matrixIdx int NOT NULL,
   UNIQUE (boomId, layerId, ancillaryValueId),
   UNIQUE (boomId, matrixIdx)
);


-- ----------------------------------------------------------------------------

GRANT SELECT ON TABLE 
lm_v3.lmuser, 
lm_v3.computeresource, lm_v3.computeresource_computeresourceid_seq,
lm_v3.jobchain, lm_v3.jobchain_jobchainid_seq,
lm_v3.taxonomysource, lm_v3.taxonomysource_taxonomysourceid_seq,
lm_v3.taxon, lm_v3.taxon_taxonid_seq,
lm_v3.keyword, lm_v3.keyword_keywordid_seq, 
lm_v3.layertype, lm_v3.layertype_layertypeid_seq,
lm_v3.layertypekeyword, 
lm_v3.layer, lm_v3.layer_layerid_seq, 
lm_v3.scenario, lm_v3.scenario_scenarioid_seq,
lm_v3.scenariokeywords, 
lm_v3.scenariolayers,
lm_v3.algorithm, 
lm_v3.occurrenceset, lm_v3.occurrenceset_occurrencesetid_seq, 
lm_v3.sdmmodel, lm_v3.sdmmodel_sdmmodelid_seq, 
lm_v3.sdmprojection, lm_v3.sdmprojection_sdmprojectionid_seq,
lm_v3.boom, lm_v3.boom_boomid_seq
TO GROUP reader;

GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE 
lm_v3.lmuser, 
lm_v3.computeresource, 
lm_v3.jobchain,
lm_v3.taxonomysource,
lm_v3.taxon,
lm_v3.keyword,
lm_v3.layertype,
lm_v3.layertypekeyword, 
lm_v3.layer, 
lm_v3.scenario,
lm_v3.scenariokeywords, 
lm_v3.scenariolayers,
lm_v3.algorithm, 
lm_v3.occurrenceset, 
lm_v3.sdmmodel,  
lm_v3.sdmprojection,
lm_v3.boom
TO GROUP writer;

GRANT SELECT, UPDATE ON TABLE 
lm_v3.computeresource_computeresourceid_seq,
lm_v3.jobchain_jobchainid_seq,
lm_v3.taxonomysource_taxonomysourceid_seq,
lm_v3.taxon_taxonid_seq,
lm_v3.keyword_keywordid_seq,
lm_v3.layertype_layertypeid_seq,
lm_v3.layer_layerid_seq,
lm_v3.scenario_scenarioid_seq,
lm_v3.occurrenceset_occurrencesetid_seq,
lm_v3.sdmmodel_sdmmodelid_seq,
lm_v3.sdmprojection_sdmprojectionid_seq,
lm_v3.boom_boomid_seq
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
