-- ----------------------------------------------------------------------------
-- file:   createBorg.sql
-- author: Aimee Stewart
-- \i /opt/lifemapper/LmDbServer/dbsetup/createBorg.sql
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
create table lm_v3.TaxonomySource
(
   taxonomySourceId serial UNIQUE PRIMARY KEY,
   url text UNIQUE,
   datasetIdentifier text UNIQUE,
   modTime double precision
);

-- -------------------------------
create table lm_v3.JobChain
(
   jobchainId serial UNIQUE PRIMARY KEY,
   userid varchar(20) NOT NULL REFERENCES lm_v3.LMUser ON DELETE CASCADE,
   dlocation text,
   priority int,
   status int,
   statusmodtime double precision
);

-- -------------------------------
create table lm_v3.Taxon
(
   taxonId serial UNIQUE PRIMARY KEY,
   taxonomySourceId int REFERENCES lm_v3.TaxonomySource,
   userid varchar(20) REFERENCES lm_v3.LMUser ON DELETE CASCADE,
   taxonomyKey int,
   -- hash of userid/sciname or taxonomySourceId/taxonomyKey
   squid varchar(64) NOT NULL UNIQUE,
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
   -- Species-thread/squid using taxonomy provider
   UNIQUE (taxonomySourceId, taxonomyKey),
   -- Unhinged species-thread/squid for users 
   UNIQUE (userid, sciname)
);
CREATE INDEX taxon_squid on lm_v3.Taxon(squid);
CREATE INDEX idx_lower_canonical on lm_v3.Taxon(lower(canonical));
CREATE INDEX idx_lower_sciname on lm_v3.Taxon(lower(sciname));
CREATE INDEX idx_lower_genus on lm_v3.Taxon(lower(genus));

-- -------------------------------
create table lm_v3.EnvironmentalType
(
   environmentalTypeId serial UNIQUE PRIMARY KEY,
   userid varchar(20) NOT NULL REFERENCES lm_v3.LMUser ON DELETE CASCADE,
   
   -- type of data (elevation, bioclimatic variable types , etc)
   envCode varchar(20),
   -- Global Climate Model
   gcmCode varchar(20),
   -- Representative Concentration Pathways (AR5+) or Scenario Family (AR4 and earlier) 
   altpredCode varchar(20),
   -- Environmental conditions for date (past = mid, lgm; current = 1950-2000, AR5 = 2050, 2070)
   dateCode varchar(20),
   
   metadata text,
   modTime double precision
);
ALTER TABLE lm_v3.EnvironmentalType ADD CONSTRAINT unique_environmentalType 
   UNIQUE (userid, envCode, gcmCode, altpredCode, dateCode);

-- -------------------------------
-- TODO: Enforce unique userid/name/epsg for display layers only?
create table lm_v3.Layer
(
   layerId serial UNIQUE PRIMARY KEY,
   userid varchar(20) NOT NULL REFERENCES lm_v3.LMUser ON DELETE CASCADE,
   squid varchar(64) REFERENCES lm_v3.Taxon(squid),
   verify varchar(64),
   name text,
   dlocation text,
   metadataUrl text UNIQUE,

   -- JSON with title, author, description, valAttribute if vector data
   metadata text,

   -- GDAL/OGR codes indicating driver to use when writing files
   dataFormat varchar(32),
   gdalType int,
   ogrType int,
   
   -- valunits=categorical if non-scalar
   valUnits varchar(60),
   nodataVal double precision,
   minVal double precision,
   maxVal double precision,

   epsgcode int,
   mapunits varchar(20),
   resolution double precision,
   bbox varchar(60),
   modTime double precision,
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

    -- JSON with title, author, description
    metadata text,
    
    -- Codes for GCM, RCP, past/current/projected Date
    gcmCode varchar(20),
    altpredCode varchar(20),
    dateCode varchar(20),
    
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
-- Join table
create table lm_v3.ScenarioLayer
(
   scenarioId int REFERENCES lm_v3.Scenario MATCH FULL ON DELETE CASCADE,
   layerId int NOT NULL REFERENCES lm_v3.Layer ON DELETE CASCADE,
   environmentalTypeId int REFERENCES lm_v3.EnvironmentalType,
   PRIMARY KEY (scenarioId, layerId, environmentalTypeId)
);

-- -------------------------------
create table lm_v3.OccurrenceSet
(
   occurrenceSetId serial UNIQUE PRIMARY KEY,
   squid varchar(64) REFERENCES lm_v3.Taxon(squid),
   verify varchar(64),
   userId varchar(20) NOT NULL REFERENCES lm_v3.LMUser ON DELETE CASCADE,
   displayName text,
   metadataUrl text UNIQUE,
   dlocation text,
   rawDlocation text,
   queryCount int,
   bbox varchar(60),
   epsgcode integer,
   metadata text,
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
CREATE INDEX idx_min_queryCount ON lm_v3.OccurrenceSet((queryCount >= 30));
CREATE INDEX idx_occUserId ON lm_v3.OccurrenceSet(userId);
CREATE INDEX idx_occStatus ON lm_v3.OccurrenceSet(status);
CREATE INDEX idx_occStatusModTime ON lm_v3.OccurrenceSet(statusModTime);
CREATE INDEX idx_occSquid on lm_v3.OccurrenceSet(squid);

-- -------------------------------
create table lm_v3.Algorithm
(
   algorithmCode varchar(30) UNIQUE PRIMARY KEY,
   metadata text,
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
   occurrenceSetId int REFERENCES lm_v3.OccurrenceSet ON DELETE CASCADE,
   scenarioId int REFERENCES lm_v3.Scenario ON DELETE CASCADE,
   scenarioCode varchar(30),
   maskId int REFERENCES lm_v3.Layer,
   status int,
   statusModTime double precision,
   dlocation text,
   email varchar(64), 
   algorithmParams text,
   algorithmCode varchar(30) NOT NULL REFERENCES lm_v3.Algorithm(algorithmCode)
);
CREATE INDEX idx_mdlStatusModTime ON lm_v3.SDMModel(statusModTime);
CREATE INDEX idx_mdlUserId ON lm_v3.SDMModel(userId);
CREATE INDEX idx_mdlStatus ON lm_v3.SDMModel(status);

-- -------------------------------
-- Holds projection of a ruleset on to a set of environmental layers
-- 
create table lm_v3.SDMProjection
(
   sdmprojectionId serial UNIQUE PRIMARY KEY,
   layerid int NOT NULL REFERENCES lm_v3.Layer ON DELETE CASCADE,
   sdmmodelid int REFERENCES lm_v3.SDMModel ON DELETE CASCADE,
   scenarioId int REFERENCES lm_v3.Scenario ON DELETE CASCADE,
   scenarioCode varchar(30),
   maskId int REFERENCES lm_v3.Layer,
   status int,
   statusModTime double precision
);  
CREATE INDEX idx_prjStatusModTime ON lm_v3.SDMProjection(statusModTime);
CREATE INDEX idx_prjStatus ON lm_v3.SDMProjection(status);

-- -------------------------------
create table lm_v3.Process
(
   processId serial UNIQUE PRIMARY KEY,
   processType int NOT NULL,
   referenceType int NOT NULL,
   referenceId int NOT NULL,
   userid varchar(20) REFERENCES lm_v3.LMUser ON DELETE CASCADE,
   isSinglespecies boolean NOT NULL,
   inputs text,
   outputs text,
   dlocation text,
   status int,
   statusmodtime double precision
);

-- -------------------------------
-- One to one relationship with Layer
create table lm_v3.ShapeGrid
(
   shapeGridId serial UNIQUE PRIMARY KEY,
   layerId int UNIQUE NOT NULL REFERENCES lm_v3.Layer ON DELETE CASCADE,
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
-- original Tree in user space, or modified tree in Bucket
create table lm_v3.Tree 
(
   treeId serial UNIQUE PRIMARY KEY,
   userId varchar(20) NOT NULL REFERENCES lm_v3.LMUser ON DELETE CASCADE,
   -- original (Newick or JSON)
   treeDlocation text,
   hasBranchLengths boolean,
   isUltrametric boolean,
   metadata text,
   modTime double precision
);

-- -------------------------------
-- Organizing object for set of layers/computations
create table lm_v3.Bucket
(
   bucketId serial UNIQUE PRIMARY KEY,
   userId varchar(20) NOT NULL REFERENCES lm_v3.LMUser ON DELETE CASCADE,
   name varchar(100) NOT NULL,
   
   -- Must have shapegrid or siteIndices (siteId, centerX, centerY)
   shapeGridId int REFERENCES lm_v3.ShapeGrid,
   siteIndices text,
   
   treeId int REFERENCES lm_v3.Tree,
   epsgcode int,
   metadata text,
   modTime double precision,
   UNIQUE (userId, name)
);

-- -------------------------------
-- In Bucket space: PAM, GRIM, BioGeoMtx, MCPA output
create table lm_v3.Matrix
(
   matrixId serial UNIQUE PRIMARY KEY,
   -- Constants in LmCommon.common.lmconstants.MatrixType
   matrixType int NOT NULL,
   bucketId int NOT NULL REFERENCES lm_v3.Bucket ON DELETE CASCADE,
   matrixDlocation text,
   siteLayerIndices text,
   metadata text,  
   status int,
   statusmodtime double precision
);

-- -------------------------------
-- Join user Tree to a bucket
create table lm_v3.BucketTree 
(
   bucketTreeId serial UNIQUE PRIMARY KEY,
   treeId int NOT NULL REFERENCES lm_v3.Tree ON DELETE CASCADE,
   bucketId int NOT NULL REFERENCES lm_v3.Bucket ON DELETE CASCADE,
   isPruned boolean,
   isBinary boolean,
   
   -- TODO: Names?!
   -- JSON, with PAM_MtxIds, used for display, input for RAD, MCPA
   treePamLinkDlocation text,
   -- Encoded Tree Matrix, used in MCPA calcs, can be used with multiple BioGeoHypotheses
   treeEncodedMatrixDlocation text,
   -- TreeCorrelationLink, JSON, used for display
   treeCorrLinkDlocation text,

   status int,
   statusmodtime double precision
);

-- -------------------------------
-- aka PAV, PAM Vector or GRIM Vector
create table lm_v3.MatrixColumn 
(
   matrixColumnId  serial UNIQUE PRIMARY KEY,
   bucketId int NOT NULL REFERENCES lm_v3.Bucket ON DELETE CASCADE,

   -- layerId could be empty, just squid or ident
   layerId int REFERENCES lm_v3.Layer,
   squid varchar(64) REFERENCES lm_v3.Taxon(squid),
   ident varchar(64),

   matrixId int NOT NULL REFERENCES lm_v3.Matrix ON DELETE CASCADE,
   matrixIndex int NOT NULL,
   
   -- filterString, nameValue, minPercent, weightedMean, largestClass, 
   -- minPresence, maxPresence
   intersectParams text,   
      
   metadata text,  
   status int,
   statusmodtime double precision,
   UNIQUE (bucketId, matrixIndex),
   UNIQUE (bucketId, layerId, intersectParams)
);

-- ----------------------------------------------------------------------------

GRANT SELECT ON TABLE 
lm_v3.lmuser, 
lm_v3.jobchain, lm_v3.jobchain_jobchainid_seq,
lm_v3.taxonomysource, lm_v3.taxonomysource_taxonomysourceid_seq,
lm_v3.taxon, lm_v3.taxon_taxonid_seq,
lm_v3.environmentalType, lm_v3.environmentalType_environmentalTypeid_seq,
lm_v3.layer, lm_v3.layer_layerid_seq, 
--  lm_v3.environmentallayer, lm_v3.environmentallayer_environmentallayerid_seq, 
lm_v3.scenario, lm_v3.scenario_scenarioid_seq,
lm_v3.scenariolayer,
lm_v3.occurrenceset, lm_v3.occurrenceset_occurrencesetid_seq, 
lm_v3.algorithm, 
lm_v3.sdmmodel, lm_v3.sdmmodel_sdmmodelid_seq, 
lm_v3.sdmprojection, lm_v3.sdmprojection_sdmprojectionid_seq,
lm_v3.shapegrid, lm_v3.shapegrid_shapegridid_seq,
lm_v3.bucket, lm_v3.bucket_bucketid_seq,
lm_v3.matrix, lm_v3.matrix_matrixid_seq,
lm_v3.buckettree, lm_v3.buckettree_buckettreeid_seq,
lm_v3.matrixcolumn, lm_v3.matrixcolumn_matrixcolumnid_seq
TO GROUP reader;

GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE 
lm_v3.lmuser, 
lm_v3.jobchain,
lm_v3.taxonomysource,
lm_v3.taxon,
lm_v3.environmentalType,
lm_v3.layer, 
--  lm_v3.environmentallayer,  
lm_v3.scenario,
lm_v3.scenariolayer,
lm_v3.occurrenceset, 
lm_v3.algorithm, 
lm_v3.sdmmodel,  
lm_v3.sdmprojection,
lm_v3.shapegrid,
lm_v3.bucket,
lm_v3.buckettree,
lm_v3.matrix,
lm_v3.matrixcolumn
TO GROUP writer;

GRANT SELECT, UPDATE ON TABLE 
lm_v3.jobchain_jobchainid_seq,
lm_v3.taxonomysource_taxonomysourceid_seq,
lm_v3.taxon_taxonid_seq,
lm_v3.environmentalType_environmentalTypeid_seq,
lm_v3.layer_layerid_seq,
--  lm_v3.environmentallayer_environmentallayerid_seq, 
lm_v3.scenario_scenarioid_seq,
lm_v3.occurrenceset_occurrencesetid_seq,
lm_v3.sdmmodel_sdmmodelid_seq,
lm_v3.sdmprojection_sdmprojectionid_seq,
lm_v3.shapegrid_shapegridid_seq,
lm_v3.bucket_bucketid_seq,
lm_v3.buckettree_buckettreeid_seq,
lm_v3.matrix_matrixid_seq,
lm_v3.matrixcolumn_matrixcolumnid_seq
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
