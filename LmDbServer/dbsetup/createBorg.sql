-- ----------------------------------------------------------------------------
-- @license: gpl2
-- @copyright: Copyright (C) 2017, University of Kansas Center for Research
 
--           Lifemapper Project, lifemapper [at] ku [dot] edu, 
--           Biodiversity Institute,
--           1345 Jayhawk Boulevard, Lawrence, Kansas, 66045, USA
--     
--           This program is free software; you can redistribute it and/or  
--           modify it under the terms of the GNU General Public License as 
--           published by the Free Software Foundation; either version 2 of the 
--           License, or (at your option) any later version.
--    
--           This program is distributed in the hope that it will be useful, but 
--           WITHOUT ANY WARRANTY; without even the implied warranty of 
--           MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU 
--           General Public License for more details.
--    
--           You should have received a copy of the GNU General Public License 
--           along with this program; if not, write to the Free Software 
--           Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 
--           02110-1301, USA.
-- ----------------------------------------------------------------------------

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
-- Modifier (for everything)
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
-- Modifier (for Taxon)
create table lm_v3.TaxonomySource
(
   taxonomySourceId serial UNIQUE PRIMARY KEY,
   url text UNIQUE,
   datasetIdentifier text UNIQUE,
   modTime double precision
);

-- -------------------------------
-- Modifier (for Layer, MatrixColumn, others)
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
-- Modifier (for Layer)
create table lm_v3.EnvType
(
   envTypeId serial UNIQUE PRIMARY KEY,
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
   modTime double precision,
   UNIQUE (userid, envCode, gcmCode, altpredCode, dateCode)
);
   
-- -------------------------------
-- Object
-- TODO: Enforcee unique userid/name/epsg for display layers only?
create table lm_v3.Layer
(
   layerId serial UNIQUE PRIMARY KEY,
   userid varchar(20) NOT NULL REFERENCES lm_v3.LMUser ON DELETE CASCADE,
   squid varchar(64) REFERENCES lm_v3.Taxon(squid),
   verify varchar(64),
   name text,
   dlocation text,
   -- metadataUrl text UNIQUE,

   -- JSON with title, author, description ...
   metadata text,

   -- GDAL/OGR codes indicating driver to use when writing files
   dataFormat varchar(32),
   gdalType int,
   ogrType int,
   
   -- Data descriptors
   valUnits varchar(60),				-- measurement units or 'categorical'
   valAttribute varchar(20),			-- fieldname or 'pixel'
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
 CREATE INDEX spidx_layer ON lm_v3.Layer USING GIST ( geom );
 CREATE INDEX idx_lyrSquid on lm_v3.Layer(squid);
 CREATE INDEX idx_lyrVerify on lm_v3.Layer(verify);

-- -------------------------------
-- Object (via join)
create table lm_v3.EnvLayer
(
   envLayerId serial UNIQUE PRIMARY KEY,
   envTypeId int NOT NULL REFERENCES lm_v3.EnvType ON DELETE CASCADE,
   layerId int NOT NULL REFERENCES lm_v3.Layer ON DELETE CASCADE,
   UNIQUE (envTypeId, layerId)
);

-- -------------------------------
-- Object
 create table lm_v3.Scenario
 (
    scenarioId serial UNIQUE PRIMARY KEY,
    userid  varchar(20) NOT NULL REFERENCES lm_v3.LMUser ON DELETE CASCADE,
    scenarioCode varchar(60),
    -- metadataUrl text UNIQUE,

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
-- Object
create table lm_v3.ScenPackage
(
	scenPackageId serial UNIQUE PRIMARY KEY,
   userid  varchar(20) NOT NULL REFERENCES lm_v3.LMUser ON DELETE CASCADE,
   name varchar(60) NOT NULL,
   metadata text,
   
   units varchar(20),
   epsgcode int,
   bbox varchar(60),

   modTime double precision,
   UNIQUE (name, userid)
);
Select AddGeometryColumn('lm_v3', 'scenpackage', 'geom', 4326, 'POLYGON', 2);
ALTER TABLE lm_v3.ScenPackage ADD CONSTRAINT geometry_valid_check CHECK (st_isvalid(geom));
ALTER TABLE lm_v3.ScenPackage ADD CONSTRAINT enforce_srid_geom CHECK (st_srid(geom) = 4326);
ALTER TABLE lm_v3.ScenPackage ADD CONSTRAINT enforce_dims_geom CHECK (st_ndims(geom) = 2);
CREATE INDEX spidx_scenpackage ON lm_v3.ScenPackage USING GIST ( geom );

-- -------------------------------
-- Object (via join)
create table lm_v3.ScenPackageScenario
(
	scenPackageScenarioId serial UNIQUE PRIMARY KEY,
	scenPackageId int REFERENCES lm_v3.ScenPackage MATCH FULL ON DELETE CASCADE,
   scenarioId int REFERENCES lm_v3.Scenario MATCH FULL ON DELETE CASCADE,
   UNIQUE (scenPackageId, scenarioId)
);

-- -------------------------------
-- Object (via join)
create table lm_v3.ScenarioLayer
(
	scenarioLayerId serial UNIQUE PRIMARY KEY,
   scenarioId int REFERENCES lm_v3.Scenario MATCH FULL ON DELETE CASCADE,
   envLayerId int REFERENCES lm_v3.EnvLayer ON DELETE CASCADE
);

-- -------------------------------
-- Process (for objects with more than one process)
-- after process is complete, move params/inputs to metadata on object, then delete
create table lm_v3.Process
(
   processId serial UNIQUE PRIMARY KEY,
   -- LmCommon.common.lmconstants.ProcessType
   processType int,
	--input
   inputId int,
   --output 
   outputId int,

   status int,
   statusmodtime double precision
);

-- -------------------------------
-- Process and Output Object
create table lm_v3.OccurrenceSet 
(
   occurrenceSetId serial UNIQUE PRIMARY KEY,
   userId varchar(20) NOT NULL REFERENCES lm_v3.LMUser ON DELETE CASCADE,
   squid varchar(64) REFERENCES lm_v3.Taxon(squid),
   verify varchar(64),
   displayName text,
   -- metadataUrl text UNIQUE,
   dlocation text,
   rawDlocation text,
   queryCount int,
   bbox varchar(60),
   epsgcode integer,
   metadata text,
   status int,
   statusModTime double precision,
   UNIQUE (userId, squid, epsgcode)
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
-- Metadata (options for input to SDMProject Process)
create table lm_v3.Algorithm
(
   algorithmCode varchar(30) UNIQUE PRIMARY KEY,
   metadata text,
   modTime double precision
);

-- -------------------------------
-- Process and Output Object (1-to-1 join with Layer)
create table lm_v3.SDMProject
(
   sdmProjectId serial UNIQUE PRIMARY KEY,
   -- output
   layerid int UNIQUE NOT NULL REFERENCES lm_v3.Layer ON DELETE CASCADE,
   userId varchar(20) NOT NULL REFERENCES lm_v3.LMUser ON DELETE CASCADE,
   
   -- inputs
   occurrenceSetId int REFERENCES lm_v3.OccurrenceSet ON DELETE CASCADE,
   algorithmCode varchar(30) NOT NULL REFERENCES lm_v3.Algorithm(algorithmCode),
   algParams text,
   mdlscenarioId int REFERENCES lm_v3.Scenario ON DELETE CASCADE,
   mdlmaskId int REFERENCES lm_v3.Layer,
   prjscenarioId int REFERENCES lm_v3.Scenario ON DELETE CASCADE,
   prjmaskId int REFERENCES lm_v3.Layer,
   -- includes algorithmParams
   metadata text,

   -- LmCommon.common.lmconstants.ProcessType
   processType int,
   						
   status int,
   statusModTime double precision,
   UNIQUE (userId, occurrenceSetId, algorithmCode, algParams, 
           mdlscenarioId, mdlmaskId, prjscenarioId, prjmaskId)
);  
CREATE INDEX idx_prjStatusModTime ON lm_v3.SDMProject(statusModTime);
CREATE INDEX idx_prjStatus ON lm_v3.SDMProject(status);
CREATE INDEX idx_layerid ON lm_v3.SDMProject(layerid);

-- -------------------------------
-- Process and Output Object (1-to-1 join with Layer)
create table lm_v3.ShapeGrid
(
   layerId int UNIQUE NOT NULL REFERENCES lm_v3.Layer ON DELETE CASCADE,
   cellsides int,
   cellsize double precision,
   vsize int,
   idAttribute varchar(20),
   xAttribute varchar(20),
   yAttribute varchar(20),
   status int,
   statusModTime double precision,
   PRIMARY KEY (layerid)
);
CREATE INDEX idx_shpgrdStatusModTime ON lm_v3.ShapeGrid(statusModTime);
CREATE INDEX idx_shpgrdStatus ON lm_v3.ShapeGrid(status);

-- -------------------------------
-- original Tree in user space
-- Object
create table lm_v3.Tree 
(
   treeId serial UNIQUE PRIMARY KEY,
   userId varchar(20) NOT NULL REFERENCES lm_v3.LMUser ON DELETE CASCADE,
   name varchar(100) NOT NULL,
   dlocation text,
   
   isBinary boolean,
   isUltrametric boolean,
   hasBranchLengths boolean,
   
   metadata text,
   modTime double precision,
   UNIQUE (userId, name)
);

-- -------------------------------
-- Organizing object for set of matrices all using same grid/extent/resolution
create table lm_v3.Gridset
(
   gridsetId serial UNIQUE PRIMARY KEY,
   userId varchar(20) NOT NULL REFERENCES lm_v3.LMUser ON DELETE CASCADE,
   name varchar(100) NOT NULL,
   treeId int REFERENCES lm_v3.Tree,   
   
   -- optional shapegrid 
   layerId int REFERENCES lm_v3.ShapeGrid,
   
   -- Optional configuration file created by boominput for boomer workflow
   dlocation text,

   epsgcode int,
   metadata text,
   modTime double precision,
   UNIQUE (userId, name)
);

-- -------------------------------
-- Process and Output Object (Makeflow document, created by archivist)
-- Status is aggregation of component process statuses
create table lm_v3.MFProcess
(
   mfProcessId serial UNIQUE PRIMARY KEY,
   userid varchar(20) NOT NULL REFERENCES lm_v3.LMUser ON DELETE CASCADE,
   dlocation text,
   priority int,
   metadata text,
   status int,
   statusmodtime double precision
);

-- -------------------------------
-- In Gridset space: PAM, GRIM, BioGeoMtx, MCPA output
-- Object 
-- Status is aggregation of component MatrixColumn statuses
create table lm_v3.Matrix
(
   matrixId serial UNIQUE PRIMARY KEY,
   -- Constants in LmCommon.common.lmconstants.MatrixType
   matrixType int NOT NULL,
   gridsetId int NOT NULL REFERENCES lm_v3.Gridset ON DELETE CASCADE,

   -- ONLY relevant for PAM
   -- Codes for GCM, RCP, past/current/projected Date
   gcmCode varchar(20),
   altpredCode varchar(20),
   dateCode varchar(20),

   matrixDlocation text,
   -- metadataUrl text UNIQUE,
   metadata text,
   status int,
   statusmodtime double precision,
   UNIQUE (gridsetId, matrixType, gcmCode, altpredCode, dateCode)
);

-- -------------------------------
-- aka PAV or GRIM Vector, 
-- Process and Output Object
-- Process IntersectParams: in LmServer.legion.matrixColumn.MatrixColumn.PARAM_*
--    filterString, valName, valUnits, minPercent, weightedMean, largestClass, 
--    minPresence, maxPresence
create table lm_v3.MatrixColumn 
(
   matrixColumnId  serial UNIQUE PRIMARY KEY,
   -- Global PAM will have an entry
   matrixId int NOT NULL REFERENCES lm_v3.Matrix ON DELETE CASCADE,
   matrixIndex int,
	
   squid varchar(64) REFERENCES lm_v3.Taxon(squid),
   ident varchar(64),
   metadata text, 
   
   -- if process
   layerId int REFERENCES lm_v3.Layer,
   intersectParams text,
   status int,
   statusmodtime double precision,
   
   -- matrixIndex is NULL for Global PAM 
   UNIQUE (matrixId, matrixIndex),
   -- TODO: will Global PAM only reference a layer once? 
   UNIQUE (matrixId, layerid, intersectParams)
);
CREATE INDEX idx_mtxcolStatusModTime ON lm_v3.MatrixColumn(statusModTime);
CREATE INDEX idx_mtxcolStatus ON lm_v3.MatrixColumn(status);
-- ----------------------------------------------------------------------------

GRANT SELECT ON TABLE 
lm_v3.lmuser, 
lm_v3.taxonomysource, lm_v3.taxonomysource_taxonomysourceid_seq,
lm_v3.taxon, lm_v3.taxon_taxonid_seq,
lm_v3.envtype, lm_v3.envtype_envtypeid_seq,
lm_v3.layer, lm_v3.layer_layerid_seq, 
lm_v3.envlayer, lm_v3.envlayer_envlayerid_seq,
lm_v3.scenario, lm_v3.scenario_scenarioid_seq,
lm_v3.scenpackage, lm_v3.scenpackage_scenpackageid_seq,
lm_v3.scenpackagescenario, lm_v3.scenpackagescenario_scenpackagescenarioid_seq,
lm_v3.scenariolayer, lm_v3.scenariolayer_scenariolayerid_seq,
lm_v3.process, lm_v3.process_processid_seq,
lm_v3.occurrenceset, lm_v3.occurrenceset_occurrencesetid_seq, 
lm_v3.algorithm, 
lm_v3.sdmproject, lm_v3.sdmproject_sdmprojectid_seq,
lm_v3.shapegrid, 
lm_v3.tree, lm_v3.tree_treeid_seq,
lm_v3.gridset, lm_v3.gridset_gridsetid_seq,
lm_v3.mfprocess, lm_v3.mfprocess_mfprocessid_seq,
lm_v3.matrix, lm_v3.matrix_matrixid_seq,
lm_v3.matrixcolumn, lm_v3.matrixcolumn_matrixcolumnid_seq
TO GROUP reader;

GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE 
lm_v3.lmuser, 
lm_v3.taxonomysource,
lm_v3.taxon,
lm_v3.envtype,
lm_v3.layer, 
lm_v3.envlayer,  
lm_v3.scenario,
lm_v3.scenpackage, 
lm_v3.scenpackagescenario,
lm_v3.scenariolayer,
lm_v3.process,
lm_v3.occurrenceset, 
lm_v3.algorithm, 
lm_v3.sdmproject,
lm_v3.shapegrid,
lm_v3.tree,
lm_v3.gridset,
lm_v3.mfprocess,
lm_v3.matrix,
lm_v3.matrixcolumn
TO GROUP writer;

GRANT SELECT, UPDATE ON TABLE 
lm_v3.taxonomysource_taxonomysourceid_seq,
lm_v3.taxon_taxonid_seq,
lm_v3.envtype_envtypeid_seq,
lm_v3.layer_layerid_seq,
lm_v3.envlayer_envlayerid_seq,
lm_v3.scenario_scenarioid_seq,
lm_v3.scenpackage_scenpackageid_seq,
lm_v3.scenpackagescenario_scenpackagescenarioid_seq,
lm_v3.scenariolayer_scenariolayerid_seq,
lm_v3.occurrenceset_occurrencesetid_seq,
lm_v3.sdmproject_sdmprojectid_seq,
lm_v3.process_processid_seq,
lm_v3.tree_treeid_seq,
lm_v3.gridset_gridsetid_seq,
lm_v3.mfprocess_mfprocessid_seq,
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
