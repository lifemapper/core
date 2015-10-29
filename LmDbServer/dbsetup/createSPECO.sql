-- ----------------------------------------------------------------------------
-- file:   createSPECO.sql
-- author: Aimee Stewart
-- ----------------------------------------------------------------------------
-- ----------------------------------------------------------------------------
\c template1 admin

-- ----------------------------------------------------------------------------
CREATE DATABASE speco ENCODING='UTF8'
                    LC_COLLATE='en_US.UTF-8'
                    LC_CTYPE='en_US.UTF-8'
                    TEMPLATE=template1;
\c speco
-- ----------------------------------------------------------------------------

-- Note: LM_SCHEMA = 'lm3' is in LmServer.common.localconstants, 
--       originally set in config.ini
-- TODO: Take schema name out of local configuration
-- Note: Layer table is also present in 'topology' schema, so here must use
--       schema name when referring to ours
CREATE SCHEMA lm3;
ALTER DATABASE "speco" SET search_path=lm3,public;
GRANT USAGE ON SCHEMA lm3 TO reader, writer; 

-- ----------------------------------------------------------------------------
-- -------------------------------

-- -------------------------------
create table lm3.ComputeResource
(
   computeResourceId serial UNIQUE PRIMARY KEY,
   name varchar(32) NOT NULL,
   ipaddress varchar(16) UNIQUE NOT NULL,
   ipmask varchar(2),
   fqdn varchar(100),
   
   -- Contact info will be in mal.LMUser table
   userId varchar(32) NOT NULL,
   
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
   
   UNIQUE (jobFamily, referenceType, referenceId, stage)
);

-- ----------------------------------------------------------------------------

-- -------------------------------
create table lm3.Layer
(
   layerId serial UNIQUE PRIMARY KEY, --
   userId varchar(20) NOT NULL, --
   layername varchar(50) NOT NULL,
   title varchar(100),
   description varchar(256),
   dlocation varchar(256),
   metadataUrl varchar(256),
   layerurl varchar(512),
   ogrType int,
   gdalType int,
   -- GDAL/OGR codes indicating driver to use when writing files
   dataFormat varchar(32),
   epsgcode integer,
   mapunits varchar(20),
   resolution double precision,
   startDate double precision,
   endDate double precision,
   metalocation varchar(512), --
   datecreated double precision, --
   datelastmodified double precision, --
   bbox varchar(128),
   UNIQUE (userId, layername, epsgcode)
);
Select AddGeometryColumn('lm3', 'layer', 'geom', 4326, 'POLYGON', 2);
ALTER TABLE lm3.Layer ADD CONSTRAINT geometry_valid_check CHECK (st_isvalid(geom));
CREATE INDEX spidx_lyr ON lm3.Layer USING GIST ( geom );

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

-- -------------------------------
create table lm3.Experiment
(
   experimentId serial UNIQUE PRIMARY KEY,
   userId varchar(20) NOT NULL,
   expname varchar(100) NOT NULL,
   attrMatrixDlocation varchar(256),
   attrTreeDlocation varchar(256),
   email varchar(64),
   epsgcode int,
   keywords  varchar(256),
   metadataUrl varchar(256),
   description text,
   datelastmodified double precision,
   datecreated double precision,
   UNIQUE (userId, expname)
);

-- -------------------------------
create table lm3.Bucket
(
   bucketId serial UNIQUE PRIMARY KEY,
   experimentId int NOT NULL REFERENCES lm3.Experiment ON DELETE CASCADE,
   shapeGridId int NOT NULL REFERENCES lm3.ShapeGrid,
   slIndicesDlocation varchar(256),   
   -- Uncompressed PAM and GRIM
   pamDlocation varchar(256),
   grimDlocation varchar(256),
   keywords  varchar(256),
   metadataUrl varchar(256),
   -- status of current processing stage
   status int,
   statusmodtime double precision,
   -- Intersect only (Experiment envLayerset or orgLayerset with Bucket.shapegrid), 
   stage int,
   stagemodtime double precision,
   datecreated double precision,
   computeResourceId int REFERENCES lm3.ComputeResource,
   UNIQUE (experimentId, shapeGridId)
);

-- -------------------------------
create table lm3.PamSum
(
   pamSumId serial UNIQUE PRIMARY KEY,
   bucketId int NOT NULL REFERENCES lm3.Bucket ON DELETE CASCADE,
   randomMethod int NOT NULL,
   
   -- random-only fields
   randomParams text,
   -- uncompressed randomized (Splotch interim step)
   splotchPamDlocation varchar(256),
   splotchSitesDlocation varchar(256),   
   
   -- PAM written to a numpy file, SUM written to a pickled file
   pamDlocation varchar(256),
   sumDlocation varchar(256),
   
   metadataUrl varchar(256),

   -- status of current processing stage
   status int,
   statusmodtime double precision,
   
   -- Compress
   --       * fullPam to pam
   --       * splotchPam to pam
   -- Randomize
   --       * original Bucket.compressedPam.pam to new SWAP Random PamSum.pam 
   --       * original Bucket fullPAM to new Random PamSum.splotchPam 
   -- Calculate (pam to sum)
   stage int,
   stagemodtime double precision,
   computeResourceId int REFERENCES lm3.ComputeResource,
   
   datecreated double precision
);

-- -------------------------------
create table lm3.ExperimentPALayer
(
   experimentPALayerId  serial UNIQUE PRIMARY KEY,
   experimentId int NOT NULL REFERENCES lm3.Experiment ON DELETE CASCADE,
   layerId int NOT NULL REFERENCES lm3.Layer ON DELETE CASCADE,
   presenceAbsenceId int NOT NULL REFERENCES lm3.PresenceAbsence ON DELETE CASCADE,
   expPALayername varchar(20),
   -- initialized as -1
   matrixIdx int NOT NULL,
   UNIQUE (experimentId, layerId, presenceAbsenceId),
   UNIQUE (experimentId, matrixIdx)
);

-- -------------------------------
create table lm3.ExperimentAncLayer
(
   experimentAncLayerId  serial UNIQUE PRIMARY KEY,
   experimentId int NOT NULL REFERENCES lm3.Experiment ON DELETE CASCADE,
   layerId int NOT NULL REFERENCES lm3.Layer ON DELETE CASCADE,
   ancillaryValueId int NOT NULL REFERENCES lm3.AncillaryValue ON DELETE CASCADE,
   expAncLayername varchar(20),
   -- initialized as -1
   matrixIdx int NOT NULL,
   UNIQUE (experimentId, layerId, ancillaryValueId),
   UNIQUE (experimentId, matrixIdx)
);

-- -------------------------------
create table lm3.BucketPALayer
(
   bucketId int NOT NULL REFERENCES lm3.Bucket ON DELETE CASCADE,
   experimentPALayerId int REFERENCES lm3.ExperimentPALayer ON DELETE CASCADE,
   -- status of Intersect
   status int,
   statusmodtime double precision,
   computeResourceId int REFERENCES lm3.ComputeResource,
   UNIQUE (bucketId, experimentPALayerId)
);

-- -------------------------------
create table lm3.BucketAncLayer
(
   bucketId int NOT NULL REFERENCES lm3.Bucket ON DELETE CASCADE,
   experimentAncLayerId int REFERENCES lm3.ExperimentAncLayer ON DELETE CASCADE,
   -- status of Intersect
   status int,
   statusmodtime double precision,
   computeResourceId int REFERENCES lm3.ComputeResource,
   UNIQUE (bucketId, experimentAncLayerId)
);

-- -------------------------------
-- Tables lm3.ComputeResource, lm3.LMJob, lm3.LMUser in createCommon.sql

-- ----------------------------------------------------------------------------
GRANT SELECT ON TABLE 
lm3.layer, lm3.layer_layerid_seq,
lm3.presenceabsence, lm3.presenceabsence_presenceabsenceid_seq,
lm3.ancillaryvalue, lm3.ancillaryvalue_ancillaryvalueid_seq,
lm3.shapegrid, lm3.shapegrid_shapegridid_seq,
lm3.experiment, lm3.experiment_experimentid_seq,
lm3.bucket, lm3.bucket_bucketid_seq,
lm3.experimentpalayer, lm3.experimentpalayer_experimentpalayerid_seq,
lm3.experimentanclayer, lm3.experimentanclayer_experimentanclayerid_seq,
lm3.bucketpalayer, 
lm3.bucketanclayer,
lm3.pamsum, lm3.pamsum_pamsumid_seq,
lm3.computeresource, lm3.computeresource_computeresourceid_seq,
lm3.lmjob, lm3.lmjob_lmjobid_seq
TO GROUP reader;

GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE 
lm3.layer,
lm3.presenceabsence, 
lm3.ancillaryvalue,
lm3.shapegrid,
lm3.experiment,
lm3.bucket, 
lm3.experimentpalayer,
lm3.experimentanclayer,
lm3.bucketpalayer, 
lm3.bucketanclayer,
lm3.pamsum,
lm3.computeresource,
lm3.lmjob
TO GROUP writer;

GRANT SELECT, UPDATE ON TABLE 
lm3.layer, lm3.layer_layerid_seq,
lm3.presenceabsence, lm3.presenceabsence_presenceabsenceid_seq,
lm3.ancillaryvalue, lm3.ancillaryvalue_ancillaryvalueid_seq,
lm3.shapegrid, lm3.shapegrid_shapegridid_seq,
lm3.experiment, lm3.experiment_experimentid_seq,
lm3.bucket, lm3.bucket_bucketid_seq,
lm3.experimentpalayer, lm3.experimentpalayer_experimentpalayerid_seq,
lm3.experimentanclayer, lm3.experimentanclayer_experimentanclayerid_seq,
lm3.bucketpalayer, 
lm3.bucketanclayer,
lm3.pamsum, lm3.pamsum_pamsumid_seq,
lm3.computeresource, lm3.computeresource_computeresourceid_seq,
lm3.lmjob, lm3.lmjob_lmjobid_seq
TO GROUP writer;
      
