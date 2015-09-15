-- ----------------------------------------------------------------------------
\c mal

--ALTER TABLE lm3.lmjob DROP CONSTRAINT IF EXISTS lmjob_jobfamily_referencetype_referenceid_stage_key;
--ALTER TABLE lm3.lmjob ADD CONSTRAINT lmjob_jobfamily_referencetype_referenceid_reqsoftware_key 
--   UNIQUE (jobFamily, referenceType, referenceId, reqSoftware);
ALTER TABLE lm3.scenario DROP CONSTRAINT IF EXISTS scenario_scenariocode_key;
ALTER TABLE lm3.scenario ADD CONSTRAINT scenario_scenariocode_userid_key 
   UNIQUE (scenarioCode, userid);


-- ----------------------------------------------------------------------------
\c speco
-- New columns also change views: lm_fullradbucket, lm_shapegrid, 
--                                lm_intjob, lm_mtxjob, lm_grdjob
ALTER TABLE lm3.shapegrid ADD COLUMN status int;
ALTER TABLE lm3.shapegrid ADD COLUMN statusmodtime double precision;
ALTER TABLE lm3.shapegrid ADD COLUMN computeResourceId int REFERENCES lm3.ComputeResource

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

-- ----------------------------------------------------------------------------
