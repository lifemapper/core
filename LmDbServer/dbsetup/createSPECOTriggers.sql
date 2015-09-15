-- ----------------------------------------------------------------------------
-- ----------------------------------------------------------------------------
-- These procedures depend on the values of:
--   JobStatus.INITIALIZE, JobStatus.COMPLETE and JobStatus.GENERAL_ERROR 
-- in LmCommon.common.lmconstants
-- ----------------------------------------------------------------------------
\c speco
-- ----------------------------------------------------------------------------

CREATE FUNCTION lm3.lm_triggerBucketDependencies() 
   RETURNS trigger AS $lm_triggerBucketDependencies$
   BEGIN
      -- Intersect succeed
      IF NEW.stage = 10 AND NEW.status = 300 THEN
         begin
            -- Ready Compress Original Pamsum (random = 0)
            UPDATE lm3.Pamsum SET (status, statusmodtime) = (1, NEW.statusmodtime) 
               WHERE bucketId = NEW.bucketId 
                 AND randomMethod = 0 AND stage = 20 AND status = 0;
            UPDATE lm3.LMJob SET (status, statusmodtime) = (1, NEW.statusmodtime)
               WHERE referenceType = 203 
                 AND referenceID in 
                   (SELECT pamsumId from lm3.Pamsum WHERE bucketId = NEW.bucketId)  
                 AND stage = 20 AND status = 0;
               
            -- Ready Splotch
            UPDATE lm3.Pamsum SET (status, statusmodtime) = (1, NEW.statusmodtime)
               WHERE bucketId = NEW.bucketId AND stage = 32 AND status = 0;
            UPDATE lm3.LMJob SET (status, statusmodtime) = (1, NEW.statusmodtime)
               WHERE referenceType = 204 
                 AND referenceID in 
                   (SELECT pamsumId from lm3.Pamsum WHERE bucketId = NEW.bucketId)  
                 AND stage = 32 AND status = 0;
         end;
         
      -- Intersect fail
      ELSEIF NEW.stage = 10 AND NEW.status >= 1000 THEN
         begin
            -- Delete Pamsum jobs
            DELETE FROM lm3.LMJob 
               WHERE referenceType in (203,204) 
                 AND referenceId in 
                   (SELECT pamsumId from lm3.Pamsum WHERE bucketId = NEW.bucketId);
            -- Ready Notify
            UPDATE lm3.LMJob SET (status, statusmodtime) = (1, NEW.statusmodtime)
               WHERE referenceType = 202 AND referenceID = NEW.bucketId AND stage = 500; 
         end;
      END IF;
   END;
$lm_triggerBucketDependencies$ LANGUAGE plpgsql;


CREATE TRIGGER lm_triggerBucketDependencies AFTER UPDATE OF status ON lm3.Bucket
   FOR EACH ROW EXECUTE PROCEDURE lm3.lm_triggerBucketDependencies();


-- ----------------------------------------------------------------------------
CREATE FUNCTION lm3.lm_triggerPamsumDependencies() 
   RETURNS trigger AS $lm_triggerPamsumDependencies$
   BEGIN
      -- Compress succeed
      IF NEW.stage = 20 AND NEW.status = 300 THEN
         begin
            -- Ready Calculate 
            UPDATE lm3.Pamsum SET (status, statusmodtime, stage, stagemodtime) 
                                = (1, NEW.statusmodtime) 
               WHERE pamsumId = NEW.pamsumId 
                 AND randomMethod = 0 AND stage = 20 AND status = 0;
            UPDATE lm3.LMJob SET (status, statusmodtime) = (1, NEW.statusmodtime)
               WHERE referenceType = 203 
                 AND referenceID in 
                   (SELECT pamsumId from lm3.Pamsum WHERE bucketId = NEW.bucketId)  
                 AND stage = 20 AND status = 0;
               
            -- Ready Splotch
            UPDATE lm3.Pamsum SET (status, statusmodtime) = (1, NEW.statusmodtime)
               WHERE bucketId = NEW.bucketId AND stage = 32 AND status = 0;
            UPDATE lm3.LMJob SET (status, statusmodtime) = (1, NEW.statusmodtime)
               WHERE referenceType = 204 
                 AND referenceID in 
                   (SELECT pamsumId from lm3.Pamsum WHERE bucketId = NEW.bucketId)  
                 AND stage = 32 AND status = 0;
         end;
         
      -- Intersect fail
      ELSEIF NEW.stage = 10 AND NEW.status >= 1000 THEN
         begin
            -- Delete Pamsum jobs
            DELETE FROM lm3.LMJob 
               WHERE referenceType in (203,204) 
                 AND referenceId in 
                   (SELECT pamsumId from lm3.Pamsum WHERE bucketId = NEW.bucketId)
            -- Ready Notify
            UPDATE lm3.LMJob SET (status, statusmodtime) = (1, NEW.statusmodtime)
               WHERE referenceType = 202 AND referenceID = NEW.bucketId AND stage = 500; 
         end;
      END IF;
   END;
$lm_triggerPamsumDependencies$ LANGUAGE plpgsql;


CREATE TRIGGER lm3.lm_triggerPamsumDependencies AFTER UPDATE OF status ON lm3.Bucket
   FOR EACH STATEMENT EXECUTE PROCEDURE lm3.lm_triggerPamsumDependencies();


   