# mal=> \o /home/astewart/notaxOccids.txt
# mal=> select occurrencesetid from occurrenceset where userid = 'lm2' and scientificnameid is null;
# mal=> \o /home/astewart/subtaxOccids.txt
# mal=> select occurrencesetid from lm_fulloccurrenceset where userid = 'lm2' and scientificnameid is not null and taxonomykey != specieskey and taxonomykey != genuskey;
# mal=> \q



-- -----------------------------------------------------------------------------
-- Delete all jobs associated with old (with no taxonomy) occurrencesets
-- -----------------------------------------------------------------------------
DELETE FROM lmjob WHERE lmjobid IN 
   (SELECT lmjobid FROM lm_prjJob 
      WHERE occuserid = 'lm2' AND scientificnameid IS NULL);
   
DELETE FROM lmjob WHERE lmjobid IN 
   (SELECT lmjobid FROM lm_mdlJob 
      WHERE occuserid = 'lm2' AND scientificnameid IS NULL);

DELETE FROM lmjob WHERE lmjobid IN 
   (SELECT lmjobid FROM lm_occJob 
      WHERE userid = 'lm2' AND scientificnameid IS NULL);
   
-- -----------------------------------------------------------------------------
-- Delete all projections, models, occurrencesets associated with old (with no taxonomy) occurrencesets
-- -----------------------------------------------------------------------------
DROP INDEX IF EXISTS idx_lower_displayName;
DROP INDEX IF EXISTS idx_pattern_lower_displayname;
DROP INDEX IF EXISTS idx_queryCount;
DROP INDEX IF EXISTS idx_min_queryCount;
DROP INDEX IF EXISTS idx_occLastModified;
DROP INDEX IF EXISTS idx_occLastChecked;
DROP INDEX IF EXISTS idx_occUser;
DROP INDEX IF EXISTS idx_occStatus;

DROP INDEX IF EXISTS idx_mdlLastModified;
DROP INDEX IF EXISTS idx_modelUser;
DROP INDEX IF EXISTS idx_mdlStatus;

DROP INDEX IF EXISTS spidx_projection;
DROP INDEX IF EXISTS idx_projLastModified;
DROP INDEX IF EXISTS idx_prjStatus;

   
-- Models and Projections should cascade-delete as occurrencesets are deleted   
DELETE FROM occurrenceset WHERE userid = 'lm2' AND scientificnameid IS NULL;

-- -----------------------------------------------------------------------------
-- Delete all jobs associated with sub-species occurrencesets
-- -----------------------------------------------------------------------------
DELETE FROM lmjob USING lm_fullprojection p, scientificname sn
  WHERE referencetype = 102 AND referenceid = p.projectionid 
    AND p.occuserid = 'lm2' 
    AND p.scientificnameid = sn.scientificnameid
    AND sn.taxonomykey != sn.specieskey AND sn.taxonomykey != sn.genuskey;

DELETE FROM lmjob USING lm_fullmodel m, scientificname sn
  WHERE referencetype = 101 AND referenceid = m.modelid 
    AND m.occuserid = 'lm2' AND m.scientificnameid = sn.scientificnameid
    AND sn.taxonomykey != sn.specieskey AND sn.taxonomykey != sn.genuskey;

DELETE FROM lmjob USING occurrenceset o, scientificname sn
  WHERE referencetype = 104 AND referenceid = o.occurrencesetid 
    AND o.userid = 'lm2' AND o.scientificnameid = sn.scientificnameid
    AND sn.taxonomykey != sn.specieskey AND sn.taxonomykey != sn.genuskey;
   
-- -----------------------------------------------------------------------------
-- Delete all projections, models, occurrencesets associated with sub-species occurrencesets
-- -----------------------------------------------------------------------------
   
DELETE FROM projection USING model m, lm_fulloccurrenceset o
   WHERE modelid = m.modelid AND m.userid = 'lm2' 
     AND m.occurrencesetid = o.occurrencesetid
     AND o.taxonomykey != o.specieskey 
     AND o.taxonomykey != o.genuskey;
   
DELETE FROM model USING lm_fulloccurrenceset o
   WHERE userid = 'lm2' AND occurrencesetid = o.occurrencesetid
     AND o.userid = 'lm2' AND o.taxonomykey != o.specieskey 
     AND o.taxonomykey != o.genuskey;

DELETE FROM occurrenceset USING scientificname sn 
         WHERE userid = 'lm2' AND scientificnameid = sn.scientificnameid
           AND sn.taxonomykey != sn.specieskey AND sn.taxonomykey != sn.genuskey;


CREATE INDEX idx_lower_displayName on lm3.OccurrenceSet(lower(displayName));
CREATE INDEX idx_pattern_lower_displayname on lm3.OccurrenceSet  (lower(displayname) varchar_pattern_ops );
CREATE INDEX idx_queryCount ON lm3.OccurrenceSet(queryCount);
CREATE INDEX idx_min_queryCount ON lm3.OccurrenceSet((queryCount >= 50));
CREATE INDEX idx_occLastModified ON lm3.OccurrenceSet(dateLastModified);
CREATE INDEX idx_occLastChecked ON lm3.OccurrenceSet(dateLastChecked);
CREATE INDEX idx_occUser ON lm3.OccurrenceSet(userId);
CREATE INDEX idx_occStatus ON lm3.OccurrenceSet(status);

CREATE INDEX idx_mdlLastModified ON lm3.Model(statusModTime);
CREATE INDEX idx_modelUser ON lm3.Model(userId);
CREATE INDEX idx_mdlStatus ON lm3.Model(status);

CREATE INDEX spidx_projection ON lm3.Projection USING GIST ( geom );
CREATE INDEX idx_projLastModified ON lm3.Projection(statusModTime);
CREATE INDEX idx_prjStatus ON lm3.Projection(status);

   