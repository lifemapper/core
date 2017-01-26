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
-- ----------------------------------------------------------------------------
-- FUNCTIONS
-- Note: All column names are returned in lower case
-- Todo: REMOVE LM_SCHEMA = 'lm3' is in config.ini  
--       and LmServer.common.localconstants, put into lmconstants
-- ----------------------------------------------------------------------------

-- ----------------------------------------------------------------------------
\c speco
-- ----------------------------------------------------------------------------

-- ----------------------------------------------------------------------------
DROP FUNCTION IF EXISTS lm3.lm_countGrdJobs(varchar, int, int);
CREATE OR REPLACE FUNCTION lm3.lm_countGrdJobs(usrid varchar(20), 
                                               stat int,
                                               proctype int)
   RETURNS int AS
$$
DECLARE
   num int;
   cmd varchar;
   wherecls varchar;
BEGIN
   cmd = 'select count(*) FROM lm3.lm_grdjob ';
   wherecls = ' WHERE status = ' || quote_literal(stat) || 
              ' AND reqsoftware = ' || quote_literal(proctype);

   -- filter by user
   IF usrid is not null THEN
      wherecls = wherecls || ' AND userid = ' || quote_literal(usrid) ;
   END IF;

   cmd := cmd || wherecls;
   RAISE NOTICE 'cmd = %', cmd;

   EXECUTE cmd INTO num;
   RETURN num;
END;
$$  LANGUAGE 'plpgsql' STABLE;

-- ----------------------------------------------------------------------------
DROP FUNCTION IF EXISTS lm3.lm_countIntJobs(varchar, int, int);
CREATE OR REPLACE FUNCTION lm3.lm_countIntJobs(usrid varchar(20), 
    	                                         stat int,
    	                                         proctype int)
   RETURNS int AS
$$
DECLARE
   num int;
   cmd varchar;
   wherecls varchar;
BEGIN
   cmd = 'select count(*) FROM lm3.lm_intjob ';
   wherecls = ' WHERE status = ' || quote_literal(stat) || 
              ' AND reqsoftware = ' || quote_literal(proctype);

   -- filter by user
   IF usrid is not null THEN
      wherecls = wherecls || ' AND userid = ' || quote_literal(usrid) ;
   END IF;

   cmd := cmd || wherecls;
   RAISE NOTICE 'cmd = %', cmd;

   EXECUTE cmd INTO num;
   RETURN num;
END;
$$  LANGUAGE 'plpgsql' STABLE;

-- ----------------------------------------------------------------------------
DROP FUNCTION IF EXISTS lm3.lm_countMtxJobs(varchar, int, int);
CREATE OR REPLACE FUNCTION lm3.lm_countMtxJobs(usrid varchar(20), 
    	                                         stat int,
    	                                         proctype int)
   RETURNS int AS
$$
DECLARE
   num int;
   cmd varchar;
   wherecls varchar;
BEGIN
   cmd = 'select count(*) FROM lm3.lm_mtxjob ';
   wherecls = ' WHERE status = ' || quote_literal(stat) || 
              ' AND reqsoftware = ' || quote_literal(proctype);

   -- filter by user
   IF usrid is not null THEN
      wherecls = wherecls || ' AND userid = ' || quote_literal(usrid) ;
   END IF;

   cmd := cmd || wherecls;
   RAISE NOTICE 'cmd = %', cmd;

   EXECUTE cmd INTO num;
   RETURN num;
END;
$$  LANGUAGE 'plpgsql' STABLE;

-- ----------------------------------------------------------------------------
DROP FUNCTION IF EXISTS lm3.lm_countMsgJobs(varchar, int, int);
CREATE OR REPLACE FUNCTION lm3.lm_countMsgJobs(usrid varchar(20), 
    	                                         stat int,
    	                                         proctype int)
   RETURNS int AS
$$
DECLARE
   num int;
   cmd varchar;
   wherecls varchar;
BEGIN
   cmd = 'select count(*) FROM lm3.lm_msgjob ';
   wherecls = ' WHERE status = ' || quote_literal(stat) || 
              ' AND reqsoftware = ' || quote_literal(proctype);

   -- filter by user
   IF usrid is not null THEN
      wherecls = wherecls || ' AND userid = ' || quote_literal(usrid) ;
   END IF;

   cmd := cmd || wherecls;
   RAISE NOTICE 'cmd = %', cmd;

   EXECUTE cmd INTO num;
   RETURN num;
END;
$$  LANGUAGE 'plpgsql' STABLE;

-- ----------------------------------------------------------------------------
-- Experiments
-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lm3.lm_countExperiments(usrid varchar(20), 
                                         beforetime double precision,
                                         aftertime double precision,
                                         epsg int,
                                         ename varchar)
   RETURNS int AS
$$
DECLARE
   num int;
   cmd varchar;
   wherecls varchar;
BEGIN
   cmd = 'select count(*) FROM lm3.Experiment ';
   wherecls = ' WHERE userId =  ' || quote_literal(usrid) ;

   -- filter by modified before given time
   IF beforetime is not null THEN
      wherecls = wherecls || ' AND datelastmodified <=  ' || quote_literal(beforetime);
   END IF;
   
   -- filter by modified after given time
   IF aftertime is not null THEN
      wherecls = wherecls || ' AND datelastmodified >=  ' || quote_literal(aftertime);
   END IF;

   -- filter by epsgcode
   IF epsg is not null THEN
      wherecls = wherecls || ' AND  epsgcode =  ' || epsg;
   END IF;
   
   -- filter by ExperimentName
   IF ename is not null THEN
      wherecls = wherecls || ' AND expname =  ' || quote_literal(ename);
   END IF;

   cmd := cmd || wherecls;
   RAISE NOTICE 'cmd = %', cmd;

   EXECUTE cmd INTO num;
   RETURN num;
END;
$$  LANGUAGE 'plpgsql' STABLE;

-- ----------------------------------------------------------------------------
-- lm_listExperiments
CREATE OR REPLACE FUNCTION lm3.lm_listExperiments(firstRecNum int, maxNum int, 
                                         usrid varchar(20), 
                                         beforetime double precision,
                                         aftertime double precision,
                                         epsg int,
                                         ename varchar)
   RETURNS SETOF lm3.lm_atom AS
$$
DECLARE
   rec lm3.lm_atom;
   cmd varchar;
   wherecls varchar;
   limitcls varchar;
   ordercls varchar;
BEGIN
   cmd = 'SELECT experimentId, expname, epsgcode, description, datelastmodified
               FROM lm3.Experiment ';
   wherecls = ' WHERE userid =  ' || quote_literal(usrid) ;
   ordercls = ' ORDER BY datelastmodified DESC ';
   limitcls = ' LIMIT ' || quote_literal(maxNum) || ' OFFSET ' || quote_literal(firstRecNum);

   -- filter by modified before given time
   IF beforetime is not null THEN
      wherecls = wherecls || ' AND datelastmodified <=  ' || quote_literal(beforetime);
   END IF;

   -- filter by modified after given time
   IF aftertime is not null THEN
      wherecls = wherecls || ' AND datelastmodified >=  ' || quote_literal(aftertime);
   END IF;

   -- filter by epsgcode
   IF epsg is not null THEN
      wherecls = wherecls || ' AND  epsgcode =  ' || epsg;
   END IF;
   
   -- filter by Experiment name
   IF ename is not null THEN
      wherecls = wherecls || ' AND expname =  ' || quote_literal(ename);
   END IF;

   cmd := cmd || wherecls || ordercls || limitcls;
   RAISE NOTICE 'cmd = %', cmd;

   FOR rec in EXECUTE cmd
      LOOP
         RETURN NEXT rec;
      END LOOP;
   RETURN;
END;
$$  LANGUAGE 'plpgsql' STABLE;

-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lm3.lm_listExperimentObjects(firstRecNum int, maxNum int, 
                                         usrid varchar(20), 
                                         beforetime double precision,
                                         aftertime double precision,
                                         epsg int,
                                         ename varchar)
   RETURNS SETOF lm3.Experiment AS
$$
DECLARE
   rec lm3.Experiment;
   cmd varchar;
   wherecls varchar;
   limitcls varchar;
   ordercls varchar;
BEGIN
   cmd = 'SELECT * FROM lm3.Experiment ';
   wherecls = ' WHERE userid =  ' || quote_literal(usrid) ;
   ordercls = ' ORDER BY datelastmodified DESC ';
   limitcls = ' LIMIT ' || quote_literal(maxNum) || ' OFFSET ' || quote_literal(firstRecNum);

   -- filter by modified before given time
   IF beforetime is not null THEN
      wherecls = wherecls || ' AND datelastmodified <=  ' || quote_literal(beforetime);
   END IF;

   -- filter by modified after given time
   IF aftertime is not null THEN
      wherecls = wherecls || ' AND datelastmodified >=  ' || quote_literal(aftertime);
   END IF;

   -- filter by epsgcode
   IF epsg is not null THEN
      wherecls = wherecls || ' AND  epsgcode =  ' || epsg;
   END IF;
   
   -- filter by Experiment name
   IF ename is not null THEN
      wherecls = wherecls || ' AND expname =  ' || quote_literal(ename);
   END IF;

   cmd := cmd || wherecls || ordercls || limitcls;
   RAISE NOTICE 'cmd = %', cmd;

   FOR rec in EXECUTE cmd
      LOOP
         RETURN NEXT rec;
      END LOOP;
   RETURN;
END;
$$  LANGUAGE 'plpgsql' STABLE;

-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lm3.lm_insertExperiment(usr varchar,
                                               name varchar,
                                               matdloc varchar,
                                               treedloc varchar,
                                               descr text,
                                               emale varchar,
                                               epsg int,
                                               kywds varchar,
                                               createtime double precision,
                                               murlprefix varchar)
RETURNS int AS
$$
DECLARE
   expid int;
   idstr varchar;
   murl varchar;
BEGIN
   SELECT INTO expid experimentid FROM lm3.experiment 
      WHERE userid = usr AND expname = name;
   IF NOT FOUND THEN
      begin
         INSERT INTO lm3.Experiment (userId, expname, attrMatrixDlocation, 
                                     attrTreeDlocation, description, email, 
                                     epsgcode, datelastmodified, datecreated, 
                                     keywords)
                             values (usr, name, matdloc, treedloc, descr, 
                                     emale, epsg, createtime, createtime, kywds);
         IF FOUND THEN
            SELECT INTO expid last_value FROM lm3.experiment_experimentid_seq;
            idstr := cast(expid as varchar);
            murl := replace(murlprefix, '#id#', idstr);
            UPDATE lm3.Experiment SET metadataurl = murl WHERE experimentid = expid;            
         END IF;
      end;
   END IF;
   RETURN expid;
END;
$$  LANGUAGE 'plpgsql' VOLATILE;

-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lm3.lm_updateExperimentInfo(expid int,
                                               matdloc varchar,
                                               treedloc varchar,
                                               descr text,
                                               emale varchar,
                                               kywds varchar,
                                               modtime double precision)
RETURNS int AS
$$
DECLARE
   success int = -1;
BEGIN
   UPDATE lm3.Experiment SET (attrMatrixDlocation, attrTreeDlocation, email, 
                              keywords, description, datelastmodified) 
                   = (matdloc, treedloc, emale, kywds, descr, modtime) 
      WHERE experimentid = expid;
   IF FOUND THEN
      success := 0;
   END IF;
   RETURN success;
END;
$$  LANGUAGE 'plpgsql' VOLATILE;

-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lm3.lm_insertBucket(expid int,
                                           shpgrdid int,
                                           stat int,
                                           stattime double precision,
                                           stg int,
                                           stgtime double precision,
                                           kywds varchar,
                                           createtime double precision,
                                           murlprefix varchar)
RETURNS lm3.lm_fullradbucket AS
$$
DECLARE
   bktid int = -1;
   expepsg int;
   shpepsg int;
   idstr varchar;
   murl varchar;
   rec lm3.lm_fullradbucket%ROWTYPE;
BEGIN
   SELECT INTO rec * FROM lm3.lm_fullradbucket 
      WHERE experimentid = expid AND shapegridid = shpgrdid;
   IF NOT FOUND THEN
      begin
         SELECT INTO expepsg epsgcode FROM lm3.Experiment WHERE experimentid = expid;
         SELECT INTO shpepsg epsgcode FROM lm3.lm_shapegrid WHERE shapegridid = shpgrdid;
         IF expepsg = shpepsg THEN
            begin
               INSERT INTO lm3.Bucket (experimentId, shapeGridId,
                                   status, statusmodtime, 
                                   stage, stagemodtime, 
                                   datecreated, keywords)
                           values (expid, shpgrdid, 
                                   stat, stattime, stg, stgtime, 
                                   createtime, kywds);
               IF FOUND THEN
                  SELECT INTO bktid last_value FROM lm3.bucket_bucketid_seq;
                  idstr := cast(bktid as varchar);
                  murl := replace(murlprefix, '#id#', idstr);
                  UPDATE lm3.Bucket SET metadataurl = murl WHERE bucketId = bktid;
               END IF;
            end;
         ELSE
            RAISE EXCEPTION 'Unable to add EPSG % Bucket to % Experiment', 
                             shpepsg, expepsg; 
         END IF;

         SELECT INTO rec * FROM lm3.lm_fullradbucket WHERE bucketid = bktid;
      end;
   END IF;
   
   
   RETURN rec;
END;
$$  LANGUAGE 'plpgsql' VOLATILE;

-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lm3.lm_updateBucketInfo(bckid int,
                                               sldloc varchar,
                                               pamdloc varchar,
                                               grimdloc varchar,
                                               murl varchar,
                                               stat int, 
                                               stattime double precision,
                                               stg int, 
                                               stgtime double precision,
                                               crid int)
RETURNS int AS
$$
DECLARE
   success int = -1;
BEGIN
   UPDATE lm3.Bucket SET (slIndicesDlocation, pamDlocation, grimDlocation, 
                       metadataurl, status, statusmodtime, computeResourceId) 
                   = (sldloc, pamdloc, grimdloc, murl, stat, stattime, crid) 
      WHERE bucketid = bckid;
   IF FOUND THEN
      success := 0;
   END IF;
   IF stg IS NOT NULL AND stgtime IS NOT NULL THEN
      UPDATE lm3.Bucket SET (stage, stagemodtime) = (stg, stgtime) 
         WHERE bucketid = bckid;
   END IF;
   RETURN success;
END;
$$  LANGUAGE 'plpgsql' VOLATILE;

-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lm3.lm_updatePamsumInfo(psid int,
                                               pamdloc varchar,
                                               sumdloc varchar,
                                               splotchdloc varchar,
                                               splotchsitesdloc varchar,
                                               murl varchar,
                                               stat int, 
                                               stattime double precision,
                                               stg int, 
                                               stgtime double precision,
                                               crid int)
RETURNS int AS
$$
DECLARE
   success int = -1;
BEGIN
   UPDATE lm3.PamSum SET (pamDlocation, sumDlocation, splotchPamDlocation,
                      splotchSitesDlocation, metadataurl, 
                      status, statusmodtime, computeResourceId) 
                   = (pamdloc, sumdloc, splotchdloc, splotchsitesdloc,
                      murl, stat, stattime, crid) 
      WHERE pamsumid = psid;
   IF FOUND THEN
      success := 0;
   END IF;
   IF stg IS NOT NULL AND stgtime IS NOT NULL THEN
      UPDATE lm3.PamSum SET (stage, stagemodtime) = (stg, stgtime) 
         WHERE pamsumid = psid;
   END IF;
   RETURN success;
END;
$$  LANGUAGE 'plpgsql' VOLATILE;

-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lm3.lm_getUserId(expid int, 
                                            bktid int, 
                                            psid int, 
                                            sgid int)
RETURNS varchar AS
$$
DECLARE
   usr varchar = '';
BEGIN
   begin
      IF sgid IS NOT NULL THEN
         SELECT lyruserid INTO STRICT usr FROM lm3.lm_shapegrid 
         	WHERE shapeGridId = sgid;
      ELSEIF psid IS NOT NULL THEN
         SELECT expuserid INTO STRICT usr FROM lm3.lm_pamsum 
         	WHERE pamsumid = psid;
      ELSEIF bktid IS NOT NULL THEN
         SELECT e.userid INTO STRICT usr FROM lm3.bucket b, lm3.experiment e 
         	WHERE b.experimentid = e.experimentid and b.bucketid = bktid;
      ELSEIF expid IS NOT NULL THEN
         SELECT userid INTO STRICT usr FROM lm3.experiment 
         	WHERE experimentid = expid;
      END IF;
      
      EXCEPTION
         WHEN NO_DATA_FOUND THEN
            RAISE NOTICE 'User not found for shapegrid %, pamsum %, bucket %, or experiment %', 
                          sgid, psid, bktid, expid;
   end;
   
   RETURN usr;
END;
$$  LANGUAGE 'plpgsql' STABLE;

-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lm3.lm_getJobType(jobid int)
RETURNS int AS
$$
DECLARE
   reftype int;
BEGIN
   begin
      SELECT referenceType INTO STRICT reftype FROM lm3.LMJob 
         WHERE lmjobid = jobid;
      
      EXCEPTION
         WHEN NO_DATA_FOUND THEN
            RAISE NOTICE 'JobId % not found', jid;
   end;
   
   RETURN reftype;
END;
$$  LANGUAGE 'plpgsql' STABLE;

-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lm3.lm_getIntJob(jid int)
   RETURNS lm3.lm_intJob AS
$$
DECLARE
   rec lm3.lm_intJob;
BEGIN
   begin
      SELECT * INTO STRICT rec FROM lm3.lm_intJob WHERE lmjobid = jid;

      EXCEPTION
         WHEN NO_DATA_FOUND THEN
            RAISE NOTICE 'IntJob not found for %', jid;
   end;
   RETURN rec;
END;
$$  LANGUAGE 'plpgsql' VOLATILE;

-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lm3.lm_getMtxJob(jid int)
   RETURNS lm3.lm_mtxJob AS
$$
DECLARE
   rec lm3.lm_mtxJob;
BEGIN
   begin
      SELECT * INTO STRICT rec FROM lm3.lm_mtxJob WHERE lmjobid = jid;

      EXCEPTION
         WHEN NO_DATA_FOUND THEN
            RAISE NOTICE 'MtxJob not found for %', jid;
   end;
   RETURN rec;
END;
$$  LANGUAGE 'plpgsql' VOLATILE;


-- ----------------------------------------------------------------------------

-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lm3.lm_insertJob(jfam int,
                                        software int,
                                        datatype int,
                                        reftype int,
                                        refid int,
                                        crid int, 
                                        notify boolean,
                                        prior int,
                                        stat int,
                                        stg int,
                                        currtime double precision, 
                                        endstat int)
RETURNS int AS
$$
DECLARE
   rec lm3.LmJob%ROWTYPE;
   jid int = -1;
BEGIN
   SELECT * INTO rec FROM lm3.LMJob 
      WHERE jobFamily = jfam 
        AND referenceType = reftype 
        AND referenceId = refid 
        AND stage = stg;

   IF FOUND THEN
      begin
         jid := rec.lmjobid;
         RAISE NOTICE 'LMJob % exists jobFamily %, refType % / %, stage %', 
                       jid, jfam, reftype, refid, stg;
         -- if existing job is completed or error, reset
         IF rec.status > endstat THEN
            UPDATE lm3.LMJob SET (computeResourceId, priority, progress, 
                              status, statusmodtime, lastheartbeat) 
                           = (crid, prior, 0, stat, currtime, currtime)
                         WHERE lmJobId = jid;
         END IF;
      end;
   ELSE
      begin
         INSERT INTO lm3.LMJob (jobFamily, reqSoftware, reqData, referenceType, referenceId, 
                             computeResourceId, priority, progress, donotify, 
                             status, statusmodtime, stage, stagemodtime, 
                             datecreated, lastheartbeat)
                 VALUES (jfam, software, datatype, reftype, refid, crid, 
                         prior, 0, notify, stat, currtime, stg, currtime, 
                         currtime, currtime);
         IF FOUND THEN 
            SELECT INTO jid last_value FROM lm3.lmjob_lmjobid_seq;
         END IF;
      end;
   END IF;

   RETURN jid;

END;
$$  LANGUAGE 'plpgsql' VOLATILE;
   
-- ----------------------------------------------------------------------------
-- lm3.lm_updateJob  Now in createCommonExtras.sql

-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lm3.lm_updateJobAndObjLite(jid int,
                                                      ipaddr varchar,
                                                      stat int,
                                                      prg int,
                                                      newpull boolean,
                                                      currtime double precision)
RETURNS int AS
$$
DECLARE
   crid int;
   retries int;
   reftype int;
   refid int;
   jsuccess int = -1;
   osuccess int = -1;
BEGIN
   SELECT INTO crid computeResourceId FROM lm3.computeResource WHERE ipaddress = ipaddr;
   IF NOT FOUND THEN 
      crid = null;
   END IF;

   IF newpull THEN
      begin
         SELECT INTO retries retryCount FROM lm3.lmjob WHERE lmjobid = jid;
         retries = retries + 1;
         UPDATE lm3.LMJob SET  (computeResourceId, status, statusmodtime, 
                                progress, retryCount, lastheartbeat) 
                             = (crid, stat, currtime, prg, retries, currtime)
         WHERE lmjobid = jid;
      end;
   ELSE
      UPDATE lm3.LMJob SET  (computeResourceId, status, statusmodtime, 
                                progress, lastheartbeat) 
                             = (crid, stat, currtime, prg, currtime)
         WHERE lmjobid = jid;
   END IF;
   IF FOUND THEN 
      jsuccess := 0;
   END IF;
   
   SELECT referenceType, referenceId INTO reftype, refid 
      FROM lm3.LMJob WHERE lmjobid = jid; 
   IF reftype = 202 THEN
      UPDATE lm3.Bucket SET (computeResourceId, status, statusModTime) 
                          = (crid, stat, currtime)
         WHERE bucketId = refid;
   ELSEIF reftype in (203,204) THEN
      UPDATE lm3.PamSum SET (computeResourceId, status, statusModTime) 
                          = (crid, stat, currtime)
         WHERE pamsumId = refid;
   END IF;
   IF FOUND THEN 
      osuccess := 0;
   END IF;
      
   RETURN osuccess + jsuccess;
END;
$$  LANGUAGE 'plpgsql' VOLATILE;

-- ----------------------------------------------------------------------------
-- select * from lm3.lm_updateBucketDependentJobs(554,10,300,1000,20,32,0,1,56596.9238874);
-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lm3.lm_updateBucketDependentJobs(bktid int,
                                                            completestage int,
                                                            completestat int,
                                                            errorstat int,
                                                            depOpsstage int,
                                                            depRpsstage int,
                                                            depnotreadystat int,
                                                            depreadystat int,
                                                            currtime double precision)
RETURNS int AS
$$
DECLARE
   xrec lm3.lm_mtxJob%ROWTYPE;
   mrec lm3.lm_msgJob%ROWTYPE;
   rem   int := 0;
   movjb int := 0;
BEGIN
   -- On successful bucket Complete-stage,  
   -- Move pamsum and Dependent-stage jobs to ready 
   --    SUCCESS Bkt Intersect -> OPS Compress
   FOR xrec IN 
      SELECT * FROM lm3.lm_mtxjob
         WHERE stage = depOpsstage
           AND status = depnotreadystat 
           -- Original PamSum = 203
           AND referenceType = 203
           AND bucketId = bktid
           AND bktstage = completestage AND bktstatus = completestat
      LOOP
         UPDATE lm3.PamSum SET (status, statusmodtime, stage, stagemodtime) 
                             = (depreadystat, currtime, depOpsstage, currtime)
            WHERE pamsumid = xrec.referenceid;
         UPDATE lm3.LMJob SET (status, statusmodtime) 
                            = (depreadystat, currtime)
            WHERE lmjobid = xrec.lmjobid;
         IF FOUND THEN
            movjb := movjb + 1;
         END IF;
      END LOOP;
      
   --    SUCCESS Bkt Intersect -> RPS Splotch
   FOR xrec IN 
      SELECT * FROM lm3.lm_mtxjob
         WHERE stage = depRpsstage
           AND status = depnotreadystat 
           -- Random PamSum = 204
           AND referenceType = 204
           AND bucketId = bktid
           AND bktstage = completestage AND bktstatus = completestat
      LOOP
         UPDATE lm3.PamSum SET (status, statusmodtime, stage, stagemodtime) 
                             = (depreadystat, currtime, depRpsstage, currtime)
            WHERE pamsumid = xrec.referenceid;
         UPDATE lm3.LMJob SET (status, statusmodtime) 
                            = (depreadystat, currtime)
            WHERE lmjobid = xrec.lmjobid;
         IF FOUND THEN
            movjb := movjb + 1;
         END IF;
      END LOOP;

   --   Complete (success or fail) All bucket/pamsum jobs -> Notify Job 
   FOR mrec IN 
      SELECT * FROM lm3.lm_msgjob
         WHERE bucketid = bktid 
           AND bktstage = completestage AND bktstatus >= completestat
      LOOP
         SELECT INTO rem count(*) FROM lm3.lm_mtxjob WHERE bucketid = bktid;
         IF rem = 0 THEN
            UPDATE lm3.LMJob SET (status, statusmodtime) 
                               = (depreadystat, currtime)
               WHERE lmjobid = mrec.lmjobid;
         END IF;
         IF FOUND THEN
            movjb := movjb + 1;
         END IF;
      END LOOP;
      
   --    FAIL Bkt Intersect -> DELETE all OPS Jobs (leave OPS)
   FOR xrec IN 
      SELECT * FROM lm3.lm_mtxjob
         WHERE status = depnotreadystat 
           AND referenceType = 203
           AND bucketId = bktid
           AND bktstage = completestage AND bktstatus = errorstat
      LOOP
         DELETE FROM lm3.LMJob WHERE lmjobid = xrec.lmjobid;
      END LOOP;

   --   FAIL Bkt Intersect -> DELETE all RPSs and Jobs
   FOR xrec IN 
      SELECT * FROM lm3.lm_mtxjob
         WHERE referenceType = 204
           AND bucketId = bktid
           AND bktstage = completestage AND bktstatus = errorstat
      LOOP
         DELETE FROM lm3.LMJob WHERE lmjobid = xrec.lmjobid;
         DELETE FROM lm3.PamSum WHERE pamsumid = xrec.referenceId;
      END LOOP;

   --   FAIL Bkt Intersect -> Notify Job 
   FOR mrec IN 
      SELECT * FROM lm3.lm_msgjob
         WHERE bucketid = bktid 
           AND bktstage = completestage AND bktstatus >= errorstat
      LOOP
         UPDATE lm3.LMJob SET (status, statusmodtime) 
                            = (depreadystat, currtime)
            WHERE lmjobid = mrec.lmjobid;
         IF FOUND THEN
            movjb := movjb + 1;
         END IF;
      END LOOP;
      
   RETURN movjb;
END;
$$  LANGUAGE 'plpgsql' VOLATILE;

-- ----------------------------------------------------------------------------
-- select * from lm3.lm_updateAllBucketDependentJobs(10, 300,1000,20,0,1,1002,56575);
-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lm3.lm_updateAllBucketDependentJobs(completestage int,
                                                            completestat int,
                                                            errorstat int,
                                                            depOpsstage int,
                                                            depRpsstage int,
                                                            depnotreadystat int,
                                                            depreadystat int,
                                                            currtime double precision)
RETURNS int AS
$$
DECLARE
   xrec lm3.lm_mtxJob%ROWTYPE;
   mrec lm3.lm_msgJob%ROWTYPE;
   rem   int := 0;
   movjb int := 0;
BEGIN
   -- On successful bucket Complete-stage,  
   -- Move pamsum and Dependent-stage jobs to ready 
   --    SUCCESS Bkt Intersect -> OPS Compress
   FOR xrec IN 
      SELECT * FROM lm3.lm_mtxjob
         WHERE stage = depOpsstage
           AND status = depnotreadystat 
           AND referenceType = 203
           AND bktstage = completestage AND bktstatus = completestat
      LOOP
         UPDATE lm3.PamSum SET (status, statusmodtime, stage, stagemodtime) 
                             = (depreadystat, currtime, depOpsstage, currtime)
            WHERE pamsumid = xrec.referenceid;
         UPDATE lm3.LMJob SET (status, statusmodtime) 
                            = (depreadystat, currtime)
            WHERE lmjobid = xrec.lmjobid;
         IF FOUND THEN
            movjb := movjb + 1;
         END IF;
      END LOOP;
      
   --    SUCCESS Bkt Intersect -> RPS Splotch
   FOR xrec IN 
      SELECT * FROM lm3.lm_mtxjob
         WHERE stage = depRpsstage
           AND status = depnotreadystat 
           AND referenceType = 204
           AND bktstage = completestage AND bktstatus = completestat
      LOOP
         UPDATE lm3.PamSum SET (status, statusmodtime, stage, stagemodtime) 
                             = (depreadystat, currtime, depRpsstage, currtime)
            WHERE pamsumid = xrec.referenceid;
         UPDATE lm3.LMJob SET (status, statusmodtime) 
                            = (depreadystat, currtime)
            WHERE lmjobid = xrec.lmjobid;
         IF FOUND THEN
            movjb := movjb + 1;
         END IF;
      END LOOP;

   --   Complete (success or fail) All bucket/pamsum jobs -> Notify Job 
   FOR mrec IN 
      SELECT * FROM lm3.lm_msgjob
         WHERE bktstage = completestage AND bktstatus >= completestat
      LOOP
         UPDATE lm3.LMJob SET (status, statusmodtime) 
                            = (depreadystat, currtime)
            WHERE lmjobid = mrec.lmjobid;
         IF FOUND THEN
            movjb := movjb + 1;
         END IF;
      END LOOP;
      
   --    FAIL Bkt Intersect -> DELETE all OPS Jobs (leave OPS)
   FOR xrec IN 
      SELECT * FROM lm3.lm_mtxjob
         WHERE status = depnotreadystat 
           AND referenceType = 203
           AND bktstage = completestage AND bktstatus = errorstat
      LOOP
         DELETE FROM lm3.LMJob WHERE lmjobid = xrec.lmjobid;
      END LOOP;

   --   FAIL Bkt Intersect -> DELETE all RPSs and Jobs
   FOR xrec IN 
      SELECT * FROM lm3.lm_mtxjob
         WHERE referenceType = 204
           AND bktstage = completestage AND bktstatus = errorstat
      LOOP
         DELETE FROM lm3.LMJob WHERE lmjobid = xrec.lmjobid;
         DELETE FROM lm3.PamSum WHERE pamsumid = xrec.referenceId;
      END LOOP;

   --   FAIL Bkt Intersect -> Notify Job 
   FOR mrec IN 
      SELECT * FROM lm3.lm_msgjob
         WHERE bktstage = completestage AND bktstatus >= errorstat
      LOOP
         UPDATE lm3.LMJob SET (status, statusmodtime) 
                            = (depreadystat, currtime)
            WHERE lmjobid = mrec.lmjobid;
         IF FOUND THEN
            movjb := movjb + 1;
         END IF;
      END LOOP;
      
   RAISE NOTICE 'Moved % jobs', movjb;

   RETURN movjb;
END;
$$  LANGUAGE 'plpgsql' VOLATILE;

-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lm3.lm_updateOPSDependentJobs(opsid int,
                                                         completestage int,
                                                         completestat int,
                                                         errorstat int,
                                                         depOpsstage int,
                                                         depRpsstage int,
                                                         depnotreadystat int,
                                                         depreadystat int,
                                                         currtime double precision)
RETURNS int AS
$$
DECLARE
   xrec lm3.lm_mtxJob%ROWTYPE;
   movjb int := 0;
BEGIN
   -- On successful Original PamSum (OPS) Complete-stage,
   -- Move pamsum and Dependent-stage jobs to ready 
   --    SUCCESS OPS job -> INIT dependent OPS job
   --     (OPS Compress -> OPS Calculate)
   FOR xrec IN 
      SELECT * FROM lm3.lm_mtxjob
         WHERE opspamsumid = opsid 
           AND stage = depOpsstage
           AND status = depnotreadystat 
           AND referenceType = 203
           AND opsstage = completestage AND opsstatus = completestat
      LOOP
         UPDATE lm3.PamSum SET (status, statusmodtime, stage, stagemodtime) 
                             = (depreadystat, currtime, depOpsstage, currtime)
            WHERE pamsumid = xrec.referenceid;
         UPDATE lm3.LMJob SET (status, statusmodtime) 
                            = (depreadystat, currtime)
            WHERE lmjobid = xrec.lmjobid;
         IF FOUND THEN
            movjb := movjb + 1;
         END IF;
      END LOOP;

   --    SUCCESS OPS job -> INIT dependent RPS job
   --    (OPS Compress -> RPS Swap)
   FOR xrec IN 
      SELECT * FROM lm3.lm_mtxjob
         WHERE opspamsumid = opsid
           AND stage = depRpsstage
           AND status = depnotreadystat 
           AND referenceType = 204
           AND opsstage = completestage AND opsstatus = completestat
      LOOP
         UPDATE lm3.PamSum SET (status, statusmodtime, stage, stagemodtime) 
                             = (depreadystat, currtime, depRpsstage, currtime)
            WHERE pamsumid = xrec.referenceid;
         UPDATE lm3.LMJob SET (status, statusmodtime) 
                            = (depreadystat, currtime)
            WHERE lmjobid = xrec.lmjobid;
         IF FOUND THEN
            movjb := movjb + 1;
         END IF;
      END LOOP;
      
   --  SUCCESS or FAIL OPS Calculate -> No action

   -- On OPS FAIL, delete all dependent jobs, dependent pamsums (SWAP only)
   --     FAIL OPS Compress -> DELETE OPS Calculate Job and PamSum
   FOR xrec IN 
      SELECT * FROM lm3.lm_mtxjob
         WHERE opspamsumid = opsid 
           AND stage = depOpsstage
           AND referenceType = 203
           AND opsstage = completestage AND opsstatus = errorstat
      LOOP
         DELETE FROM lm3.LMJob WHERE lmjobid = xrec.lmjobid;
      END LOOP;

   --     FAIL OPS Compress -> DELETE RPS Swap Job and PamSum
   FOR xrec IN 
      SELECT * FROM lm3.lm_mtxjob
         WHERE opspamsumid = opsid 
           AND stage = depOpsstage
           AND referenceType = 204
           AND opsstage = completestage AND opsstatus = errorstat
      LOOP
         DELETE FROM lm3.LMJob WHERE lmjobid = xrec.lmjobid;
         DELETE FROM lm3.PamSum WHERE pamsumid = xrec.referenceId;      
      END LOOP;

   RETURN movjb;
END;
$$  LANGUAGE 'plpgsql' VOLATILE;

-- ----------------------------------------------------------------------------
-- select * from lm3.lm_updateAllOPSDependentJobs(40,300,1000,null,31,0,1,56601);
-- select * from lm3.lm_updateAllOPSDependentJobs(20,300,1000,40,31,0,1,56616.8269991);

CREATE OR REPLACE FUNCTION lm3.lm_updateAllOPSDependentJobs(completestage int,
                                                         completestat int,
                                                         errorstat int,
                                                         depOpsstage int,
                                                         depRpsstage int,
                                                         depnotreadystat int,
                                                         depreadystat int,
                                                         currtime double precision)
RETURNS int AS
$$
DECLARE
   xrec lm3.lm_mtxJob%ROWTYPE;
   movjb int := 0;
BEGIN
   -- On successful Original PamSum (OPS) Complete-stage,
   -- Move pamsum and Dependent-stage jobs to ready 
   --    SUCCESS OPS Compress -> OPS Calculate (self)
   FOR xrec IN 
      SELECT * FROM lm3.lm_mtxjob
         WHERE stage = depOpsstage
           AND status = depnotreadystat 
           AND referenceType = 203
           AND opsstage = completestage AND opsstatus = completestat
      LOOP
         UPDATE lm3.PamSum SET (status, statusmodtime, stage, stagemodtime) 
                             = (depreadystat, currtime, depOpsstage, currtime)
            WHERE pamsumid = xrec.referenceid;
         UPDATE lm3.LMJob SET (status, statusmodtime) 
                            = (depreadystat, currtime)
            WHERE lmjobid = xrec.lmjobid;
         IF FOUND THEN
            movjb := movjb + 1;
         END IF;
      END LOOP;

   --  SUCCESS OPS Compress -> RPS Swap
   FOR xrec IN 
      SELECT * FROM lm3.lm_mtxjob
         WHERE stage = depRpsstage
           AND status = depnotreadystat 
           AND referenceType = 204
           AND opsstage = completestage AND opsstatus = completestat
      LOOP
         UPDATE lm3.PamSum SET (status, statusmodtime, stage, stagemodtime) 
                             = (depreadystat, currtime, depRpsstage, currtime)
            WHERE pamsumid = xrec.referenceid;
         UPDATE lm3.LMJob SET (status, statusmodtime) 
                            = (depreadystat, currtime)
            WHERE lmjobid = xrec.lmjobid;
         IF FOUND THEN
            movjb := movjb + 1;
         END IF;
      END LOOP;
      
   --  SUCCESS or FAIL OPS Calculate -> No action

   -- On OPS FAIL, delete all dependent jobs, dependent pamsums (SWAP only)
   --     FAIL OPS Compress -> DELETE OPS Calculate Job and PamSum
   FOR xrec IN 
      SELECT * FROM lm3.lm_mtxjob
         WHERE stage = depOpsstage
           AND referenceType = 203
           AND opsstage = completestage AND opsstatus = errorstat
      LOOP
         DELETE FROM lm3.LMJob WHERE lmjobid = xrec.lmjobid;
      END LOOP;

   --     FAIL OPS Compress -> DELETE RPS Swap Job and PamSum
   FOR xrec IN 
      SELECT * FROM lm3.lm_mtxjob
         WHERE stage = depOpsstage
           AND referenceType = 204
           AND opsstage = completestage AND opsstatus = errorstat
      LOOP
         DELETE FROM lm3.LMJob WHERE lmjobid = xrec.lmjobid;
         DELETE FROM lm3.PamSum WHERE pamsumid = xrec.referenceId;      
      END LOOP;

      
   RETURN movjb;
END;
$$  LANGUAGE 'plpgsql' VOLATILE;

-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lm3.lm_updateRPSDependentJobs(rpsid int,
                                                         completestage int,
                                                         completestat int,
                                                         errorstat int,
                                                         depRpsstage int,
                                                         depnotreadystat int,
                                                         depreadystat int,
                                                         currtime double precision)
RETURNS int AS
$$
DECLARE
   xrec lm3.lm_mtxJob%ROWTYPE;
   movps int := 0;
   movjb int := 0;
   delps int := 0;
   deljb int := 0;
BEGIN
   -- On successful Random PamSum (RPS) Complete-stage,
   -- Move pamsum and Dependent-stage jobs to ready 
   --    SUCCESS RPS Splotch -> RPS Compress (self)
   --    SUCCESS RPS Compress -> RPS Caclulate (self)
   --    SUCCESS RPS Swap -> RPS Caclulate (self)
   FOR xrec IN 
      SELECT * FROM lm3.lm_mtxjob
         WHERE pamsumid = referenceId 
           AND referenceType = 204
           AND stage = depRpsstage
           AND status = depnotreadystat 
           AND psstage = completestage AND psstatus = completestat
      LOOP
         UPDATE lm3.PamSum SET (status, statusmodtime, stage, stagemodtime) 
                             = (depreadystat, currtime, depRpsstage, currtime)
            WHERE pamsumid = xrec.referenceid;
         IF FOUND THEN
            movps = movps + 1;
         END IF;
         UPDATE lm3.LMJob SET (status, statusmodtime) = (depreadystat, currtime)
            WHERE lmjobid = xrec.lmjobid;
         IF FOUND THEN
            movjb := movjb + 1;
         END IF;
      END LOOP;

   -- On error RPS (any stage), delete self
   FOR xrec IN 
      SELECT * FROM lm3.lm_mtxJob
         WHERE pamsumid = referenceId 
           AND referenceType = 204 
           AND stage = completestage 
           AND status >= errorstat
      LOOP
         DELETE FROM lm3.LMJob WHERE referenceid = xrec.referenceid;
         IF FOUND THEN
            deljb = deljb + 1;
         END IF;

         DELETE FROM lm3.PamSum WHERE pamsumid = xrec.referenceId;
         IF FOUND THEN
            delps = delps + 1;
         END IF;         
         
      END LOOP;
      
   RAISE NOTICE 'Deleted % pamsums, % jobs', delps, deljb;
   RETURN 0;
END;
$$  LANGUAGE 'plpgsql' VOLATILE;

-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lm3.lm_updateAllRPSDependentJobs(completestage int,
                                                         completestat int,
                                                         errorstat int,
                                                         depRpsstage int,
                                                         depnotreadystat int,
                                                         depreadystat int,
                                                         currtime double precision)
RETURNS int AS
$$
DECLARE
   xrec lm3.lm_mtxJob%ROWTYPE;
   movjb int := 0;
BEGIN
   -- On successful Random PamSum (RPS) Complete-stage,
   -- Move pamsum and Dependent-stage jobs to ready 
   --    SUCCESS RPS Splotch -> RPS Compress (self)
   --    SUCCESS RPS Compress -> RPS Caclulate (self)
   --    SUCCESS RPS Swap -> RPS Caclulate (self)
   FOR xrec IN 
      SELECT * FROM lm3.lm_mtxjob
         WHERE referenceType = 204
           AND stage = depRpsstage
           AND status = depnotreadystat 
           AND psstage = completestage AND psstatus = completestat
      LOOP
         UPDATE lm3.PamSum SET (status, statusmodtime, stage, stagemodtime) 
                             = (depreadystat, currtime, depRpsstage, currtime)
            WHERE pamsumid = xrec.referenceid;
         UPDATE lm3.LMJob SET (status, statusmodtime) = (depreadystat, currtime)
            WHERE lmjobid = xrec.lmjobid;
         IF FOUND THEN
            movjb := movjb + 1;
         END IF;
      END LOOP;

   -- On error RPS (any stage), delete all jobs for this pamsum, delete pamsum
   FOR xrec IN 
      SELECT * FROM lm3.lm_mtxJob
         WHERE referenceType = 204 
           AND psstage = completestage 
           AND psstatus >= errorstat
      LOOP
         DELETE FROM lm3.LMJob WHERE referenceid = xrec.referenceid;
         DELETE FROM lm3.PamSum WHERE pamsumid = xrec.referenceId;
      END LOOP;
      
   RETURN movjb;
END;
$$  LANGUAGE 'plpgsql' VOLATILE;

-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lm3.lm_deleteJob(jid int)
RETURNS int AS
$$
DECLARE
   success int = -1;
BEGIN
   DELETE FROM lm3.LMJob WHERE lmjobId = jid; 

   IF FOUND THEN
      success := 0;
   END IF;

   RETURN success;

END;
$$  LANGUAGE 'plpgsql' VOLATILE;

-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lm3.lm_deleteJobsForBucket(bktid int)
RETURNS int AS
$$
DECLARE
   success int = -1;
   currtotal int;
   total int = 0;
BEGIN
   DELETE FROM lm3.LMJob WHERE lmjobId in 
      (SELECT lmjobid FROM lm3.lm_intjob WHERE bucketid = bktid); 
   GET DIAGNOSTICS currtotal = ROW_COUNT;
   total = total + currtotal;

   DELETE FROM lm3.LMJob WHERE lmjobId in 
      (SELECT lmjobid FROM lm3.lm_mtxjob WHERE bucketid = bktid); 
   GET DIAGNOSTICS currtotal = ROW_COUNT;
   total = total + currtotal;

   IF FOUND THEN
      success := 0;
   END IF;

   RETURN total;

END;
$$  LANGUAGE 'plpgsql' VOLATILE;

-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lm3.lm_deleteJobsForExperiment(expid int)
RETURNS int AS
$$
DECLARE
   success int = -1;
   currtotal int;
   total int = 0;
BEGIN
   DELETE FROM lm3.LMJob WHERE lmjobId in 
      (SELECT lmjobid FROM lm3.lm_intjob WHERE experimentid = expid); 
   GET DIAGNOSTICS currtotal = ROW_COUNT;
   total = total + currtotal;

   DELETE FROM lm3.LMJob WHERE lmjobId in 
      (SELECT lmjobid FROM lm3.lm_mtxjob WHERE experimentid = expid); 
   GET DIAGNOSTICS currtotal = ROW_COUNT;
   total = total + currtotal;

   IF FOUND THEN
      success := 0;
   END IF;

   RETURN total;

END;
$$  LANGUAGE 'plpgsql' VOLATILE;

-- ----------------------------------------------------------------------------
-- lm_assembleFilters
CREATE OR REPLACE FUNCTION lm3.lm_assembleFilters(usr varchar, indatatype int, calctype int)
   RETURNS varchar AS
$$
DECLARE
   filter varchar := '';
BEGIN
   IF usr is not null THEN 
      filter := filter || ' mdluserid = ' || quote_literal(usr) ;
   END IF;
   IF indatatype is not null THEN 
      begin
         IF length(filter) > 1 THEN
            filter := filter || ' AND ';
         END IF;
         filter := filter || ' reqdata = ' || quote_literal(indatatype) ;
      end;
   END IF;
   IF calctype is not null THEN 
      begin
         IF length(filter) > 1 THEN
            filter := filter || ' AND ';
         END IF;
         filter := filter || ' reqsoftware = ' || quote_literal(calctype) ;
      end;
   END IF;
   return filter;
END;
$$  LANGUAGE 'plpgsql' STABLE;

-- ----------------------------------------------------------------------------
-- lm_resetLifelessJobs
CREATE OR REPLACE FUNCTION lm3.lm_resetLifelessJobs(giveuptime double precision,
                                                    currtime double precision, 
                                                    pulledStat int,
                                                    initStat int, 
                                                    completeStat int)
   RETURNS int AS
$$
DECLARE
   irec lm3.lm_intJob;
   xrec lm3.lm_mtxJob;
   mrec lm3.lm_msgJob;
   total int := 0;
BEGIN
   
   -- LOCK rows with 'for update'
   FOR irec in 
      SELECT * FROM lm3.lm_intjob 
         WHERE lastheartbeat < giveuptime AND status >= pulledStat AND status < completeStat
         ORDER BY priority DESC, statusmodtime ASC FOR UPDATE
   LOOP
      RAISE NOTICE 'Reseting bucket %, job %', irec.bucketid, irec.lmJobId;
      UPDATE lm3.LmJob SET (status, statusmodtime, lastheartbeat, computeResourceId) 
                         = (initStat, currtime, NULL, NULL) 
         WHERE lmJobId = irec.lmJobId;
      UPDATE lm3.bucket SET (status, statusmodtime, computeResourceId) 
                         = (initStat, currtime, NULL) 
         WHERE bucketid = irec.bucketId;
      total = total + 1;
   END LOOP;   
   
   FOR xrec in 
      SELECT * FROM lm3.lm_mtxjob 
         WHERE lastheartbeat < giveuptime AND status >= pulledStat AND status < completeStat
         ORDER BY priority DESC, statusmodtime ASC FOR UPDATE
   LOOP
      RAISE NOTICE 'Reseting pamsum %, job %', xrec.pamsumId, xrec.lmJobId;
      UPDATE lm3.LmJob SET (status, statusmodtime, lastheartbeat, computeResourceId) 
                         = (initStat, currtime, NULL, NULL) 
         WHERE lmJobId = xrec.lmJobId;
      UPDATE lm3.PamSum SET (status, statusmodtime, computeResourceId) 
                              = (initStat, currtime, NULL) 
         WHERE pamsumId = xrec.pamsumId;
      total = total + 1;
   END LOOP;   

   FOR mrec in 
      SELECT * FROM lm3.lm_msgjob 
         WHERE lastheartbeat < giveuptime AND status >= pulledStat AND status < completeStat
         ORDER BY priority DESC, statusmodtime ASC FOR UPDATE
   LOOP
      RAISE NOTICE 'Reseting notification job %', prec.pamsumId, prec.lmJobId;
      UPDATE lm3.LmJob SET (status, statusmodtime, lastheartbeat, computeResourceId) 
                         = (initStat, currtime, NULL, NULL) 
         WHERE lmJobId = mrec.lmJobId;
      total = total + 1;
   END LOOP;   

   RETURN total;
END;
$$  LANGUAGE 'plpgsql' VOLATILE;

-- ----------------------------------------------------------------------------
-- Pulls lm_intJobs
-- Make sure to lock tables using SELECT ... FOR UPDATE whenever pulling jobs
CREATE OR REPLACE FUNCTION lm3.lm_pullBucketJobs(total int,
                                                 ptype int, 
                                                 startStat int, 
                                                 endStat int,
                                                 usr varchar, 
                                                 inputtype int, 
                                                 currtime double precision,
                                                 ipaddr varchar)
   RETURNS SETOF lm3.lm_intJob AS
$$
DECLARE
   rec lm3.lm_intJob;
   retrec lm3.lm_intJob;
   crid int;
   cmd varchar;
   start varchar;
   extra varchar;
   filters varchar;
BEGIN
   SELECT INTO crid computeResourceId 
      FROM lm3.computeResource WHERE ipaddress = ipaddr;
      
   -- Pull intersect jobs
   start := 'SELECT * FROM lm3.lm_intJob WHERE ';
   filters := ' reqsoftware = ' || quote_literal(ptype) || ' AND ' ||
              ' status = ' || quote_literal(startStat);
   extra := ' ORDER BY priority DESC, datecreated ASC LIMIT ' 
            || quote_literal(total) || ' FOR UPDATE';

   IF usr is not null THEN 
      filters := filters || ' AND userid = ' || quote_literal(usr) ;
   END IF;
   IF inputtype is not null THEN 
      filters := filters || ' AND reqdata = ' || quote_literal(inputtype) ;
   END IF;
   
   cmd := start || filters || extra;
   RAISE NOTICE 'Loop1: %', cmd;

   FOR rec in EXECUTE cmd
   LOOP
      RAISE NOTICE 'Moving intersect job %', rec.lmJobId;
      UPDATE lm3.LmJob SET (status, statusmodtime, computeResourceId, lastheartbeat) 
         = (endStat, currtime, crid, currtime) WHERE lmJobId = rec.lmJobId;
      UPDATE lm3.Bucket SET (status, statusmodtime, stage, stagemodtime, 
                             computeResourceId) = 
                            (endStat, currtime, rec.stage, currtime, crid) 
         WHERE bucketId = rec.bucketId;
      UPDATE lm3.ComputeResource SET lastheartbeat = currtime 
         WHERE computeResourceId = crid;
      SELECT * INTO retrec FROM lm3.lm_intjob WHERE lmjobid = rec.lmjobid;
      RETURN NEXT rec;
   END LOOP;   
   
   RETURN;
END;
$$  LANGUAGE 'plpgsql' VOLATILE;


-- ----------------------------------------------------------------------------
-- Make sure to lock table using SELECT ... FOR UPDATE whenever pulling jobs
-- NOTE: lmjob.referenceType == ReferenceType.PamSum (203)
CREATE OR REPLACE FUNCTION lm3.lm_pullMatrixJobs(total int,
                                                 ptype int, 
                                                 startStat int, 
                                                 endStat int,
                                                 usr varchar, 
                                                 indatatype int, 
                                                 currtime double precision,
                                                 ipaddr varchar)
   RETURNS SETOF lm3.lm_mtxJob AS
$$
DECLARE
   rec lm3.lm_mtxJob;
   retrec lm3.lm_mtxJob;
   crid int;
   cmd varchar;
   start varchar;
   extra varchar;
   filters varchar;
BEGIN
   begin
      -- Get computeresource id for requesting resource.
      SELECT computeResourceId INTO STRICT crid FROM lm3.ComputeResource
         WHERE ipaddress = ipaddr;
      EXCEPTION
         WHEN NO_DATA_FOUND THEN
            RAISE NOTICE 'ComputeResource not found for IP %', ipaddr;
   end;

   -- Pull matrix Jobs
   start := 'SELECT * FROM lm3.lm_mtxJob WHERE ';
   filters := ' reqsoftware = ' || quote_literal(ptype) || ' AND ' ||
              ' status = ' || quote_literal(startStat);
   extra := ' ORDER BY priority DESC, datecreated ASC LIMIT ' 
            || quote_literal(total) || ' FOR UPDATE';

   IF usr is not null THEN 
      filters := filters || ' AND userid = ' || quote_literal(usr) ;
   END IF;
   IF indatatype is not null THEN 
      filters := filters || ' AND reqdata = ' || quote_literal(indatatype) ;
   END IF;
      
   cmd := start || filters || extra;
   RAISE NOTICE 'Loop1: %', cmd;
   
   FOR rec in EXECUTE cmd
   LOOP
      RAISE NOTICE 'Moving bucket %', rec.lmJobId;
      UPDATE lm3.LmJob 
         SET (status, statusmodtime, computeResourceId, lastheartbeat) 
           = (endStat, currtime, crid, currtime) WHERE lmJobId = rec.lmJobId;
      UPDATE lm3.PamSum 
         SET (status, statusmodtime, stage, stagemodtime, computeResourceId) 
           = (endStat, currtime, rec.stage, currtime, crid) 
         WHERE pamsumId = rec.pamsumId;
      UPDATE lm3.ComputeResource SET lastheartbeat = currtime 
         WHERE computeResourceId = crid;
      SELECT * INTO retrec FROM lm3.lm_mtxjob WHERE lmjobid = rec.lmjobid; 
      RETURN NEXT retrec;
   END LOOP;   
   
   RETURN;
END;
$$  LANGUAGE 'plpgsql' VOLATILE;

-- ----------------------------------------------------------------------------
-- Make sure to lock table using SELECT ... FOR UPDATE whenever pulling jobs
-- NOTE: lmjob.referenceType == ReferenceType.PamSum (203)
CREATE OR REPLACE FUNCTION lm3.lm_pullGridJobs(total int,
                                                 ptype int, 
                                                 startStat int, 
                                                 endStat int,
                                                 usr varchar, 
                                                 currtime double precision,
                                                 ipaddr varchar)
   RETURNS SETOF lm3.lm_grdJob AS
$$
DECLARE
   rec lm3.lm_grdJob;
   retrec lm3.lm_grdJob;
   crid int;
   cmd varchar;
   start varchar;
   extra varchar;
   filters varchar;
BEGIN
   begin
      -- Get computeresource id for requesting resource.
      SELECT computeResourceId INTO STRICT crid FROM lm3.ComputeResource
         WHERE ipaddress = ipaddr;
      EXCEPTION
         WHEN NO_DATA_FOUND THEN
            RAISE NOTICE 'ComputeResource not found for IP %', ipaddr;
   end;

   -- Pull buildgrid Jobs
   start := 'SELECT * FROM lm3.lm_grdJob WHERE ';
   filters := ' reqsoftware = ' || quote_literal(ptype) || ' AND ' ||
              ' status = ' || quote_literal(startStat);
   extra := ' ORDER BY priority DESC, datecreated ASC LIMIT ' 
            || quote_literal(total) || ' FOR UPDATE';

   IF usr is not null THEN 
      filters := filters || ' AND userid = ' || quote_literal(usr) ;
   END IF;
      
   cmd := start || filters || extra;
   RAISE NOTICE 'Loop1: %', cmd;
   
   FOR rec in EXECUTE cmd
   LOOP
      RAISE NOTICE 'Moving shapegrid job %', rec.lmJobId;
      UPDATE lm3.LmJob 
         SET (status, statusmodtime, computeResourceId, lastheartbeat) 
           = (endStat, currtime, crid, currtime) WHERE lmJobId = rec.lmJobId;
      UPDATE lm3.ShapeGrid 
         SET (status, statusmodtime, computeResourceId) 
           = (endStat, currtime, crid) 
         WHERE shapegridId = rec.shapegridId;
      UPDATE lm3.ComputeResource SET lastheartbeat = currtime 
         WHERE computeResourceId = crid;
      SELECT * INTO retrec FROM lm3.lm_grdjob WHERE lmjobid = rec.lmjobid; 
      RETURN NEXT retrec;
   END LOOP;   
   
   RETURN;
END;
$$  LANGUAGE 'plpgsql' VOLATILE;

-- ----------------------------------------------------------------------------
-- Make sure to lock lmJob table whenever pulling jobs
CREATE OR REPLACE FUNCTION lm3.lm_pullMessageJobs(total int, 
                                                  processType int,
                                                  startStat int,
                                                  endStat int,
                                                  usr varchar,
                                                  currtime double precision,
                                                  crip varchar)
   RETURNS SETOF lm3.lm_msgJob AS
$$
DECLARE
   crid int;
   rec lm3.lm_msgJob;
   cmd varchar;
   start varchar;
   extra varchar;
   filters varchar := '';
   retrec lm3.lm_msgJob;
BEGIN
   begin
      -- Get computeresource id for requesting resource.
      SELECT computeResourceId INTO STRICT crid FROM lm3.computeResource 
         WHERE ipaddress = crip;
      EXCEPTION
         WHEN NO_DATA_FOUND THEN
            RAISE NOTICE 'ComputeResource not found for IP %', crip;
   end;
       
   -- Move continuing NotifyJobs to next status 
   start := 'SELECT * FROM lm3.lm_msgJob WHERE ';
   filters := ' status = ' || quote_literal(startStat);
   extra := ' ORDER BY priority DESC, statusmodtime ASC LIMIT ' 
            || quote_literal(total) || ' FOR UPDATE';

   IF usr is not null THEN 
      filters := filters || ' AND mdluserid = ' || quote_literal(usr) ;
   END IF;
   
   cmd := start || filters || extra;
   RAISE NOTICE 'Loop1: %', cmd;
   
   FOR rec in EXECUTE cmd
   LOOP
      RAISE NOTICE 'Pulling bucket/notify %, job %', rec.bucketId, rec.lmJobId;
      IF crid IS NOT NULL THEN
         begin
            UPDATE lm3.LmJob SET (status, statusmodtime, lastheartbeat, computeResourceId) 
                               = (endStat, currtime, currtime, crid) 
               WHERE lmJobId = rec.lmJobId;
            UPDATE lm3.Bucket SET (status, statusmodtime, lastheartbeat, computeResourceId) 
                               = (endStat, currtime, currtime, crid) 
               WHERE bucketId = rec.bucketId;
            UPDATE lm3.ComputeResource SET lastheartbeat = currtime 
               WHERE computeResourceId = crid;
         end;
      ELSE
         begin
            UPDATE lm3.LmJob SET (status, statusmodtime) = (endStat, currtime) 
               WHERE lmJobId = rec.lmJobId;
            UPDATE lm3.Bucket SET (status, statusmodtime) = (endStat, currtime) 
               WHERE bucketId = rec.bucketId;
         end;
      END IF;
      SELECT * FROM lm3.lm_msgJob INTO retrec WHERE lmJobId = rec.lmJobId;       
      RETURN NEXT retrec;
   END LOOP;   
   
   RETURN;
END;
$$  LANGUAGE 'plpgsql' VOLATILE;



-- ----------------------------------------------------------------------------
-- Deletes all connected jobs and Bucket, cascade deletes PamSums 
CREATE OR REPLACE FUNCTION lm3.lm_deleteBucket(bktid int)
RETURNS int AS
$$
DECLARE
   success int = -1;
BEGIN
   DELETE FROM lm3.LMJob WHERE lmjobId in 
      (SELECT lmjobid FROM lm3.lm_intjob WHERE bucketid = bktid); 

   DELETE FROM lm3.LMJob WHERE lmjobId in 
      (SELECT lmjobid FROM lm3.lm_mtxjob WHERE bucketid = bktid); 
   
   DELETE FROM lm3.Bucket WHERE bucketid = bktid;
   
   IF FOUND THEN
      success = 0;
   END IF;
   RETURN success;
END;
$$  LANGUAGE 'plpgsql' VOLATILE;

-- ----------------------------------------------------------------------------
-- Deletes all connected jobs and PamSums 
CREATE OR REPLACE FUNCTION lm3.lm_deletePamsum(psid int)
RETURNS int AS
$$
DECLARE
   success int = -1;
BEGIN
   DELETE FROM lm3.LMJob WHERE lmjobId in 
      (SELECT lmjobid FROM lm3.lm_mtxjob WHERE pamsumId = psid); 
   
   DELETE FROM lm3.PamSum WHERE pamsumId = psid;
   
   IF FOUND THEN
      success = 0;
   END IF;
   RETURN success;
END;
$$  LANGUAGE 'plpgsql' VOLATILE;


-- ----------------------------------------------------------------------------
-- Deletes all connected jobs and Experiment, cascade deletes Bucket, PamSum, 
-- ExperimentPALayer, ExperimentAncLayer, BucketPALayer, BucketAncLayer
CREATE OR REPLACE FUNCTION lm3.lm_deleteExperiment(expid int)
RETURNS int AS
$$
DECLARE
   success int = -1;
   bktid int;
BEGIN
   DELETE FROM lm3.LMJob WHERE lmjobId in 
      (SELECT lmjobid FROM lm3.lm_intjob WHERE experimentid = expid); 

   DELETE FROM lm3.LMJob WHERE lmjobId in 
      (SELECT lmjobid FROM lm3.lm_mtxjob WHERE experimentid = expid); 

   DELETE FROM lm3.LMJob WHERE lmjobId in 
      (SELECT lmjobid FROM lm3.lm_msgjob WHERE experimentid = expid); 

   DELETE FROM lm3.Experiment WHERE experimentid = expid;

   IF FOUND THEN
      success = 0;
   END IF;
   RETURN success;
END;
$$  LANGUAGE 'plpgsql' VOLATILE;

-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lm3.lm_getExperiment(expid int, usr varchar)
RETURNS lm3.experiment AS
$$
DECLARE
   rec lm3.experiment%ROWTYPE;
BEGIN
   begin
      SELECT * INTO STRICT rec FROM lm3.experiment
         WHERE experimentid = expid AND userid = usr;

      EXCEPTION
         WHEN NO_DATA_FOUND THEN
            RAISE NOTICE 'Experiment for user %, id = % not found', usr, expid;
   end;
   
   RETURN rec;
END;
$$  LANGUAGE 'plpgsql' STABLE;

-- ----------------------------------------------------------------------------
-- Buckets
-- ----------------------------------------------------------------------------
-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lm3.lm_countBuckets(usrid varchar(20), 
                                           beforetime double precision,
                                           aftertime double precision,
                                           epsg int,
                                           eid int,
                                           ename varchar,
                                           sid int,
                                           sname varchar)
   RETURNS int AS
$$
DECLARE
   num int;
   cmd varchar;
   wherecls varchar;
BEGIN
   cmd = 'select count(*) FROM lm3.lm_fullradbucket ';
   wherecls = ' WHERE expuserId =  ' || quote_literal(usrid) ;

   -- filter by modified before given time
   IF beforetime is not null THEN
      wherecls = wherecls || ' AND bktstatusmodtime <=  ' || beforetime;
   END IF;

   -- filter by modified after given time
   IF aftertime is not null THEN
      wherecls = wherecls || ' AND bktstatusmodtime >=  ' || aftertime;
   END IF;

   -- filter by epsgcode
   IF epsg is not null THEN
      wherecls = wherecls || ' AND  expepsgcode =  ' || epsg;
   END IF;
   
   -- filter by ExperimentId
   IF eid is not null THEN
      wherecls = wherecls || ' AND experimentId =  ' || quote_literal(eid);
   END IF;

   -- filter by ExperimentName
   IF ename is not null THEN
      wherecls = wherecls || ' AND expname =  ' || quote_literal(ename);
   END IF;

   -- filter by ShapeGridId
   IF sid is not null THEN
      wherecls = wherecls || ' AND shapeGridId =  ' || quote_literal(sid);
   END IF;

   -- filter by ShapeGridName
   IF sname is not null THEN
      wherecls = wherecls || ' AND layername =  ' || quote_literal(sname);
   END IF;

   cmd := cmd || wherecls;
   RAISE NOTICE 'cmd = %', cmd;

   EXECUTE cmd INTO num;
   RETURN num;
END;
$$  LANGUAGE 'plpgsql' STABLE;

-- ----------------------------------------------------------------------------
-- lm_listBuckets
CREATE OR REPLACE FUNCTION lm3.lm_listBuckets(firstRecNum int, maxNum int, 
                                         usrid varchar(20), 
                                         beforetime double precision,
                                         aftertime double precision,
                                         epsg int,
                                         eid int,
                                         ename varchar,
                                         sid int,
                                         sname varchar)
   RETURNS SETOF lm3.lm_atom AS
$$
DECLARE
   rec lm3.lm_atom;
   cmd varchar;
   wherecls varchar;
   limitcls varchar;
   ordercls varchar;
BEGIN
   cmd = 'SELECT bucketId, layername, expepsgcode, bktstatusmodtime
               FROM lm3.lm_fullradbucket ';
   wherecls = ' WHERE expuserId =  ' || quote_literal(usrid) ;
   ordercls = ' ORDER BY bktstatusmodtime DESC ';
   limitcls = ' LIMIT ' || quote_literal(maxNum) || ' OFFSET ' || quote_literal(firstRecNum);

   -- filter by modified before given time
   IF beforetime is not null THEN
      wherecls = wherecls || ' AND bktstatusmodtime <=  ' || beforetime;
   END IF;

   -- filter by modified after given time
   IF aftertime is not null THEN
      wherecls = wherecls || ' AND bktstatusmodtime >=  ' || aftertime;
   END IF;

   -- filter by epsgcode
   IF epsg is not null THEN
      wherecls = wherecls || ' AND  expepsgcode =  ' || epsg;
   END IF;
   
   -- filter by ExperimentId
   IF eid is not null THEN
      wherecls = wherecls || ' AND experimentId =  ' || quote_literal(eid);
   END IF;

   -- filter by ExperimentName
   IF ename is not null THEN
      wherecls = wherecls || ' AND expname =  ' || quote_literal(ename);
   END IF;

   -- filter by ShapeGridId
   IF sid is not null THEN
      wherecls = wherecls || ' AND shapeGridId =  ' || quote_literal(sid);
   END IF;

   -- filter by ShapeGridName
   IF sname is not null THEN
      wherecls = wherecls || ' AND layername =  ' || quote_literal(sname);
   END IF;

   cmd := cmd || wherecls || ordercls || limitcls;
   RAISE NOTICE 'cmd = %', cmd;

   FOR rec in EXECUTE cmd
      LOOP
         RETURN NEXT rec;
      END LOOP;
   RETURN;
END;
$$  LANGUAGE 'plpgsql' STABLE;

-- ----------------------------------------------------------------------------
-- lm_listBuckets
CREATE OR REPLACE FUNCTION lm3.lm_listBucketObjects(firstRecNum int, maxNum int, 
                                         usrid varchar(20), 
                                         beforetime double precision,
                                         aftertime double precision,
                                         epsg int,
                                         eid int,
                                         ename varchar,
                                         sid int,
                                         sname varchar)
   RETURNS SETOF lm3.lm_fullradbucket AS
$$
DECLARE
   rec lm3.lm_fullradbucket;
   cmd varchar;
   wherecls varchar;
   limitcls varchar;
   ordercls varchar;
BEGIN
   cmd = 'SELECT * FROM lm3.lm_fullradbucket ';
   wherecls = ' WHERE expuserId =  ' || quote_literal(usrid) ;
   ordercls = ' ORDER BY bktstatusmodtime DESC ';
   limitcls = ' LIMIT ' || quote_literal(maxNum) || ' OFFSET ' || quote_literal(firstRecNum);

   -- filter by modified before given time
   IF beforetime is not null THEN
      wherecls = wherecls || ' AND bktstatusmodtime <=  ' || beforetime;
   END IF;

   -- filter by modified after given time
   IF aftertime is not null THEN
      wherecls = wherecls || ' AND bktstatusmodtime >=  ' || aftertime;
   END IF;

   -- filter by epsgcode
   IF epsg is not null THEN
      wherecls = wherecls || ' AND  expepsgcode =  ' || epsg;
   END IF;
   
   -- filter by ExperimentId
   IF eid is not null THEN
      wherecls = wherecls || ' AND experimentId =  ' || quote_literal(eid);
   END IF;

   -- filter by ExperimentName
   IF ename is not null THEN
      wherecls = wherecls || ' AND expname =  ' || quote_literal(ename);
   END IF;

   -- filter by ShapeGridId
   IF sid is not null THEN
      wherecls = wherecls || ' AND shapeGridId =  ' || quote_literal(sid);
   END IF;

   -- filter by ShapeGridName
   IF sname is not null THEN
      wherecls = wherecls || ' AND layername =  ' || quote_literal(sname);
   END IF;

   cmd := cmd || wherecls || ordercls || limitcls;
   RAISE NOTICE 'cmd = %', cmd;

   FOR rec in EXECUTE cmd
      LOOP
         RETURN NEXT rec;
      END LOOP;
   RETURN;
END;
$$  LANGUAGE 'plpgsql' STABLE;

-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lm3.lm_getExperimentBucket(expid int, bktid int, usr varchar)
RETURNS lm3.lm_fullradbucket AS
$$
DECLARE
   rec lm3.lm_fullradbucket%ROWTYPE;
BEGIN
   begin
      SELECT * INTO STRICT rec FROM lm3.lm_fullradbucket
         WHERE experimentid = expid AND bucketid = bktid AND expuserid = usr;

      EXCEPTION
         WHEN NO_DATA_FOUND THEN
            RAISE NOTICE 'Experiment/Bucket for user %, id = % not found', usr, expid;
   end;
   
   RETURN rec;
END;
$$  LANGUAGE 'plpgsql' STABLE;


-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lm3.lm_getBucket(bktid int)
RETURNS lm3.lm_fullradbucket AS
$$
DECLARE
   rec lm3.lm_fullradbucket%ROWTYPE;
BEGIN
   begin
      SELECT * INTO STRICT rec FROM lm3.lm_fullradbucket WHERE bucketid = bktid;

      EXCEPTION
         WHEN NO_DATA_FOUND THEN
            RAISE NOTICE 'Experiment/Bucket id = % not found', bktid;
   end;
   
   RETURN rec;
END;
$$  LANGUAGE 'plpgsql' STABLE;

-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lm3.lm_getBucketByShape(expid int, 
                                               sgname varchar, sgid int)
RETURNS lm3.lm_fullradbucket AS
$$
DECLARE
   rec lm3.lm_fullradbucket%ROWTYPE;
BEGIN
   begin
      IF sgname IS NOT null THEN
         SELECT * INTO STRICT rec FROM lm3.lm_fullradbucket
            WHERE experimentid = expid and layername = sgname;
      ELSE
         SELECT * INTO STRICT rec FROM lm3.lm_fullradbucket
            WHERE experimentid = expid and shapegridid = sgid;
      END IF;
      
      EXCEPTION
         WHEN NO_DATA_FOUND THEN
            RAISE NOTICE 'Experiment/Bucket ExperimentId = %, ShapeGrid % not found', 
                          expid, sgname;
   end;
   
   RETURN rec;
END;
$$  LANGUAGE 'plpgsql' STABLE;

-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lm3.lm_getExperimentBuckets(expid int, name varchar, usr varchar)
RETURNS SETOF lm3.lm_fullradbucket AS
$$
DECLARE
   rec lm3.lm_fullradbucket%ROWTYPE;
BEGIN
   IF expid IS NOT null THEN
      FOR rec in 
         SELECT * FROM lm3.lm_fullradbucket 
            WHERE experimentid = expid AND expuserid = usr
         LOOP
            RETURN NEXT rec;
      END LOOP;
   ELSE
      FOR rec in 
         SELECT * FROM lm3.lm_fullradbucket 
            WHERE expname = name AND expuserid = usr
         LOOP
            RETURN NEXT rec;
      END LOOP;
   END IF;
      
      EXCEPTION
         WHEN NO_DATA_FOUND THEN
            RAISE NOTICE 'Experiment for user %, id %, name = % not found', usr, name, expid;
   RETURN;

END;
$$  LANGUAGE 'plpgsql' STABLE;


-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lm3.lm_getBucketForNames(usr varchar, ename varchar,
                                                sgname varchar)
RETURNS lm3.lm_fullradbucket AS
$$
DECLARE
   rec lm3.lm_fullradbucket%ROWTYPE;
BEGIN
   begin
      SELECT * INTO STRICT rec FROM lm3.lm_fullradbucket
         WHERE expname = ename AND layername = sgname AND expuserid = usr;

      EXCEPTION
         WHEN NO_DATA_FOUND THEN
            RAISE NOTICE 'Experiment % / Bucket-shapeGrid % for user % not found', 
                          ename, sgname, usr;
   end;
   
   RETURN rec;
END;
$$  LANGUAGE 'plpgsql' STABLE;

-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lm3.lm_getBucketsForShapeName(usr varchar, sgname varchar)
RETURNS SETOF lm3.lm_fullradbucket AS
$$
DECLARE
   rec lm3.lm_fullradbucket%ROWTYPE;
BEGIN
   FOR rec IN 
      SELECT * FROM lm3.lm_fullradbucket
         WHERE layername = sgname AND expuserid = usr
      LOOP
         RETURN NEXT rec;
      END LOOP;      
   RETURN;
END;
$$  LANGUAGE 'plpgsql' STABLE;

-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lm3.lm_getBucketsForExperimentName(name varchar, usr varchar)
RETURNS SETOF lm3.lm_fullradbucket AS
$$
DECLARE
   rec lm3.lm_fullradbucket%ROWTYPE;
BEGIN
   FOR rec in 
      SELECT * FROM lm3.lm_fullradbucket 
         WHERE expname = name AND expuserid = usr
      LOOP
         RETURN NEXT rec;
      END LOOP;
      
   RETURN;
END;
$$  LANGUAGE 'plpgsql' STABLE;

-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lm3.lm_getBucketsByShapeId(usr varchar, shpid int)
RETURNS SETOF lm3.lm_fullradbucket AS
$$
DECLARE
   rec lm3.lm_fullradbucket%ROWTYPE;
BEGIN
   FOR rec in SELECT * FROM lm3.lm_fullradbucket 
      WHERE userid = usr and shapegridid = shpid
   
      LOOP
         RETURN NEXT rec;
      END LOOP;
   RETURN;
END;
$$  LANGUAGE 'plpgsql' STABLE;

-- ----------------------------------------------------------------------------
-- PamSums
-- ----------------------------------------------------------------------------
-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lm3.lm_insertPamSum(bckid int,
                                           rndmethod int,
                                           rndparams varchar,
                                           stat int,
                                           stattime double precision,
                                           stg int,
                                           stgtime double precision,
                                           crttime double precision,
                                           murlprefix varchar)
RETURNS int AS
$$
DECLARE
   psid int = -1;
   oldid int;
   murl varchar;
   idstr varchar;
BEGIN
   IF rndmethod = 0 THEN
      begin
         SELECT INTO oldid pamsumid FROM lm3.PamSum 
            WHERE bucketid = bckid AND randommethod = 0;
         IF FOUND THEN
            RAISE EXCEPTION 'Original PamSum % already exists for Bucket %', 
                             oldid, bckid;
         END IF;
      end;
   END IF;

   INSERT INTO lm3.PamSum (bucketId, randommethod, randomparams, 
                       status, statusmodtime, stage, stagemodtime, datecreated)
               values (bckid, rndmethod, rndparams, 
                       stat, stattime, stg, stgtime, crttime);
   IF FOUND THEN
      SELECT INTO psid last_value FROM lm3.pamsum_pamsumid_seq;
      idstr := cast(psid as varchar);
      murl := replace(murlprefix, '#id#', idstr);
      UPDATE lm3.PamSum SET metadataurl = murl WHERE pamsumId = psid;
   END IF;
   
   RETURN psid;
END;
$$  LANGUAGE 'plpgsql' VOLATILE;

-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lm3.lm_updatePamSumFilenames(psid int,
                                                    pdloc varchar,
                                                    sdloc varchar,
                                                    spdloc varchar)
RETURNS int AS
$$
DECLARE
   success int = -1;
   origcount int;
BEGIN
   UPDATE lm3.PamSum SET (pamdlocation, sumdlocation, splotchpamdlocation) 
                   = (pdloc, sdloc, spdloc) WHERE pamsumid = psid;
   IF FOUND THEN
      success := 0;
   END IF;
   
   RETURN success;
END;
$$  LANGUAGE 'plpgsql' VOLATILE;

-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lm3.lm_updatePamSumStatus(psid int,
                                                 stat int,
                                                 stattime double precision,
                                                 stg int,
                                                 stgtime double precision,
                                                 crid int)
RETURNS int AS
$$
DECLARE
   success int = -1;
   origcount int;
BEGIN
   UPDATE lm3.PamSum 
       SET (status, statusmodtime, stage, stagemodtime, computeresourceid) 
         = (stat, stattime, stg, stgtime, crid) WHERE pamsumid = psid;
   IF FOUND THEN
      success := 0;
   END IF;
   
   RETURN success;
END;
$$  LANGUAGE 'plpgsql' VOLATILE;

-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lm3.lm_getRandomPamSumsForBucket(bktid int)
RETURNS SETOF lm3.lm_pamsum AS
$$
DECLARE
   rec lm3.lm_pamsum%ROWTYPE;
BEGIN
   FOR rec in SELECT * FROM lm3.lm_pamsum 
      WHERE bucketid = bktid AND randomMethod > 0
   
      LOOP
         RETURN NEXT rec;
      END LOOP;
   RETURN;
END;
$$  LANGUAGE 'plpgsql' STABLE;

-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lm3.lm_getOriginalPamSumForBucket(bktid int)
RETURNS lm3.lm_pamsum AS
$$
DECLARE
   rec lm3.lm_pamsum%ROWTYPE;
BEGIN
   begin
      SELECT * INTO STRICT rec FROM lm3.lm_pamsum 
         WHERE bucketid = bktid AND randomMethod = 0;
      
      EXCEPTION
         WHEN NO_DATA_FOUND THEN
            RAISE NOTICE 'Original PamSum for bucket % not found', bktid;
   end;
   
   RETURN rec;
END;
$$  LANGUAGE 'plpgsql' STABLE;

-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lm3.lm_getPamSum(psid int)
RETURNS lm3.lm_pamsum AS
$$
DECLARE
   rec lm3.lm_pamsum%ROWTYPE;
BEGIN
   begin
      SELECT *  INTO STRICT rec FROM lm3.lm_pamsum 
         WHERE pamsumid = psid;
      
      EXCEPTION
         WHEN NO_DATA_FOUND THEN
            RAISE NOTICE 'PamSum for id = % not found', psid;
   end;
   
   RETURN rec;
END;
$$  LANGUAGE 'plpgsql' STABLE;


-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lm3.lm_listPamSums(firstRecNum int, maxNum int, 
                                            usrid varchar(20), 
                                          beforetime double precision,
                                          aftertime double precision,
                                          epsg int,
                                          eid int,
                                          bid int,
                                          israndom boolean,
                                          rndmethod int)
RETURNS SETOF lm3.lm_atom AS
$$
DECLARE
   rec lm3.lm_atom;
   cmd varchar;
   wherecls varchar;
   limitcls varchar;
   ordercls varchar;
   rmthd int;
BEGIN
   cmd = 'SELECT pamsumId, expname, expepsgcode, psstatusModTime, randomMethod FROM lm3.lm_pamsum ';
   wherecls = ' WHERE expuserid =  ' || quote_literal(usrid) ;
   ordercls = ' ORDER BY psstatusModTime DESC ';
   limitcls = ' LIMIT ' || quote_literal(maxNum) || ' OFFSET ' || quote_literal(firstRecNum);

   -- filter by modified before given time
   IF beforetime is not null THEN
      wherecls = wherecls || ' AND psstatusModTime <=  ' || quote_literal(beforetime);
   END IF;

   -- filter by modified after given time
   IF aftertime is not null THEN
      wherecls = wherecls || ' AND psstatusModTime >=  ' || quote_literal(aftertime);
   END IF;

   -- filter by epsgcode
   IF epsg is not null THEN
      wherecls = wherecls || ' AND  epsgcode =  ' || epsg;
   END IF;
   
   -- filter by IsRandom
   IF israndom is not null THEN
      IF israndom is False THEN
         wherecls = wherecls || ' AND randomMethod = 0 ';
      ELSE
         wherecls = wherecls || ' AND randomMethod > 0 ';
      END IF;
   END IF;
   
      -- filter by RandomMethod
   IF rndmethod is not null THEN
      wherecls = wherecls || ' AND randomMethod = ' || quote_literal(rndmethod) ;
   END IF;

   -- filter by ExperimentId
   IF eid is not null THEN
      wherecls = wherecls || ' AND experimentId = ' || quote_literal(eid);
   END IF;

   -- filter by BucketId
   IF bid is not null THEN
      wherecls = wherecls || ' AND bucketId =  ' || quote_literal(bid);
   END IF;

   cmd := cmd || wherecls || ordercls || limitcls;
   RAISE NOTICE 'cmd = %', cmd;

   FOR rec.id, rec.title, rec.epsgcode, rec.modtime, rmthd in EXECUTE cmd
      LOOP
         IF rmthd > 0 THEN
            rec.title := 'Random PamSum for ' || rec.title;
         END IF;
         RETURN NEXT rec;
      END LOOP;
   RETURN;
END;
$$  LANGUAGE 'plpgsql' STABLE;

-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lm3.lm_listPamSumObjects(firstRecNum int, maxNum int, 
                                          usrid varchar(20), 
                                          beforetime double precision,
                                          aftertime double precision,
                                          epsg int,
                                          eid int,
                                          bid int,
                                          israndom boolean,
                                          rndmethod int)
RETURNS SETOF lm3.lm_pamsum AS
$$
DECLARE
   rec lm3.lm_pamsum;
   cmd varchar;
   wherecls varchar;
   limitcls varchar;
   ordercls varchar;
BEGIN
   cmd = 'SELECT * FROM lm3.lm_pamsum ';
   wherecls = ' WHERE expuserid =  ' || quote_literal(usrid) ;
   ordercls = ' ORDER BY psstatusModTime DESC ';
   limitcls = ' LIMIT ' || quote_literal(maxNum) || ' OFFSET ' || quote_literal(firstRecNum);

   -- filter by modified before given time
   IF beforetime is not null THEN
      wherecls = wherecls || ' AND psstatusModTime <=  ' || quote_literal(beforetime);
   END IF;

   -- filter by modified after given time
   IF aftertime is not null THEN
      wherecls = wherecls || ' AND psstatusModTime >=  ' || quote_literal(aftertime);
   END IF;

   -- filter by epsgcode
   IF epsg is not null THEN
      wherecls = wherecls || ' AND  epsgcode =  ' || epsg;
   END IF;
   
      -- filter by RandomMethod
   IF rndmethod is not null THEN
      wherecls = wherecls || ' AND randomMethod = ' || quote_literal(rndmethod) ;
   ELSE
      begin
         -- filter by IsRandom
         IF israndom is not null THEN
            IF israndom is False THEN
               wherecls = wherecls || ' AND randomMethod = 0 ';
            ELSE
               wherecls = wherecls || ' AND randomMethod > 0 ';
            END IF;
         END IF;
      end;
   END IF;
   
   -- filter by ExperimentId
   IF eid is not null THEN
      wherecls = wherecls || ' AND experimentId = ' || quote_literal(eid);
   END IF;

   -- filter by BucketId
   IF bid is not null THEN
      wherecls = wherecls || ' AND bucketId =  ' || quote_literal(bid);
   END IF;

   cmd := cmd || wherecls || ordercls || limitcls;
   RAISE NOTICE 'cmd = %', cmd;

   FOR rec in EXECUTE cmd
      LOOP
         RETURN NEXT rec;
      END LOOP;
   RETURN;
END;
$$  LANGUAGE 'plpgsql' STABLE;

-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lm3.lm_countPamSums(usrid varchar(20), 
                                           beforetime double precision,
                                           aftertime double precision,
                                           epsg int,
                                           eid int,
                                           bid int,
                                           israndom boolean,
                                           rndmethod int)
RETURNS int AS
$$
DECLARE
   num int;
   cmd varchar;
   wherecls varchar;
BEGIN
   cmd = 'SELECT count(*) FROM lm3.lm_pamsum ';
   wherecls = ' WHERE expuserid =  ' || quote_literal(usrid) ;

   -- filter by modified before given time
   IF beforetime is not null THEN
      wherecls = wherecls || ' AND psstatusModTime <=  ' || quote_literal(beforetime);
   END IF;

   -- filter by modified after given time
   IF aftertime is not null THEN
      wherecls = wherecls || ' AND psstatusModTime >=  ' || quote_literal(aftertime);
   END IF;

   -- filter by epsgcode
   IF epsg is not null THEN
      wherecls = wherecls || ' AND  epsgcode =  ' || epsg;
   END IF;
   
   -- filter by IsRandom
   IF israndom is not null THEN
      IF israndom is False THEN
         wherecls = wherecls || ' AND randomMethod = 0 ';
      ELSE
         wherecls = wherecls || ' AND randomMethod > 0 ';
      END IF;
   END IF;
   
   -- filter by RandomMethod
   IF rndmethod is not null THEN
      wherecls = wherecls || ' AND randomMethod = ' || quote_literal(rndmethod) ;
   END IF;

   -- filter by ExperimentId
   IF eid is not null THEN
      wherecls = wherecls || ' AND experimentId = ' || quote_literal(eid);
   END IF;

   -- filter by BucketId
   IF bid is not null THEN
      wherecls = wherecls || ' AND bucketId =  ' || quote_literal(bid);
   END IF;

   cmd := cmd || wherecls;
   RAISE NOTICE 'cmd = %', cmd;

   EXECUTE cmd INTO num;
   RETURN num;
END;
$$  LANGUAGE 'plpgsql' STABLE;

-- ----------------------------------------------------------------------------
-- Experiments
-- ----------------------------------------------------------------------------
-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lm3.lm_findExperimentsByUser(usr varchar)
RETURNS SETOF lm3.experiment AS
$$
DECLARE
   rec lm3.experiment%ROWTYPE;
BEGIN
   FOR rec in SELECT * FROM lm3.experiment WHERE userid = usr 
      LOOP
         RETURN NEXT rec;
      END LOOP;
   RETURN;
END;
$$  LANGUAGE 'plpgsql' STABLE;

-- ----------------------------------------------------------------------------
-- select * FROM lm3.lm_findExperimentsByUser(E'astewart');
-- select * FROM lm3.lm_findExperimentsByUser(E'astewart',55811.8589232);
CREATE OR REPLACE FUNCTION lm3.lm_getExperimentByName(ename varchar,usr varchar)
RETURNS lm3.experiment AS
$$
DECLARE
   rec lm3.experiment%ROWTYPE;
BEGIN
   SELECT * INTO rec FROM lm3.experiment WHERE expname=ename AND userid = usr;

   RETURN rec;
END;
$$  LANGUAGE 'plpgsql' STABLE;

-- ----------------------------------------------------------------------------
-- Obsolete with next update
CREATE OR REPLACE FUNCTION lm3.lm_resetJobs(stattime double precision, 
                                        inprocess_stat int, 
                                        preprocess_stat int)
   RETURNS int AS
$$
DECLARE
   rowcount int;
   total int;
   rec lm3.lmjob%ROWTYPE;
BEGIN
   UPDATE lm3.LMJob SET (status, statusModTime) = (preprocess_stat, stattime)
      WHERE status = inprocess_stat;
   GET DIAGNOSTICS total = ROW_COUNT;

   UPDATE lm3.Bucket SET (status, statusModTime) = (preprocess_stat, stattime) 
      WHERE status = inprocess_stat;
   UPDATE lm3.PamSum SET (status, statusModTime) = (preprocess_stat, stattime) 
      WHERE status = inprocess_stat;

   RETURN total;
END;
$$  LANGUAGE 'plpgsql' VOLATILE;

-- ----------------------------------------------------------------------------
-- Rolls back status and stage of bucket, deletes original and random PamSums
CREATE OR REPLACE FUNCTION lm3.lm_rollbackExperiment(expid int,
                                                     rollbackStatus int, 
                                                     rollbackStage int,
                                                     stattime double precision)
   RETURNS int AS
$$
DECLARE
   total int := 0;
   bktid int;
   psid int;
BEGIN
   -- Rollback Buckets
   FOR bktid IN 
      SELECT bucketid FROM lm3.bucket WHERE experimentid = expid
   LOOP 
      UPDATE lm3.Bucket SET (status, statusModTime, stage, stageModTime) 
                          = (rollbackStatus, stattime, rollbackStage, stattime) 
      WHERE bucketid = bktid;
      total := total + 1;
   END LOOP;
   
   -- Rollback PamSums
   FOR psid IN 
      SELECT pamsumid FROM lm3.lm_pamsum WHERE experimentid = expid
   LOOP 
      DELETE FROM lm3.PamSum WHERE pamsumid = psid;
   END LOOP;
   
   RETURN total;
END;
$$  LANGUAGE 'plpgsql' VOLATILE;

-- ----------------------------------------------------------------------------
-- Rolls back status and stage of bucket, deletes original and random PamSums
CREATE OR REPLACE FUNCTION lm3.lm_rollbackBucket(bktid int,
                                                 rollbackStatus int, 
                                                 rollbackStage int,
                                                 stattime double precision)
   RETURNS int AS
$$
DECLARE
   total int := 0;
   psid int;
BEGIN
   -- Rollback Buckets
   UPDATE lm3.Bucket SET (status, statusModTime, stage, stageModTime) 
                       = (rollbackStatus, stattime, rollbackStage, stattime) 
      WHERE bucketid = bktid;
   
   -- Rollback PamSums
   FOR psid IN 
      SELECT pamsumid FROM lm3.lm_pamsum WHERE bucketid = bktid
   LOOP 
      DELETE FROM lm3.PamSum WHERE pamsumid = psid;
      total := total + 1;
   END LOOP;
   
   RETURN total;
END;
$$  LANGUAGE 'plpgsql' VOLATILE;

-- ----------------------------------------------------------------------------
-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lm3.lm_updatePaths(olddir varchar, newdir varchar)
   RETURNS void AS
$$
DECLARE
   start int = 0;
BEGIN
   start = char_length(olddir) + 1;
   UPDATE lm3.Experiment SET attrmatrixdlocation = newdir || substr(attrmatrixdlocation, start)  
	   WHERE attrmatrixdlocation like olddir || '%';
	UPDATE lm3.Experiment SET attrtreedlocation = newdir || substr(attrtreedlocation, start)  
	   WHERE attrtreedlocation like olddir || '%';
   
   UPDATE lm3.Bucket SET slindicesdlocation = newdir || substr(slindicesdlocation, start)  
	   WHERE slindicesdlocation like olddir || '%';
   UPDATE lm3.Bucket SET pamdlocation = newdir || substr(pamdlocation, start)  
	   WHERE pamdlocation like olddir || '%';
   UPDATE lm3.Bucket SET grimdlocation = newdir || substr(grimdlocation, start)  
	   WHERE grimdlocation like olddir || '%';

   UPDATE lm3.Pamsum SET splotchpamdlocation = newdir || substr(splotchpamdlocation, start)  
	   WHERE splotchpamdlocation like olddir || '%';
   UPDATE lm3.Pamsum SET splotchsitesdlocation = newdir || substr(splotchsitesdlocation, start)  
	   WHERE splotchsitesdlocation like olddir || '%';
   UPDATE lm3.Pamsum SET pamdlocation = newdir || substr(pamdlocation, start)  
	   WHERE pamdlocation like olddir || '%';
   UPDATE lm3.Pamsum SET sumdlocation = newdir || substr(sumdlocation, start)  
	   WHERE sumdlocation like olddir || '%';
	
   UPDATE lm3.Layer SET dlocation = newdir || substr(dlocation, start)  
	   WHERE dlocation like olddir || '%';
   
END;
$$ LANGUAGE 'plpgsql' VOLATILE; 

-- ----------------------------------------------------------------------------
-- ----------------------------------------------------------------------------
-- SELECT * from lm3.lm_updateUrls('http://lifemapper.org', 'http://yeti.lifemapper.org');

CREATE OR REPLACE FUNCTION lm3.lm_updateUrls(oldbase varchar, newbase varchar)
   RETURNS void AS
$$
DECLARE
   start int = 0;
BEGIN
   start = char_length(oldbase) + 1;
   UPDATE lm3.Layer SET metadataUrl = newbase || substr(metadataUrl, start)  
	   WHERE metadataUrl like oldbase || '%';
   
	UPDATE lm3.Experiment SET metadataUrl = newbase || substr(metadataUrl, start)  
	   WHERE metadataUrl like oldbase || '%';
   UPDATE lm3.Bucket SET metadataUrl = newbase || substr(metadataUrl, start)  
	   WHERE metadataUrl like oldbase || '%';
   UPDATE lm3.Pamsum SET metadataUrl = newbase || substr(metadataUrl, start)  
	   WHERE metadataUrl like oldbase || '%';
END;
$$ LANGUAGE 'plpgsql' VOLATILE; 
