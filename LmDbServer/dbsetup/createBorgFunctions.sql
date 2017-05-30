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
-- From APP_DIR
-- psql -U admin -d borg --file=LmDbServer/dbsetup/createBorgFunctions.sql
-- ----------------------------------------------------------------------------
\c borg
-- ----------------------------------------------------------------------------
-- Cleanup
-- ----------------------------------------------------------------------------
-- 
-- ----------------------------------------------------------------------------
-- ----------------------------------------------------------------------------
-- Functions  
-- Note: All column names are returned in lower case
-- ----------------------------------------------------------------------------
-- Algorithm
-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lm_v3.lm_findOrInsertAlgorithm(code varchar, 
                                                    		 meta varchar, 
                                                    		 mtime double precision)
   RETURNS lm_v3.algorithm AS
$$
DECLARE
   rec lm_v3.algorithm%rowtype;
BEGIN
   SELECT * INTO rec FROM lm_v3.algorithm WHERE algorithmcode = code;
   IF NOT FOUND THEN
      begin
         INSERT INTO lm_v3.Algorithm (algorithmcode, metadata, modtime)
            VALUES (code, meta, mtime);
         IF NOT FOUND THEN
            RAISE EXCEPTION 'Unable to find or insert Algorithm';
         ELSE
            SELECT * INTO rec FROM lm_v3.algorithm WHERE algorithmcode = code;
         END IF;
      end;
   END IF;
   RETURN rec;
END;
$$  LANGUAGE 'plpgsql' VOLATILE;    


-- ----------------------------------------------------------------------------
-- TaxonomySource
-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lm_v3.lm_findOrInsertTaxonSource(name varchar,
                                         		             taxsrcurl varchar,
                                                          mtime double precision)
RETURNS lm_v3.TaxonomySource AS
$$
DECLARE
   rec lm_v3.TaxonomySource%rowtype;
BEGIN
   SELECT * INTO rec FROM lm_v3.TaxonomySource
      WHERE datasetIdentifier = name or url = taxsrcurl;
   IF NOT FOUND THEN
      INSERT INTO lm_v3.TaxonomySource 
         (url, datasetIdentifier, modTime) VALUES (taxsrcurl, name, mtime);
      IF NOT FOUND THEN
         RAISE EXCEPTION 'Unable to find or insert TaxonomySource';
      ELSE
         SELECT INTO rec * FROM lm_v3.taxonomysource WHERE datasetIdentifier = name;
      END IF;
   END IF;
   RETURN rec;
END;
$$  LANGUAGE 'plpgsql' VOLATILE;

-- ----------------------------------------------------------------------------
-- Tree
-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lm_v3.lm_findOrInsertTree(trid int, 
                                                     usr varchar,
                                                     nm varchar,
                                                     dloc text,
                                                     isbin boolean,
                                                     isultra boolean,
                                                     haslen boolean,
                                                     meta text,
                                                     mtime double precision)
   RETURNS lm_v3.tree AS
$$
DECLARE
   rec lm_v3.tree%rowtype;
   newid int = -1;
BEGIN
   IF trid IS NOT NULL THEN
      SELECT * INTO rec FROM lm_v3.tree WHERE treeid = trid;
   ELSIF usr IS NOT NULL AND nm IS NOT NULL THEN
      SELECT * INTO rec FROM lm_v3.tree WHERE userId = usr AND name = nm;
   END IF;
   IF NOT FOUND THEN
      begin
         INSERT INTO lm_v3.Tree (userId, name, dlocation,  
                                 isBinary, isUltrametric, hasBranchLengths, 
                                 metadata, modTime) 
            VALUES (usr, nm, dloc, isbin, isultra, haslen, meta, mtime);
         IF NOT FOUND THEN
            RAISE EXCEPTION 'Unable to find or insert Tree';
         ELSE
            -- get updated record
            SELECT INTO newid last_value FROM lm_v3.tree_treeid_seq;
            SELECT * INTO rec from lm_v3.Tree WHERE treeId = newid;
         END IF;
      end;
   END IF;
   RETURN rec;
END;
$$  LANGUAGE 'plpgsql' VOLATILE;    

-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lm_v3.lm_getTree(trid int, 
                                            usr varchar,
                                            nm varchar)
   RETURNS lm_v3.tree AS
$$
DECLARE
   rec lm_v3.tree%rowtype;
BEGIN
   begin
      IF trid IS NOT NULL THEN
         SELECT * INTO STRICT rec FROM lm_v3.tree WHERE treeid = trid;
      ELSIF usr IS NOT NULL AND nm IS NOT NULL THEN
         SELECT * INTO STRICT rec FROM lm_v3.tree WHERE userId = usr AND name = nm;
      END IF;
      EXCEPTION
         WHEN NO_DATA_FOUND THEN
            RAISE NOTICE 'Tree id/user/name = %/%/% not found', trid, usr, nm;
   end;
   RETURN rec;
END;
$$  LANGUAGE 'plpgsql' STABLE;    

-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lm_v3.lm_getFilterTrees(usr varchar,
                                                  aftertime double precision,
                                                  beforetime double precision,
                                                  nm varchar,
                                                  meta varchar,
                                                  isbin boolean,
                                                  isultra boolean,
                                                  haslen boolean)
   RETURNS varchar AS
$$
DECLARE
   wherecls varchar;
BEGIN
   wherecls = ' WHERE userid =  ' || quote_literal(usr) || ' ';
   
   -- filter by trees modified after given time
   IF aftertime is not null THEN
      wherecls = wherecls || ' AND modTime >=  ' || quote_literal(aftertime);
   END IF;

   -- filter by trees modified before given time
   IF beforetime is not null THEN
      wherecls = wherecls || ' AND modTime <=  ' || quote_literal(beforetime);
   END IF;

   -- filter by name (assume wildcards within the string)
   IF nm is not null THEN
      wherecls = wherecls || ' AND name like ' || nm;
   END IF;

   -- Metadata text (assume wildcards around the string)
   IF meta is not null THEN
      wherecls = wherecls || ' AND metadata like  ' || quote_literal(meta);
   END IF;

   -- filter by isBinary, boolean values sent as uppercase TRUE or FALSE
   IF isbin is not null THEN
      wherecls = wherecls || ' AND isBinary IS  ' || quote_literal(isbin);
   END IF;

   -- filter by isUltrametric
   IF isultra is not null THEN
      wherecls = wherecls || ' AND isUltrametric IS  ' || quote_literal(isultra);
   END IF;

   -- filter by hasBranchLengths
   IF haslen is not null THEN
      wherecls = wherecls || ' AND hasBranchLengths IS  ' || quote_literal(haslen);
   END IF;

   RETURN wherecls;
END;
$$  LANGUAGE 'plpgsql' STABLE;

-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lm_v3.lm_countTrees(usr varchar,
                                               aftertime double precision,
                                               beforetime double precision,
                                               nm varchar,
                                               meta varchar,
                                               isbin boolean,
                                               isultra boolean,
                                               haslen boolean)
   RETURNS int AS
$$
DECLARE
   num int;
   cmd varchar;
   fromcls varchar;
   wherecls varchar;
BEGIN
   cmd = 'SELECT count(*) FROM lm_v3.tree ';
   SELECT * INTO wherecls FROM lm_v3.lm_getFilterTrees(usr, 
                     aftertime, beforetime, nm, meta, isbin, isultra, haslen);
   cmd := cmd || wherecls;
   RAISE NOTICE 'cmd = %', cmd;

   EXECUTE cmd INTO num;
   RETURN num;
END;
$$  LANGUAGE 'plpgsql' STABLE;

-- ----------------------------------------------------------------------------
-- Note: order by modTime descending
CREATE OR REPLACE FUNCTION lm_v3.lm_listTreeObjects(firstRecNum int, 
                                                    maxNum int,
                                                    usr varchar,
                                                    aftertime double precision,
                                                    beforetime double precision,
                                                    nm varchar,
                                                    meta varchar,
                                                    isbin boolean,
                                                    isultra boolean,
                                                    haslen boolean)
   RETURNS SETOF lm_v3.tree AS
$$
DECLARE
   rec lm_v3.tree;
   cmd varchar;
   wherecls varchar;
   ordercls varchar;
   limitcls varchar;
BEGIN
   cmd = 'SELECT * FROM lm_v3.tree ';
   SELECT * INTO wherecls FROM lm_v3.lm_getFilterTrees(usr, 
                     aftertime, beforetime, nm, meta, isbin, isultra, haslen);
   ordercls = ' ORDER BY modTime DESC ';
   limitcls = ' LIMIT ' || quote_literal(maxNum) || ' OFFSET ' || quote_literal(firstRecNum);

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
-- Note: order by modTime descending
CREATE OR REPLACE FUNCTION lm_v3.lm_listTreeAtoms(firstRecNum int, 
                                                  maxNum int,
                                                  usr varchar,
                                                  aftertime double precision,
                                                  beforetime double precision,
                                                  nm varchar,
                                                  meta varchar,
                                                  isbin boolean,
                                                  isultra boolean,
                                                  haslen boolean)
   RETURNS SETOF lm_v3.lm_atom AS
$$
DECLARE
   rec lm_v3.lm_atom;
   cmd varchar;
   wherecls varchar;
   ordercls varchar;
   limitcls varchar;
BEGIN
   cmd = 'SELECT treeid, name, null, modTime FROM lm_v3.tree ';
   SELECT * INTO wherecls FROM lm_v3.lm_getFilterTrees(usr, 
                     aftertime, beforetime, nm, meta, isbin, isultra, haslen);
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
CREATE OR REPLACE FUNCTION lm_v3.lm_deleteTree(trid int)
RETURNS int AS
$$
DECLARE
   success int := -1;
   lyr_success int := -1;
BEGIN
   DELETE FROM lm_v3.Tree WHERE treeId = trid;
   IF FOUND THEN
      success = 0;
   END IF;
   RETURN success;
END;
$$  LANGUAGE 'plpgsql' VOLATILE;

-- ----------------------------------------------------------------------------
-- Scenario
-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lm_v3.lm_getScenario(scenid int,
                                                usr varchar,
                                                code varchar)
   RETURNS lm_v3.Scenario AS
$$
DECLARE
   rec lm_v3.Scenario%rowtype;
BEGIN
   SELECT * INTO rec FROM lm_v3.Scenario s 
      WHERE s.scenariocode = code and s.userid = usr;
   IF NOT FOUND THEN
      begin
         SELECT * INTO STRICT rec FROM lm_v3.Scenario s WHERE s.scenarioid = scenid;
         EXCEPTION
            WHEN NO_DATA_FOUND THEN
               RAISE NOTICE 'Scenario id/user/code = %/%/% not found', scenid, usr, code;
            WHEN TOO_MANY_ROWS THEN
               RAISE NOTICE 'Scenario id/user/code = %/%/% not unique', scenid, usr, code;
      end;
   END IF;
   
   IF NOT FOUND THEN
      RAISE NOTICE 'Scenario id = % or user/code = %/% not found', scenid, usr, code;
   END IF;
   
   RETURN rec;
END;
$$  LANGUAGE 'plpgsql' STABLE;

-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lm_v3.lm_getFilterScenarios(usr varchar,
                                                  aftertime double precision,
                                                  beforetime double precision,
                                                  epsg int,
                                                  gcm varchar,
                                                  altpred varchar,
                                                  dt varchar)
   RETURNS varchar AS
$$
DECLARE
   wherecls varchar;
BEGIN
   wherecls = ' WHERE userid =  ' || quote_literal(usr) || ' ';
   
   -- filter by scenarios modified after given time
   IF aftertime is not null THEN
      wherecls = wherecls || ' AND modTime >=  ' || quote_literal(aftertime);
   END IF;

   -- filter by scenarios modified before given time
   IF beforetime is not null THEN
      wherecls = wherecls || ' AND modTime <=  ' || quote_literal(beforetime);
   END IF;

   -- filter by epsg code
   IF epsg is not null THEN
      wherecls = wherecls || ' AND epsgcode =  ' || epsg;
   END IF;

   -- filter by gcm code
   IF gcm is not null THEN
      wherecls = wherecls || ' AND gcmcode =  ' || quote_literal(gcm);
   END IF;

   -- filter by alternate predictor code
   IF altpred is not null THEN
      wherecls = wherecls || ' AND altpredcode =  ' || quote_literal(altpred);
   END IF;

   -- filter by date code
   IF dt is not null THEN
      wherecls = wherecls || ' AND datecode =  ' || quote_literal(dt);
   END IF;

   RETURN wherecls;
END;
$$  LANGUAGE 'plpgsql' STABLE;

-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lm_v3.lm_countScenarios(usr varchar,
                                                   aftertime double precision,
                                                   beforetime double precision,
                                                   epsg int,
                                                   gcm varchar,
                                                   altpred varchar,
                                                   dt varchar)
   RETURNS int AS
$$
DECLARE
   num int;
   cmd varchar;
   fromcls varchar;
   wherecls varchar;
BEGIN
   cmd = 'SELECT count(*) FROM lm_v3.scenario ';
   SELECT * INTO wherecls FROM lm_v3.lm_getFilterScenarios(usr, 
                          aftertime, beforetime, epsg, gcm, altpred, dt);
   cmd := cmd || wherecls;
   RAISE NOTICE 'cmd = %', cmd;

   EXECUTE cmd INTO num;
   RETURN num;
END;
$$  LANGUAGE 'plpgsql' STABLE;

-- ----------------------------------------------------------------------------
-- Note: order by modTime descending
CREATE OR REPLACE FUNCTION lm_v3.lm_listScenarioObjects(firstRecNum int, 
                                                   maxNum int,
                                                   usr varchar,
                                                   aftertime double precision,
                                                   beforetime double precision,
                                                   epsg int,
                                                   gcm varchar,
                                                   altpred varchar,
                                                   dt varchar)
   RETURNS SETOF lm_v3.scenario AS
$$
DECLARE
   rec lm_v3.scenario;
   cmd varchar;
   wherecls varchar;
   ordercls varchar;
   limitcls varchar;
BEGIN
   cmd = 'SELECT * FROM lm_v3.scenario ';
   SELECT * INTO wherecls FROM lm_v3.lm_getFilterScenarios(usr, 
                          aftertime, beforetime, epsg, gcm, altpred, dt);
   ordercls = ' ORDER BY modTime DESC ';
   limitcls = ' LIMIT ' || quote_literal(maxNum) || ' OFFSET ' || quote_literal(firstRecNum);

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
-- Note: order by modTime descending
CREATE OR REPLACE FUNCTION lm_v3.lm_listScenarioAtoms(firstRecNum int, 
                                                   maxNum int,
                                                   usr varchar,
                                                   aftertime double precision,
                                                   beforetime double precision,
                                                   epsg int,
                                                   gcm varchar,
                                                   altpred varchar,
                                                   dt varchar)
   RETURNS SETOF lm_v3.lm_atom AS
$$
DECLARE
   rec lm_v3.lm_atom;
   cmd varchar;
   wherecls varchar;
   ordercls varchar;
   limitcls varchar;
BEGIN
   cmd = 'SELECT scenarioid, scenarioCode, epsgcode, modTime FROM lm_v3.scenario ';
   SELECT * INTO wherecls FROM lm_v3.lm_getFilterScenarios(usr, 
                          aftertime, beforetime, epsg, gcm, altpred, dt);
   ordercls = ' ORDER BY modTime DESC ';
   limitcls = ' LIMIT ' || quote_literal(maxNum) || ' OFFSET ' || quote_literal(firstRecNum);

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
CREATE OR REPLACE FUNCTION lm_v3.lm_deleteScenario(scenid int)
RETURNS int AS
$$
DECLARE
   success int := -1;
   lyr_success int := -1;
   elyrid int;
   
BEGIN
   FOR elyrid IN 
      SELECT envLayerId FROM lm_v3.ScenarioLayer WHERE scenarioId = scenid
   LOOP
      SELECT * INTO lyr_success FROM lm_v3.lm_deleteScenarioLayer(elyrid, scenid);
      RAISE NOTICE 'Deleted EnvLayer % from scenario', elyrid;
   END LOOP;
   
   DELETE FROM lm_v3.Scenario WHERE scenarioId = scenid;
   IF FOUND THEN
      success = 0;
   END IF;
   RETURN success;
END;
$$  LANGUAGE 'plpgsql' VOLATILE;

-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lm_v3.lm_findOrInsertScenario(usr varchar,
                                             code varchar, 
                                             meta text,
                                             gcm varchar,
                                             altpred varchar,
                                             tm varchar,
                                             unts varchar,
                                             res double precision,
                                             epsg int,
                                             bndsstring varchar, 
                                             bboxwkt varchar,
                                             mtime double precision)
   RETURNS lm_v3.Scenario AS
$$
DECLARE
   newid int;
   rec lm_v3.Scenario%rowtype;
BEGIN
   SELECT * INTO rec FROM lm_v3.Scenario s 
      WHERE s.scenariocode = code and s.userid = usr;
   IF NOT FOUND THEN
      INSERT INTO lm_v3.Scenario 
         (userid, scenarioCode, metadata, gcmCode, altpredCode, 
         dateCode, units, resolution, epsgcode, bbox, modTime)
      VALUES 
         (usr, code, meta, gcm, altpred, tm, unts, res, epsg, bndsstring, mtime);
                       
      IF NOT FOUND THEN
         RAISE EXCEPTION 'Unable to find or insert Scenario';
      ELSE
         SELECT INTO newid last_value FROM lm_v3.scenario_scenarioid_seq;
         IF bboxwkt IS NOT NULL THEN 
            UPDATE lm_v3.scenario SET geom = ST_GeomFromText(bboxwkt, epsg)
               WHERE scenarioId = newid;
         END IF; 
         SELECT * INTO rec FROM lm_v3.Scenario s WHERE s.scenarioid = newid;
      END IF; -- end if inserted
   END IF;  -- end if not existing
   
   RETURN rec;
END;
$$  LANGUAGE 'plpgsql' VOLATILE;

-- ----------------------------------------------------------------------------
-- LmUser
-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lm_v3.lm_findOrInsertUser(usr varchar, 
                                                     name1 varchar, 
                                                     name2 varchar,
                                                     inst varchar, 
                                                     addr1 varchar, 
                                                     addr2 varchar, 
                                                     addr3 varchar,
                                                     fone varchar, 
                                                     emale varchar, 
                                                     mtime double precision, 
                                                     psswd varchar)
   RETURNS lm_v3.LMUser AS
$$
DECLARE
   success int = -1;
   rec lm_v3.LMUser%rowtype;
BEGIN
   SELECT * into rec FROM lm_v3.LMUser WHERE lower(userid) = lower(usr) 
                                          OR lower(email) = lower(emale);
   IF NOT FOUND THEN 
      INSERT INTO lm_v3.LMUser
         (userId, firstname, lastname, institution, address1, address2, address3, phone,
          email, modTime, password)
         VALUES 
         (usr, name1, name2, inst, addr1, addr2, addr3, fone, emale, mtime, psswd);

      IF FOUND THEN
         SELECT INTO rec * FROM lm_v3.LMUser WHERE userid = usr;
      END IF;
   END IF;
   
   RETURN rec;
END;
$$  LANGUAGE 'plpgsql' VOLATILE; 

-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lm_v3.lm_findUser(usr varchar, 
                                           emale varchar)
   RETURNS lm_v3.lmuser AS
$$
DECLARE
   rec lm_v3.lmuser%rowtype;
BEGIN
   SELECT * into rec FROM lm_v3.LMUser WHERE lower(userid) = lower(usr) 
                                          OR lower(email) = lower(emale);
   RETURN rec;
END;
$$  LANGUAGE 'plpgsql' STABLE; 

-- ----------------------------------------------------------------------------
-- Gridset
-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lm_v3.lm_findOrInsertGridset(grdid int,
                                                        usr varchar, 
                                                        nm varchar,
                                                        lyrid int,
                                                        dloc text,
                                                        epsg int,
                                                        meta varchar, 
                                                    	  mtime double precision)
   RETURNS lm_v3.lm_gridset AS
$$
DECLARE
   rec lm_v3.lm_gridset%rowtype;
   newid int;
BEGIN
   SELECT * INTO rec FROM lm_v3.lm_getGridset(grdid, usr, nm);
   IF rec.gridsetid IS NULL THEN
      begin
         INSERT INTO lm_v3.gridset (userId, name, layerId, 
                                    dlocation, epsgcode, metadata, modTime) 
                             VALUES (usr, nm, lyrid, dloc, epsg, meta, mtime);
         IF NOT FOUND THEN
            RAISE EXCEPTION 'Unable to find or insert Gridset';
         ELSE
            SELECT INTO newid last_value FROM lm_v3.gridset_gridsetid_seq;
            SELECT * INTO rec from lm_v3.lm_gridset WHERE gridsetId = newid;
         END IF;
      end;
   END IF;
   RETURN rec;
END;
$$  LANGUAGE 'plpgsql' VOLATILE;    

-- ----------------------------------------------------------------------------
-- Get an existing gridset
CREATE OR REPLACE FUNCTION lm_v3.lm_getGridset(grdid int,
                                               usr varchar, 
                                               nm varchar)
   RETURNS lm_v3.lm_gridset AS
$$
DECLARE
   rec lm_v3.lm_gridset%rowtype;
BEGIN
   IF grdid IS NOT NULL THEN
      SELECT * INTO rec FROM lm_v3.lm_gridset WHERE gridsetid = grdid;
   ELSE
      SELECT * INTO rec FROM lm_v3.lm_gridset WHERE userid = usr AND grdname = nm;
   END IF;

   IF NOT FOUND THEN
      RAISE NOTICE 'Gridset with id: %, user: %, name: % not found', grdid, usr, nm;
   END IF;
   RETURN rec;
END;
$$  LANGUAGE 'plpgsql' STABLE;    


-- ----------------------------------------------------------------------------
-- Matrix
-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lm_v3.lm_findOrInsertMatrix(mtxid int,
                                                       mtxtype int,
                                                       grdid int,
                                                       gcm varchar,
                                                       altpred varchar,
                                                       dt varchar,
                                                       dloc text,
                                                       meta varchar, 
                                                       stat int,
                                                   	 stattime double precision)
   RETURNS lm_v3.Matrix AS
$$
DECLARE
   rec lm_v3.matrix%rowtype;
   newid int;
BEGIN
   IF mtxid IS NOT NULL THEN
      SELECT * INTO rec FROM lm_v3.matrix WHERE matrixid = mtxid;
   ELSE
      SELECT * INTO rec FROM lm_v3.matrix WHERE matrixtype = mtxtype 
                                            AND gridsetid = grdid
                                            AND gcmCode = gcm
                                            AND altpredCode = altpred
                                            AND dateCode = dt;
   END IF;
   IF NOT FOUND THEN
      begin
         INSERT INTO lm_v3.matrix (matrixType, gridsetId, matrixDlocation, 
                                   gcmCode, altpredCode, dateCode, metadata, 
                                   status, statusmodtime) 
                           VALUES (mtxtype, grdid, dloc, gcm, altpred, dt,
                                   meta, stat, stattime);
         IF NOT FOUND THEN
            RAISE EXCEPTION 'Unable to find or insert Matrix';
         ELSE
            SELECT INTO newid last_value FROM lm_v3.matrix_matrixid_seq;
            SELECT * INTO rec from lm_v3.matrix WHERE matrixId = newid;
         END IF;
      end;
   END IF;
   RETURN rec;
END;
$$  LANGUAGE 'plpgsql' VOLATILE;    

-- ----------------------------------------------------------------------------
-- Gets a matrix with its lm_gridset (including optional shapegrid)
-- Unique: gridsetId, matrixType, gcmCode, altpredCode, dateCode
CREATE OR REPLACE FUNCTION lm_v3.lm_getMatrix(mtxid int, 
                                              mtxtype int, 
                                              gsid int,
                                              gcm varchar,
                                              altpred varchar,
                                              dt varchar,
                                              gsname varchar,
                                              usr varchar)
   RETURNS lm_v3.lm_matrix AS
$$
DECLARE
   rec lm_v3.lm_matrix%rowtype;
BEGIN
   IF mtxid IS NOT NULL THEN
      SELECT * INTO rec FROM lm_v3.lm_matrix WHERE matrixid = mtxid;
   ELSE
	   IF gsid IS NULL THEN
         SELECT * INTO gsid FROM lm_v3.gridset WHERE grdname = gsname 
                                                 AND userid = usr;
      END IF;
      
      SELECT * INTO rec FROM lm_v3.matrix WHERE matrixtype = mtxtype 
                                            AND gridsetid = gsid
                                            AND gcmCode = gcm
                                            AND altpredCode = altpred
                                            AND dateCode = dt;
   END IF;
   RETURN rec;
END;
$$  LANGUAGE 'plpgsql' STABLE;    

-- ----------------------------------------------------------------------------
-- Gets all (bare) matrices for a gridset 
CREATE OR REPLACE FUNCTION lm_v3.lm_getMatricesForGridset(gsid int)
   RETURNS SETOF lm_v3.matrix AS
$$
DECLARE
   rec lm_v3.matrix%rowtype;
BEGIN
   FOR rec IN SELECT * FROM lm_v3.Matrix WHERE gridsetId = gsid
      LOOP
         RETURN NEXT rec;
      END LOOP;
   RETURN;
END;
$$  LANGUAGE 'plpgsql' STABLE;    

-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lm_v3.lm_getFilterMtx(usr varchar,
                                                    mtxtype int,
                                                    gcm varchar,
                                                    altpred varchar,
                                                    tm varchar,
                                                    meta varchar, 
                                                    grdid int,
                                                    aftertime double precision,
                                                    beforetime double precision,
                                                    epsg int,
                                                    afterstat int,
                                                    beforestat int,
                                                    mtxid int,
                                                    lyrid int)
   RETURNS varchar AS
$$
DECLARE
   wherecls varchar;
BEGIN
   wherecls = ' WHERE userid = ' || quote_literal(usr);

   -- filter by MatrixType
   IF mtxtype is not null THEN
      wherecls = wherecls || ' AND matrixtype =  ' || quote_literal(mtxtype);
   END IF;
                
   -- filter by codes - gcm, altpred, date
   IF gcm is not null THEN
      wherecls = wherecls || ' AND gcmcode like  ' || quote_literal(gcm);
   END IF;
   IF altpred is not null THEN
      wherecls = wherecls || ' AND altpredcode like  ' || quote_literal(altpred);
   END IF;
   IF tm is not null THEN
      wherecls = wherecls || ' AND datecode like  ' || quote_literal(tm);
   END IF;

   -- Metadata
   IF meta is not null THEN
      wherecls = wherecls || ' AND metadata like  ' || quote_literal(meta);
   END IF;

   -- filter by Gridset
   IF grdid is not null THEN
      wherecls = wherecls || ' AND gridsetid =  ' || quote_literal(grdid);
   END IF;

   -- filter by layers modified after given time
   IF aftertime is not null THEN
      wherecls = wherecls || ' AND statusModTime >=  ' || quote_literal(aftertime);
   END IF;
   
   -- filter by layers modified before given time
   IF beforetime is not null THEN
      wherecls = wherecls || ' AND statusModTime <=  ' || quote_literal(beforetime);
   END IF;

   -- filter by epsgcode
   IF epsg is not null THEN
      wherecls = wherecls || ' AND  epsgcode =  ' || epsg;
   END IF;

   -- filter by status
   IF afterstat is not null OR beforestat is not null THEN
      begin
         IF afterstat = beforestat THEN
            wherecls = wherecls || ' AND status =  ' || afterstat;
         ELSE
            -- filter by status >= given value
            IF afterstat is not null THEN
                wherecls = wherecls || ' AND status >=  ' || afterstat;
            END IF;
   
            -- filter by status <= given value
            IF beforestat is not null THEN
               wherecls = wherecls || ' AND status <=  ' || beforestat;
            END IF;
         END IF;
      end;
   END IF;

   return wherecls;
END;
$$  LANGUAGE 'plpgsql' STABLE;

-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lm_v3.lm_countMatrices(usr varchar,
                                                    mtxtype int,
                                                    gcm varchar,
                                                    altpred varchar,
                                                    tm varchar,
                                                    meta varchar, 
                                                    grdid int,
                                                    aftertime double precision,
                                                    beforetime double precision,
                                                    epsg int,
                                                    afterstat int,
                                                    beforestat int)
   RETURNS int AS
$$
DECLARE
   num int;
   cmd varchar;
   wherecls varchar;
BEGIN
   cmd = 'SELECT count(*) FROM lm_v3.lm_fullmatrix ';
   SELECT * INTO wherecls FROM lm_v3.lm_v3.lm_getFilterMatrix(usr, mtxtype, gcm, 
                 altpred, tm, meta, grdid, aftertime, beforetime, epsg, 
                 afterstat, beforestat);
   cmd := cmd || wherecls;
   RAISE NOTICE 'cmd = %', cmd;

   EXECUTE cmd INTO num;
   RETURN num;
END;
$$  LANGUAGE 'plpgsql' STABLE;

-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lm_v3.lm_listMatrixAtoms(firstRecNum int, maxNum int, 
                                                    usr varchar,
                                                    mtxtype int,
                                                    gcm varchar,
                                                    altpred varchar,
                                                    tm varchar,
                                                    meta varchar, 
                                                    grdid int,
                                                    aftertime double precision,
                                                    beforetime double precision,
                                                    epsg int,
                                                    afterstat int,
                                                    beforestat int)
   RETURNS SETOF lm_v3.lm_atom AS
$$
DECLARE
   rec lm_v3.lm_atom;
   cmd varchar;
   wherecls varchar;
   limitcls varchar;
   ordercls varchar;
BEGIN
   cmd = 'SELECT matrixId, null, grdepsgcode, statusmodtime FROM lm_v3.lm_fullmatrix ';
   SELECT * INTO wherecls FROM lm_v3.lm_v3.lm_getFilterMatrix(usr, mtxtype, gcm, 
                 altpred, tm, meta, grdid, aftertime, beforetime, epsg, 
                 afterstat, beforestat);
   ordercls = 'ORDER BY statusmodtime DESC';
   limitcls = ' LIMIT ' || quote_literal(maxNum) || ' OFFSET ' || quote_literal(firstRecNum);

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
CREATE OR REPLACE FUNCTION lm_v3.lm_listMatrixObjects(firstRecNum int, maxNum int, 
                                                    usr varchar,
                                                    mtxtype int,
                                                    gcm varchar,
                                                    altpred varchar,
                                                    tm varchar,
                                                    meta varchar, 
                                                    grdid int,
                                                    aftertime double precision,
                                                    beforetime double precision,
                                                    epsg int,
                                                    afterstat int,
                                                    beforestat int)
   RETURNS SETOF lm_v3.lm_fullmatrix AS
$$
DECLARE
   rec lm_v3.lm_fullmatrix;
   cmd varchar;
   wherecls varchar;
   limitcls varchar;
   ordercls varchar;
BEGIN
   cmd = 'SELECT * FROM lm_v3.lm_fullmatrix ';
   SELECT * INTO wherecls FROM lm_v3.lm_v3.lm_getFilterMatrix(usr, mtxtype, gcm, 
                 altpred, tm, meta, grdid, aftertime, beforetime, epsg, 
                 afterstat, beforestat);
   ordercls = 'ORDER BY statusmodtime DESC';
   limitcls = ' LIMIT ' || quote_literal(maxNum) || ' OFFSET ' || quote_literal(firstRecNum);

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
CREATE OR REPLACE FUNCTION lm_v3.lm_findUserForObject(lyrid int, 
                                                      scode varchar, 
                                                      occid int, 
                                                      mtxid int, 
                                                      gsid int, 
                                                      mfid int)
RETURNS varchar AS
$$
DECLARE
   usr varchar;
BEGIN
   -- EnvLayer, SDMProject, ShapeGrid
   IF lyrid IS NOT NULL THEN
      begin
         SELECT userId INTO STRICT usr FROM lm_v3.Layer WHERE layerId = lyrid;
         EXCEPTION
            WHEN NO_DATA_FOUND THEN
               RAISE NOTICE 'Layer % not found', lyrid;
            WHEN TOO_MANY_ROWS THEN
               RAISE EXCEPTION 'Layer % not unique', lyrid;
      end;
   -- Scenario
   ELSEIF scode IS NOT NULL THEN
      begin
         SELECT userId INTO STRICT usr FROM lm_v3.Scenario 
            WHERE scenarioCode = scode;
         EXCEPTION
            WHEN NO_DATA_FOUND THEN
               RAISE NOTICE 'Scenario % not found', scode;
            WHEN TOO_MANY_ROWS THEN
               RAISE EXCEPTION 'Scenario % not unique', scode;
      end;
   -- OccurrenceSet
   ELSEIF occid IS NOT NULL THEN
      begin
         SELECT userId INTO STRICT usr FROM lm_v3.OccurrenceSet 
            WHERE occurrenceSetId = occid;
         EXCEPTION
            WHEN NO_DATA_FOUND THEN
               RAISE NOTICE 'OccurrenceSet % not found', occid;
            WHEN TOO_MANY_ROWS THEN
               RAISE EXCEPTION 'OccurrenceSet % not unique', occid;
      end;
   -- Matrix
   ELSEIF mtxid IS NOT NULL THEN
      begin
         SELECT userId INTO STRICT usr FROM lm_v3.lm_matrix 
            WHERE matrixId = mtxid;
         EXCEPTION
            WHEN NO_DATA_FOUND THEN
               RAISE NOTICE 'Matrix % not found', mtxid;
            WHEN TOO_MANY_ROWS THEN
               RAISE EXCEPTION 'Matrix % not unique', mtxid;
      end;
   -- Gridset
   ELSEIF
      begin
         SELECT distinct(userId) INTO STRICT usr FROM lm_v3.lm_matrix 
            WHERE gridsetId = gsid;
         EXCEPTION
            WHEN NO_DATA_FOUND THEN
               RAISE NOTICE 'Gridset % not found', gsid;
            WHEN TOO_MANY_ROWS THEN
               RAISE EXCEPTION 'Gridset % not unique', gsid;
      end;
   END IF;
   RETURN usr;
END;
$$  LANGUAGE 'plpgsql' STABLE;

-- ----------------------------------------------------------------------------
-- JobChain
-- ----------------------------------------------------------------------------
-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lm_v3.lm_countMFProcess(usr varchar(20), 
    	                                             stat int)
   RETURNS int AS
$$
DECLARE
   num int;
   cmd varchar;
   wherecls varchar;
BEGIN
   IF usr IS null THEN
      SELECT count(*) INTO num FROM lm_v3.MFProcess WHERE status = stat;
   ELSE
      SELECT count(*) INTO num FROM lm_v3.MFProcess WHERE status = stat 
                                                      AND userid = usr;
   END IF;

   RETURN num;
END;
$$  LANGUAGE 'plpgsql' STABLE;

-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lm_v3.lm_findTaxonSource(tsourcename varchar)
RETURNS lm_v3.TaxonomySource AS
$$
DECLARE
   rec lm_v3.TaxonomySource%ROWTYPE;
BEGIN
   begin
      SELECT * INTO STRICT rec FROM lm_v3.TaxonomySource
         WHERE datasetIdentifier = tsourcename;
      
      EXCEPTION
         WHEN NO_DATA_FOUND THEN
            RAISE NOTICE 'TaxonomySource % not found', tsourcename;
         WHEN TOO_MANY_ROWS THEN
            RAISE EXCEPTION 'TaxonomySource % not unique', tsourcename;
      end;
   RETURN rec;
END;
$$  LANGUAGE 'plpgsql' STABLE;

-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lm_v3.lm_findTaxon(tsourceid int,
                                              tkey int)
RETURNS lm_v3.Taxon AS
$$
DECLARE
   rec lm_v3.Taxon%ROWTYPE;
BEGIN
   SELECT * INTO rec FROM lm_v3.Taxon
      WHERE taxonomysourceid = tsourceid and taxonomykey = tkey;
   RETURN rec;
END;
$$  LANGUAGE 'plpgsql' STABLE;

-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lm_v3.lm_getTaxon(tsourceid int,
                                             tkey int, 
                                             usr varchar,
                                             tname varchar)
RETURNS lm_v3.Taxon AS
$$
DECLARE
   rec lm_v3.Taxon%ROWTYPE;
BEGIN
   IF tsourceid IS NOT NULL AND tkey IS NOT NULL THEN
      SELECT * INTO rec FROM lm_v3.Taxon
          WHERE taxonomysourceid = tsourceid and taxonomykey = tkey;
   ELSE
      SELECT * INTO rec FROM lm_v3.Taxon
          WHERE userid = usr and sciname = tname;
   END IF;   
   RETURN rec;
END;
$$  LANGUAGE 'plpgsql' STABLE;

-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lm_v3.lm_findOrInsertTaxon(tsourceid int,
                                              tkey int,
                                              usr varchar,
                                              sqd varchar,
                                              king varchar,
                                              phyl varchar,
                                              clss varchar,
                                              ordr varchar,
                                              fam  varchar,
                                              gen  varchar,
                                              rnk varchar,
                                              can varchar,
                                              sname varchar,
                                              gkey int,
                                              skey int,
                                              hierkey varchar,
                                              cnt  int,
                                              currtime double precision)
RETURNS lm_v3.Taxon AS
$$
DECLARE
   rec lm_v3.Taxon%ROWTYPE;
   tid int := -1;
BEGIN
   IF tkey IS NOT NULL THEN
      SELECT * INTO rec FROM lm_v3.Taxon
         WHERE taxonomysourceid = tsourceid and taxonomykey = tkey;
   ELSE
      SELECT * INTO rec FROM lm_v3.Taxon
         WHERE userid = usr and squid = sqd;
   END IF;
      
   IF NOT FOUND THEN
      begin
         -- if no squid, do not insert, return empty record
         IF sqd IS NOT NULL THEN
            INSERT INTO lm_v3.Taxon (taxonomysourceid, userid, taxonomykey, squid,
                                  kingdom, phylum, tx_class, tx_order, family, 
                                  genus, rank, canonical, sciname, genuskey, 
                                  specieskey, keyHierarchy, lastcount, modtime)
                 VALUES (tsourceid, usr, tkey, sqd, king, phyl, clss, ordr, fam, 
                         gen, rnk, can, sname, gkey, skey, hierkey, cnt, currtime);
            IF NOT FOUND THEN
               RAISE EXCEPTION 'Unable to find or insert Taxon';
            ELSE
               SELECT INTO tid last_value FROM lm_v3.taxon_taxonid_seq;
               SELECT * INTO rec FROM lm_v3.Taxon WHERE taxonid = tid;
            END IF;
         END IF;
      end;
   END IF;
   RETURN rec;
END;
$$  LANGUAGE 'plpgsql' VOLATILE;


-- ----------------------------------------------------------------------------
-- OccurrenceSet
-- ----------------------------------------------------------------------------

-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lm_v3.lm_getOccurrenceSet(occid int,
                                                      usr varchar, 
                                                      sqd varchar, 
                                                      epsg int)
   RETURNS lm_v3.occurrenceset AS
$$
DECLARE
   rec lm_v3.occurrenceset%ROWTYPE;                             
BEGIN
   IF occid IS NOT NULL then                     
      SELECT * INTO rec from lm_v3.OccurrenceSet WHERE occurrenceSetId = occid;
   ELSE
      SELECT * INTO rec from lm_v3.OccurrenceSet 
             WHERE userid = usr AND squid = sqd AND epsgcode = epsg;
   END IF;                                                 
   RETURN rec;                                              
END; 
$$ LANGUAGE 'plpgsql' STABLE; 
                                                                        

-- ----------------------------------------------------------------------------
-- Find or insert occurrenceSet and return record.
CREATE OR REPLACE FUNCTION lm_v3.lm_findOrInsertOccurrenceSet(occid int,
                                                  usr varchar,
                                                  sqd varchar,
                                                  vrfy varchar,
                                                  name varchar,
                                                  dloc varchar,
                                                  rdloc varchar,
                                                  total int,
                                                  bounds varchar, 
                                                  epsg int,
                                                  meta text,
                                                  stat int,
                                                  stattime double precision,
                                                  polywkt text,
                                                  pointswkt text)
   RETURNS lm_v3.occurrenceset AS
$$
DECLARE
   rec lm_v3.occurrenceset%ROWTYPE;                             
   newid int = -1;
BEGIN
   IF occid IS NOT NULL then                     
      SELECT * INTO rec from lm_v3.OccurrenceSet WHERE occurrenceSetId = occid;
   ELSE
      SELECT * INTO rec from lm_v3.OccurrenceSet 
             WHERE userid = usr AND squid = sqd AND epsgcode = epsg;
   END IF;                                                 
   IF NOT FOUND THEN
      BEGIN
         INSERT INTO lm_v3.OccurrenceSet 
            (userId, squid, verify, displayName, dlocation, rawDlocation, 
             queryCount, bbox, epsgcode, metadata, status, statusModTime)
         VALUES 
            (usr, sqd, vrfy, name, dloc, rdloc, total, bounds, epsg, meta, 
             stat, stattime);

         IF NOT FOUND THEN
            RAISE EXCEPTION 'Unable to find or insert OccurrenceSet';
         ELSE
            -- add geometries if valid
            IF ST_IsValid(ST_GeomFromText(polywkt, epsg)) THEN
               UPDATE lm_v3.OccurrenceSet SET geom = ST_GeomFromText(polywkt, epsg) 
                  WHERE occurrenceSetId = occid;
            END IF;
            IF ST_IsValid(ST_GeomFromText(pointswkt, epsg)) THEN
               UPDATE lm_v3.OccurrenceSet SET geompts = ST_GeomFromText(pointswkt, epsg) 
                  WHERE occurrenceSetId = occid;
            END IF;

            -- get updated record
            SELECT INTO newid last_value FROM lm_v3.occurrenceset_occurrencesetid_seq;
            SELECT * INTO rec from lm_v3.OccurrenceSet WHERE occurrenceSetId = newid;
         END IF;
         
      END;  -- end alternatives for epsgcode
   END IF;  -- end if occurrenceset found
   RETURN rec;
END;
$$  LANGUAGE 'plpgsql' VOLATILE;

-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lm_v3.lm_updateOccurrenceSet(occid int,
                                                  vrfy varchar,
                                                  name varchar,
                                                  dloc varchar,
                                                  rdloc varchar,
                                                  total int,
                                                  bounds varchar, 
                                                  epsg int,
                                                  meta text,
                                                  stat int,
                                                  stattime double precision,
                                                  polywkt text,
                                                  pointswkt text)
   RETURNS int AS
$$
DECLARE
   success int = -1;
BEGIN
   UPDATE lm_v3.OccurrenceSet SET 
      (verify, displayName, dlocation, rawDlocation, queryCount, bbox, metadata, 
       status, statusModTime)
    = (vrfy, name, dloc, rdloc, total, bounds, meta, stat, stattime)
   WHERE occurrenceSetId = occid;

   IF FOUND THEN
      success = 0;
      IF ST_IsValid(ST_GeomFromText(polywkt, epsg)) THEN
         UPDATE lm_v3.OccurrenceSet SET geom = ST_GeomFromText(polywkt, epsg) 
            WHERE occurrenceSetId = occid;
      END IF;

      IF ST_IsValid(ST_GeomFromText(pointswkt, epsg)) THEN
         UPDATE lm_v3.OccurrenceSet SET geompts = ST_GeomFromText(pointswkt, epsg) 
            WHERE occurrenceSetId = occid;
      END IF;
   END IF;

   RETURN success;
END;
$$  LANGUAGE 'plpgsql' VOLATILE;

-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lm_v3.lm_deleteOccurrenceSet(occid int)
RETURNS int AS
$$
DECLARE
   success int := -1;
BEGIN
   DELETE FROM lm_v3.OccurrenceSet WHERE occurrencesetid = occid;
   IF FOUND THEN
      success = 0;
   END IF;
   RETURN success;
END;
$$  LANGUAGE 'plpgsql' VOLATILE;

-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lm_v3.lm_getFilterOccSets(usr varchar,
                                                    sqd varchar,
                                                    minOccCount int,
                                                    dispname varchar,
                                                    aftertime double precision,
                                                    beforetime double precision,
                                                    epsg int,
                                                    afterstat int,
                                                    beforestat int)
   RETURNS varchar AS
$$
DECLARE
   wherecls varchar;
BEGIN
   wherecls = ' WHERE userid = ' || quote_literal(usr);
                
   -- filter by squid
   IF sqd is not null THEN
      wherecls = wherecls || ' AND squid =  ' || quote_literal(sqd);
   END IF;

   -- filter by count
   IF minOccCount is not null THEN
      wherecls = wherecls || ' AND querycount >= ' || minOccCount;
   END IF;

   -- filter by displayname
   IF dispname is not null THEN
      wherecls = wherecls || ' AND displayname like ' || quote_literal(dispname);
   END IF;

   -- filter by layers modified after given time
   IF aftertime is not null THEN
      wherecls = wherecls || ' AND statusModTime >=  ' || quote_literal(aftertime);
   END IF;
   
   -- filter by layers modified before given time
   IF beforetime is not null THEN
      wherecls = wherecls || ' AND statusModTime <=  ' || quote_literal(beforetime);
   END IF;

   -- filter by epsgcode
   IF epsg is not null THEN
      wherecls = wherecls || ' AND  epsgcode =  ' || epsg;
   END IF;

   -- filter by status
   IF afterstat is not null OR beforestat is not null THEN
      begin
         IF afterstat = beforestat THEN
            wherecls = wherecls || ' AND status =  ' || afterstat;
         ELSE
            -- filter by status >= given value
            IF afterstat is not null THEN
                wherecls = wherecls || ' AND status >=  ' || afterstat;
            END IF;
   
            -- filter by status <= given value
            IF beforestat is not null THEN
               wherecls = wherecls || ' AND status <=  ' || beforestat;
            END IF;
         END IF;
      end;
   END IF;
   
   return wherecls;
END;
$$  LANGUAGE 'plpgsql' STABLE;

-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lm_v3.lm_countOccSets(usr varchar,
                                                    sqd varchar,
                                                    minOccCount int,
                                                    dispname varchar,
                                                    aftertime double precision,
                                                    beforetime double precision,
                                                    epsg int,
                                                    afterstat int,
                                                    beforestat int)
   RETURNS int AS
$$
DECLARE
   num int;
   cmd varchar;
   wherecls varchar;
BEGIN
   cmd = 'SELECT count(*) FROM lm_v3.occurrenceset ';
   SELECT * INTO wherecls FROM lm_v3.lm_getFilterOccSets(usr, sqd,
                        minOccCount, dispname, aftertime, beforetime, epsg, 
                        afterstat, beforestat);
   cmd := cmd || wherecls;
   RAISE NOTICE 'cmd = %', cmd;

   -- run command
   EXECUTE cmd INTO num;
   return num;
END;
$$  LANGUAGE 'plpgsql' STABLE;

-- ----------------------------------------------------------------------------
-- Note: order by statusModTime desc
CREATE OR REPLACE FUNCTION lm_v3.lm_listOccSetObjects(firstRecNum int, 
                                                    maxNum int,
                                                    usr varchar,
                                                    sqd varchar,
                                                    minOccCount int,
                                                    dispname varchar,
                                                    aftertime double precision,
                                                    beforetime double precision,
                                                    epsg int,
                                                    afterstat int,
                                                    beforestat int)
   RETURNS SETOF lm_v3.OccurrenceSet AS
$$
DECLARE
   rec lm_v3.OccurrenceSet;
   cmd varchar;
   wherecls varchar;
   ordercls varchar;
   limitcls varchar;
BEGIN
   cmd = 'SELECT * FROM lm_v3.occurrenceset ';
   SELECT * INTO wherecls FROM lm_v3.lm_getFilterOccSets(usr, sqd,
                        minOccCount, dispname, aftertime, beforetime, epsg, 
                        afterstat, beforestat);
   ordercls = ' ORDER BY statusModTime DESC ';
   limitcls = ' LIMIT ' || quote_literal(maxNum) || ' OFFSET ' || quote_literal(firstRecNum);

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
-- Note: order by statusModTime desc
CREATE OR REPLACE FUNCTION lm_v3.lm_listOccSetAtoms(firstRecNum int, 
                                                    maxNum int,
                                                    usr varchar,
                                                    sqd varchar,
                                                    minOccCount int,
                                                    dispname varchar,
                                                    aftertime double precision,
                                                    beforetime double precision,
                                                    epsg int,
                                                    afterstat int,
                                                    beforestat int)
   RETURNS SETOF lm_v3.lm_atom AS
$$
DECLARE
   rec lm_v3.lm_atom;
   cmd varchar;
   wherecls varchar;
   ordercls varchar;
   limitcls varchar;
BEGIN
   cmd = 'SELECT occurrencesetid, displayname, epsgcode, statusmodtime FROM lm_v3.occurrenceset ';
   SELECT * INTO wherecls FROM lm_v3.lm_getFilterOccSets(usr, sqd,
                        minOccCount, dispname, aftertime, beforetime, epsg, 
                        afterstat, beforestat);
   ordercls = ' ORDER BY statusModTime DESC ';
   limitcls = ' LIMIT ' || quote_literal(maxNum) || ' OFFSET ' || quote_literal(firstRecNum);

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
-- MFProcess
-- ----------------------------------------------------------------------------
-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lm_v3.lm_insertMFChain(usr varchar,
                                                  dloc varchar,
                                                  prior int,
                                                  meta text,  
                                                  stat int,
                                                  stattime double precision)
RETURNS lm_v3.MFProcess AS
$$
DECLARE
   rec lm_v3.MFProcess%ROWTYPE;
   mfid int = -1;
BEGIN
   INSERT INTO lm_v3.MFProcess 
             (userid, dlocation, priority, metadata, status, statusmodtime)
      VALUES (usr, dloc, prior, meta, stat, stattime);
   IF NOT FOUND THEN
      RAISE EXCEPTION 'Unable to insert MFProcess';
   ELSE 
      SELECT INTO mfid last_value FROM lm_v3.mfprocess_mfprocessid_seq;
      SELECT * INTO rec FROM lm_v3.MFProcess WHERE mfProcessId = mfid;      
   END IF;

   RETURN rec;

END;
$$  LANGUAGE 'plpgsql' VOLATILE;

-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lm_v3.lm_findMFChains(total int, 
                                                 usr varchar,
                                                 oldstat int,
                                                 newstat int,
                                                 modtime double precision)
RETURNS SETOF lm_v3.MFProcess AS
$$
DECLARE
   rec lm_v3.MFProcess%ROWTYPE;
   cmd varchar;
   limitcls varchar;
BEGIN
   cmd = 'SELECT * FROM lm_v3.MFProcess WHERE status = ' || quote_literal(oldstat); 
   limitcls = ' LIMIT ' || quote_literal(total);

   IF usr IS NOT NULL THEN
      cmd = cmd || ' AND userid = ' || quote_literal(usr);
   END IF;
   
   cmd := cmd || limitcls;
   RAISE NOTICE 'cmd = %', cmd;

   FOR rec in EXECUTE cmd
      LOOP
         UPDATE lm_v3.MFProcess SET (status, statusmodtime) = (newstat, modtime)
            WHERE mfProcessId = rec.mfProcessId;
         rec.status = newstat;
         rec.statusmodtime = modtime;
         RETURN NEXT rec;
      END LOOP;
   RETURN;
END;
$$  LANGUAGE 'plpgsql' VOLATILE;

-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lm_v3.lm_updateMFChain(mfid int, 
                                                  dloc varchar,
                                                  stat int,
                                                  modtime double precision)
RETURNS int AS
$$
DECLARE
   success int := -1;
BEGIN
   UPDATE lm_v3.MFProcess SET (dlocation, status, statusmodtime) 
                            = (dloc, stat, modtime)
      WHERE mfProcessId = mfid;
   IF FOUND THEN
      success = 0;
   END IF;
   RETURN success;
END;
$$  LANGUAGE 'plpgsql' VOLATILE;

-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lm_v3.lm_deleteMFChain(mfid int)
RETURNS int AS
$$
DECLARE
   success int := -1;
BEGIN
   DELETE FROM lm_v3.MFProcess WHERE mfProcessId = mfid;
   IF FOUND THEN
      success = 0;
   END IF;
   RETURN success;
END;
$$  LANGUAGE 'plpgsql' VOLATILE;


-- ----------------------------------------------------------------------------
-- MatrixColumn
-- ----------------------------------------------------------------------------
-- ----------------------------------------------------------------------------
-- Gets a matrixColumn with its matrix
CREATE OR REPLACE FUNCTION lm_v3.lm_getMatrixColumn(mtxcolid int,
                                              mtxid int, 
                                              mtxindex int, 
                                              lyrid int, 
                                              intparams int)
   RETURNS lm_v3.lm_matrixcolumn AS
$$
DECLARE
   rec lm_v3.lm_matrixcolumn%rowtype;
BEGIN
   IF mtxcolid IS NOT NULL THEN
      SELECT * INTO rec FROM lm_v3.lm_matrixcolumn WHERE matrixcolumnid = mtxcolid;
   ELSIF mtxid IS NOT NULL THEN
      IF mtxindex IS NOT NULL THEN
         SELECT * INTO rec FROM lm_v3.lm_v3.lm_matrixcolumn 
                                             WHERE matrixId = mtxid 
                                               AND matrixIndex = mtxindex;
      ELSE
         SELECT * INTO rec FROM lm_v3.lm_v3.lm_matrixcolumn 
                                             WHERE matrixId = mtxid 
                                               AND layerid = lyrid 
                                               AND intersectParams = intparams;
      END IF;      
   END IF;
   RETURN rec;
END;
$$  LANGUAGE 'plpgsql' STABLE;    


-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lm_v3.lm_findOrInsertMatrixColumn(usr varchar,
                                                             mtxcolid int,
                                                             mtxid int,
                                                             mtxidx int,
                                                             lyrid int,
                                                             sqd varchar,
                                                             idnt varchar,
                                                             meta text,
                                                             intparams text,
                                                             stat int,
                                                             stattime double precision)
RETURNS lm_v3.lm_matrixcolumn AS
$$
DECLARE
   lyrcount int;
   mtxcount int;
   newid int;
   rec_lyr lm_v3.layer%rowtype;
   rec_mtxcol lm_v3.lm_matrixcolumn%rowtype;
BEGIN
   -- check existence of required referenced matrix
   SELECT count(*) INTO mtxcount FROM lm_v3.Matrix WHERE matrixid = mtxid;
   IF mtxcount < 1 THEN
      RAISE EXCEPTION 'Matrix with id % does not exist', mtxid;
   END IF;
   
   -- check existence of optional layerid
   IF lyrid IS NOT NULL THEN
      SELECT * INTO rec_lyr FROM lm_v3.layer WHERE layerid = lyrid;
      IF NOT FOUND THEN
         RAISE EXCEPTION 'Layer with id %, does not exist', lyrid; 
      END IF;
   END IF;
   
   -- Find unique combo of matrixid, matrixIndex
   IF mtxidx IS NOT NULL AND mtxidx > -1 THEN
      begin
         SELECT * INTO rec_mtxcol FROM lm_v3.lm_matrixcolumn 
            WHERE matrixid = mtxid AND matrixIndex = mtxidx;
         IF FOUND THEN
            RAISE NOTICE 'Returning existing MatrixColumn for Matrix % and Column %',
               mtxid, mtxidx;
         END IF;
      end;
      
   -- Find unique combo of matrixid, layer, intersect params
   ELSEIF lyrid IS NOT NULL THEN
      begin
         SELECT * INTO rec_mtxcol FROM lm_v3.lm_matrixcolumn 
            WHERE matrixid = mtxid AND layerid = lyrid AND intersectParams = intparams;
         IF FOUND THEN
            RAISE NOTICE 
            'Returning existing MatrixColumn for Matrix/Layer/Params % / % / %',
               mtxid, lyrid, intparams;
         END IF;
      end;
         
   -- or insert new column at location or undefined location for gpam
   ELSE
      INSERT INTO lm_v3.MatrixColumn (matrixId, matrixIndex, squid, ident, 
            metadata, layerId, intersectParams, status, statusmodtime)
         VALUES (mtxid, mtxidx, sqd, idnt, meta, lyrid, intparams, stat, stattime);
      IF NOT FOUND THEN
         RAISE EXCEPTION 'Unable to findOrInsertMatrixColumn';
      ELSE
         SELECT INTO newid last_value FROM lm_v3.matrixcolumn_matrixcolumnid_seq;
         SELECT * INTO rec_mtxcol FROM lm_v3.lm_matrixcolumn 
            WHERE matrixColumnId = newid;
      END IF;
   END IF;
   
   RETURN rec_mtxcol;
END;
$$  LANGUAGE 'plpgsql' VOLATILE;

-- ----------------------------------------------------------------------------
-- status, matrixIndex=None, metadata=None, modTime
CREATE OR REPLACE FUNCTION lm_v3.lm_updateMatrixColumn(mtxcolid int,
                                                       mtxidx int,
                                                       mtxcolmeta varchar,
                                                       intparams text,
                                                       stat int,
                                                       stattime double precision)
RETURNS int AS
$$
DECLARE
   rec lm_v3.lm_matrixcolumn%rowtype;
   success int = -1;
BEGIN
   -- find layer 
   IF mtxcolid IS NOT NULL then                     
      SELECT * INTO rec from lm_v3.lm_matrixcolumn WHERE matrixColumnId = mtxcolid;
   ELSE
      RAISE EXCEPTION 'MatrixColumnId required';
	END IF;
	
   IF NOT FOUND THEN
      RAISE EXCEPTION 'Unable to find lm_matrixcolumn';      
   ELSE
      -- Update MatrixColumn record
      UPDATE lm_v3.MatrixColumn
           SET (matrixIndex, metadata, intersectParams, 
                status, statusmodtime) 
             = (mtxidx, mtxcolmeta, intparams, stat, stattime) 
           WHERE matrixColumnId = mtxcolid;
      IF FOUND THEN 
         success = 0;
      ELSE
         RAISE EXCEPTION 'Unable to update MatrixColumn';
      END IF;
   END IF;   
   
   RETURN success;
END;
$$  LANGUAGE 'plpgsql' VOLATILE;

-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lm_v3.lm_deleteMatrixColumn(mtxcolid int)
RETURNS int AS
$$
DECLARE
   success int := -1;
BEGIN
   DELETE FROM lm_v3.MatrixColumn WHERE matrixColumnId = mtxcolid;
   IF FOUND THEN
      success = 0;
   END IF;
   RETURN success;
END;
$$  LANGUAGE 'plpgsql' VOLATILE;



-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lm_v3.lm_getFilterMtxCols(usr varchar,
                                                    sqd varchar,
                                                    idt varchar,
                                                    aftertime double precision,
                                                    beforetime double precision,
                                                    epsg int,
                                                    afterstat int,
                                                    beforestat int,
                                                    mtxid int,
                                                    lyrid int)
   RETURNS varchar AS
$$
DECLARE
   wherecls varchar;
BEGIN
   wherecls = ' WHERE userid = ' || quote_literal(usr);
                
   -- filter by squid
   IF sqd is not null THEN
      wherecls = wherecls || ' AND squid like  ' || quote_literal(sqd);
   END IF;

   -- filter by ident
   IF idt is not null THEN
      wherecls = wherecls || ' AND ident like  ' || quote_literal(idt);
   END IF;

   -- filter by layers modified after given time
   IF aftertime is not null THEN
      wherecls = wherecls || ' AND statusModTime >=  ' || quote_literal(aftertime);
   END IF;
   
   -- filter by layers modified before given time
   IF beforetime is not null THEN
      wherecls = wherecls || ' AND statusModTime <=  ' || quote_literal(beforetime);
   END IF;

   -- filter by epsgcode
   IF epsg is not null THEN
      wherecls = wherecls || ' AND  epsgcode =  ' || epsg;
   END IF;

   -- filter by status
   IF afterstat is not null OR beforestat is not null THEN
      begin
         IF afterstat = beforestat THEN
            wherecls = wherecls || ' AND status =  ' || afterstat;
         ELSE
            -- filter by status >= given value
            IF afterstat is not null THEN
                wherecls = wherecls || ' AND status >=  ' || afterstat;
            END IF;
   
            -- filter by status <= given value
            IF beforestat is not null THEN
               wherecls = wherecls || ' AND status <=  ' || beforestat;
            END IF;
         END IF;
      end;
   END IF;

   -- filter by Matrix
   IF mtxid is not null THEN
      wherecls = wherecls || ' AND matrixid =  ' || quote_literal(mtxid);
   END IF;

   -- filter by Layer input
   IF lyrid is not null THEN
      wherecls = wherecls || ' AND layerid =  ' || quote_literal(lyrid);
   END IF;
   
   return wherecls;
END;
$$  LANGUAGE 'plpgsql' STABLE;

-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lm_v3.lm_countMtxCols(usr varchar,
                                                    sqd varchar,
                                                    idt varchar,
                                                    aftertime double precision,
                                                    beforetime double precision,
                                                    epsg int,
                                                    afterstat int,
                                                    beforestat int,
                                                    mtxid int,
                                                    lyrid int)
   RETURNS int AS
$$
DECLARE
   num int;
   cmd varchar;
   wherecls varchar;
BEGIN
   cmd = 'SELECT count(*) FROM lm_v3.lm_matrixcolumn ';
   SELECT * INTO wherecls FROM lm_v3.lm_v3.lm_getFilterMtxCols(usr, sqd, idt, 
            aftertime, beforetime, epsg, afterstat, beforestat, mtxid, lyrid);
   cmd := cmd || wherecls;
   RAISE NOTICE 'cmd = %', cmd;

   EXECUTE cmd INTO num;
   RETURN num;
END;
$$  LANGUAGE 'plpgsql' STABLE;

-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lm_v3.lm_listMtxColAtoms(firstRecNum int, maxNum int, 
                                                    usr varchar,
                                                    sqd varchar,
                                                    idt varchar,
                                                    aftertime double precision,
                                                    beforetime double precision,
                                                    epsg int,
                                                    afterstat int,
                                                    beforestat int,
                                                    mtxid int,
                                                    lyrid int)
   RETURNS SETOF lm_v3.lm_atom AS
$$
DECLARE
   rec lm_v3.lm_atom;
   cmd varchar;
   wherecls varchar;
   limitcls varchar;
   ordercls varchar;
BEGIN
   cmd = 'SELECT matrixColumnId, squid, null, null, mtxcolstatusmodtime FROM lm_v3.lm_matrixcolumn ';
   SELECT * INTO wherecls FROM lm_v3.lm_v3.lm_getFilterMtxCols(usr, sqd, idt, 
            aftertime, beforetime, epsg, afterstat, beforestat, mtxid, lyrid);
   ordercls = 'ORDER BY mtxcolstatusmodtime DESC';
   limitcls = ' LIMIT ' || quote_literal(maxNum) || ' OFFSET ' || quote_literal(firstRecNum);

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
CREATE OR REPLACE FUNCTION lm_v3.lm_listMtxColObjects(firstRecNum int, maxNum int, 
                                                    usr varchar,
                                                    sqd varchar,
                                                    idt varchar,
                                                    aftertime double precision,
                                                    beforetime double precision,
                                                    epsg int,
                                                    afterstat int,
                                                    beforestat int,
                                                    mtxid int,
                                                    lyrid int)
   RETURNS SETOF lm_v3.lm_matrixcolumn AS
$$
DECLARE
   rec lm_v3.lm_matrixcolumn;
   cmd varchar;
   wherecls varchar;
   limitcls varchar;
   ordercls varchar;
BEGIN
   cmd = 'SELECT * FROM lm_v3.lm_matrixcolumn ';
   SELECT * INTO wherecls FROM lm_v3.lm_v3.lm_getFilterMtxCols(usr, sqd, idt, 
            aftertime, beforetime, epsg, afterstat, beforestat, mtxid, lyrid);
   ordercls = 'ORDER BY mtxcolstatusmodtime DESC';
   limitcls = ' LIMIT ' || quote_literal(maxNum) || ' OFFSET ' || quote_literal(firstRecNum);

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
-- Matrix
-- ----------------------------------------------------------------------------
-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lm_v3.lm_updateMatrix(mtxid int,
                                                 meta varchar,
                                                 stat int,
                                                 stattime double precision)
RETURNS int AS
$$
DECLARE
   success int = -1;
BEGIN
   UPDATE lm_v3.Matrix SET (metadata, status, statusmodtime) 
                         = (meta, stat, stattime) WHERE matrixId = mtxid;
   IF FOUND THEN 
      success = 0;
   END IF;
      
   RETURN success;
END;
$$  LANGUAGE 'plpgsql' VOLATILE;

-- ----------------------------------------------------------------------------
-- Update or Rollback
-- ----------------------------------------------------------------------------

-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lm_v3.lm_clearComputedUserData(usr varchar)
RETURNS int AS
$$
DECLARE
   currCount int := -1;
BEGIN
   -- MFProcesses
	DELETE FROM lm_v3.MFProcess WHERE userid = usr;
	GET DIAGNOSTICS currCount = ROW_COUNT;
   RAISE NOTICE 'Deleted % MF processes for User %', currCount, user;

   -- Matrix Intersect Columns from SDM
	DELETE FROM lm_v3.MatrixColumn WHERE layerid IN 
	   (SELECT layerid FROM lm_v3.lm_sdmproject WHERE userid = usr);
	GET DIAGNOSTICS currCount = ROW_COUNT;
   RAISE NOTICE 'Deleted % MatrixColumns using SDMProject layers for User %', currCount, user;
   
   -- Matrix SDMProject Layers
	DELETE FROM lm_v3.Layer WHERE layerid IN 
	   (SELECT layerid FROM lm_v3.lm_sdmproject WHERE userid = usr);
	GET DIAGNOSTICS currCount = ROW_COUNT;
   RAISE NOTICE 'Deleted % Layers for SDMProjects for User %', currCount, user;

   -- OccurrenceSet and SDMProjects
	DELETE FROM lm_v3.OccurrenceSet WHERE userid = usr;
	GET DIAGNOSTICS currCount = ROW_COUNT;
   RAISE NOTICE 'Deleted % Occurrencesets for User %', currCount, user;

   RETURN currCount;
END;
$$  LANGUAGE 'plpgsql' VOLATILE;

-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lm_v3.lm_clearUserData(usr varchar)
RETURNS int AS
$$
DECLARE
   currCount int := -1;
   total int := 0;
BEGIN
   SELECT * INTO currCount FROM lm_v3.lm_clearComputedUserData(usr);
   
   -- Gridsets (Cascades to Matrix)
	DELETE FROM lm_v3.Gridset WHERE userid = usr;
	GET DIAGNOSTICS currCount = ROW_COUNT;
   RAISE NOTICE 'Deleted % Gridsets for User %', currCount, user;
   total = total + currCount;
   
   -- Scenarios
	DELETE FROM lm_v3.Scenario WHERE userid = usr;
	GET DIAGNOSTICS currCount = ROW_COUNT;
   RAISE NOTICE 'Deleted % Scenarios for User %', currCount, user;
   total = total + currCount;
   
   -- Layers (Cascades to EnvLayer, ShapeGrid)
	DELETE FROM lm_v3.Layer WHERE userid = usr;
	GET DIAGNOSTICS currCount = ROW_COUNT;
   RAISE NOTICE 'Deleted % Layers for User %', currCount, user;
   total = total + currCount;

   -- EnvType
	DELETE FROM lm_v3.EnvType WHERE userid = usr;
	GET DIAGNOSTICS currCount = ROW_COUNT;
   RAISE NOTICE 'Deleted % EnvTypes for User %', currCount, user;
   total = total + currCount;
   
   -- Tree
	DELETE FROM lm_v3.Tree WHERE userid = usr;
	GET DIAGNOSTICS currCount = ROW_COUNT;
   RAISE NOTICE 'Deleted % Trees for User %', currCount, user;
   total = total + currCount;
   
   RETURN total;
END;
$$  LANGUAGE 'plpgsql' VOLATILE;

-- ----------------------------------------------------------------------------
-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lm_v3.lm_updatePaths(olddir varchar, newdir varchar)
   RETURNS void AS
$$
DECLARE
   start int = 0;
BEGIN
   start = char_length(olddir) + 1;
   UPDATE lm_v3.JobChain SET dlocation = newdir || substr(dlocation, start)  
	   WHERE dlocation like olddir || '%';

   UPDATE lm_v3.Layer SET dlocation = newdir || substr(dlocation, start)  
	   WHERE dlocation like olddir || '%';

   UPDATE lm_v3.Occurrenceset SET dlocation = newdir || substr(dlocation, start)  
	   WHERE dlocation like olddir || '%';

   UPDATE lm_v3.SDMModel SET dlocation = newdir || substr(dlocation, start)  
	   WHERE dlocation like olddir || '%';

   UPDATE lm_v3.Process SET dlocation = newdir || substr(dlocation, start)  
	   WHERE dlocation like olddir || '%';
   
END;
$$ LANGUAGE 'plpgsql' VOLATILE; 
