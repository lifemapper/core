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
         IF FOUND THEN
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
      IF FOUND THEN
         SELECT INTO rec * FROM lm_v3.taxonomysource WHERE datasetIdentifier = name;
      END IF;
   END IF;
   RETURN rec;
END;
$$  LANGUAGE 'plpgsql' VOLATILE;


-- ----------------------------------------------------------------------------
-- OccurrenceSet
-- ----------------------------------------------------------------------------

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

-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lm_v3.lm_findOrInsertScenario(usr varchar,
                                             code varchar, 
                                             metaUrlprefix text,
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
   id int;
   idstr varchar;
   scenmetadataUrl varchar;
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
                       
      IF FOUND THEN
         SELECT INTO id last_value FROM lm_v3.scenario_scenarioid_seq;
         idstr = cast(id as varchar);
         scenmetadataUrl := replace(metaUrlprefix, '#id#', idstr);
         IF bboxwkt is NULL THEN 
            UPDATE lm_v3.scenario SET metadataUrl = scenmetadataUrl WHERE scenarioId = id;
         ELSE
            UPDATE lm_v3.scenario SET (metadataUrl, geom) 
               = (scenmetadataUrl, ST_GeomFromText(bboxwkt, epsg)) 
               WHERE scenarioId = id;
         END IF;          
         SELECT * INTO rec FROM lm_v3.Scenario s 
            WHERE s.scenariocode = code and s.userid = usr;
      END IF; -- end if inserted
   END IF;  -- end if not existing
   
   RETURN rec;
END;
$$  LANGUAGE 'plpgsql' VOLATILE;

-- ----------------------------------------------------------------------------
-- LmUser
-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lm_v3.lm_findOrInsertUser(usrid varchar, 
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
   SELECT * into rec FROM lm_v3.LMUser WHERE lower(userid) = lower(usrid) 
                                          OR lower(email) = lower(emale);
   IF NOT FOUND THEN 
      INSERT INTO lm_v3.LMUser
         (userId, firstname, lastname, institution, address1, address2, address3, phone,
          email, modTime, password)
         VALUES 
         (usrid, name1, name2, inst, addr1, addr2, addr3, fone, emale, mtime, psswd);

      IF FOUND THEN
         SELECT INTO rec * FROM lm_v3.LMUser WHERE userid = usrid;
      END IF;
   END IF;
   
   RETURN rec;
END;
$$  LANGUAGE 'plpgsql' VOLATILE; 

-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lm_v3.lm_findUser(usrid varchar, 
                                           emale varchar)
   RETURNS lm_v3.lmuser AS
$$
DECLARE
   rec lm_v3.lmuser%rowtype;
BEGIN
   SELECT * into rec FROM lm_v3.LMUser WHERE lower(userid) = lower(usrid) 
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
                                                        metaurlprefix varchar,
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
   idstr varchar;
   newurl varchar;
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
            -- update metadataUrl
            SELECT INTO newid last_value FROM lm_v3.gridset_gridsetid_seq;
            idstr = cast(newid as varchar);
            newurl := replace(metaurlprefix, '#id#', idstr);
            UPDATE lm_v3.Gridset SET metadataUrl = newurl WHERE gridsetId = newid;

            -- get updated record
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
                                                       dloc text,
                                                       metaurlprefix varchar,
                                                       meta varchar, 
                                                       stat int,
                                                   	 stattime double precision)
   RETURNS lm_v3.Matrix AS
$$
DECLARE
   rec lm_v3.matrix%rowtype;
   newid int;
   idstr varchar;
   newurl varchar;
BEGIN
   IF mtxid IS NOT NULL THEN
      SELECT * INTO rec FROM lm_v3.matrix WHERE matrixid = mtxid;
   ELSE
      SELECT * INTO rec FROM lm_v3.matrix WHERE matrixtype = mtxtype 
                                            AND gridsetid = grdid;
   END IF;
   IF NOT FOUND THEN
      begin
         INSERT INTO lm_v3.matrix (matrixType, gridsetId, matrixDlocation, 
                                   metadata, status, statusmodtime) 
                           VALUES (mtxtype, grdid, dloc, 
                                   meta, stat, stattime);
         IF FOUND THEN
            -- update metadataUrl
            SELECT INTO newid last_value FROM lm_v3.matrix_matrixid_seq;
            idstr = cast(newid as varchar);
            newurl := replace(metaurlprefix, '#id#', idstr);
            UPDATE lm_v3.matrix SET metadataUrl = newurl WHERE matrixId = newid;

            -- get updated record
            SELECT * INTO rec from lm_v3.matrix WHERE matrixId = newid;
         END IF;
      end;
   END IF;
   RETURN rec;
END;
$$  LANGUAGE 'plpgsql' VOLATILE;    

-- ----------------------------------------------------------------------------
-- Gets a matrix with its lm_gridset (including optional shapegrid)
CREATE OR REPLACE FUNCTION lm_v3.lm_getMatrix(mtxid int, 
                                              mtxtype int, 
                                              gsid int,
                                              gsname varchar,
                                              usr varchar)
   RETURNS lm_v3.lm_matrix AS
$$
DECLARE
   rec lm_v3.lm_matrix%rowtype;
BEGIN
   IF mtxid IS NOT NULL THEN
      SELECT * INTO rec FROM lm_v3.lm_matrix WHERE matrixid = mtxid;
   ELSIF mtxtype IS NOT NULL AND gsid IS NOT NULL THEN
      SELECT * INTO rec FROM lm_v3.lm_matrix WHERE matrixtype = mtxtype 
                                            AND gridsetid = gsid;
   ELSIF mtxtype IS NOT NULL AND gsname IS NOT NULL AND usr IS NOT NULL THEN
      SELECT * INTO rec FROM lm_v3.lm_matrix WHERE matrixtype = mtxtype 
                                            AND grdname = gsname 
                                            AND userid = usr;
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
-- JobChain
-- ----------------------------------------------------------------------------
-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lm_v3.lm_countMFProcess(usrid varchar(20), 
    	                                             stat int)
   RETURNS int AS
$$
DECLARE
   num int;
   cmd varchar;
   wherecls varchar;
BEGIN
   IF usrid IS null THEN
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
            IF FOUND THEN 
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
                                                  metaurlprefix varchar,
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
   idstr varchar = '';
   occmetadataUrl varchar = '';
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

         IF FOUND THEN
            -- add geometries if valid
            IF ST_IsValid(ST_GeomFromText(polywkt, epsg)) THEN
               UPDATE lm3.OccurrenceSet SET geom = ST_GeomFromText(polywkt, epsg) 
                  WHERE occurrenceSetId = occid;
            END IF;
            IF ST_IsValid(ST_GeomFromText(pointswkt, epsg)) THEN
               UPDATE lm3.OccurrenceSet SET geompts = ST_GeomFromText(pointswkt, epsg) 
                  WHERE occurrenceSetId = occid;
            END IF;

            -- update metadataUrl
            SELECT INTO newid last_value FROM lm_v3.occurrenceset_occurrencesetid_seq;
            idstr = cast(newid as varchar);
            occmetadataUrl := replace(metaurlprefix, '#id#', idstr);
            UPDATE lm_v3.OccurrenceSet SET metadataUrl = occmetadataUrl WHERE occurrenceSetId = newid;

            -- get updated record
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
   END IF;

   IF ST_IsValid(ST_GeomFromText(polywkt, epsg)) THEN
      UPDATE lm_v3.OccurrenceSet SET geom = ST_GeomFromText(polywkt, epsg) 
         WHERE occurrenceSetId = occid;
   END IF;

   IF ST_IsValid(ST_GeomFromText(pointswkt, epsg)) THEN
      UPDATE lm_v3.OccurrenceSet SET geompts = ST_GeomFromText(pointswkt, epsg) 
         WHERE occurrenceSetId = occid;
   END IF;

   RETURN success;
END;
$$  LANGUAGE 'plpgsql' VOLATILE;

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
      VALUES (usr, dloc, prior, meta, stat, currtime);
   IF FOUND THEN 
      SELECT INTO mfid last_value FROM lm3.mfprocess_mfprocessid_seq;
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
   cmd = 'SELECT * FROM lm3.MFProcess WHERE status = ' || quote_literal(oldstat); 
   limitcls = ' LIMIT ' || quote_literal(total);

   IF usr IS NOT NULL THEN
      cmd = cmd || ' AND userid = ' || quote_literal(usrid);
   END IF;
   
   cmd := cmd || limitcls;
   RAISE NOTICE 'cmd = %', cmd;

   FOR rec in EXECUTE cmd
      LOOP
         UPDATE lm3.MFProcess SET (status, statusmodtime) = (newstat, modtime)
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
                                                  stat int,
                                                  modtime double precision)
RETURNS int AS
$$
DECLARE
   success int := -1;
BEGIN
   UPDATE lm3.MFProcess SET (status, statusmodtime) = (newstat, modtime)
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
   DELETE FROM lm3.MFProcess WHERE mfProcessId = mfid;
   IF FOUND THEN
      success = 0;
   END IF;
   RETURN success;
END;
$$  LANGUAGE 'plpgsql' VOLATILE;

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

-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lm_v3.lm_findOrInsertMatrixColumn(-- MatrixColumn
                                                             usr varchar,
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

   -- check existence of optional referenced layer
   IF lyrid IS NOT NULL THEN
      SELECT * INTO rec_lyr FROM lm_v3.layer WHERE layerid = lyrid;
      IF NOT FOUND THEN
         RAISE EXCEPTION 'Layer with id %, does not exist', lyrid; 
      END IF;
   END IF;
   
   -- Find existing column at position in matrix
   IF mtxidx IS NOT NULL AND mtxidx > -1 THEN
      SELECT * INTO rec_mtxcol FROM lm_v3.lm_matrixcolumn 
         WHERE matrixid = mtxid AND matrixIndex = mtxidx;
      IF FOUND THEN
         RAISE NOTICE 'Returning existing MatrixColumn for Matrix % and Column %',
            mtxid, mtxidx;
      END IF;
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
