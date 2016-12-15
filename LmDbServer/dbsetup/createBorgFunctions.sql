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
-- deleted lm_countModeledSpecies, lm_getPointMaplayer

-- ----------------------------------------------------------------------------
-- SDMModel
-- ----------------------------------------------------------------------------
-- deleted lm_getLatestModelTime


-- ----------------------------------------------------------------------------
-- SDMProjection
-- ----------------------------------------------------------------------------
-- deleted lm_getLatestProjectionTime

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
CREATE OR REPLACE FUNCTION lm_v3.lm_joinScenarioKeyword(scenid int,
                                                    kywd varchar)
   RETURNS int AS
$$
DECLARE
   success int := -1;
   wdid int;
   tmpid int;
   poly varchar;
BEGIN
   -- insert keyword if it is not there 
   SELECT k.keywordid INTO wdid FROM lm_v3.Keyword k WHERE k.keyword = kywd;
   IF NOT FOUND THEN
      INSERT INTO lm_v3.Keyword (keyword) VALUES (kywd);
      IF FOUND THEN
         SELECT INTO wdid last_value FROM lm_v3.keyword_keywordid_seq;
      END IF;
   END IF;
   
   IF FOUND THEN
      BEGIN
         SELECT sk.scenarioId INTO tmpid
            FROM lm_v3.ScenarioKeywords sk
            WHERE sk.scenarioId = scenid
              AND sk.keywordId = wdid;
         IF NOT FOUND THEN
            INSERT INTO lm_v3.ScenarioKeywords (scenarioId, keywordId) VALUES (scenid, wdid);
            IF FOUND THEN
               success := 0;
            END IF;
         ELSE
            success := 0;
         END IF;
      END;
   END IF;
   
   RETURN success;
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
CREATE OR REPLACE FUNCTION lm_v3.lm_countJobChains(usrid varchar(20), 
    	                                           stat int)
   RETURNS int AS
$$
DECLARE
   num int;
   cmd varchar;
   wherecls varchar;
BEGIN
   IF usrid IS null THEN
      SELECT count(*) INTO num FROM lm_v3.jobchain WHERE status = stat;
   ELSE
      SELECT count(*) INTO num FROM lm_v3.jobchain WHERE status = stat 
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
   SELECT * INTO rec FROM lm_v3.Taxon
      WHERE taxonomysourceid = tsourceid and taxonomykey = tkey;

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
CREATE OR REPLACE FUNCTION lm3.lm_insertMFChain(usr varchar,
                                                  dloc varchar,
                                                  stat int,
                                                  prior int, 
                                                  currtime double precision)
RETURNS int AS
$$
DECLARE
   rec lm3.JobChain%ROWTYPE;
   jid int = -1;
BEGIN
   INSERT INTO lm3.JobChain 
             (userid, dlocation, priority, status, statusmodtime, datecreated)
      VALUES (usr, dloc, prior, stat, currtime, currtime);
   IF FOUND THEN 
      SELECT INTO jid last_value FROM lm3.jobchain_jobchainid_seq;
   END IF;

   RETURN jid;

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
