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
CREATE OR REPLACE FUNCTION lm_v3.lm_insertAlgorithm(code varchar, 
                                                    aname varchar, 
                                                    mtime double precision)
   RETURNS lm_v3.algorithm AS
$$
DECLARE
   rec lm_v3.algorithm%rowtype;
BEGIN
   SELECT * INTO rec FROM lm_v3.algorithm WHERE algorithmcode = code;
   IF NOT FOUND THEN
      INSERT INTO lm_v3.Algorithm (algorithmcode, name, modtime)
         VALUES (code, aname, mtime);
      IF FOUND THEN
         SELECT * INTO rec FROM lm_v3.algorithm WHERE algorithmcode = code;
      END IF;
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
-- COMPUTERESOURCE
-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lm_v3.lm_getComputeRec(crip int, sigbits varchar)
   RETURNS lm_v3.ComputeResource AS
$$
DECLARE
   rec lm_v3.ComputeResource;
BEGIN
   begin
      -- Get computeresource id for requesting resource.
      SELECT * INTO STRICT rec FROM lm_v3.ComputeResource
         WHERE ipaddress = crip AND ipsigbits = sigbits;
      EXCEPTION
         WHEN NO_DATA_FOUND THEN
            RAISE NOTICE 'ComputeResource not found for IP %, mask %', crip, sigbits;
   end;
   RETURN rec;
END;
$$  LANGUAGE 'plpgsql' STABLE;

-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lm_v3.lm_findOrInsertCompute(cmpname varchar,
                                                ip varchar,
                                                sigbits varchar,
                                                domname text,
                                                usr varchar,
                                                mtime double precision)
   RETURNS lm_v3.ComputeResource AS
$$
DECLARE
   rec lm_v3.ComputeResource%ROWTYPE;
BEGIN
   IF sigbits IS NULL THEN
      SELECT * INTO rec FROM lm_v3.ComputeResource WHERE ipaddress = ip;
   ELSE
      SELECT * INTO rec FROM lm_v3.ComputeResource
         WHERE ipaddress = ip AND ipsigbits = sigbits;
   END IF;
      
   IF NOT FOUND THEN
      INSERT INTO lm_v3.ComputeResource 
         (name, ipaddress, ipsigbits, fqdn, userId, modtime)
      VALUES 
         (cmpname, ip, sigbits, domname, usr, mtime);

      IF FOUND THEN
         SELECT * INTO rec FROM lm_v3.ComputeResource 
            WHERE ipaddress = ip AND ipsigbits = sigbits;
      END IF;         
   END IF;  -- end if not found
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
CREATE OR REPLACE FUNCTION lm_v3.lm_findOrInsertScenario(code varchar, 
                                             ttl text, 
                                             authr text,
                                             dsc text,
                                             metadataUrlprefix text,
                                             startdt double precision,
                                             enddt double precision,
                                             unts varchar,
                                             res double precision,
                                             epsg int,
                                             bndsstring varchar, 
                                             bboxwkt varchar,
                                             modTime double precision,
                                             usr varchar)
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
         (scenarioCode, title, author, description, startDate, endDate, units, 
          resolution, bbox, dateLastModified, epsgcode, userid)
      VALUES (code, ttl, authr, dsc, startdt, enddt, unts, 
              res, bndsstring, modTime, epsg, usr);
                       
      IF FOUND THEN
         SELECT INTO id last_value FROM lm_v3.scenario_scenarioid_seq;
         idstr = cast(id as varchar);
         scenmetadataUrl := replace(metadataUrlprefix, '#id#', idstr);
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
CREATE OR REPLACE FUNCTION lm_v3.lm_findOrInsertUser(usrid varchar, name1 varchar, 
                                         name2 varchar,
                                         inst varchar, addr1 varchar, 
                                         addr2 varchar, addr3 varchar,
                                         fone varchar, emale varchar, 
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
