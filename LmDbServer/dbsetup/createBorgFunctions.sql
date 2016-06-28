-- ----------------------------------------------------------------------------
-- From APP_DIR
-- psql -U admin -d mal --file=LmDbServer/dbsetup/createBorgFunctions.sql
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
                                                  modtime double precision)
   RETURNS int AS
$$
DECLARE
   retval int = -1;
   rec lm_v3.algorithm;
BEGIN
   SELECT * INTO rec 
      FROM lm_v3.algorithm
      WHERE algorithmcode = code;
   IF NOT FOUND THEN
      INSERT INTO lm_v3.Algorithm (algorithmcode, name, datelastmodified)
         VALUES (code, aname, modtime);
      IF FOUND THEN
         retval = 0;
      END IF;
   END IF;
   RETURN retval;
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
-- LayerType (EnvLayer)
-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lm_v3.lm_getLayerType(usr varchar,
                                               ltype varchar)
   RETURNS lm_v3.lm_layerTypeAndKeywords AS
$$
DECLARE
   rec lm_v3.lm_layerTypeAndKeywords%rowtype;
   keystr varchar;
BEGIN
   BEGIN
      SELECT layerTypeId, code, title, userid, description, modTime 
      INTO rec FROM lm_v3.LayerType WHERE code = ltype and userid = usr;
      
      EXCEPTION
         WHEN NO_DATA_FOUND THEN
            RAISE NOTICE 'LayerType % for % not found', usr, ltype;
         WHEN TOO_MANY_ROWS THEN
            RAISE EXCEPTION 'LayerType % for % not unique', usr, ltype;
   END;
   IF FOUND THEN
      SELECT INTO keystr lm_v3.lm_getLayerTypeKeywordString(rec.layertypeid);
      rec.keywords = keystr;
   END IF;
   RETURN rec;
END;
$$  LANGUAGE 'plpgsql' STABLE; 

-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lm_v3.lm_getLayerType(id int)
   RETURNS lm_v3.lm_layerTypeAndKeywords AS
$$
DECLARE
   rec lm_v3.lm_layerTypeAndKeywords%rowtype;
   keystr varchar;
BEGIN
   BEGIN
      SELECT layerTypeId, code, title, userid, description, modTime 
      INTO rec FROM lm_v3.LayerType WHERE layertypeid = id;
      
      EXCEPTION
         WHEN NO_DATA_FOUND THEN
            RAISE NOTICE 'LayerType % for % not found', usr, ltype;
         WHEN TOO_MANY_ROWS THEN
            RAISE EXCEPTION 'LayerType % for % not unique', usr, ltype;
                 
   END;
   IF FOUND THEN
      SELECT INTO keystr lm_v3.lm_getLayerTypeKeywordString(rec.layertypeid);
      rec.keywords = keystr;
   END IF;
   RETURN rec;
END;
$$  LANGUAGE 'plpgsql' STABLE; 

-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lm_v3.lm_insertLayerType(usr varchar,
                                                  ltype varchar,
                                                  ltypetitle varchar,
                                                  ltypedesc varchar,
                                                  mtime double precision)
   RETURNS int AS
$$
DECLARE
   typeid int;
BEGIN
   INSERT INTO lm_v3.LayerType (code, title, userid, description, modTime) 
      VALUES (ltype, ltypetitle, usr, ltypedesc, mtime);
   IF FOUND THEN
      SELECT INTO typeid last_value FROM lm_v3.layertype_layertypeid_seq;
   END IF;
   
   RETURN typeid;
END;
$$  LANGUAGE 'plpgsql' VOLATILE; 

-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lm_v3.lm_insertLayerTypeKeyword(typid int, kywd varchar)
   RETURNS int AS
$$
DECLARE
   retval int := -1;
   typid int;
   wdid int;
   total int;
BEGIN
   -- insert keyword if it is not there 
   SELECT k.keywordid INTO wdid FROM lm_v3.Keyword k WHERE k.keyword = kywd;
   IF NOT FOUND THEN
      INSERT INTO lm_v3.Keyword (keyword) VALUES (kywd);
      IF FOUND THEN
         SELECT INTO wdid last_value FROM lm_v3.keyword_keywordid_seq;
      END IF;
   END IF;
   -- if found or inserted, join
   IF FOUND THEN
      SELECT count(*) INTO total FROM lm_v3.LayerTypeKeyword 
         WHERE layerTypeId = typid AND keywordId = wdid;
      IF total = 0 THEN
         INSERT INTO lm_v3.LayerTypeKeyword (layerTypeId, keywordId) 
            VALUES (typid, wdid);
         IF FOUND THEN 
            retval := 0;
         END IF;
      ELSE
         retval := 0;
      END IF;
   END IF;
   
   RETURN retval;
END;
$$  LANGUAGE 'plpgsql' VOLATILE;

-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lm_v3.lm_countTypeCodes(usrid varchar, 
                                                 beforetime double precision, 
                                                 aftertime double precision)
   RETURNS int AS
$$
DECLARE
   num int;
   cmd varchar;
   wherecls varchar;
BEGIN
   cmd = 'SELECT count(*) FROM lm_v3.LayerType ';
   wherecls = ' WHERE userid =  ' || quote_literal(usrid) ;

   -- filter by modified before given time
   IF beforetime is not null THEN
      wherecls = wherecls || ' AND dateLastModified <=  ' || quote_literal(beforetime);
   END IF;

   -- filter by modified after given time
   IF aftertime is not null THEN
      wherecls = wherecls || ' AND dateLastModified >=  ' || quote_literal(aftertime);
   END IF;

   cmd := cmd || wherecls;
   RAISE NOTICE 'cmd = %', cmd;

   EXECUTE cmd INTO num;
   RETURN num;
END;
$$  LANGUAGE 'plpgsql' STABLE;

-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lm_v3.lm_listTypeCodes(firstRecNum int, maxNum int, 
                                                usrid varchar(20), 
                                                beforetime double precision,
                                                aftertime double precision)
   RETURNS SETOF lm_v3.lm_atom AS
$$
DECLARE
   rec lm_v3.lm_atom;
   ltTitle varchar;
   cmd varchar;
   wherecls varchar;
   limitcls varchar;
   ordercls varchar;
BEGIN
   cmd = 'SELECT layerTypeId, code, description, datelastmodified, title
               FROM lm_v3.LayerType ';
   wherecls = ' WHERE userid =  ' || quote_literal(usrid) ;
   ordercls = ' ORDER BY code ASC ';
   limitcls = ' LIMIT ' || quote_literal(maxNum) || ' OFFSET ' || quote_literal(firstRecNum);

   -- filter by modified before given time
   IF beforetime is not null THEN
      wherecls = wherecls || ' AND dateLastModified <=  ' || quote_literal(beforetime);
   END IF;

   -- filter by modified after given time
   IF aftertime is not null THEN
      wherecls = wherecls || ' AND dateLastModified >=  ' || quote_literal(aftertime);
   END IF;

   cmd := cmd || wherecls || ordercls || limitcls;
   RAISE NOTICE 'cmd = %', cmd;

   FOR rec.id, rec.title, rec.description, rec.modtime, ltTitle in EXECUTE cmd
      LOOP
         IF ltTitle IS not null THEN
            rec.title = rec.title || ': ' || ltTitle;
         END IF;
         RETURN NEXT rec;
      END LOOP;
   RETURN;
END;
$$  LANGUAGE 'plpgsql' STABLE;

-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lm_v3.lm_listTypeCodeObjects(firstRecNum int, maxNum int, 
                                                usrid varchar(20), 
                                                beforetime double precision,
                                                aftertime double precision)
   RETURNS SETOF lm_v3.lm_layerTypeAndKeywords AS
$$
DECLARE
   rec lm_v3.lm_layerTypeAndKeywords;
   keystr varchar;
   ltTitle varchar;
   cmd varchar;
   wherecls varchar;
   limitcls varchar;
   ordercls varchar;
BEGIN
   cmd = 'SELECT * FROM lm_v3.LayerType ';
   wherecls = ' WHERE userid =  ' || quote_literal(usrid) ;
   ordercls = ' ORDER BY code ASC ';
   limitcls = ' LIMIT ' || quote_literal(maxNum) || ' OFFSET ' || quote_literal(firstRecNum);

   -- filter by modified before given time
   IF beforetime is not null THEN
      wherecls = wherecls || ' AND dateLastModified <=  ' || quote_literal(beforetime);
   END IF;

   -- filter by modified after given time
   IF aftertime is not null THEN
      wherecls = wherecls || ' AND dateLastModified >=  ' || quote_literal(aftertime);
   END IF;

   cmd := cmd || wherecls || ordercls || limitcls;
   RAISE NOTICE 'cmd = %', cmd;

   FOR rec in EXECUTE cmd
      LOOP
         SELECT INTO keystr lm_v3.lm_getLayerTypeKeywordString(rec.layertypeid);
         rec.keywords = keystr;
         RETURN NEXT rec;
      END LOOP;
   RETURN;
END;
$$  LANGUAGE 'plpgsql' STABLE;


-- ----------------------------------------------------------------------------
-- EnvLayer
-- ----------------------------------------------------------------------------
-- ----------------------------------------------------------------------------

-- ----------------------------------------------------------------------------
-- Scenario
-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lm_v3.lm_insertScenario(code varchar, 
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
   RETURNS int AS
$$
DECLARE
   id int;
   idstr varchar;
   scenmetadataUrl varchar;
BEGIN
   SELECT s.scenarioid INTO id
     FROM lm_v3.Scenario s
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
      END IF; -- end if inserted
   END IF;  -- end if not existing
   
   RETURN id;
END;
$$  LANGUAGE 'plpgsql' VOLATILE;

-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lm_v3.lm_insertScenarioKeyword(scenid int,
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
-- Insert a new Lifemapper User
CREATE OR REPLACE FUNCTION lm_v3.lm_insertUser(usrid varchar, name1 varchar, 
                                         name2 varchar,
                                         inst varchar, addr1 varchar, 
                                         addr2 varchar, addr3 varchar,
                                         fone varchar, emale varchar, 
                                         modTime double precision, 
                                         psswd varchar)
   RETURNS int AS
$$
DECLARE
   success int = -1;
   rec record;
BEGIN
   SELECT * into rec FROM lm_v3.LMUser
      WHERE userid = usrid;
   IF NOT FOUND THEN 
      INSERT INTO lm_v3.LMUser
         (userId, firstname, lastname, institution, address1, address2, address3, phone,
          email, dateLastModified, password)
         VALUES 
         (usrid, name1, name2, inst, addr1, addr2, addr3, fone, emale, modTime, psswd);

      IF FOUND THEN
         success := 0;
      END IF;
   END IF;
   
   RETURN success;
END;
$$  LANGUAGE 'plpgsql' VOLATILE; 

-- ----------------------------------------------------------------------------
-- ShapeGrid
-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lm_v3.lm_insertShapeGrid(lyrverify varchar,
                                              usr varchar,
                                              csides int,
                                              csize double precision,
                                              vsz int,
                                              idAttr varchar,
                                              xAttr varchar,
                                              yAttr varchar,
                                              lyrname varchar,
                                              lyrtitle varchar,
                                              lyrdesc varchar,
                                              dloc varchar,
                                              vtype int,
                                              datafmt varchar,
                                              epsg int,
                                              mpunits varchar,
                                              metaloc varchar,
                                              modtime double precision,
                                              bboxstr varchar,
                                              bboxwkt varchar,
                                              murlprefix varchar)
RETURNS lm_v3.lm_shapegrid AS
$$
DECLARE
   lyrid int;
   shpid int;
   rec lm_v3.lm_shapegrid%ROWTYPE;
BEGIN
   SELECT shapegridid INTO shpid
     FROM lm_v3.lm_shapegrid WHERE lyruserid = usr and layername = lyrname;
   IF NOT FOUND THEN
      begin
         -- get or insert layer 
         SELECT lm_v3.lm_insertLayer(lyrverify, null, usr, lyrname, lyrtitle, lyrdesc, dloc, vtype, 
                               null, datafmt, epsg, mpunits, null, null, null, 
                               metaloc, modtime, modtime, bboxstr, bboxwkt, murlprefix)  
                INTO lyrid;          
         IF lyrid = -1 THEN
            RAISE EXCEPTION 'Unable to insert layer';
         END IF;
         
         INSERT INTO lm_v3.ShapeGrid (layerId, cellsides, cellsize, vsize, 
                                idAttribute, xAttribute, yAttribute)
                       values (lyrid, csides, csize, vsz, idAttr, xAttr, yAttr);
   
         IF FOUND THEN
            SELECT INTO shpid last_value FROM lm_v3.shapegrid_shapegridid_seq;
            RAISE NOTICE 'Inserted shapegrid into %', shpid;
         ELSE
            RAISE EXCEPTION 'Unable to insert shapegrid';
         END IF;
      end;
   END IF;
   
   SELECT * INTO rec FROM lm_v3.lm_shapegrid WHERE shapegridid = shpid;    
   
   RETURN rec;
END;
$$  LANGUAGE 'plpgsql' VOLATILE;
 

-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lm_v3.lm_insertShapeGrid(lyrid int,
                                              csides int,
                                              csize double precision,
                                              vsz int,
                                              idAttr varchar,
                                              xAttr varchar,
                                              yAttr varchar,
                                              stat int,
                                              stattime double precision)
RETURNS int AS
$$
DECLARE
   shpid int = -1;
BEGIN
   SELECT shapegridid INTO shpid
     FROM lm_v3.lm_shapegrid WHERE layerid = lyrid;
   IF NOT FOUND THEN
      INSERT INTO lm_v3.ShapeGrid (layerId, cellsides, cellsize, vsize, 
                     idAttribute, xAttribute, yAttribute, status, statusmodtime)
          values (lyrid, csides, csize, vsz, idAttr, xAttr, yAttr);
   
      IF FOUND THEN
         SELECT INTO shpid last_value FROM lm_v3.shapegrid_shapegridid_seq;
         RAISE NOTICE 'Inserted shapegrid into %', shpid;
      ELSE
         RAISE EXCEPTION 'Unable to insert shapegrid';
      END IF;
   END IF;
   
   RETURN shpid;
END;
$$  LANGUAGE 'plpgsql' VOLATILE;
 
-- ----------------------------------------------------------------------------
-- LAYER
-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lm_v3.lm_insertLayer(lyrverify varchar,
                                          lyrsquid varchar,
                                          usr varchar,
                                          txid int,
                                          lyrname varchar, 
                                          lyrtitle varchar,
                                          lyrauthor varchar,
                                          lyrdesc varchar,
                                          dloc varchar,
                                          mloc varchar,
                                          vtype int,
                                          rtype int,
                                          iscat int,
                                          datafmt varchar,
                                          epsg int,
                                          munits varchar,
                                          res double precision,
                                          startdt double precision,
                                          enddt double precision,
                                          mtime double precision,
                                          bboxstr varchar,
                                          bboxwkt varchar,
                                          vattr varchar, 
                                          vnodata double precision,
                                          vmin double precision,
                                          vmax double precision,
                                          vunits varchar,
                                          lyrtypeid int,
                                          murlprefix varchar)
RETURNS int AS
$$
DECLARE
   lyrid int = -1;
   idstr varchar;
   murl varchar;
BEGIN
   -- get or insert layer 
   SELECT layerid INTO lyrid
      FROM lm_v3.Layer
      WHERE userId = usr
        AND layername = lyrname
        AND epsgcode = epsg;
                
   IF FOUND THEN
      RAISE NOTICE 'User/Name/EPSG Layer % / % / % found with id %', 
                    usr, lyrname, epsg, lyrid;
   ELSE
      INSERT INTO lm_v3.Layer (verify, squid, userId, taxonId, name, title, author, 
                             description, dlocation, metalocation, gdalType, 
                             ogrType, isCategorical, dataFormat, epsgcode, 
                             mapunits, resolution, startDate, endDate, modTime, 
                             bbox, valAttribute, nodataVal, minVal, maxVal, 
                             valUnits, layerTypeId)
         VALUES (lyrverify, lyrsquid, usr, txid, lyrname, lyrtitle, lyrauthor,
                 lyrdesc, dloc, mloc, rtype, vtype, iscat, datafmt, epsg, munits, 
                 res, startdt, enddt, mtime, bboxstr, vattr, vnodata, vmin, vmax,
                 vunits, lyrtypeid);         
                  
      IF FOUND THEN
         SELECT INTO lyrid last_value FROM lm_v3.layer_layerid_seq;
         RAISE NOTICE 'This layer inserted with id %', lyrid;
         idstr := cast(lyrid as varchar);
         murl := replace(murlprefix, '#id#', idstr);
         IF bboxwkt is NOT NULL THEN
            UPDATE lm_v3.Layer SET (metadataurl, geom) 
               = (murl, ST_GeomFromText(bboxwkt, epsg)) WHERE layerid = lyrid;
         ELSE
            UPDATE lm_v3.Layer SET metalocation = murl WHERE layerid = lyrid;
         END IF;
      END IF; -- end if layer inserted
   END IF;  
      
   RETURN lyrid;
END;
$$  LANGUAGE 'plpgsql' VOLATILE;

-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lm_v3.lm_renameLayer(lyrid int,
                                          usr varchar,
                                          lyrname varchar,
                                          epsg int)
RETURNS int AS
$$
DECLARE
   success int = -1;
   total int = -1;
BEGIN
   -- get or insert layer
   SELECT count(*) INTO total FROM lm_v3.layer 
          WHERE layerid = lyrid AND userid = usr;
   IF total = 1 THEN
      SELECT count(*) INTO total FROM lm_v3.layer 
             WHERE layername = lyrname AND userid = usr AND epsgcode = epsg; 
      IF total = 0 THEN 
         BEGIN
            UPDATE lm_v3.Layer SET layername = lyrname
               WHERE layerid = lyrid AND userid = usr AND epsgcode = epsg;
            IF FOUND THEN
               success = 0;
            END IF;   
         END;   
      ELSE
         RAISE NOTICE 'Layer % found for User/EPSG %', lyrname, usr, epsg;
      END IF;
   ELSE
      RAISE NOTICE 'User/Name/EPSG Layer % / %  found with id %', 
                    usr, lyrname, epsg, existingid;
   END IF;
   RETURN success;
END;
$$  LANGUAGE 'plpgsql' VOLATILE;

