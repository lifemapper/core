-- From APP_DIR
-- psql -U admin -d mal --file=LmDbServer/dbsetup/createMALExtras.sql
-- \i LmDbServer/dbsetup/createMALExtras.sql
-- ----------------------------------------------------------------------------
\c mal
-- ----------------------------------------------------------------------------
-- FUNCTIONS
-- Note: All column names are returned in lower case
-- ----------------------------------------------------------------------------
-- ----------------------------------------------------------------------------
-- ----------------------------------------------------------------------------
-- Insert an algorithm into the database.  Return 
CREATE OR REPLACE FUNCTION lm3.lm_insertAlgorithm(code varchar, aname varchar, modtime double precision)
   RETURNS int AS
$$
DECLARE
   id int = -1;
BEGIN
   SELECT algorithmid INTO id 
      FROM lm3.algorithm
      WHERE algorithmcode = code;
   IF NOT FOUND THEN
      INSERT INTO lm3.Algorithm (algorithmcode, name, datelastmodified)
         VALUES (code, aname, modtime);
      IF FOUND THEN
         SELECT INTO id last_value FROM lm3.algorithm_algorithmid_seq;
      END IF;
   END IF;
   RETURN id;
END;
$$  LANGUAGE 'plpgsql' VOLATILE;    


-- ----------------------------------------------------------------------------
-- Returns 0 or 1 records
CREATE OR REPLACE FUNCTION lm3.lm_getLatestModelTime(stat int)
   RETURNS double precision AS
$$
DECLARE
   timestmp double precision;
BEGIN
   SELECT statusModTime INTO timestmp
   FROM lm3.model
   WHERE status = stat
   ORDER BY statusModTime DESC limit 1;
   RETURN timestmp;
END;
$$  LANGUAGE 'plpgsql' STABLE;

-- ----------------------------------------------------------------------------
-- returns 0 or 1 records
CREATE OR REPLACE FUNCTION lm3.lm_getLatestProjectionTime(stat int)
   RETURNS double precision AS
$$
DECLARE
   timestmp double precision;
BEGIN
   SELECT statusModTime INTO timestmp
   FROM lm3.projection
   WHERE status = stat
   ORDER BY statusModTime DESC limit 1;
   RETURN timestmp;
END;
$$  LANGUAGE 'plpgsql' STABLE;

-- ----------------------------------------------------------------------------
-- Returns 1 record (integer)
CREATE OR REPLACE FUNCTION lm3.lm_countModeledSpecies(completeStat int, usr varchar)
   RETURNS int AS
$$
DECLARE
   total int;
BEGIN
   SELECT count(distinct(occurrenceSetId)) INTO total
   FROM lm3.model 
   WHERE status = completeStat and userid = usr;
   RETURN total;
END;
$$  LANGUAGE 'plpgsql' STABLE;
   
-- ----------------------------------------------------------------------------
-- Raises exception if > 1 records are found
-- Returns record of nulls if no records are found 
-- Get occurrenceSet information for W*S
CREATE OR REPLACE FUNCTION lm3.lm_getPointMaplayer(occsetid int)
   RETURNS lm3.occurrenceSet AS
$$
DECLARE
   rec lm3.occurrenceset%ROWTYPE;
BEGIN
   BEGIN
      SELECT o.* INTO STRICT rec FROM lm3.occurrenceset o 
         WHERE o.occurrenceSetId = occsetid;

      EXCEPTION
         WHEN NO_DATA_FOUND THEN
            RAISE NOTICE 'OccurrenceSet % not found', occsetid;
         WHEN TOO_MANY_ROWS THEN
            RAISE EXCEPTION 'OccurrenceSet % not unique', occsetid;
   END;
   RETURN rec;
END;
$$  LANGUAGE 'plpgsql' STABLE;


-- ----------------------------------------------------------------------------

-- ----------------------------------------------------------------------------
-- TODO? Stop storing REST request URL as metadataUrl - just construct dynamically
-- ----------------------------------------------------------------------------
-- ----------------------------------------------------------------------------
-- Inserts scenario, returns new scenarioid whether new or existing.
-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lm3.lm_insertScenario(code varchar, 
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
   poly varchar;
   scenmetadataUrl varchar;
BEGIN
   SELECT s.scenarioid INTO id
     FROM lm3.Scenario s
      WHERE s.scenariocode = code and s.userid = usr;
   IF NOT FOUND THEN
      BEGIN
         -- Default LM EPSG Code
         IF epsg = 4326 THEN 
            INSERT INTO lm3.Scenario 
             (scenarioCode, title, author, description, startDate, endDate, units, 
              resolution, bbox, dateLastModified, epsgcode, geom, userid)
            VALUES (code, ttl, authr, dsc, startdt, enddt, unts, 
                    res, bndsstring, modTime, epsg, ST_GeomFromText(bboxwkt, epsg), usr);
      
         -- Other EPSG Code
         ELSE 
            INSERT INTO lm3.Scenario 
             (scenarioCode, title, author, description, startDate, endDate, units, 
              resolution, bbox, dateLastModified, epsgcode, userid)
            VALUES (code, ttl, authr, dsc, startdt, enddt, unts, 
                    res, bndsstring, modTime, epsg, usr);
                 
         END IF;
      
         IF FOUND THEN
            SELECT INTO id last_value FROM lm3.scenario_scenarioid_seq;
            idstr = cast(id as varchar);
            scenmetadataUrl := replace(metadataUrlprefix, '#id#', idstr);
            UPDATE lm3.scenario SET metadataUrl = scenmetadataUrl WHERE scenarioId = id;         
         END IF;
      END;
   END IF;  -- end if not found
   RETURN id;
END;
$$  LANGUAGE 'plpgsql' VOLATILE;
-- ----------------------------------------------------------------------------
-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lm3.lm_getLayerType(usr varchar,
                                               ltype varchar)
   RETURNS lm3.lm_layerTypeAndKeywords AS
$$
DECLARE
   rec lm3.lm_layerTypeAndKeywords%rowtype;
   keystr varchar;
BEGIN
   BEGIN
      SELECT layerTypeId, code, title, userid, description, dateLastModified 
      INTO rec FROM lm3.LayerType WHERE code = ltype and userid = usr;
      
      EXCEPTION
         WHEN NO_DATA_FOUND THEN
            RAISE NOTICE 'LayerType % for % not found', usr, ltype;
         WHEN TOO_MANY_ROWS THEN
            RAISE EXCEPTION 'LayerType % for % not unique', usr, ltype;
   END;
   IF FOUND THEN
      SELECT INTO keystr lm3.lm_getLayerTypeKeywordString(rec.layertypeid);
      rec.keywords = keystr;
   END IF;
   RETURN rec;
END;
$$  LANGUAGE 'plpgsql' STABLE; 

-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lm3.lm_getLayerType(id int)
   RETURNS lm3.lm_layerTypeAndKeywords AS
$$
DECLARE
   rec lm3.lm_layerTypeAndKeywords%rowtype;
   keystr varchar;
BEGIN
   BEGIN
      SELECT layerTypeId, code, title, userid, description, dateLastModified 
      INTO rec FROM lm3.LayerType WHERE layertypeid = id;
      
      EXCEPTION
         WHEN NO_DATA_FOUND THEN
            RAISE NOTICE 'LayerType % for % not found', usr, ltype;
         WHEN TOO_MANY_ROWS THEN
            RAISE EXCEPTION 'LayerType % for % not unique', usr, ltype;
                 
   END;
   IF FOUND THEN
      SELECT INTO keystr lm3.lm_getLayerTypeKeywordString(rec.layertypeid);
      rec.keywords = keystr;
   END IF;
   RETURN rec;
END;
$$  LANGUAGE 'plpgsql' STABLE; 

-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lm3.lm_insertLayerType(usr varchar,
                                                  ltype varchar,
                                                  ltypetitle varchar,
                                                  ltypedesc varchar,
                                                  modtime double precision)
   RETURNS int AS
$$
DECLARE
   typeid int;
BEGIN
   INSERT INTO lm3.LayerType (code, title, userid, description, datelastmodified) 
      VALUES (ltype, ltypetitle, usr, ltypedesc, modtime);
   IF FOUND THEN
      SELECT INTO typeid last_value FROM lm3.layertype_layertypeid_seq;
   END IF;
   
   RETURN typeid;
END;
$$  LANGUAGE 'plpgsql' VOLATILE; 

-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lm3.lm_insertEnvLayer(lyrverify varchar,
                                             lyrurl varchar, 
                                             lyrtypeid int,
                                             lyrtitle varchar,
                                             lyrname varchar, 
                                             vmin double precision, 
                                             vmax double precision,
                                             vnodata double precision, 
                                             vunits varchar,
                                             iscat boolean,
                                             dloc varchar,
                                             mloc varchar,
                                             dformat varchar,
                                             gtype int,
                                             stdt double precision,
                                             enddt  double precision,
                                             munits varchar,
                                             res  double precision,
                                             epsg int,
                                             usr varchar,              
                                             descr text,                                    
                                             modtime double precision, 
                                             bboxstring varchar, 
                                             bboxwkt varchar)
   RETURNS int AS
$$
DECLARE
   success int = -1;
   typeid int;
   lyrid int;  
   idstr varchar; 
   lyrmetadataUrl varchar;
   tmpid int;
   retval int = -1;
BEGIN
   -- *** Special cases, legacy duplicate layernames ***
   IF usr = 'lm2' or usr = 'changeThinking' THEN
      -- get or insert layer 
      SELECT l.layerid INTO lyrid FROM lm3.Layer l
         WHERE l.metadataUrl = lyrurl;
   ELSE      
      -- get or insert layer 
      SELECT l.layerid INTO lyrid FROM lm3.Layer l 
         WHERE l.name = lyrname AND l.userid = usr;
   END IF;

   IF NOT FOUND THEN
      BEGIN      
         -- Default LM EPSG Code
         IF epsg = 4326 THEN 
            INSERT INTO lm3.Layer (verify, layerTypeId, metadataUrl, title, name, 
                               minVal, maxVal, nodataVal, valUnits, 
                               iscategorical, dlocation, metalocation, 
                               dataformat, gdalType, startDate, endDate, mapunits, 
                               resolution, epsgcode, userid, dateLastModified, 
                               description, bbox, geom)
            VALUES (lyrverify, lyrtypeid, lyrurl, lyrtitle, lyrname, 
                    vmin, vmax, vnodata, vunits, iscat, dloc, mloc, 
                    dformat, gtype, stdt, enddt, munits, 
                    res, epsg, usr, modtime, 
                    descr, bboxstring, ST_GeomFromText(bboxwkt, epsg));
         
         -- Other EPSG Codes skip geometry
         ELSE
            INSERT INTO lm3.Layer (verify, layerTypeId, metadataUrl, title, name, 
                               minVal, maxVal, nodataVal, valUnits, 
                               iscategorical, dlocation, metalocation, 
                               dataformat, gdalType, startDate, endDate, mapunits, 
                               resolution, epsgcode, userid, dateLastModified, 
                               description, bbox)
            VALUES (lyrverify, lyrtypeid, lyrurl, lyrtitle, lyrname, 
                    vmin, vmax, vnodata, vunits, iscat, dloc, mloc, 
                    dformat, gtype, stdt, enddt, munits, 
                    res, epsg, usr, modtime, descr, bboxstring);
         END IF;
         
         IF FOUND THEN
            SELECT INTO lyrid last_value FROM lm3.layer_layerid_seq;
            idstr = cast(lyrid as varchar);
            lyrmetadataUrl := replace(lyrurl, '#id#', idstr);
            UPDATE lm3.layer SET metadataUrl = lyrmetadataUrl WHERE layerid = lyrid;         
         END IF;
      END; -- end if layer not found
      
   -- if layer is found 
   ELSE
      success := 0;
      RAISE NOTICE 'Layer with matching user/name or URL found with id %', lyrid;
   END IF;
      
   RETURN lyrid;
END;
$$  LANGUAGE 'plpgsql' VOLATILE;

-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lm3.lm_countTypeCodes(usrid varchar, 
                                                 beforetime double precision, 
                                                 aftertime double precision)
   RETURNS int AS
$$
DECLARE
   num int;
   cmd varchar;
   wherecls varchar;
BEGIN
   cmd = 'SELECT count(*) FROM lm3.LayerType ';
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
CREATE OR REPLACE FUNCTION lm3.lm_listTypeCodes(firstRecNum int, maxNum int, 
                                                usrid varchar(20), 
                                                beforetime double precision,
                                                aftertime double precision)
   RETURNS SETOF lm3.lm_atom AS
$$
DECLARE
   rec lm3.lm_atom;
   ltTitle varchar;
   cmd varchar;
   wherecls varchar;
   limitcls varchar;
   ordercls varchar;
BEGIN
   cmd = 'SELECT layerTypeId, code, description, datelastmodified, title
               FROM lm3.LayerType ';
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
CREATE OR REPLACE FUNCTION lm3.lm_listTypeCodeObjects(firstRecNum int, maxNum int, 
                                                usrid varchar(20), 
                                                beforetime double precision,
                                                aftertime double precision)
   RETURNS SETOF lm3.lm_layerTypeAndKeywords AS
$$
DECLARE
   rec lm3.lm_layerTypeAndKeywords;
   keystr varchar;
   ltTitle varchar;
   cmd varchar;
   wherecls varchar;
   limitcls varchar;
   ordercls varchar;
BEGIN
   cmd = 'SELECT * FROM lm3.LayerType ';
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
         SELECT INTO keystr lm3.lm_getLayerTypeKeywordString(rec.layertypeid);
         rec.keywords = keystr;
         RETURN NEXT rec;
      END LOOP;
   RETURN;
END;
$$  LANGUAGE 'plpgsql' STABLE;

-- ----------------------------------------------------------------------------
-- Insert entry into ScenarioLayer table to associate a layer with a scenario.
-- If the entry already exists, print a message and return success.  
-- Return 0 on success, -1 on failure. 
-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lm3.lm_joinScenarioLayer(scenid int, lyrid int)
   RETURNS int AS
$$
DECLARE
   success int = -1;
   tmpcount1 int;
   tmpcount2 int;
   tmpcount3 int;
BEGIN
   -- if layer is found
   SELECT count(*) into tmpcount1 FROM lm3.scenario WHERE scenarioid = scenid;
   IF tmpcount1 != 1 THEN
      RAISE EXCEPTION 'Scenario with id % does not exist or is not unique', scenid;
   END IF;
   
   SELECT count(*) into tmpcount2 FROM lm3.layer WHERE layerid = lyrid;
   IF tmpcount2 != 1 THEN
      RAISE EXCEPTION 'Layer with id % does not exist or is not unique', lyrid;
   END IF;
   
   SELECT count(*) INTO tmpcount3 FROM lm3.ScenarioLayers
      WHERE scenarioId = scenid AND layerId = lyrid;
   IF tmpcount3 = 0 THEN      
      BEGIN
         -- get or insert scenario x layer entry
         INSERT INTO lm3.ScenarioLayers (scenarioId, layerId) 
                     VALUES (scenid, lyrid);
         IF FOUND THEN
            success := 0;
         END IF;
      END;
   ELSE
      RAISE NOTICE 'Scenario % and Layer % are already joined', scenid, lyrid;
      success := 0;
   END IF;
   
   RETURN success;
END;
$$  LANGUAGE 'plpgsql' VOLATILE;

-- ----------------------------------------------------------------------------
--  select s.scenarioid, s.scenariocode, k.keywordid, k.keyword from scenariokeywords sk, keyword k, scenario s where s.scenarioid = 169 and sk.keywordid = k.keywordid and s.scenarioid = sk.scenarioid and sk.keywordid = k.keywordid;;
CREATE OR REPLACE FUNCTION lm3.lm_insertScenarioKeyword(scenid int,
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
   SELECT k.keywordid INTO wdid FROM lm3.Keyword k WHERE k.keyword = kywd;
   IF NOT FOUND THEN
      INSERT INTO lm3.Keyword (keyword) VALUES (kywd);
      IF FOUND THEN
         SELECT INTO wdid last_value FROM lm3.keyword_keywordid_seq;
      END IF;
   END IF;
   
   IF FOUND THEN
      BEGIN
         SELECT sk.scenarioId INTO tmpid
            FROM lm3.ScenarioKeywords sk
            WHERE sk.scenarioId = scenid
              AND sk.keywordId = wdid;
         IF NOT FOUND THEN
            INSERT INTO lm3.ScenarioKeywords (scenarioId, keywordId) VALUES (scenid, wdid);
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
-- Must have either layerId or layerTypeId
CREATE OR REPLACE FUNCTION lm3.lm_insertLayerTypeKeyword(lyrid int, opttypid int, kywd varchar)
   RETURNS int AS
$$
DECLARE
   retval int := -1;
   typid int;
   wdid int;
   total int;
BEGIN
   -- insert keyword if it is not there 
   SELECT k.keywordid INTO wdid FROM lm3.Keyword k WHERE k.keyword = kywd;
   IF NOT FOUND THEN
      BEGIN
         INSERT INTO lm3.Keyword (keyword) VALUES (kywd);
         IF FOUND THEN
            SELECT INTO wdid last_value FROM lm3.keyword_keywordid_seq;
         END IF;
      END;
   END IF;
   
   IF opttypid is null THEN
      SELECT layertypeid INTO typid FROM lm3.layer WHERE layerid = lyrid;
   ELSE
      typid := opttypid; 
   END IF;
   
   IF FOUND THEN
      BEGIN
         SELECT count(*) INTO total FROM lm3.LayerTypeKeyword 
            WHERE layerTypeId = typid AND keywordId = wdid;
         IF total = 0 THEN
            BEGIN
               INSERT INTO lm3.LayerTypeKeyword (layerTypeId, keywordId) 
                  VALUES (typid, wdid);
               IF FOUND THEN 
                  retval := typid;
               END IF;
            END;
         ELSE
            retval := typid;
         END IF;
      END;
   END IF;
   
   RETURN retval;
END;
$$  LANGUAGE 'plpgsql' VOLATILE;

-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lm3.lm_deleteKeyword(kwdid int)
RETURNS int AS
$$
DECLARE
   lkcount int;
   skcount int;
   success int := -1;
BEGIN
   SELECT count(*) INTO lkcount FROM lm3.LayerTypeKeyword 
      WHERE keywordid = kwdid;
   SELECT count(*) INTO skcount FROM lm3.ScenarioKeywords
      WHERE keywordid = kwdid;
      
   IF lkcount = 0 AND skcount = 0 THEN
      begin
         RAISE NOTICE 'Deleting Keyword %, used in % layers and % scenarios', 
                       kwdid, lkcount, skcount;
      
         DELETE FROM lm3.keyword WHERE keywordid = kwdid;
         IF FOUND THEN
            BEGIN 
               success := 0;
               RAISE NOTICE 'Deleted keywordid %', kwdid;
            END;
         END IF ;
      end;
   ELSE 
      RAISE NOTICE 'Not deleting Keyword %, used in % layers and % scenarios', 
                    kwdid, lkcount, skcount;
   END IF;
   RETURN success;
END;
$$  LANGUAGE 'plpgsql' VOLATILE;

-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lm3.lm_deleteLayerType(typeid int)
RETURNS int AS
$$
DECLARE
   kwdid int;
   typecount int;
   lkcount int;
   skcount int;
   success int := -1;
   subsuccess int := 0;
BEGIN
   -- Is this LayerType used anywhere else?
   SELECT count(*) INTO typecount FROM lm3.layer WHERE layertypeid = typeid;
         
   -- It is used, do not delete
   IF typecount > 0 THEN
      success := 0;
      subsuccess := 0;
      RAISE NOTICE 'Unable to delete LayerType % connected to % Layers', typeid, typecount;
      
   -- Ok, delete orphan
   ELSE
      begin
      -- Delete LayerType-Keyword joins for this orphaned LayerType
         DELETE FROM lm3.LayerTypeKeyword WHERE layerTypeId = typeid;
               
         -- Check keywords associated with this LayerType
         FOR kwdid in 
            SELECT k.keywordid FROM lm3.keyword k, lm3.LayerTypeKeyword ltk 
              WHERE ltk.layerTypeId = typeid AND ltk.keywordid = k.keywordid
         LOOP
            SELECT INTO subsuccess lm3.lm_deleteKeyword(kwdid) ;
         END LOOP;
               
         -- Delete the LayerType
         DELETE FROM lm3.layertype WHERE layertypeid = typeid;
         IF FOUND THEN 
            begin
               success := 0;
               RAISE NOTICE 'Deleted layerTypeId %', typeid;
            end;
         ELSE
            RAISE NOTICE 'Unable to delete layerTypeId %', typeid;
         END IF;
      end;
   END IF; 
  
   RETURN success;
END;
$$  LANGUAGE 'plpgsql' VOLATILE;

-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lm3.lm_deleteLayerTypeByCode(usr varchar,
                                                        ltype varchar)
   RETURNS int AS
$$
DECLARE
   typeid int;
   success int := -1;
BEGIN
   -- get or insert layertype
   SELECT lt.layertypeid INTO typeid FROM lm3.LayerType lt 
     WHERE lt.code = ltype and lt.userid = usr;
     
   IF FOUND THEN
      SELECT INTO success lm3.lm_deleteLayerType;
   END IF;
   
   RETURN success;
END;
$$  LANGUAGE 'plpgsql' VOLATILE;

-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lm3.lm_deleteEnvLayer(lyrid int)
RETURNS int AS
$$
DECLARE
   scencount int;
   typeid int;
   lyrcount int;
   success int := -1;
BEGIN
   -- Are any Scenarios using this layer? 
   SELECT count(*) INTO scencount FROM lm3.scenariolayers WHERE layerid = lyrid;
   
   -- Yes!
   IF lyrcount > 0 THEN
      RAISE NOTICE 'Unable to delete Layer % connected to % Scenarios', lyrid, scencount;
      
   -- No, Orphaned layer
   ELSE
      begin
         -- What LayerType?
         SELECT layertypeid INTO typeid FROM lm3.layer l WHERE layerid = lyrid;
         SELECT INTO success lm_deleteLayerType(typeid) ;

         -- Finally, delete the layer
         DELETE FROM lm3.layer WHERE layerid = lyrid;
         IF FOUND THEN 
            success := 0;
         END IF;
      end;
   END IF;   
   RETURN success;
END;
$$  LANGUAGE 'plpgsql' VOLATILE;

-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lm3.lm_deleteScenario(scenid int)
RETURNS int AS
$$
DECLARE
   mcount int;
   pcount int;
   lyrid int;
   kwdid int;
   lsuccess int := -1;
   ksuccess int := -1;
   success int := -1;
BEGIN
   -- Are any SDMExperiments using this layer? 
   SELECT count(*) INTO mcount FROM lm3.model WHERE scenarioid = scenid;
   SELECT count(*) INTO pcount FROM lm3.projection WHERE scenarioid = scenid;
   
   IF mcount = 0 AND pcount = 0 THEN
      BEGIN

         -- Layers 
         FOR lyrid in 
            SELECT l.layerid
               from lm3.layer l, lm3.scenariolayers sl 
               where sl.scenarioid = scenid and sl.layerid = l.layerid
         LOOP
            -- Detach layer from scenario
            DELETE FROM lm3.scenariolayers WHERE scenarioid = scenid AND layerid = lyrid;
            -- Delete (if orphaned) layer
              SELECT INTO lsuccess lm_deleteEnvLayer(lyrid) ;
         END LOOP;
   
         -- Keywords
         FOR kwdid in 
            SELECT k.keywordid FROM lm3.keyword k, lm3.scenariokeywords sk 
               WHERE sk.scenarioid = scenid AND sk.keywordid = k.keywordid
         LOOP
            -- Detach keyword from scenario
            DELETE FROM lm3.scenariokeywords WHERE scenarioid = scenid AND keywordid = kwdid;
            -- Delete (if orphaned) keyword
            SELECT INTO ksuccess lm3.lm_deleteKeyword(kwdid) ;
         END LOOP;

         DELETE FROM lm3.scenario where scenarioid = scenid;
         IF FOUND THEN 
            success := 0;
         END IF;
      END;
   ELSE
      RAISE NOTICE 'Unable to delete Scenario %, used in % models and % projections',
                    scenid, mcount, pcount;
   END IF;
   RETURN success;
END;
$$  LANGUAGE 'plpgsql' VOLATILE;

-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lm3.lm_findProblemModels(oldTime double Precision, 
                                                    startStat int,
                                                    endStat int,
                                                    notUser varchar,
                                                    total int)
   RETURNS SETOF lm3.lm_fullmodel AS
$$
DECLARE
   rec lm3.lm_fullmodel%ROWTYPE;
   cmd varchar;
   wherecls varchar;
   limitcls varchar;
   ordercls varchar;
BEGIN
   cmd = 'SELECT * FROM lm3.lm_fullmodel ';
   wherecls = ' WHERE mdlstatusmodtime <  ' || quote_literal(oldTime) ;
   ordercls = ' ORDER BY mdlstatusmodtime ASC ';
   limitcls = ' ';

   -- filter by lower status
   IF startStat is not null THEN
      wherecls = wherecls || ' AND mdlstatus >  ' || quote_literal(startStat);
   END IF;

   -- filter by upper status
   IF endStat is not null THEN
      wherecls = wherecls || ' AND mdlstatus <  ' || quote_literal(endStat);
   END IF;
   
   -- exclude user (default, lm2)
   IF notUser is not null THEN
      wherecls = wherecls || ' AND mdluserid !=  ' || quote_literal(notUser);
   END IF;
   
   -- limit total?
   IF total is not null THEN
      limitcls = ' LIMIT ' || quote_literal(total);
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
CREATE OR REPLACE FUNCTION lm3.lm_findProblemProjections(oldTime double Precision, 
                                                         startStat int,
                                                         endStat int,
                                                         notUser varchar,
                                                         total int)
   RETURNS SETOF lm3.lm_fullprojection AS
$$
DECLARE
   rec lm3.lm_fullprojection%ROWTYPE;
   cmd varchar;
   wherecls varchar;
   limitcls varchar;
   ordercls varchar;
BEGIN
   cmd = 'SELECT * FROM lm3.lm_fullprojection ';
   wherecls = ' WHERE prjstatusmodtime <  ' || quote_literal(oldTime) ;
   ordercls = ' ORDER BY prjstatusmodtime ASC ';
   limitcls = ' ';

   -- filter by lower status
   IF startStat is not null THEN
      wherecls = wherecls || ' AND prjstatus >  ' || quote_literal(startStat);
   END IF;

   -- filter by upper status
   IF endStat is not null THEN
      wherecls = wherecls || ' AND prjstatus <  ' || quote_literal(endStat);
   END IF;
   
   -- exclude user (default, lm2)
   IF notUser is not null THEN
      wherecls = wherecls || ' AND mdluserid !=  ' || quote_literal(notUser);
   END IF;

   -- limit total?
   IF total is not null THEN
      limitcls = ' LIMIT ' || quote_literal(total);
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
CREATE OR REPLACE FUNCTION lm3.lm_getModel(id int)
   RETURNS lm3.lm_fullmodel AS
$$
DECLARE
   rec_mdl lm3.lm_fullmodel;
BEGIN
   BEGIN
      SELECT *
      INTO STRICT rec_mdl 
      FROM lm3.lm_fullmodel
      WHERE modelid = id;
      
      EXCEPTION
         WHEN NO_DATA_FOUND THEN
            RAISE NOTICE 'Model id % not found', id;
         WHEN TOO_MANY_ROWS THEN
            RAISE EXCEPTION 'Model id % not unique', id;
   END;
   RETURN rec_mdl;
END;
$$ LANGUAGE 'plpgsql' STABLE; 

-- ----------------------------------------------------------------------------
-- Models in the LM Archive (userid = lm2), will have only a single model per 
-- combination of occurrenceset, modelScenario, and Algorithm (ignoring parameters)
-- User models are not constrained
CREATE OR REPLACE FUNCTION lm3.lm_findModels(usrid varchar, 
                                             occid int, 
                                             scenid int,
                                             algcode varchar)
   RETURNS SETOF lm3.lm_fullmodel AS
$$
DECLARE
   rec lm3.lm_fullmodel;
BEGIN
   FOR rec IN
      SELECT * FROM lm3.lm_fullmodel WHERE occurrencesetid = occid
                                       AND mdlscenarioid = scenid
                                       AND algorithmcode = algcode
                                       AND mdlUserId = usrid
   LOOP
      RETURN NEXT rec;
   END LOOP;
   RETURN;
END;
$$ LANGUAGE 'plpgsql' STABLE; 

-- ----------------------------------------------------------------------------
-- Raises exception if 0 or > 1 records are found
CREATE OR REPLACE FUNCTION lm3.lm_getModelForProjection(projid int)
   RETURNS lm3.lm_fullmodel AS
$$
DECLARE
   rec_mdl lm3.lm_fullmodel;
BEGIN
   BEGIN
      SELECT *
      INTO STRICT rec_mdl 
      FROM lm3.lm_fullmodel m, lm3.projection p
      WHERE m.modelid = p.modelid
        AND p.projectionid = projid;
      
      EXCEPTION
         WHEN NO_DATA_FOUND THEN
            RAISE NOTICE 'Model not found for projid %', projid;
         WHEN TOO_MANY_ROWS THEN
            RAISE EXCEPTION 'Model not unique for projid %', projid;
   END;
   RETURN rec_mdl;
END;
$$ LANGUAGE 'plpgsql' STABLE; 

-- ----------------------------------------------------------------------------
-- lm_getModelsNeedingJobs
CREATE OR REPLACE FUNCTION lm3.lm_getModelsNeedingJobs(count int, 
                                                       usr varchar, 
                                                       readyStat int)
   RETURNS SETOF lm3.lm_fullmodel AS
$$
DECLARE
   rec lm3.lm_fullmodel;
BEGIN
   FOR rec in 
      SELECT fm.* FROM lm3.lm_fullmodel fm
         WHERE fm.mdlstatus = readyStat
           AND fm.mdluserid = usr
           AND (SELECT count(*) FROM lm3.lmjob j 
                   WHERE referencetype = 101 
                     AND j.referenceid = fm.modelid) = 0
         LIMIT count
   LOOP
      RETURN NEXT rec;
   END LOOP;
   RETURN;
END;
$$  LANGUAGE 'plpgsql' STABLE;

-- ----------------------------------------------------------------------------
-- lm_getProjectionsNeedingJobs
CREATE OR REPLACE FUNCTION lm3.lm_getProjectionsNeedingJobs(count int, 
                                                            usr varchar,
                                                            readyStat int,
                                                            completeStat int)
   RETURNS SETOF lm3.lm_fullprojection AS
$$
DECLARE
   rec lm3.lm_fullprojection;
BEGIN
   FOR rec in 
      SELECT fp.* FROM lm3.lm_fullprojection fp
         WHERE fp.mdlstatus = completeStat
           AND fp.prjstatus = readyStat
           AND fp.mdluserid = usr
           AND (SELECT count(*) FROM lm3.lmjob j 
                   WHERE referencetype = 102 
                     AND j.referenceid = fp.projectionid) = 0
         LIMIT count
   LOOP
      RETURN NEXT rec;
   END LOOP;
   RETURN;
END;
$$  LANGUAGE 'plpgsql' STABLE;

-- ----------------------------------------------------------------------------
-- lm_getModelsByOccurrenceSet
CREATE OR REPLACE FUNCTION lm3.lm_getModelsByOccurrenceSet(occId int, usr varchar, completeStat int)
   RETURNS SETOF lm3.lm_fullmodel AS
$$
DECLARE
   rec lm3.lm_fullmodel;
BEGIN
   IF usr is null then 
      FOR rec in 
         SELECT *
         FROM lm3.lm_fullmodel
         WHERE occurrenceSetId = occId
           AND mdlstatus = completeStat
         LOOP
            RETURN NEXT rec;
         END LOOP;
   ELSE
      FOR rec in 
         SELECT *
         FROM lm3.lm_fullmodel
         WHERE occurrenceSetId = occId
           AND mdluserid = usr
           AND mdlstatus = completeStat
         LOOP
            RETURN NEXT rec;
         END LOOP;
   END IF;
   RETURN;
END;
$$  LANGUAGE 'plpgsql' STABLE;

-- ----------------------------------------------------------------------------
-- lm_getModelsByOccurrenceSetUserAndStatus
CREATE OR REPLACE FUNCTION lm3.lm_getModelsByOccurrenceSetUserAndStatus(occId int, 
                                                                    usr varchar, 
                                                                    stat int)
   RETURNS SETOF lm3.lm_fullmodel AS
$$
DECLARE
   rec lm3.lm_fullmodel;
   cmd varchar;
   wherecls varchar;
BEGIN
   cmd = 'SELECT * FROM lm3.lm_fullmodel ';
   wherecls = ' WHERE occurrenceSetId = ' || occId;

   IF usr is not null THEN
      wherecls = wherecls || ' AND (mdlUserId = ' || quote_literal(usr) || ')';
   END IF;
   IF stat is not null THEN
      wherecls = wherecls || ' AND (mdlstatus = ' || stat || ')';
   END IF;

   cmd = cmd || wherecls;
   FOR rec in EXECUTE cmd
      LOOP
         RETURN NEXT rec;
      END LOOP;
   RETURN;
END;
$$  LANGUAGE 'plpgsql' STABLE;

-- ----------------------------------------------------------------------------
-- lm_getModelsByUserAndStatus
CREATE OR REPLACE FUNCTION lm3.lm_getModelsByUserAndStatus(usr varchar, stat int)
   RETURNS SETOF lm3.lm_fullmodel AS
$$
DECLARE
   rec lm3.lm_fullmodel;
   cmd varchar;
   wherecls varchar;
BEGIN
   cmd = 'SELECT * FROM lm3.lm_fullmodel ';
   wherecls = ' WHERE mdlUserId = ' || quote_literal(usr) ;

   IF stat is not null THEN
      wherecls = wherecls || ' AND mdlstatus = ' || stat;
   END IF;

   cmd = cmd || wherecls;
   RAISE NOTICE 'cmd = %', cmd;
   FOR rec in EXECUTE cmd
      LOOP
         RETURN NEXT rec;
      END LOOP;
   RETURN;
END;
$$  LANGUAGE 'plpgsql' STABLE;

-- ----------------------------------------------------------------------------
-- lm3.lm_getModelsByStatus
CREATE OR REPLACE FUNCTION lm3.lm_getModelsByStatus(total int, stat int)
   RETURNS SETOF lm3.lm_fullmodel AS
$$
DECLARE
   rec lm3.lm_fullmodel;
BEGIN
   FOR rec in 
      SELECT *
      FROM lm3.lm_fullmodel
      WHERE mdlstatus = stat
        LIMIT total
      
      LOOP
         RETURN NEXT rec;
      END LOOP;
   RETURN;
END;
$$  LANGUAGE 'plpgsql' STABLE;

-- ----------------------------------------------------------------------------
-- lm_getExpiredModels
-- Gets models calculated before the last GBIF UPDATE lm3.that have an  
-- occurrenceset updated after that date.
CREATE OR REPLACE FUNCTION lm3.lm_getExpiredModels(total int, lastdate double precision)
   RETURNS SETOF lm3.lm_fullmodel AS
$$
DECLARE
   rec lm3.lm_fullmodel;
BEGIN
   FOR rec in 
      SELECT *
      FROM lm3.lm_fullmodel
      WHERE dateLastModified > lastdate
        AND mdlstatusModTime <= lastdate
      ORDER BY mdlstatus asc LIMIT total
      
      LOOP
         RETURN NEXT rec;
      END LOOP;
   RETURN;
END;
$$  LANGUAGE 'plpgsql' STABLE;

-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lm3.lm_getModelsNeverProjected(total int, completeStat int)
   RETURNS SETOF lm3.lm_fullmodel AS
$$
DECLARE
   rec lm3.lm_fullmodel;
BEGIN
   FOR rec in 
      SELECT pxm.modelId, pxm.mdlUserId, pxm.occurrenceSetId, 
             pxm.mdlscenarioCode, pxm.mdlscenarioId, pxm.mdlMaskId, pxm.algorithmCode, 
             pxm.algorithmParams,
             pxm.mdlcreateTime, pxm.mdlstatus, pxm.mdlstatusModTime, pxm.mdlpriority, 
             pxm.mdldlocation, pxm.qc, pxm.mdljobId,
             pxm.occUserId, pxm.fromGbif, pxm.displayName,  pxm.occmetadataUrl, pxm.query, 
             pxm.queryCount, pxm.dateLastModified, pxm.epsgcode, pxm.occbbox,
             pxm.algorithmid, pxm.algorithmcode, pxm.paramsettype, 
             pxm.trainingProportion, pxm.totalRuns, pxm.hardOmissionThreshold, 
             pxm.modelsUnderOmissionThreshold, pxm.commissionThreshold, 
             pxm.commissionSampleSize, pxm.maxThreads, pxm.maxGenerations, 
             pxm.convergenceLimit, pxm.populationSize, pxm.resamples, 
             pxm.standardDeviationCutoff, pxm.randomisations, 
             pxm.standardDeviations, pxm.minComponents, pxm.verboseDebugging, 
             pxm.distanceType, pxm.nearPointsToGetMean, 
             pxm.distance, pxm.numberOfPseudoAbsences, 
             pxm.numberOfIterations, pxm.trainingMethod, 
             pxm.gaussianPriorSmoothingCoeficient, pxm.terminateTolerance,
             pxm.linearfeature, pxm.quadraticfeature, pxm.productfeature,
             pxm.thresholdfeature, pxm.hingefeature, pxm.svmtype, pxm.kerneltype, 
             pxm.degree, pxm.gamma, pxm.coef0, pxm.cost, pxm.nu, 
             pxm.probabilisticoutput, pxm.hiddenLayerNeurons, pxm.learningRate, 
             pxm.momentum, pxm.choice, pxm.epoch, pxm.minimumError, 
             pxm.UseSurfaceLayers, pxm.UseDepthRange, pxm.UseIceConcentration, 
             pxm.UseDistanceToLand, pxm.UsePrimaryProduction, pxm.UseSalinity, 
             pxm.UseTemperature
                  FROM (SELECT p.modelid AS projmodelid, m.* FROM lm3.lm_fullmodel m
                        LEFT JOIN lm3.projection p ON p.modelid = m.modelid ) 
                     AS pxm
                  WHERE pxm.projmodelid IS NULL
                     AND pxm.mdlstatus = completeStat
                  ORDER BY pxm.mdlpriority DESC 
                  LIMIT total
   LOOP
      RETURN NEXT rec;
   END LOOP;
   RETURN;
END;
$$  LANGUAGE 'plpgsql' STABLE;    

-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lm3.lm_getNextProjectedModels(total int, obsoleteStat int)
   RETURNS SETOF lm3.lm_fullmodel AS
$$
DECLARE
   rec lm3.lm_fullmodel;
BEGIN
   FOR rec in 
      SELECT *
      FROM lm3.lm_fullmodel
      WHERE mdlstatus = obsoleteStat
      ORDER BY mdlpriority DESC 
      LIMIT total
   LOOP
      RETURN NEXT rec;
   END LOOP;
   RETURN;
END;
$$  LANGUAGE 'plpgsql' STABLE;   
             
-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lm3.lm_findUnfinishedJoblessOccurrenceSets(
                                       usr varchar, total int, completeStat int)
   RETURNS SETOF lm3.occurrenceset AS
$$
DECLARE
   rec_occ lm3.occurrenceset%ROWTYPE;
BEGIN
   FOR rec_occ in 
      SELECT o.*
        FROM lm3.occurrenceset o
        LEFT JOIN lm3.lm_occjob j ON o.occurrenceSetId = j.occurrenceSetId 
      WHERE j.occurrenceSetId IS NULL
         AND o.userid = usr
         AND o.status < completeStat
         AND o.status > 0
      LIMIT total
   LOOP
      RETURN NEXT rec_occ;
   END LOOP;
   RETURN;
END;
$$  LANGUAGE 'plpgsql' STABLE; 

-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lm3.lm_getOccurrenceSetsMissingModels(usr varchar,
                                                                 primenv int, 
                                                                 total int,
                                                                 mincount int,
                                                                 algcode varchar, 
                                                                 scenid int, 
                                                                 errstat int)
   RETURNS SETOF lm3.occurrenceset AS
$$
DECLARE
   rec_occ lm3.occurrenceset%ROWTYPE;
BEGIN
   FOR rec_occ in 
      SELECT o.*
        FROM lm3.occurrenceset o
        LEFT JOIN lm3.model m ON o.occurrenceSetId = m.occurrenceSetId 
                         AND m.algorithmCode = algcode 
                         AND m.scenarioId = scenid
      WHERE m.occurrenceSetId IS NULL
         AND o.userid = usr
         AND o.status < errstat
         AND o.primaryenv = primenv
         AND o.querycount >= mincount
      LIMIT total
   LOOP
      RETURN NEXT rec_occ;
   END LOOP;
   RETURN;
END;
$$  LANGUAGE 'plpgsql' STABLE; 

-- ----------------------------------------------------------------------------
-- ----------------------------------------------------------------------------
-- ----------------------------------------------------------------------------
-- Updates occurrenceSet with primary environment (terrestrial or marine
-- *NEW*
CREATE OR REPLACE FUNCTION lm3.lm_setOccurrenceSetPrimaryEnv(occid int,
                                                             habitat int)
   RETURNS int AS
$$
DECLARE
   success int = -1;
BEGIN
   UPDATE lm3.OccurrenceSet SET primaryEnv = habitat
      WHERE occurrenceSetId = occid;

   IF FOUND THEN
      success = 0;
   END IF;

   RETURN success;
END;
$$  LANGUAGE 'plpgsql' VOLATILE;

-- ----------------------------------------------------------------------------
-- Returns models never projected for given projection scenarios.  This will be 
-- most useful when additional scenarios are added after initial experiment 
-- creation.
CREATE OR REPLACE FUNCTION lm3.lm_getArchiveModelsNotProjectedOntoScen(usr varchar,
                                                            total int, 
                                                            algcode varchar, 
                                                            mdlscenid int,
                                                            prjscenid int)
   RETURNS SETOF lm3.lm_fullmodel AS
$$
DECLARE
   rec lm3.lm_fullmodel;
BEGIN
   FOR rec in
      SELECT m.*
      FROM lm3.lm_fullmodel m
      LEFT OUTER JOIN lm3.projection p 
        ON m.modelid = p.modelid AND p.scenarioId = prjscenid
      WHERE p.projectionId IS NULL
        AND m.mdlUserId = usr
        AND m.algorithmCode = algcode 
        AND m.mdlscenarioId = mdlscenid
        
      LIMIT total
   LOOP
      RETURN NEXT rec;
   END LOOP;
   RETURN;
END;
$$  LANGUAGE 'plpgsql' STABLE;    

-- ----------------------------------------------------------------------------
-- lm_getRandomModel
CREATE OR REPLACE FUNCTION lm3.lm_getRandomModel(usr varchar, completestat int)
   RETURNS lm3.lm_fullmodel AS
$$
DECLARE
   rec lm3.lm_fullmodel;
   lastid int;
   rndm int;
BEGIN
   SELECT last_value INTO lastid FROM lm3.model_modelid_seq;
   rndm := (RANDOM() * lastid)::int OFFSET 0;
   RAISE NOTICE 'lastid %; rndm %', lastid, rndm;
   
   SELECT * FROM lm3.lm_fullmodel INTO rec
      WHERE modelid >= rndm 
        AND mdlUserId = usr
        AND mdlstatus = completestat
      ORDER BY modelId LIMIT 1;
      
   IF NOT FOUND THEN
      SELECT * FROM lm3.lm_fullmodel INTO rec
         WHERE modelid < rndm 
           AND mdlUserId = usr
           AND mdlstatus = completestat
         ORDER BY modelId LIMIT 1;
   END IF;
   
   RETURN rec;
END;
$$  LANGUAGE 'plpgsql' STABLE;

-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lm3.lm_getModelsToRollback(total int, 
                                                      usr varchar, 
                                                      endstat int)
   RETURNS SETOF lm3.lm_fullmodel AS
$$
DECLARE
   rec_mdl lm3.lm_fullmodel%ROWTYPE;
BEGIN
   FOR rec_mdl in 
      SELECT * FROM lm3.lm_fullmodel
      WHERE mdluserid = usr 
        AND mdlstatus = endstat
        -- occurrenceset is newer than model
        AND mdlstatusmodtime < datelastmodified
        ORDER BY datelastmodified asc
      LIMIT total
   LOOP
      RETURN NEXT rec_mdl;
   END LOOP;
   RETURN;
END;
$$  LANGUAGE 'plpgsql' STABLE;

-- ----------------------------------------------------------------------------
-- TODO: Delete? Currently NOT USED 
CREATE OR REPLACE FUNCTION lm3.lm_getOccurrenceSetsToUpdate(total int, 
                                                            usr varchar,
                                                            expdate double precision)
   RETURNS SETOF lm3.occurrenceset AS
$$
DECLARE
   rec_occ lm3.occurrenceset%ROWTYPE;
BEGIN
   FOR rec_occ in 
      SELECT o.*
        FROM lm3.occurrenceset o
      WHERE o.userid = usr
        AND (o.querycount = -1 OR o.datelastchecked < expdate)
        ORDER BY o.datelastchecked asc
      LIMIT total
   LOOP
      RETURN NEXT rec_occ;
   END LOOP;
   RETURN;
END;
$$  LANGUAGE 'plpgsql' STABLE;

-- ----------------------------------------------------------------------------
-- Updates occurrenceSet with dateLastChecked
CREATE OR REPLACE FUNCTION lm3.lm_touchOccurrenceSet(occid int,
                                                 checktime double precision)
   RETURNS int AS
$$
DECLARE
   success int = -1;
BEGIN
   UPDATE lm3.OccurrenceSet SET dateLastChecked = checktime
      WHERE occurrenceSetId = occid;

   IF FOUND THEN
      success = 0;
   END IF;

   RETURN success;
END;
$$  LANGUAGE 'plpgsql' VOLATILE;

-- ----------------------------------------------------------------------------
-- Updates occurrenceSet with metadataUrl, filename(query), point count, updatetime, 
-- touchtime and geometry
CREATE OR REPLACE FUNCTION lm3.lm_updateOccurrenceSet(occid int,
                                                occmetadataUrl varchar,
                                                dloc varchar,
                                                rdloc varchar, 
                                                total int, 
                                                epsg int,
                                                bounds varchar,
                                                polywkt text, 
                                                pointswkt text,
                                                stat int,
                                                stattime double precision,
                                                tchtime double precision,
                                                prmtime double precision)
   RETURNS int AS
$$
DECLARE
   success int = -1;
BEGIN
   IF ST_IsValid(ST_GeomFromText(polywkt, epsg)) THEN
      UPDATE lm3.OccurrenceSet SET geom = ST_GeomFromText(polywkt, epsg) WHERE occurrenceSetId = occid;
   ELSE
      UPDATE lm3.OccurrenceSet SET geom = NULL WHERE occurrenceSetId = occid;
   END IF;

   IF ST_IsValid(ST_GeomFromText(pointswkt, epsg)) THEN
      UPDATE lm3.OccurrenceSet SET geompts = ST_GeomFromText(pointswkt, epsg) WHERE occurrenceSetId = occid;
   ELSE
      UPDATE lm3.OccurrenceSet SET geompts = NULL WHERE occurrenceSetId = occid;
   END IF;

   UPDATE lm3.OccurrenceSet SET 
       (metadataurl, dlocation, rawdlocation, queryCount, epsgcode, bbox, 
        status, statusmodtime, dateLastChecked, dateLastModified) 
       = (occmetadataUrl, dloc, rdloc, total, epsg, bounds, 
         stat, stattime, tchtime, prmtime)
   WHERE occurrenceSetId = occid;        
          
   IF FOUND THEN
      success = 0;
   END IF;
   RETURN success;
END;
$$  LANGUAGE 'plpgsql' VOLATILE;

-- ----------------------------------------------------------------------------
-- Updates occurrenceSet with details and timestamps on 
-- change in dateChecked, status, data (queryCount, rawdlocation), 
CREATE OR REPLACE FUNCTION lm3.lm_updateOccurrenceStatus(occid int,
                                                         dloc varchar,
                                                         qrycount int,
                                                         rdloc varchar,
                                                         stat int,
                                                         stattime double precision,
                                                         checktime double precision,
                                                         modtime double precision)
   RETURNS int AS
$$
DECLARE
   success int = -1;
BEGIN
   -- changed   
   IF stat IS NOT NULL THEN
      -- status changed, update everything, 
      -- if complete, query, queryCount will be updated, rawdlocation cleared 
      UPDATE lm3.OccurrenceSet SET (dlocation, queryCount, rawdlocation, status, statusModTime, dateLastModified)  
                                 = (dloc, qrycount, rdloc, stat, stattime, modtime)
      WHERE occurrenceSetId = occid;
   ELSE
      -- only checked
      UPDATE lm3.OccurrenceSet SET dateLastChecked = checktime
      WHERE occurrenceSetId = occid;       
   END IF;
     
   IF FOUND THEN
      success = 0;
   END IF;
   RETURN success;
END;
$$  LANGUAGE 'plpgsql' VOLATILE;

-- ----------------------------------------------------------------------------
-- Returns species with obsolete models
CREATE OR REPLACE FUNCTION lm3.lm_getOccurrenceSetsForObsoleteModels(total int, obsoleteStat int)
   RETURNS SETOF lm3.occurrenceset AS
$$
DECLARE
   rec_occ lm3.occurrenceset%ROWTYPE;
BEGIN
   FOR rec_occ in 
      SELECT o.* 
         FROM lm3.occurrenceset o, lm3.model m 
      WHERE o.occurrenceSetId = m.occurrenceSetId
        -- Model JobStatus.OBSOLETE
        AND m.status = obsoleteStat
      ORDER BY m.priority DESC 
      LIMIT total
   LOOP
      RETURN NEXT rec_occ;
   END LOOP;
   RETURN;
END;
$$  LANGUAGE 'plpgsql' STABLE;

-- ----------------------------------------------------------------------------
-- lm_getOccurrenceSetsForUser
CREATE OR REPLACE FUNCTION lm3.lm_getOccurrenceSetsForUser(usrid varchar, epsg int)
   RETURNS SETOF lm3.occurrenceset AS
$$
DECLARE
   rec_occ lm3.occurrenceset%ROWTYPE;
BEGIN
   IF epsg is null THEN
      FOR rec_occ in 
         SELECT o.*
           FROM lm3.occurrenceset o
         WHERE o.userid = usrid
      LOOP
         RETURN NEXT rec_occ;
      END LOOP;
   ELSE   
      FOR rec_occ in 
         SELECT o.*
           FROM lm3.occurrenceset o
         WHERE o.userid = usrid
           AND o.epsgcode = epsg
      LOOP
         RETURN NEXT rec_occ;
      END LOOP;
   END IF;
   RETURN;
END;
$$  LANGUAGE 'plpgsql' STABLE;  

-- ----------------------------------------------------------------------------
--  lm_deleteOccurrenceSet
CREATE OR REPLACE FUNCTION lm3.lm_deleteOccurrenceSet(occid int)
RETURNS int AS
$$
DECLARE 
   success int = -1;
   total int;
BEGIN
   -- Find other refs to occurrences.   
   SELECT INTO total count(*)
      FROM lm3.model
      WHERE occurrenceSetId = occid;
      
   -- If no other references to this occurrences, delete it.
   IF total = 0 THEN
      BEGIN
         DELETE FROM lm3.LMJob WHERE lmjobId in 
             (SELECT lmjobid FROM lm3.lm_occJob WHERE occurrencesetid = occid); 

         DELETE FROM lm3.OccurrenceSet WHERE occurrenceSetId = occid;
         IF FOUND THEN
            success = 0;
         END IF;
      END;
   ELSE 
      raise notice '% models still referencing OccurrenceSet %', total, occid;
   END IF;

   RETURN success;
END;
$$  LANGUAGE 'plpgsql' VOLATILE;

-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lm3.lm_deleteModel(mdlid int)
RETURNS int AS
$$
DECLARE
   success int = -1;
BEGIN
   -- Delete projections and finally, the model.
   DELETE FROM lm3.LMJob WHERE lmjobId in (SELECT lmjobid FROM lm3.lm_prjJob
                                             WHERE modelid = mdlid); 
   DELETE FROM lm3.Projection WHERE modelid = mdlid; 
      
   DELETE FROM lm3.LMJob WHERE lmjobId in (SELECT lmjobid FROM lm3.lm_mdlJob
                                             WHERE modelid = mdlid); 
   DELETE FROM lm3.LMJob WHERE lmjobId in (SELECT lmjobid FROM lm3.lm_msgJob
                                             WHERE modelid = mdlid); 
   DELETE FROM lm3.Model WHERE modelid = mdlid;
      
   IF FOUND THEN
      success = 0;
   END IF;

   RETURN success;
END;
$$  LANGUAGE 'plpgsql' VOLATILE;

-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lm3.lm_deleteProjection(prjid int)
RETURNS int AS
$$
DECLARE
   success int = -1;
BEGIN
   DELETE FROM lm3.LMJob WHERE lmjobId in (SELECT lmjobid FROM lm3.lm_prjJob
                                             WHERE projectionid = prjid); 
   DELETE FROM lm3.Projection WHERE projectionid = prjid;
       
   IF FOUND THEN
      success = 0;
   END IF;
   RETURN success;
END;
$$  LANGUAGE 'plpgsql' VOLATILE;

-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lm3.lm_getProjection(id int)
   RETURNS lm3.lm_fullProjection AS
$$
DECLARE
   rec_prj lm3.lm_fullProjection;
BEGIN
   BEGIN
      SELECT * INTO STRICT rec_prj 
        FROM lm3.lm_fullprojection WHERE projectionid = id;
   
      EXCEPTION
         WHEN NO_DATA_FOUND THEN
            RAISE NOTICE 'Projection % not found', id;
         WHEN TOO_MANY_ROWS THEN
            RAISE EXCEPTION 'Projection % not unique', id;
   END;
   RETURN rec_prj;
END;
$$ LANGUAGE 'plpgsql' STABLE; 

-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lm3.lm_getProjectionsForModel(mdlid int, completeStat int)
   RETURNS SETOF lm3.lm_fullProjection AS
$$
DECLARE
   rec_prj lm3.lm_fullProjection;
BEGIN
   IF completeStat is null THEN
      FOR rec_prj in 
         SELECT *
         FROM lm3.lm_fullProjection
         WHERE modelid = mdlid
      LOOP
         RETURN NEXT rec_prj;
      END LOOP;
   ELSE
      FOR rec_prj in 
         SELECT *
         FROM lm3.lm_fullProjection
         WHERE modelid = mdlid
           AND prjstatus = completeStat
      LOOP
         RETURN NEXT rec_prj;
      END LOOP; 
   END IF;
   RETURN;
END;
$$ LANGUAGE 'plpgsql' STABLE; 

-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lm3.lm_getProjectionsByOccurrenceSet(occId int, completeStat int)
   RETURNS SETOF lm3.lm_fullProjection AS
$$
DECLARE
   rec_prj lm3.lm_fullProjection;
BEGIN
   FOR rec_prj in 
      SELECT *
      FROM lm3.lm_fullProjection
      WHERE occurrenceSetId = occId
        -- projection status
        AND prjstatus = completeStat
   LOOP
      RETURN NEXT rec_prj;
   END LOOP;   
   RETURN;
END;
$$ LANGUAGE 'plpgsql' STABLE; 

-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lm3.lm_getProjectionsByOccurrenceSetAndUser(occId int, 
                                                                   usr varchar, 
                                                                   completeStat int)
   RETURNS SETOF lm3.lm_fullProjection AS
$$
DECLARE
   rec_prj lm3.lm_fullProjection;
BEGIN
   FOR rec_prj in 
      SELECT *
      FROM lm3.lm_fullProjection
      WHERE occurrenceSetId = occId
        AND mdlUserId = usr
        -- projection status
        AND prjstatus = completeStat
   LOOP
      RETURN NEXT rec_prj;
   END LOOP;   
   RETURN;
END;
$$ LANGUAGE 'plpgsql' STABLE; 

-- ----------------------------------------------------------------------------
-- ----------------------------------------------------------------------------
-- Find or insert occurrenceSet and return id.  Return -1 on failure.
CREATE OR REPLACE FUNCTION lm3.lm_insertOccurrenceSet(lyrverify varchar,
                                                  lyrsquid varchar,
                                                  usrid varchar,
                                                  frmgbif boolean,
                                                  name varchar,
                                                  dloc varchar,
                                                  qrynum int,
                                                  qrytime double precision,
                                                  epsg int,
                                                  bounds varchar, 
                                                  polywkt text,
                                                  pointswkt text,
                                                  metadataUrlprefix varchar,
                                                  env int,
                                                  rdloc varchar,
                                                  stat int,
                                                  stattime double precision,
                                                  scinameid int)
   RETURNS int AS
$$
DECLARE
   id int = -1;
   idstr varchar = '';
   occmetadataUrl varchar = '';
BEGIN
   SELECT INTO id lm3._lm_findUniqueOccurrenceSet(usrid, frmgbif, name);
   IF id = -1 THEN
      BEGIN
         -- Default LM EPSG Code
         IF epsg = 4326 THEN 
            INSERT INTO lm3.OccurrenceSet 
               (verify, squid, userId, fromGbif, displayName, dlocation, queryCount, dateLastModified, 
                dateLastChecked, epsgcode, bbox, geom, geompts,
                primaryEnv, rawdlocation, status, statusModTime, scientificNameId)
            VALUES 
               (lyrverify, lyrsquid, usrid, frmgbif, name, dloc, qrynum, qrytime, 
                qrytime, epsg, bounds, 
                ST_GeomFromText(polywkt, epsg), ST_GeomFromText(pointswkt, epsg),
                env, rdloc, stat, stattime, scinameid);

         -- Other EPSG Codes skip geometry
         ELSE 
            INSERT INTO lm3.OccurrenceSet 
               (verify, squid, userId, fromGbif, displayName, dlocation, queryCount, dateLastModified, 
                dateLastChecked, epsgcode, bbox,
                primaryEnv, rawdlocation, status, statusModTime, scientificNameId)
            VALUES 
               (lyrverify, lyrsquid, usrid, frmgbif, name, dloc, qrynum, qrytime, 
                qrytime, epsg, bounds, env, rdloc, stat, stattime, scinameid);

         END IF;
        
         -- Create metadataUrl in the form: 'http://lifemapper.org/ogc?map=data_9999&layers=occ_9999'
         -- for an occurrenceset with id = 9999. 
         IF FOUND THEN
            SELECT INTO id last_value FROM lm3.occurrenceset_occurrencesetid_seq;
            idstr = cast(id as varchar);
            occmetadataUrl := replace(metadataUrlprefix, '#id#', idstr);
            -- occmetadataUrl := metadataUrlprefix || '?map=data_' || idstr || '&layers=occ_' || idstr;
            UPDATE lm3.OccurrenceSet SET metadataUrl = occmetadataUrl WHERE occurrenceSetId = id;
         END IF;
         
      END;  -- end alternatives for epsgcode
   END IF;  -- end if occurrenceset found
   RETURN id;
END;
$$  LANGUAGE 'plpgsql' VOLATILE;

-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lm3._lm_findUniqueOccurrenceSet(usrid varchar, 
                                                           frmgbif boolean, 
                                                           name varchar)
   RETURNS int AS
$$
DECLARE                                                    
   id int = -1;                                            
BEGIN                                                      
   if frmgbif is True then                                 
      SELECT occurrencesetid INTO id from lm3.OccurrenceSet    
         where userId = usrid                              
           and displayname = name;                         
      IF NOT FOUND then                                    
         id = -1;                                          
      END IF;                                              
   end if;                                                 
   RETURN id;                                              
END; 
$$ LANGUAGE 'plpgsql' STABLE; 
                                                                        
-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lm3.lm_insertAncillaryLayer(lyrverify varchar,
                                                lyrsquid varchar,
                                                usr varchar,
                                                lyrname varchar, 
                                                lyrtitle varchar,
                                                lyrdesc varchar,
                                                dloc varchar,
                                                lyrurl varchar,
                                                gtype int,
                                                otype int,
                                                fmtcode varchar,
                                                epsg int,
                                                munits varchar,
                                                res  double precision,
                                                vname varchar,
                                                stdt double precision,
                                                enddt  double precision,
                                                modtime double precision, 
                                                nodata double precision,
                                                minv double precision,
                                                maxv double precision,
                                                vunits double precision,
                                                bboxstring varchar, 
                                                wkt varchar)
   RETURNS int AS
$$
DECLARE
   success int = -1;
   lyrid int;
BEGIN
   -- get or insert AncLayer (user-layername combo is unique) 
   SELECT l.layerid INTO lyrid
      FROM lm3.Layer l
      WHERE l.userId = usr
        AND l.name = lyrname;
        
   raise notice 'FOUND layerid % with user % and name %', lyrid, usr, lyrname;
        
   IF NOT FOUND THEN
      BEGIN
         -- Default LM EPSG Code
         IF epsg = 4326 THEN 
            INSERT INTO lm3.Layer (verify, squid, userid, name, title, description, dlocation, 
                                metadataUrl, gdalType, ogrType, dataFormat, 
                                epsgcode, mapunits, resolution, valAttribute, 
                                startDate, endDate, dateLastModified, 
                                nodataVal, minVal, maxVal, valUnits, bbox, geom)
            VALUES (lyrverify, lyrsquid, usr, lyrname, lyrtitle, lyrdesc, dloc, lyrurl, 
                    gtype, otype, fmtcode, epsg, munits, res, vname, 
                    stdt, enddt, modtime, nodata, minv, maxv, vunits, 
                    bboxstring, ST_GeomFromText(wkt, epsg));
         
         -- Any other EPSG Code
         ELSE
            INSERT INTO lm3.Layer (verify, squid, userid, name, title, description, dlocation, 
                                metadataUrl, gdalType, ogrType, dataFormat, epsgcode, mapunits, 
                                resolution, valAttribute, startDate, endDate, 
                                dateLastModified, 
                                nodataVal, minVal, maxVal, valUnits, bbox)
            VALUES (lyrverify, lyrsquid, usr, lyrname, lyrtitle, lyrdesc, dloc, lyrurl, 
                    gtype, otype, fmtcode, epsg, munits, res, vname, 
                    stdt, enddt, modtime, nodata, minv, maxv, vunits, bboxstring);
         END IF;
         
         IF FOUND THEN
            SELECT INTO lyrid last_value FROM lm3.layer_layerid_seq;
            success := 0;
         END IF;
      END; -- end if layer not found
      
   -- if layer is found 
   ELSE
      success := 0;
      RAISE NOTICE 'Layer with matching name % found for user % found: id = %', 
                    user, lyrname, lyrid;
   END IF;
   
   
   RETURN lyrid;
END;
$$  LANGUAGE 'plpgsql' VOLATILE;

-- ----------------------------------------------------------------------------
-- Insert a new, initialized but not yet created model into the database
-- If a model with the same occurrenceSet, scenario, algorithm code and 
-- parameters already exists, return its id
CREATE OR REPLACE FUNCTION lm3.lm_insertSDMModel(usrid varchar,
                                          mdlname varchar,
                                          descr varchar,
                                          occid int,
                                          scencode varchar, 
                                          scenid int,
                                          mskid int,
                                          algcode varchar,
                                          algparams text,
                                          emale varchar, 
                                          stat int,
                                          createtm double precision,
                                          prty int)
   RETURNS int AS
$$
DECLARE
   id int = -1;
BEGIN
   SELECT INTO id modelid FROM lm3.model
      WHERE occurrencesetid = occid
        AND scenarioid = scenid
        AND algorithmCode = algcode
        AND algorithmParams = algparams
        AND maskId = mskid
        AND userid = usrid;
   IF NOT FOUND THEN 
      BEGIN
         INSERT INTO lm3.Model 
            (userId, name, description, occurrenceSetId, scenarioCode, scenarioId, maskId, 
            algorithmCode, algorithmParams, email, createTime, status, statusModTime, priority)
           VALUES (usrid, mdlname, descr, occid, scencode, scenid, mskid, algcode, algparams, 
                   emale, createtm, stat, createtm, prty);

         IF FOUND THEN
            SELECT INTO id last_value FROM lm3.model_modelid_seq;
         END IF;
      END;
   END IF;
   
   RETURN id;
END;
$$  LANGUAGE 'plpgsql' VOLATILE; 


-- ----------------------------------------------------------------------------
-- UPDATE lm3.a model
CREATE OR REPLACE FUNCTION lm3.lm_reprioritizeExperiment(mdlid int,
                                                     prty int)
   RETURNS int AS
$$
DECLARE
   success int = -1;
BEGIN
   UPDATE lm3.Model SET priority = prty WHERE modelId = mdlid;
   IF FOUND THEN
      UPDATE lm3.Projection SET priority = prty WHERE modelid = mdlid;
      IF FOUND THEN 
         success := 0;
      END IF;
   END IF;
   RETURN success;
END;
$$  LANGUAGE 'plpgsql' VOLATILE;

-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lm3.lm_updateModel(mdlid int,
                                          stat int, 
                                          stattime double precision,
                                          prty int,
                                          rsfname varchar, 
                                          qualitycontrol varchar,
                                          jbid int,
                                          crid int,
                                          errorStat int,
                                          depErrorStat int)
   RETURNS int AS
$$
DECLARE
   success int = -1;
BEGIN
   UPDATE lm3.Model SET 
      (status, statusModTime, priority, dlocation, qc, jobId) = 
     (stat, stattime, prty, rsfname, qualitycontrol, jbid)
     WHERE modelId = mdlid;
   IF FOUND THEN
      BEGIN
         IF stat IS NOT NULL AND stat >= errorStat THEN
            BEGIN
               UPDATE lm3.Projection SET status = depErrorStat WHERE modelid = mdlid;
               IF FOUND THEN 
                  success := 0;
               END IF;
            END;
         ELSE
            success := 0;
         END IF;
      END;
   END IF;
   RETURN success;
END;
$$  LANGUAGE 'plpgsql' VOLATILE;

-- ----------------------------------------------------------------------------
-- Automatically move projections when Model completes or fails
-- TODO: replaces lm_updateModel
CREATE OR REPLACE FUNCTION lm3.lm_updateModelAndDependencies(mdlid int,
                                          stat int, 
                                          stattime double precision,
                                          prty int,
                                          rsfname varchar, 
                                          qualitycontrol varchar,
                                          jbid int,
                                          crid int,
                                          completeStat int,
                                          depReadyStat int,
                                          errorStat int,
                                          depErrorStat int)
   RETURNS int AS
$$
DECLARE
   success int = -1;
BEGIN
   UPDATE lm3.Model SET 
      (status, statusModTime, priority, dlocation, qc, jobId) = 
     (stat, stattime, prty, rsfname, qualitycontrol, jbid)
     WHERE modelId = mdlid;
   IF FOUND THEN
      BEGIN
         IF stat IS NOT NULL THEN 
            BEGIN
               IF stat >= errorStat THEN
                  begin
                     UPDATE lm3.Projection SET status = depErrorStat WHERE modelid = mdlid;
                     IF FOUND THEN 
                        success := 0;
                     END IF;
                  end;
               ELSEIF stat = completeStat THEN            
                  begin
                     UPDATE lm3.Projection SET status = depReadyStat WHERE modelid = mdlid;
                     IF FOUND THEN 
                        success := 0;
                     END IF;
                  end;
               END IF;
            END;
         ELSE
            success := 0;
         END IF;
      END;
   END IF;
   RETURN success;
END;
$$  LANGUAGE 'plpgsql' VOLATILE;
-- ----------------------------------------------------------------------------
-- UPDATE lm3.a model
CREATE OR REPLACE FUNCTION lm3.lm_updateModelAlgParams(mdlid int,
                                                       algparams text)
   RETURNS int AS
$$
DECLARE
   success int = -1;
BEGIN
   UPDATE lm3.Model SET algorithmParams = algparams
     WHERE modelId = mdlid;
   IF FOUND THEN
      success = 0;
   END IF;
   RETURN success;
END;
$$  LANGUAGE 'plpgsql' VOLATILE;

-- ----------------------------------------------------------------------------
-- Insert a new Lifemapper User
CREATE OR REPLACE FUNCTION lm3.lm_insertUser(usrid varchar, name1 varchar, 
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
   SELECT * into rec FROM lm3.LMUser
      WHERE userid = usrid;
   IF NOT FOUND THEN 
      INSERT INTO lm3.LMUser
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
-- Delete a Lifemapper User
CREATE OR REPLACE FUNCTION lm3.lm_deleteUser(usrid varchar(20))
   RETURNS int AS
$$
DECLARE
   success int = -1;
   rec record;
BEGIN
   SELECT * into rec FROM lm3.LMUser
      WHERE userid = usrid;
   IF NOT FOUND THEN 
      RAISE NOTICE 'LMUser % is not found', usrid;
   ELSE
      DELETE FROM lm3.LMUser WHERE userId = usrid;
      IF FOUND THEN
         success := 0;
      END IF;
   END IF;
   
   RETURN success;
END;
$$  LANGUAGE 'plpgsql' VOLATILE; 

-- ----------------------------------------------------------------------------
-- Find an existing Lifemapper User
CREATE OR REPLACE FUNCTION lm3.lm_findUser(usrid varchar, 
                                       emale varchar)
   RETURNS lm3.lmuser AS
$$
DECLARE
   rec lm3.lmuser%rowtype;
BEGIN
   IF usrid is not null THEN
      SELECT * into rec FROM lm3.LMUser WHERE lower(userid) = lower(usrid);
   END IF;
   IF NOT FOUND THEN
      SELECT * into rec FROM lm3.LMUser WHERE lower(email) = lower(emale);
   END IF;
   RETURN rec;
END;
$$  LANGUAGE 'plpgsql' STABLE; 

-- ----------------------------------------------------------------------------
-- Find all existing Lifemapper Users
CREATE OR REPLACE FUNCTION lm3.lm_getUsers()
   RETURNS SETOF lm3.lmuser AS
$$
DECLARE
   rec lm3.lmuser%rowtype;
BEGIN
   FOR rec IN
      SELECT * FROM lm3.LMUser
      LOOP
         RETURN NEXT rec;
      END LOOP;

      EXCEPTION
         WHEN NO_DATA_FOUND THEN
            RAISE NOTICE 'No users found';
   RETURN;
END;
$$  LANGUAGE 'plpgsql' STABLE; 

-- ----------------------------------------------------------------------------
-- UPDATE a user
CREATE OR REPLACE FUNCTION lm3.lm_updateUser(usrid varchar(20), name1 varchar(50),
                                         name2 varchar(50), 
                                         inst varchar, addr1 varchar, 
                                         addr2 varchar, addr3 varchar,
                                         fone varchar(20), emale varchar(20), 
                                         modTime double precision, 
                                         psswd varchar(32))
   RETURNS int AS
$$
DECLARE
   success int = -1;
BEGIN
   UPDATE lm3.LMUser SET 
      (userId, firstname, lastname, institution, address1, address2, address3, phone,
          email, dateLastModified, password) =
      (usrid, name1, name2, inst, addr1, addr2, addr3, fone, emale, modTime, psswd)
      WHERE userId = usrid;

   IF FOUND THEN
      success := 0;
   END IF;

RETURN success;
END;
$$  LANGUAGE 'plpgsql' VOLATILE;

-- ----------------------------------------------------------------------------
-- lm_getUser
CREATE OR REPLACE FUNCTION lm3.lm_getUser(usrid varchar(20))
   RETURNS lm3.lmuser AS
$$
DECLARE
   rec lm3.lmuser%ROWTYPE;
BEGIN
   BEGIN
      SELECT u.* INTO STRICT rec FROM lm3.lmuser u 
         WHERE u.userid = usrid;

      EXCEPTION
         WHEN NO_DATA_FOUND THEN
            RAISE NOTICE 'Userid % not found', usrid;
         WHEN TOO_MANY_ROWS THEN
            RAISE EXCEPTION 'Userid % not unique', usrid;
   END;
   RETURN rec;
END;
$$  LANGUAGE 'plpgsql' STABLE;

-- ----------------------------------------------------------------------------
-- UPDATE lm3.models and projections for a modified occurrence set
CREATE OR REPLACE FUNCTION lm3.lm_resetExperimentsForOccurrenceSet(occsetid int,
                                                               newstat int, 
                                                               stattime double precision,
                                                               prty int)
   RETURNS int AS
$$
DECLARE
   total int = -1;
BEGIN
   UPDATE lm3.Model SET 
      (status, statusModTime, priority) = (newstat, stattime, prty)
      WHERE occurrencesetid = occsetid;
   GET DIAGNOSTICS total = ROW_COUNT;
   
   IF total > 0 THEN
      BEGIN
         UPDATE lm3.Projection SET (status, statusModTime, priority) = 
                               (newstat, stattime, prty)
            WHERE modelid in
                 (SELECT modelid FROM lm3.model WHERE occurrencesetid = occsetid);
      END;
   END IF;
   RETURN total;
END;
$$  LANGUAGE 'plpgsql' VOLATILE;

-- ----------------------------------------------------------------------------
-- Reset occurrence set to count = 0 (for later re-population)
CREATE OR REPLACE FUNCTION lm3.lm_clearOccurrenceSet(occsetid int,
                                                 modtime double precision)
   RETURNS int AS
$$
DECLARE
   success int = -1;
   total int = -1;
BEGIN
   SELECT INTO total count(*) FROM lm3.OccurrenceSet WHERE occurrencesetid = occsetid;
   IF total > 0 THEN
      BEGIN
         UPDATE lm3.OccurrenceSet SET 
            (queryCount, dateLastModified) = (-1, modtime)
            WHERE occurrencesetid = occsetid;
         IF FOUND THEN 
            success := 0;
         END IF;
      END;
   ELSE
      BEGIN
         success := 0;
         RAISE NOTICE 'OccurrenceSet % does not exist', occsetid;
      END;
   END IF;
   RETURN success;
END;
$$  LANGUAGE 'plpgsql' VOLATILE;

-- ----------------------------------------------------------------------------
-- Insert a new, initialized but not yet created, projection into the database
-- Returns -1 on failure (pre-existing projection with modelid,scenarioid).
CREATE OR REPLACE FUNCTION lm3.lm_insertProjection(lyrverify varchar,
                                               lyrsquid varchar,
                                               mdlid int,
                                               scenid int, 
                                               mskid int,
                                               createtm double precision,
                                               stat int,
                                               prty int,
                                               rstunits varchar,
                                               rstres double precision,
                                               bounds varchar,
                                               epsg int,
                                               wkt varchar,
                                               metadataUrlprefix varchar)
   RETURNS int AS
$$
DECLARE
   scencode varchar;
   id int = -1;
   idstr varchar = '';
   pmetadataUrl varchar = '';
BEGIN
   SELECT INTO id projectionid FROM lm3.projection 
      WHERE modelid = mdlid
        AND scenarioid = scenid
        AND maskId = mskid;
        
   IF NOT FOUND THEN
      BEGIN
   
         SELECT s.scenariocode INTO scencode FROM lm3.Scenario s
            WHERE s.scenarioid = scenid;

         -- Default LM EPSG Code
         IF epsg = 4326 THEN 
            INSERT INTO lm3.Projection 
               (verify, squid, modelId, scenarioCode, scenarioId, maskId, createTime, status, 
                statusModTime, priority, units, resolution, epsgcode, bbox, geom)
               VALUES 
               (lyrverify, lyrsquid, mdlid, scencode, scenid, mskid, createtm, stat, 
                createtm, prty, rstunits, rstres, epsg, bounds, ST_GeomFromText(wkt, epsg));
                      
         -- Other EPSG Code skip geometry
         ELSE
            INSERT INTO lm3.Projection 
               (verify, squid, modelId, scenarioCode, scenarioId, maskId, createTime, status, 
                statusModTime, priority, units, resolution, epsgcode, bbox)
               VALUES 
               (lyrverify, lyrsquid, mdlid, scencode, scenid, mskid, createtm, stat, 
                createtm, prty, rstunits, rstres, epsg, bounds);
         END IF;
         
         -- Create metadataUrl in the form: 'http://lifemapper.org/ogc?map=data_1234&layers=prj_9999'
         -- for occurrencesetid 1234 and projectionid 9999.
         IF FOUND THEN
            SELECT INTO id last_value FROM lm3.projection_projectionid_seq;
            idstr := cast(id as varchar);
            pmetadataUrl := replace(metadataUrlprefix, '#id#', idstr);
            -- pmetadataUrl := metadataUrlprefix || cast(id as varchar);
            UPDATE lm3.Projection SET metadataUrl = pmetadataUrl WHERE projectionId = id;
         END IF;
      END;
   END IF;
   
   RETURN id;
END;
$$  LANGUAGE 'plpgsql' VOLATILE;  
   
-- ----------------------------------------------------------------------------
-- ----------------------------------------------------------------------------
-- lm_updateProjectionInfo
-- This function stores the geometry only if the srs is 4326 (unprojected).
CREATE OR REPLACE FUNCTION lm3.lm_updateProjectionInfo(prjid int,
                                            stat int, 
                                            stattime double precision,
                                            prty int,
                                            fullrstfname varchar, 
                                            rsttype int,
                                            bounds varchar,
                                            epsg int,
                                            wkt varchar,
                                            jbid int, 
                                            crid int)
   RETURNS int AS
$$
DECLARE
   success int = -1;
BEGIN
   IF fullrstfname is NULL THEN
      UPDATE lm3.Projection SET 
         (status, statusModTime, priority, jobId, computeResourceId) 
       = (stat, stattime, prty, jbid, crid)
     WHERE projectionId = prjid;
   ELSE
      BEGIN
         -- Default  EPSG Code
         IF epsg = 4326 THEN 
            UPDATE lm3.Projection SET 
               (status, statusModTime, priority, dlocation, dataType, 
               bbox, geom, jobId, computeResourceId) = 
              (stat, stattime, prty, fullrstfname, rsttype, 
               bounds, ST_GeomFromText(wkt, epsg), jbid, crid)
              WHERE projectionId = prjid;
              
         -- All other EPSG Codes do not create geometry
         ELSE
            UPDATE lm3.Projection SET 
               (status, statusModTime, priority, dlocation, dataType, 
               bbox, jobId) = 
              (stat, stattime, prty, fullrstfname, rsttype, 
               bounds, jbid)
              WHERE projectionId = prjid;
              
         END IF;
     END;
   END IF;

   IF FOUND THEN
      success = 0;
   END IF;
   RETURN success;
END;
$$  LANGUAGE 'plpgsql' VOLATILE;
-- ----------------------------------------------------------------------------
-- Used for web queries
CREATE OR REPLACE FUNCTION lm3.lm_getOccurrenceSetNamesWithProj(qstring varchar, 
                                                            maxCount int, 
                                                            completeStat int)
   RETURNS SETOF varchar AS
$$
DECLARE
   disname varchar = '';
   qry varchar = lower(qstring) || '%';
BEGIN
   FOR disname IN
      SELECT o.displayname 
      FROM lm3.occurrenceset o, lm3.projection p, lm3.model m
      WHERE m.modelid = p.modelid
        AND m.occurrenceSetId = o.occurrenceSetId
        AND p.status = completeStat 
        AND lower(o.displayname) like qry
        ORDER BY o.displayname ASC limit(maxCount)
   LOOP
      RETURN NEXT disname;
   END LOOP;
   RETURN;      
END;
$$  LANGUAGE 'plpgsql' STABLE;

-- ----------------------------------------------------------------------------
-- Used for web queries
CREATE OR REPLACE FUNCTION lm3.lm_getOccurrenceSetNamesWithPoints(qstring varchar, 
                                                              minPoints int, 
                                                              maxCount int)
   RETURNS SETOF varchar AS
$$
DECLARE
   disname varchar = '';
   qry varchar = lower(qstring) || '%';
BEGIN
   FOR disname IN
      SELECT o.displayname 
      FROM lm3.occurrenceset o
      WHERE o.queryCount >= minPoints 
        AND lower(o.displayname) like lower(qstring) || '%'
        ORDER BY o.displayname ASC limit(maxCount)
   LOOP
      RETURN NEXT disname;
   END LOOP;
   RETURN;      
END;
$$  LANGUAGE 'plpgsql' STABLE;

-- ----------------------------------------------------------------------------
-- 
CREATE OR REPLACE FUNCTION lm3.lm_getOccurrenceSetNames(qstring varchar)
   RETURNS SETOF varchar AS
$$
DECLARE
   disname varchar = '';
   qry varchar = lower(qstring) || '%';
BEGIN
   FOR disname IN
      SELECT o.displayname 
      FROM lm3.occurrenceset o
      WHERE lower(o.displayname) like qry
        ORDER BY o.displayname ASC
   LOOP
      RETURN NEXT disname;
   END LOOP;
   RETURN;
END;
$$  LANGUAGE 'plpgsql' STABLE;

-- ----------------------------------------------------------------------------
-- lm_getOccurrenceSetsForScinameUser
-- returns newest first
CREATE OR REPLACE FUNCTION lm3.lm_getOccurrenceSetsForScinameUser(nameid int, 
                                                                  usrid varchar)
   RETURNS SETOF lm3.occurrenceset AS
$$
DECLARE
   rec lm3.occurrenceset%ROWTYPE;
BEGIN
   FOR rec IN 
      SELECT o.* FROM lm3.occurrenceset o 
         WHERE scientificnameid = nameid AND userid = usrid
         ORDER BY dateLastModified DESC 
   LOOP
      RETURN NEXT rec;
   END LOOP;
   RETURN;
END;
$$  LANGUAGE 'plpgsql' STABLE;

-- ----------------------------------------------------------------------------
-- lm_getOccurrenceSetsForName
-- returns newest first
CREATE OR REPLACE FUNCTION lm3.lm_getOccurrenceSetsForName(name varchar, 
                                                       usrid varchar,
                                                       defaultusrid varchar)
   RETURNS SETOF lm3.occurrenceset AS
$$
DECLARE
   rec lm3.occurrenceset%ROWTYPE;
   cmd varchar;
   wherecls varchar;
   ordercls varchar;
BEGIN
   cmd = 'SELECT o.* FROM lm3.occurrenceset o ';
   wherecls = 'WHERE lower(o.displayname) = lower(' || quote_literal(name) || ')';
   ordercls = ' ORDER BY dateLastModified DESC ';

   -- return user and default user
   IF usrid is not null AND defaultusrid is not null THEN
      wherecls = wherecls || 'AND (o.userid = ' || quote_literal(usrid) || ' or ';
      wherecls = wherecls ||      'o.userid = ' || quote_literal(defaultusrid) || ')';
      
   -- return ONLY user
   ELSEIF usrid is not null AND defaultusrid is null THEN
      wherecls = wherecls || 'AND o.userid = ' || quote_literal(usrid);
      
   -- return ONLY default user
   ELSEIF usrid is null AND defaultusrid is not null THEN
      wherecls = wherecls || 'AND o.userid = ' || quote_literal(defaultusrid);
      
   ELSE
      RAISE EXCEPTION 'Must supply UserId or DefaultUserId';
   END IF;

   cmd = cmd || wherecls || ordercls;
   RAISE NOTICE 'cmd = %', cmd;
   FOR rec in EXECUTE cmd
      LOOP
         RETURN NEXT rec;
      END LOOP;
   RETURN;

END;
$$  LANGUAGE 'plpgsql' STABLE;

-- ----------------------------------------------------------------------------
-- lm_getOccurrenceSetsForGenus
CREATE OR REPLACE FUNCTION lm3.lm_getOccurrenceSetsForGenus(genus varchar, 
                                                        defaultusrid varchar)
   RETURNS SETOF lm3.occurrenceset AS
$$
DECLARE
   rec lm3.occurrenceset%ROWTYPE;
   searchterm varchar;
   wherecls varchar;
BEGIN
   searchterm := genus || ' %';
   FOR rec in
      SELECT o.* FROM lm3.occurrenceset o 
         WHERE lower(o.displayname) LIKE lower(searchterm)
           AND o.userid = defaultusrid

      LOOP
         RETURN NEXT rec;
      END LOOP;
   RETURN;

END;
$$  LANGUAGE 'plpgsql' STABLE;

-- ----------------------------------------------------------------------------
-- lm_getOccurrenceSetsForNameAndUser  
-- OLD=lm3.lm_getGBIFOccurrenceSetsForUserAndName
CREATE OR REPLACE FUNCTION lm3.lm_getOccurrenceSetsForNameAndUser(
                                                          name varchar,
                                                          usr varchar)
   RETURNS SETOF lm3.occurrenceset AS
$$
DECLARE
   rec lm3.occurrenceset%ROWTYPE;
   cmd varchar;
   wherecls varchar;
BEGIN
   FOR rec in SELECT o.* FROM lm3.occurrenceset o 
              WHERE lower(o.displayname) = lower(name)
                AND userid = usr
      LOOP
         RETURN NEXT rec;
      END LOOP;
   RETURN;
END;
$$  LANGUAGE 'plpgsql' STABLE;

-- ----------------------------------------------------------------------------
-- lm_getOccurrenceSetsLikeNameAndUser
-- OLD=lm_getGBIFOccurrenceSetsLikeName
CREATE OR REPLACE FUNCTION lm3.lm_getOccurrenceSetsLikeNameAndUser(name varchar,
                                                                usr varchar)
   RETURNS SETOF lm3.occurrenceset AS
$$
DECLARE
   rec lm3.occurrenceset%ROWTYPE;
   cmd varchar;
   wherecls varchar;
BEGIN
   FOR rec in SELECT o.* FROM lm3.occurrenceset o 
              WHERE o.displayname like concat(name, '%') 
                AND userid = usr
      LOOP
         RETURN NEXT rec;
      END LOOP;

   FOR rec in SELECT o.* FROM lm3.occurrenceset o 
              WHERE o.displayname like concat(lower(name), '%') 
                AND userid = usr
      LOOP
         RETURN NEXT rec;
      END LOOP;

   RETURN;
END;
$$  LANGUAGE 'plpgsql' STABLE;

-- ----------------------------------------------------------------------------
-- Returns 0 if given name exists in the OccurrenceSet table, -1 if it does not.
CREATE OR REPLACE FUNCTION lm3.lm_existOccurrenceSet(name varchar)
   RETURNS int AS
$$
DECLARE
   success int = -1;
   id int = -1;
BEGIN
   SELECT occurrenceSetId INTO id
      FROM lm3.occurrenceset
      WHERE displayName = name;
   IF FOUND THEN
      success = 0;
   END IF;
   RETURN success;
END;
$$  LANGUAGE 'plpgsql' STABLE;    

-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lm3.lm_resetSDMJobsToReadyAndWaiting(stattime double precision, 
                                                                inprocess_stat int, 
                                                                ready_stat int, 
                                                                waiting_stat int)
   RETURNS int AS
$$
DECLARE
   rowcount int;
   total int;
BEGIN
   UPDATE lm3.Model SET (status, statusModTime) = (ready_stat, stattime) 
      WHERE status = inprocess_stat; 
   GET DIAGNOSTICS total = ROW_COUNT;
   

   UPDATE lm3.Projection SET (status, statusModTime) = (waiting_stat, stattime) 
      WHERE status = inprocess_stat; 
   GET DIAGNOSTICS rowcount = ROW_COUNT;
   
   total = total + rowcount;
   RETURN total;
END;
$$  LANGUAGE 'plpgsql' VOLATILE;

-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lm3.lm_getScenarioCodes()
   RETURNS SETOF varchar AS
$$
DECLARE
   scencode varchar;
BEGIN
   FOR scencode in 
       SELECT scenariocode
       FROM lm3.scenario
   LOOP
      RETURN NEXT scencode;
   END LOOP;   
   RETURN;
END;
$$  LANGUAGE 'plpgsql' STABLE;

-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lm3.lm_getKeywords()
   RETURNS SETOF varchar AS
$$
DECLARE
   kwd varchar;
BEGIN
   FOR kwd in 
       SELECT distinct(keyword)
       FROM lm3.keyword
   LOOP
      RETURN NEXT kwd;
   END LOOP;   
   RETURN;
END;
$$  LANGUAGE 'plpgsql' STABLE;


-- ----------------------------------------------------------------------------
-- uses arrays
-- differs from above in that it also returns the scenario to be matched
CREATE OR REPLACE FUNCTION lm3.lm_getMatchingScenariosNoKeywords(scenid int)
   RETURNS SETOF lm3.scenario AS
$$
DECLARE
   arrScenTypes int[];
   arrNewScenTypes int[];
   rec lm3.scenario%ROWTYPE;
BEGIN
   SELECT INTO arrScenTypes lm3.lm_getScenarioTypes(scenid) ;
   FOR rec in 
      SELECT s.*
      FROM lm3.scenario s
   LOOP
      SELECT INTO arrNewScenTypes lm3.lm_getScenarioTypes(rec.scenarioid);
      IF arrScenTypes = arrNewScenTypes THEN
         RETURN NEXT rec;
      END IF;
   END LOOP;   
   RETURN;
END;
$$ LANGUAGE 'plpgsql' STABLE; 

-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lm3.lm_getScenarioTypes(scenid int)
   RETURNS int[] AS
$$
DECLARE
   i int := 0;
   typeid int;
   arrStore int[];
BEGIN
   FOR typeid in
      SELECT l.layertypeid 
      FROM lm3.scenariolayers sl, lm3.layer l
      WHERE sl.scenarioid = scenid
        AND sl.layerid = l.layerid
        ORDER BY l.layertypeid DESC
   LOOP
      arrStore[i] = typeid;
      i = i + 1;
   END LOOP;   
   RETURN arrStore;
END;
$$ LANGUAGE 'plpgsql' STABLE; 

-- ----------------------------------------------------------------------------
-- lm_getLayersByScenarioId
CREATE OR REPLACE FUNCTION lm3.lm_getLayersByScenarioId(id int)
   RETURNS SETOF lm3.lm_envlayerAndKeywords AS
$$
DECLARE
   rec lm3.lm_envlayerAndKeywords;
   keystr varchar;
BEGIN
   FOR rec in SELECT el.*
               FROM lm3.lm_envlayer el, lm3.scenariolayers sl
               WHERE sl.scenarioid = id
                 AND sl.layerid = el.layerid
      LOOP
         SELECT INTO keystr lm3.lm_getLayerTypeKeywordString(rec.layertypeid);
         rec.keywords = keystr;
         RETURN NEXT rec;
      END LOOP;
   RETURN;
END;
$$  LANGUAGE 'plpgsql' STABLE;

-- ----------------------------------------------------------------------------
-- lm_getScenariosForLayer
CREATE OR REPLACE FUNCTION lm3.lm_getScenariosForLayer(id int)
   RETURNS SETOF lm3.scenario AS
$$
DECLARE
   rec lm3.scenario;
BEGIN
   FOR rec in 
      SELECT s.* FROM lm3.scenario s, lm3.scenariolayers sl
          WHERE s.scenarioid = sl.scenarioid AND sl.layerid = id
   LOOP
      RETURN NEXT rec;
   END LOOP;   
   RETURN;
END;
$$  LANGUAGE 'plpgsql' STABLE;

-- ----------------------------------------------------------------------------
-- lm_getEnvLayersForUser
CREATE OR REPLACE FUNCTION lm3.lm_getEnvLayersForUser(usrid varchar, epsg int)
   RETURNS SETOF lm3.lm_envlayerAndKeywords AS
$$
DECLARE
   rec lm3.lm_envlayerAndKeywords;
   keystr varchar;
BEGIN
   FOR rec in SELECT el.*
               FROM lm3.lm_envlayer el
               WHERE el.userid = usrid 
                 AND el.epsgcode = epsg
      LOOP
         SELECT INTO keystr lm3.lm_getLayerTypeKeywordString(rec.layertypeid);
         rec.keywords = keystr;
         RETURN NEXT rec;
      END LOOP;
   RETURN;
END;
$$  LANGUAGE 'plpgsql' STABLE;

-- ----------------------------------------------------------------------------
-- lm_getBaseLayersForUser
CREATE OR REPLACE FUNCTION lm3.lm_getBaseLayersForUser(usrid varchar, epsg int)
   RETURNS SETOF lm3.layer AS
$$
DECLARE
   rec lm3.layer;
BEGIN
   FOR rec in SELECT *
               FROM lm3.layer
               WHERE userid = usrid 
                 AND epsgcode = epsg
      LOOP
         RETURN NEXT rec;
      END LOOP;
   RETURN;
END;
$$  LANGUAGE 'plpgsql' STABLE;

-- ----------------------------------------------------------------------------
-- *** NEW ***
-- lm_getAncillaryLayersForUser
CREATE OR REPLACE FUNCTION lm3.lm_getAncillaryLayersForUser(usrid varchar, epsg int)
   RETURNS SETOF lm3.layer AS
$$
DECLARE
   rec lm3.layer;
BEGIN
   FOR rec in SELECT *
               FROM lm3.layer
               WHERE userid = usrid 
                 AND epsgcode = epsg
                 AND layerTypeId is null
      LOOP
         RETURN NEXT rec;
      END LOOP;
   RETURN;
END;
$$  LANGUAGE 'plpgsql' STABLE;

-- ----------------------------------------------------------------------------
-- lm_getEnvLayersForUser
-- For all users OTHER THAN lm2 and changeThinking, this should only return one.
--
CREATE OR REPLACE FUNCTION lm3.lm_getEnvLayersByNameUser(lyrname varchar, usrid varchar)
   RETURNS SETOF lm3.lm_envlayerAndKeywords AS
$$
DECLARE
   rec lm3.lm_envlayerAndKeywords;
   keystr varchar;
BEGIN
   FOR rec in SELECT el.*
               FROM lm3.lm_envlayer el
               WHERE el.name = lyrname AND el.userid = usrid
      LOOP
         SELECT INTO keystr lm3.lm_getLayerTypeKeywordString(rec.layertypeid);
         rec.keywords = keystr;
         RETURN NEXT rec;
      END LOOP;
   RETURN;
END;
$$  LANGUAGE 'plpgsql' STABLE;

-- ----------------------------------------------------------------------------
-- For all users, this should only return one.  Someday.
--
CREATE OR REPLACE FUNCTION lm3.lm_getEnvLayersByNameUserEpsg(lyrname varchar, 
                                                          usrid varchar, 
                                                          epsg int)
   RETURNS SETOF lm3.lm_envlayerAndKeywords AS
$$
DECLARE
   rec lm3.lm_envlayerAndKeywords;
   keystr varchar;
BEGIN
   FOR rec IN SELECT el.* 
              FROM lm3.lm_envlayer el
              WHERE el.name = lyrname AND el.userid = usrid AND el.epsgcode = epsg
      LOOP
         SELECT INTO keystr lm3.lm_getLayerTypeKeywordString(rec.layertypeid);
         rec.keywords = keystr;
         RETURN NEXT rec;
      END LOOP;
END;
$$  LANGUAGE 'plpgsql' STABLE;

-- ----------------------------------------------------------------------------
-- lm_fixOccurrenceSets
CREATE OR REPLACE FUNCTION lm3.lm_fixOccurrenceSets()
   RETURNS void AS
$$
DECLARE
   success int = -1;
BEGIN
   UPDATE lm3.OccurrenceSet SET query = 
     'SELECT * FROM lm3.lm_getMicroOccurrencesBySet(' || cast(occurrenceSetId as varchar) || ')'
     where fromGbif is true and (userid = 'lm2' or userid = 'usda') and query = '';
   RETURN;
END;
$$ LANGUAGE 'plpgsql' VOLATILE; 

-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lm3.lm_countModels(usrid varchar(20), 
                                         dispname varchar,
                                         beforetime double precision,
                                         aftertime double precision,
                                         epsg int,
                                         stat int,
                                         completestat int,
                                         occsetid int,
                                         algcode varchar)
   RETURNS int AS
$$
DECLARE
   num int;
   cmd varchar;
   wherecls varchar;
BEGIN
   cmd = 'select count(*) FROM lm3.lm_fullModel ';
   wherecls = ' WHERE mdluserid =  ' || quote_literal(usrid) ;

   -- filter by occurrenceset displayname
   IF dispname is not null THEN
      wherecls = wherecls || ' AND displayname like  ' || quote_literal(dispname);
   END IF;
   
   -- filter by modified before given time
   IF beforetime is not null THEN
      wherecls = wherecls || ' AND mdlstatusModTime <=  ' || quote_literal(beforetime);
   END IF;

   -- filter by modified after given time
   IF aftertime is not null THEN
      wherecls = wherecls || ' AND mdlstatusModTime >=  ' || quote_literal(aftertime);
   END IF;

   -- filter by epsgcode
   IF epsg is not null THEN
      wherecls = wherecls || ' AND  epsgcode =  ' || epsg;
   END IF;

   -- filter by OccurrenceSet
   IF occsetid is not null THEN
      wherecls = wherecls || ' AND occurrencesetid =  ' || quote_literal(occsetid);
   END IF;

   -- filter by status
   IF stat is not null THEN
      wherecls = wherecls || ' AND mdlstatus =  ' || quote_literal(stat);
   END IF;

   -- filter by 'in-pipeline', aka less than completed status
   IF completestat is not null THEN
      wherecls = wherecls || ' AND mdlstatus <  ' || quote_literal(completestat);
   END IF;
   
   -- filter by Algorithm
   IF algcode is not null THEN
      wherecls = wherecls || ' AND algorithmcode =  ' || quote_literal(algcode);
   END IF;

   cmd := cmd || wherecls;
   RAISE NOTICE 'cmd = %', cmd;

   EXECUTE cmd INTO num;
   RETURN num;
END;
$$  LANGUAGE 'plpgsql' STABLE;

-- ----------------------------------------------------------------------------
-- lm_listModels
CREATE OR REPLACE FUNCTION lm3.lm_listModels(firstRecNum int, maxNum int, 
                                         usrid varchar(20), 
                                         dispname varchar(256),
                                         beforetime double precision,
                                         aftertime double precision,
                                         epsg int,
                                         stat int,
                                         completestat int,
                                         occsetid int,
                                         algcode varchar)
   RETURNS SETOF lm3.lm_atom AS
$$
DECLARE
   rec lm3.lm_atom;
   cmd varchar;
   wherecls varchar;
   limitcls varchar;
   ordercls varchar;
BEGIN
   cmd = 'SELECT modelId, mdlname, epsgcode, mdldescription, mdlstatusModTime
               FROM lm3.lm_fullModel ';
   wherecls = ' WHERE mdluserid =  ' || quote_literal(usrid) ;
   ordercls = ' ORDER BY mdlstatusModTime DESC ';
   limitcls = ' LIMIT ' || quote_literal(maxNum) || ' OFFSET ' || quote_literal(firstRecNum);

   -- filter by occurrenceset displayname
   IF dispname is not null THEN
      wherecls = wherecls || ' AND displayname like  ' || quote_literal(dispname);
   END IF;

   -- filter by modified before given time
   IF beforetime is not null THEN
      wherecls = wherecls || ' AND mdlstatusModTime <=  ' || quote_literal(beforetime);
   END IF;

   -- filter by modified after given time
   IF aftertime is not null THEN
      wherecls = wherecls || ' AND mdlstatusModTime >=  ' || quote_literal(aftertime);
   END IF;

   -- filter by epsgcode
   IF epsg is not null THEN
      wherecls = wherecls || ' AND  epsgcode =  ' || epsg;
   END IF;

   -- filter by OccurrenceSet
   IF occsetid is not null THEN
      wherecls = wherecls || ' AND occurrencesetid =  ' || quote_literal(occsetid);
   END IF;

   -- filter by status
   IF stat is not null THEN
      wherecls = wherecls || ' AND mdlstatus =  ' || quote_literal(stat);
   END IF;

   -- filter by 'in-pipeline', aka less than completed status
   IF completestat is not null THEN
      wherecls = wherecls || ' AND mdlstatus <  ' || quote_literal(completestat);
   END IF;
   
   -- filter by Algorithm
   IF algcode is not null THEN
      wherecls = wherecls || ' AND algorithmcode =  ' || quote_literal(algcode);
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
-- lm_listModels
CREATE OR REPLACE FUNCTION lm3.lm_listModelObjects(firstRecNum int, maxNum int, 
                                         usrid varchar(20), 
                                         dispname varchar(256),
                                         beforetime double precision,
                                         aftertime double precision,
                                         epsg int,
                                         stat int,
                                         completestat int,
                                         occsetid int,
                                         algcode varchar)
   RETURNS SETOF lm3.lm_fullModel AS
$$
DECLARE
   rec lm3.lm_fullModel;
   cmd varchar;
   wherecls varchar;
   limitcls varchar;
   ordercls varchar;
BEGIN
   cmd = 'SELECT * FROM lm3.lm_fullModel ';
   wherecls = ' WHERE mdluserid =  ' || quote_literal(usrid) ;
   ordercls = ' ORDER BY mdlstatusModTime DESC ';
   limitcls = ' LIMIT ' || quote_literal(maxNum) || ' OFFSET ' || quote_literal(firstRecNum);

   -- filter by occurrenceset displayname
   IF dispname is not null THEN
      wherecls = wherecls || ' AND displayname like  ' || quote_literal(dispname);
   END IF;

   -- filter by modified before given time
   IF beforetime is not null THEN
      wherecls = wherecls || ' AND mdlstatusModTime <=  ' || quote_literal(beforetime);
   END IF;

   -- filter by modified after given time
   IF aftertime is not null THEN
      wherecls = wherecls || ' AND mdlstatusModTime >=  ' || quote_literal(aftertime);
   END IF;

   -- filter by epsgcode
   IF epsg is not null THEN
      wherecls = wherecls || ' AND  epsgcode =  ' || epsg;
   END IF;

   -- filter by OccurrenceSet
   IF occsetid is not null THEN
      wherecls = wherecls || ' AND occurrencesetid =  ' || quote_literal(occsetid);
   END IF;

   -- filter by status
   IF stat is not null THEN
      wherecls = wherecls || ' AND mdlstatus =  ' || quote_literal(stat);
   END IF;

   -- filter by 'in-pipeline', aka less than completed status
   IF completestat is not null THEN
      wherecls = wherecls || ' AND mdlstatus <  ' || quote_literal(completestat);
   END IF;
   
   -- filter by Algorithm
   IF algcode is not null THEN
      wherecls = wherecls || ' AND algorithmcode =  ' || quote_literal(algcode);
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
-- lm_countProjections
CREATE OR REPLACE FUNCTION lm3.lm_countProjections(usrid varchar, 
                                              dispname varchar,
                                              beforetime double precision, 
                                              aftertime double precision, 
                                              epsg int,
                                              stat int, 
                                              completestat int,
                                              occsetid int, 
                                              mdlid int, 
                                              algcode varchar, 
                                              scenid int)
   RETURNS int AS
$$
DECLARE
   num int;
   cmd varchar;
   wherecls varchar;
BEGIN
   cmd = 'SELECT count(*)
               FROM lm3.lm_fullProjection ';
   wherecls = 'WHERE mdluserid =  ' || quote_literal(usrid) ;

   -- filter by occurrenceset displayname
   IF dispname is not null THEN
      wherecls = wherecls || ' AND displayname like  ' || quote_literal(dispname);
   END IF;

   -- filter by modified before given time
   IF beforetime is not null THEN
      wherecls = wherecls || ' AND prjstatusModTime <=  ' || quote_literal(beforetime);
   END IF;

   -- filter by modified after given time
   IF aftertime is not null THEN
      wherecls = wherecls || ' AND prjstatusModTime >=  ' || quote_literal(aftertime);
   END IF;

   -- filter by epsgcode
   IF epsg is not null THEN
      wherecls = wherecls || ' AND  prjepsgcode =  ' || epsg;
   END IF;
   
   -- filter by OccurrenceSet
   IF occsetid is not null THEN
      wherecls = wherecls || ' AND occurrencesetid =  ' || quote_literal(occsetid);
   END IF;

   -- filter by status
   IF stat is not null THEN
      wherecls = wherecls || ' AND prjstatus =  ' || quote_literal(stat);
   END IF;

   -- filter by 'in-pipeline', aka less than completed status
   IF completestat is not null THEN
      wherecls = wherecls || ' AND prjstatus <  ' || quote_literal(completestat);
   END IF;

   -- filter by Algorithm
   IF algcode is not null THEN
      wherecls = wherecls || ' AND algorithmcode =  ' || quote_literal(algcode);
   END IF;

   -- filter by Model
   IF mdlid is not null THEN
      wherecls = wherecls || ' AND modelid =  ' || quote_literal(mdlid);
   END IF;

   -- filter by Scenario
   IF scenid is not null THEN
      wherecls = wherecls || ' AND prjscenarioId =  ' || quote_literal(scenid);
   END IF;

   cmd := cmd || wherecls;
   RAISE NOTICE 'cmd = %', cmd;

   EXECUTE cmd INTO num;
   RETURN num;
END;
$$  LANGUAGE 'plpgsql' STABLE;

-- ----------------------------------------------------------------------------
-- lm_listProjections
CREATE OR REPLACE FUNCTION lm3.lm_listProjections(firstRecNum int, maxNum int, 
                                              usrid varchar, 
                                              dispname varchar,
                                              beforetime double precision, 
                                              aftertime double precision, 
                                              epsg int,
                                              stat int, 
                                              completestat int,
                                              occsetid int, 
                                              mdlid int, 
                                              algcode varchar, 
                                              scenid int)
   RETURNS SETOF lm3.lm_atom AS
$$
DECLARE
   rec lm3.lm_atom;
   cmd varchar;
   wherecls varchar;
   limitcls varchar;
   ordercls varchar;
BEGIN
   cmd = 'SELECT projectionId, mdlname, prjepsgcode, mdldescription, prjstatusModTime
               FROM lm3.lm_fullProjection ';
   wherecls = 'WHERE mdluserid =  ' || quote_literal(usrid) ;
   ordercls = 'ORDER BY prjstatusModTime DESC, projectionId ASC';
   limitcls = ' LIMIT ' || quote_literal(maxNum) || ' OFFSET ' || quote_literal(firstRecNum);

   -- filter by occurrenceset displayname
   IF dispname is not null THEN
      wherecls = wherecls || ' AND displayname like  ' || quote_literal(dispname);
   END IF;

   -- filter by modified before given time
   IF beforetime is not null THEN
      wherecls = wherecls || ' AND prjstatusModTime <=  ' || quote_literal(beforetime);
   END IF;

   -- filter by modified after given time
   IF aftertime is not null THEN
      wherecls = wherecls || ' AND prjstatusModTime >=  ' || quote_literal(aftertime);
   END IF;

   -- filter by epsgcode
   IF epsg is not null THEN
      wherecls = wherecls || ' AND  prjepsgcode =  ' || epsg;
   END IF;
   
   -- filter by OccurrenceSet
   IF occsetid is not null THEN
      wherecls = wherecls || ' AND occurrencesetid =  ' || quote_literal(occsetid);
   END IF;

   -- filter by status
   IF stat is not null THEN
      wherecls = wherecls || ' AND prjstatus =  ' || quote_literal(stat);
   END IF;

   -- filter by 'in-pipeline', aka less than completed status
   IF completestat is not null THEN
      wherecls = wherecls || ' AND prjstatus <  ' || quote_literal(completestat);
   END IF;
   
   -- filter by Algorithm
   IF algcode is not null THEN
      wherecls = wherecls || ' AND algorithmcode =  ' || quote_literal(algcode);
   END IF;

   -- filter by Model
   IF mdlid is not null THEN
      wherecls = wherecls || ' AND modelid =  ' || quote_literal(mdlid);
   END IF;

   -- filter by Scenario
   IF scenid is not null THEN
      wherecls = wherecls || ' AND prjscenarioId =  ' || quote_literal(scenid);
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
-- lm_listProjections
CREATE OR REPLACE FUNCTION lm3.lm_listProjectionObjects(firstRecNum int, maxNum int, 
                                              usrid varchar, 
                                              dispname varchar,
                                              beforetime double precision, 
                                              aftertime double precision, 
                                              epsg int,
                                              stat int, 
                                              completestat int,
                                              occsetid int, 
                                              mdlid int, 
                                              algcode varchar, 
                                              scenid int)
   RETURNS SETOF lm3.lm_fullProjection AS
$$
DECLARE
   rec lm3.lm_fullProjection;
   cmd varchar;
   wherecls varchar;
   limitcls varchar;
   ordercls varchar;
BEGIN
   cmd = 'SELECT * FROM lm3.lm_fullProjection ';
   wherecls = 'WHERE mdluserid =  ' || quote_literal(usrid) ;
   ordercls = 'ORDER BY prjstatusModTime DESC, projectionId ASC ';
   limitcls = ' LIMIT ' || quote_literal(maxNum) || ' OFFSET ' || quote_literal(firstRecNum);

   -- filter by occurrenceset displayname
   IF dispname is not null THEN
      wherecls = wherecls || ' AND displayname like  ' || quote_literal(dispname);
   END IF;

   -- filter by modified before given time
   IF beforetime is not null THEN
      wherecls = wherecls || ' AND prjstatusModTime <=  ' || quote_literal(beforetime);
   END IF;

   -- filter by modified after given time
   IF aftertime is not null THEN
      wherecls = wherecls || ' AND prjstatusModTime >=  ' || quote_literal(aftertime);
   END IF;

   -- filter by epsgcode
   IF epsg is not null THEN
      wherecls = wherecls || ' AND  prjepsgcode =  ' || epsg;
   END IF;
   
   -- filter by OccurrenceSet
   IF occsetid is not null THEN
      wherecls = wherecls || ' AND occurrencesetid =  ' || quote_literal(occsetid);
   END IF;

   -- filter by status
   IF stat is not null THEN
      wherecls = wherecls || ' AND prjstatus =  ' || quote_literal(stat);
   END IF;

   -- filter by 'in-pipeline', aka less than completed status
   IF completestat is not null THEN
      wherecls = wherecls || ' AND prjstatus <  ' || quote_literal(completestat);
   END IF;
   
   -- filter by Algorithm
   IF algcode is not null THEN
      wherecls = wherecls || ' AND algorithmcode =  ' || quote_literal(algcode);
   END IF;

   -- filter by Model
   IF mdlid is not null THEN
      wherecls = wherecls || ' AND modelid =  ' || quote_literal(mdlid);
   END IF;

   -- filter by Scenario
   IF scenid is not null THEN
      wherecls = wherecls || ' AND prjscenarioId =  ' || quote_literal(scenid);
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
-- lm_listScenarios
CREATE OR REPLACE FUNCTION lm3.lm_listScenarios(firstRecNum int, 
                                            maxNum int,
                                            usrid varchar,
                                            beforetime double precision,
                                            aftertime double precision,
                                            epsg int,
                                            matchingId int,
                                            kywds varchar)
   RETURNS SETOF lm3.lm_atom AS
$$
DECLARE
   rec lm3.lm_atom;
   arrkeys varchar[];
   kywd varchar;
   loopcntr int := 0;
   subname varchar;
   cmd varchar;
   fromcls varchar;
   wherecls varchar;
   limitcls varchar;
   ordercls varchar;
BEGIN
   cmd = 'SELECT s.scenarioid, s.title, s.epsgcode, s.description, s.dateLastModified ';
   fromcls = ' FROM lm3.scenario s ';
   wherecls = ' WHERE s.userid =  ' || quote_literal(usrid) || ' ';
   ordercls = ' ORDER BY s.dateLastModified DESC ';
   limitcls = ' LIMIT ' || quote_literal(maxNum) || ' OFFSET ' || quote_literal(firstRecNum);

   -- filter by a scenario to match
   IF matchingId is not null THEN
      fromcls = fromcls || ', (select * FROM lm3.lm_getMatchingScenariosNoKeywords(' 
                                          || quote_literal(matchingId) 
                                          || ') ) AS ms ';
      wherecls = wherecls || ' AND s.scenarioid = ms.scenarioid ';
   END IF;

   -- filter by keywords
   IF length(kywds) > 1 THEN
      FOR kywd in (select * from regexp_split_to_table(kywds, E'(,)(\ )'))  
      LOOP
         raise notice '%', kywd;
         loopcntr = loopcntr + 1;
         subname = 'cls' || cast(loopcntr as varchar);
         fromcls = fromcls || ', (select sk.scenarioid 
                                  from lm3.scenariokeywords sk, lm3.keyword k 
                                  where sk.keywordid = k.keywordid 
                                    and k.keyword = ' || quote_literal(trim(kywd)) 
                              || ') AS ' || subname;
         wherecls = wherecls || ' AND s.scenarioid = ' || subname || '.scenarioid ';
      END LOOP;
   END IF;
   
   -- filter by scenarios modified before given time
   IF beforetime is not null THEN
      wherecls = wherecls || ' AND s.dateLastModified <=  ' || quote_literal(beforetime);
   END IF;

   -- filter by scenarios modified after given time
   IF aftertime is not null THEN
      wherecls = wherecls || ' AND s.dateLastModified >=  ' || quote_literal(aftertime);
   END IF;

   -- filter by epsg code
   IF epsg is not null THEN
      wherecls = wherecls || ' AND s.epsgcode =  ' || epsg;
   END IF;

   cmd := cmd || fromcls || wherecls || ordercls || limitcls;

   RAISE NOTICE 'cmd = %', cmd;

   FOR rec in EXECUTE cmd
      LOOP 
         RETURN NEXT rec;
      END LOOP;
   RETURN;
END;
$$  LANGUAGE 'plpgsql' STABLE;

-- ----------------------------------------------------------------------------
-- lm_listScenarioObjects
CREATE OR REPLACE FUNCTION lm3.lm_listScenarioObjects(firstRecNum int, 
                                            maxNum int,
                                            usrid varchar,
                                            beforetime double precision,
                                            aftertime double precision,
                                            epsg int,
                                            matchingId int,
                                            kywds varchar)
   RETURNS SETOF lm3.scenario AS
$$
DECLARE
   rec lm3.scenario;
   arrkeys varchar[];
   kywd varchar;
   loopcntr int := 0;
   subname varchar;
   cmd varchar;
   fromcls varchar;
   wherecls varchar;
   limitcls varchar;
   ordercls varchar;
BEGIN
   cmd = 'SELECT * ';
   fromcls = ' FROM lm3.scenario s ';
   wherecls = ' WHERE s.userid =  ' || quote_literal(usrid) || ' ';
   ordercls = ' ORDER BY s.dateLastModified DESC ';
   limitcls = ' LIMIT ' || quote_literal(maxNum) || ' OFFSET ' || quote_literal(firstRecNum);

   -- filter by a scenario to match
   IF matchingId is not null THEN
      fromcls = fromcls || ', (select * FROM lm3.lm_getMatchingScenariosNoKeywords(' 
                                          || quote_literal(matchingId) 
                                          || ') ) AS ms ';
      wherecls = wherecls || ' AND s.scenarioid = ms.scenarioid ';
   END IF;

   -- filter by keywords
   IF length(kywds) > 1 THEN
      FOR kywd in (select * from regexp_split_to_table(kywds, E'(,)(\ )'))  
      LOOP
         raise notice '%', kywd;
         loopcntr = loopcntr + 1;
         subname = 'cls' || cast(loopcntr as varchar);
         fromcls = fromcls || ', (select sk.scenarioid 
                                  from lm3.scenariokeywords sk, lm3.keyword k 
                                  where sk.keywordid = k.keywordid 
                                    and k.keyword = ' || quote_literal(trim(kywd)) 
                              || ') AS ' || subname;
         wherecls = wherecls || ' AND s.scenarioid = ' || subname || '.scenarioid ';
      END LOOP;
   END IF;
   
   -- filter by scenarios modified before given time
   IF beforetime is not null THEN
      wherecls = wherecls || ' AND s.dateLastModified <=  ' || quote_literal(beforetime);
   END IF;

   -- filter by scenarios modified after given time
   IF aftertime is not null THEN
      wherecls = wherecls || ' AND s.dateLastModified >=  ' || quote_literal(aftertime);
   END IF;

   -- filter by epsg code
   IF epsg is not null THEN
      wherecls = wherecls || ' AND s.epsgcode =  ' || epsg;
   END IF;

   cmd := cmd || fromcls || wherecls || ordercls || limitcls;

   RAISE NOTICE 'cmd = %', cmd;

   FOR rec in EXECUTE cmd
      LOOP 
         RETURN NEXT rec;
      END LOOP;
   RETURN;
END;
$$  LANGUAGE 'plpgsql' STABLE;

-- ----------------------------------------------------------------------------
-- lm_countScenarios
CREATE OR REPLACE FUNCTION lm3.lm_countScenarios(usrid varchar,
                                             beforetime double precision,
                                             aftertime double precision,
                                             epsg int,
                                             matchingId int,
                                             kywds varchar)
   RETURNS int AS
$$
DECLARE
   num int;
   arrkeys varchar[];
   kywd varchar;
   loopcntr int := 0;
   subname varchar;
   cmd varchar;
   fromcls varchar;
   wherecls varchar;
BEGIN
   cmd = 'SELECT count(*) ';
   fromcls = ' FROM lm3.scenario s ';
   wherecls = ' WHERE s.userid =  ' || quote_literal(usrid) || ' ';

   -- filter by a scenario to match
   IF matchingId is not null THEN
      fromcls = fromcls || ', (select * FROM lm3.lm_getMatchingScenariosNoKeywords(' 
                                          || quote_literal(matchingId) 
                                          || ') ) AS ms ';
      wherecls = wherecls || ' AND s.scenarioid = ms.scenarioid ';
   END IF;

   -- filter by keywords
   IF length(kywds) > 1 THEN
      FOR kywd in (select * from regexp_split_to_table(kywds, E'(,)(\ )'))  LOOP
         raise notice '%', kywd;
         loopcntr = loopcntr + 1;
         subname = 'cls' || cast(loopcntr as varchar);
         fromcls = fromcls || ', (select sk.scenarioid 
                                  from lm3.scenariokeywords sk, lm3.keyword k 
                                  where sk.keywordid = k.keywordid 
                                    and k.keyword = ' || quote_literal(trim(kywd)) 
                              || ') AS ' || subname;
         wherecls = wherecls || ' AND s.scenarioid = ' || subname || '.scenarioid ';
      END LOOP;
   END IF;
   
   -- filter by scenarios modified before given time
   IF beforetime is not null THEN
      wherecls = wherecls || ' AND s.dateLastModified <=  ' || quote_literal(beforetime);
   END IF;

   -- filter by scenarios modified after given time
   IF aftertime is not null THEN
      wherecls = wherecls || ' AND s.dateLastModified >=  ' || quote_literal(aftertime);
   END IF;

   -- filter by epsg code
   IF epsg is not null THEN
      wherecls = wherecls || ' AND s.epsgcode =  ' || epsg;
   END IF;

   cmd := cmd || fromcls || wherecls;
   RAISE NOTICE 'cmd = %', cmd;

   EXECUTE cmd INTO num;
   RETURN num;
END;
$$  LANGUAGE 'plpgsql' STABLE;

-- ----------------------------------------------------------------------------
-- lm_listLayers
CREATE OR REPLACE FUNCTION lm3.lm_listLayers(firstRecNum int, 
                                             maxNum int, 
                                             usrid varchar,
                                             tcode varchar,
                                             beforetime double precision,
                                             aftertime double precision,
                                             epsg int,
                                             iscat boolean,
                                             scenid int)
   RETURNS SETOF lm3.lm_atom AS
$$
DECLARE
   cmd varchar;
   lyrname varchar;
   fromcls varchar;
   wherecls varchar;
   limitcls varchar;
   ordercls varchar;
   rec lm3.lm_atom;
BEGIN
   cmd = 'SELECT l.layerid, l.title, l.epsgcode, l.description, l.dateLastModified, l.name ';
   fromcls = ' FROM lm3.lm_envlayer l ';
   wherecls = 'WHERE userid =  ' || quote_literal(usrid) || ' ';
   ordercls = ' ORDER BY l.dateLastModified DESC ';
   limitcls = ' LIMIT ' || quote_literal(maxNum) || ' OFFSET ' || quote_literal(firstRecNum);

   -- filter by a scenario to match
   IF scenid is not null THEN
      fromcls = fromcls || ', (select * FROM lm3.scenariolayers where scenarioid = '
                                          || quote_literal(scenid) 
                                          || ') AS lxs ';
      wherecls = wherecls || ' AND  l.layerid = lxs.layerid ';
   END IF;
   RAISE NOTICE 'fromcls = %', fromcls;
   RAISE NOTICE 'wherecls = %', wherecls;

   -- filter by typecode
   IF tcode is not null THEN
      wherecls = wherecls || ' AND  l.typecode = ' || quote_literal(tcode);
   END IF;

   -- filter by categorical layers
   IF iscat is not null THEN
      IF iscat is True THEN
         wherecls = wherecls || ' AND  l.isCategorical is True';
      ELSE
         wherecls = wherecls || ' AND  l.isCategorical is False';
      END IF;
   END IF;

   -- filter by epsgcode
   IF epsg is not null THEN
      wherecls = wherecls || ' AND  l.epsgcode =  ' || epsg;
   END IF;
   
   -- filter by layers modified before given time
   IF beforetime is not null THEN
      wherecls = wherecls || ' AND  l.dateLastModified <=  ' || quote_literal(beforetime);
   END IF;

   -- filter by layers modified after given time
   IF aftertime is not null THEN
      wherecls = wherecls || ' AND  l.dateLastModified >=  ' || quote_literal(aftertime);
   END IF;

   -- assemble query
   cmd := cmd || fromcls || wherecls || ordercls || limitcls;

   RAISE NOTICE 'cmd = %', cmd;

   -- run command
   FOR rec.id, rec.title, rec.epsgcode, rec.description, rec.modtime, lyrname 
      in EXECUTE cmd
      LOOP
         IF rec.title IS null THEN
            rec.title = lyrname;
         ELSE
            rec.title = lyrname || ': ' || rec.title;
         END IF;
         RETURN NEXT rec;
      END LOOP;
   RETURN;
END;
$$  LANGUAGE 'plpgsql' STABLE;


-- ----------------------------------------------------------------------------
-- lm_listLayers
CREATE OR REPLACE FUNCTION lm3.lm_listLayerObjects(firstRecNum int, 
                                             maxNum int, 
                                             usrid varchar,
                                             tcode varchar,
                                             beforetime double precision,
                                             aftertime double precision,
                                             epsg int,
                                             iscat boolean,
                                             scenid int)
   RETURNS SETOF lm3.lm_envlayerAndKeywords AS
$$
DECLARE
   cmd varchar;
   lyrname varchar;
   fromcls varchar;
   wherecls varchar;
   limitcls varchar;
   ordercls varchar;
   keystr varchar;
   rec lm3.lm_envlayerAndKeywords;
BEGIN
   cmd = 'SELECT * ';
   fromcls = ' FROM lm3.lm_envlayer l ';
   wherecls = 'WHERE userid =  ' || quote_literal(usrid) || ' ';
   ordercls = ' ORDER BY l.dateLastModified DESC ';
   limitcls = ' LIMIT ' || quote_literal(maxNum) || ' OFFSET ' || quote_literal(firstRecNum);

   -- filter by a scenario to match
   IF scenid is not null THEN
      fromcls = fromcls || ', (select * FROM lm3.scenariolayers where scenarioid = '
                                          || quote_literal(scenid) 
                                          || ') AS lxs ';
      wherecls = wherecls || ' AND  l.layerid = lxs.layerid ';
   END IF;
   RAISE NOTICE 'fromcls = %', fromcls;
   RAISE NOTICE 'wherecls = %', wherecls;

   -- filter by typecode
   IF tcode is not null THEN
      wherecls = wherecls || ' AND  l.typecode = ' || quote_literal(tcode);
   END IF;

   -- filter by categorical layers
   IF iscat is not null THEN
      IF iscat is True THEN
         wherecls = wherecls || ' AND  l.isCategorical is True';
      ELSE
         wherecls = wherecls || ' AND  l.isCategorical is False';
      END IF;
   END IF;

   -- filter by epsgcode
   IF epsg is not null THEN
      wherecls = wherecls || ' AND  l.epsgcode =  ' || epsg;
   END IF;
   
   -- filter by layers modified before given time
   IF beforetime is not null THEN
      wherecls = wherecls || ' AND  l.dateLastModified <=  ' || quote_literal(beforetime);
   END IF;

   -- filter by layers modified after given time
   IF aftertime is not null THEN
      wherecls = wherecls || ' AND  l.dateLastModified >=  ' || quote_literal(aftertime);
   END IF;

   -- assemble query
   cmd := cmd || fromcls || wherecls || ordercls || limitcls;

   RAISE NOTICE 'cmd = %', cmd;

   -- run command
   FOR rec in EXECUTE cmd
      LOOP
         SELECT INTO keystr lm3.lm_getLayerTypeKeywordString(rec.layertypeid);
         rec.keywords = keystr;
         RETURN NEXT rec;
      END LOOP;
   RETURN;
END;
$$  LANGUAGE 'plpgsql' STABLE;

-- ----------------------------------------------------------------------------
-- lm_countLayers
CREATE OR REPLACE FUNCTION lm3.lm_countLayers(usrid varchar,
                                          tcode varchar,
                                          beforetime double precision,
                                          aftertime double precision,
                                          epsg int,
                                          iscat boolean,
                                          scenid int)
   RETURNS int AS
$$
DECLARE
   num int := -1;
   cmd varchar;
   fromcls varchar;
   wherecls varchar;
BEGIN
   cmd = 'SELECT count(*) ';
   fromcls = ' FROM lm3.lm_envlayer l ';
   wherecls = 'WHERE userid =  ' || quote_literal(usrid) || ' ';

   -- filter by a scenario to match
   IF scenid is not null THEN
      fromcls = fromcls || ', (select * FROM lm3.scenariolayers where scenarioid = '
                                          || quote_literal(scenid) 
                                          || ') AS lxs ';
      wherecls = wherecls || ' AND l.layerid = lxs.layerid ';
   END IF;
   
   -- filter by typecode
   IF tcode is not null THEN
      wherecls = wherecls || ' AND  l.typecode = ' || quote_literal(tcode);
   END IF;

   -- filter by categorical layers
   IF iscat is not null THEN
      IF iscat is True THEN
         wherecls = wherecls || ' AND  l.isCategorical is True';
      ELSE
         wherecls = wherecls || ' AND  l.isCategorical is False';
      END IF;
   END IF;

   -- filter by epsgcode
   IF epsg is not null THEN
      wherecls = wherecls || ' AND  l.epsgcode =  ' || epsg;
   END IF;

   -- filter by layers modified before given time
   IF beforetime is not null THEN
      wherecls = wherecls || ' AND l.dateLastModified <=  ' || quote_literal(beforetime);
   END IF;

   -- filter by layers modified after given time
   IF aftertime is not null THEN
      wherecls = wherecls || ' AND l.dateLastModified >=  ' || quote_literal(aftertime);
   END IF;

   -- assemble query
   cmd := cmd || fromcls || wherecls;

   RAISE NOTICE 'cmd = %', cmd;

   -- run command
   EXECUTE cmd INTO num;
   RETURN num;
END;
$$  LANGUAGE 'plpgsql' STABLE;


-- ----------------------------------------------------------------------------
-- lm_getLayer
CREATE OR REPLACE FUNCTION lm3.lm_getLayer(id int)
   RETURNS lm3.lm_envlayerAndKeywords AS
$$
DECLARE
   rec lm3.lm_envlayerAndKeywords%ROWTYPE;
   keystr varchar;
BEGIN
   BEGIN
      SELECT *
         INTO rec FROM lm3.lm_envlayer
         WHERE layerid = id;
      
      EXCEPTION
         WHEN NO_DATA_FOUND THEN
            RAISE NOTICE 'Layer % not found', id;
         WHEN TOO_MANY_ROWS THEN
            RAISE EXCEPTION 'Layer % not unique', id;
   END;
   IF FOUND THEN
      SELECT INTO keystr lm3.lm_getLayerTypeKeywordString(rec.layertypeid);
      rec.keywords = keystr;
   END IF;
   RETURN rec;
END;
$$  LANGUAGE 'plpgsql' STABLE;

-- ----------------------------------------------------------------------------
-- lm_getLayer
-- Returns an empty row if none found
--
CREATE OR REPLACE FUNCTION lm3.lm_getLayerByUrl(url varchar)
   RETURNS lm3.lm_envlayerAndKeywords AS
$$
DECLARE
   rec lm3.lm_envlayerAndKeywords%ROWTYPE;
   keystr varchar;
BEGIN
   BEGIN
      SELECT * INTO STRICT rec FROM lm3.lm_envlayer
         WHERE metadataUrl = url;
      
      EXCEPTION
         WHEN NO_DATA_FOUND THEN
            RAISE NOTICE 'Layer not found with url %', url;
         WHEN TOO_MANY_ROWS THEN
            RAISE EXCEPTION 'Layer with url % not unique', url;
   END;
   IF FOUND THEN 
      SELECT INTO keystr lm3.lm_getLayerTypeKeywordString(rec.layertypeid);
      rec.keywords = keystr;
   END IF;
   RETURN rec;
END;
$$  LANGUAGE 'plpgsql' STABLE;

-- ----------------------------------------------------------------------------
-- ----------------------------------------------------------------------------
-- lm_countOccSets
CREATE OR REPLACE FUNCTION lm3.lm_countOccurrenceSets(minOccCount int,
                                                  hasProjections boolean,
                                                  uid varchar,
                                                  dispname varchar,
                                                  beforetime double precision,
                                                  aftertime double precision,
                                                  epsg int,
                                                  stat int, 
                                                  completestat int)
   RETURNS int AS
$$
DECLARE
   num int;
   cmd varchar;
   fromcls varchar;
   wherecls varchar;
BEGIN
   cmd = 'SELECT count(*)';
   fromcls = ' FROM lm3.occurrenceset o';
   wherecls = ' WHERE o.querycount >= ' || quote_literal(minOccCount) ||
                ' AND o.userid = ' || quote_literal(uid);
                
   RAISE NOTICE 'wherecls = %', wherecls;

   -- filter by a displayname
   IF dispname is not null THEN
      wherecls = wherecls || ' AND o.displayname like ' || quote_literal(dispname);
   END IF;

   -- filter by hasProjections
   IF hasProjections is True THEN
      wherecls = wherecls || 
      ' AND (SELECT count(*) FROM lm3.lm_fullprojection fp WHERE o.occurrencesetid = fp.occurrencesetid) > 0 ';
   END IF;

   -- filter by layers modified before given time
   IF beforetime is not null THEN
      wherecls = wherecls || ' AND o.dateLastModified <=  ' || quote_literal(beforetime);
   END IF;

   -- filter by layers modified after given time
   IF aftertime is not null THEN
      wherecls = wherecls || 'AND o.dateLastModified >=  ' || quote_literal(aftertime);
   END IF;
   
   -- filter by epsgcode
   IF epsg is not null THEN
      wherecls = wherecls || ' AND  epsgcode =  ' || epsg;
   END IF;

   -- filter by status
   IF stat is not null THEN
      wherecls = wherecls || ' AND status =  ' || quote_literal(stat);
   END IF;

   -- filter by 'in-pipeline', aka less than completed status
   IF completestat is not null THEN
      wherecls = wherecls || ' AND status <  ' || quote_literal(completestat);
   END IF;
   
   cmd := cmd || fromcls || wherecls;

   RAISE NOTICE 'cmd = %', cmd;

   -- run command
   EXECUTE cmd INTO num;
   return num;
END;
$$  LANGUAGE 'plpgsql' STABLE;

-- ----------------------------------------------------------------------------
-- lm_listOccurrenceSets
-- hasProjections filters on OccurrenceSets with models and projections if True,
-- no filter if False 
CREATE OR REPLACE FUNCTION lm3.lm_listOccurrenceSets(firstRecNum int, 
                                                 maxNum int, 
                                                 minOccCount int,
                                                 hasProjections boolean,
                                                 uid varchar,
                                                 dispname varchar,
                                                 beforetime double precision,
                                                 aftertime double precision,
                                                 epsg int, 
                                                 stat int,
                                                 completestat int)
   RETURNS SETOF lm3.lm_atom AS
$$
DECLARE
   cmd varchar;
   fromcls varchar;
   wherecls varchar;
   limitcls varchar;
   ordercls varchar;
   rec lm3.lm_atom;
BEGIN
   cmd = 'SELECT o.occurrenceSetId, o.displayName, o.epsgcode, null, o.dateLastModified';
   fromcls = ' FROM lm3.occurrenceset o ';
   wherecls = ' WHERE o.querycount >= ' || quote_literal(minOccCount) ||
                ' AND o.userid = ' || quote_literal(uid);
   ordercls = ' ORDER BY o.displayname ASC, o.dateLastChecked ASC ';
   limitcls = ' LIMIT ' || quote_literal(maxNum) || ' OFFSET ' || quote_literal(firstRecNum);

   RAISE NOTICE 'fromcls = %', fromcls;
   RAISE NOTICE 'wherecls = %', wherecls;
   RAISE NOTICE 'ordercls = %', ordercls;
   RAISE NOTICE 'limitcls = %', limitcls;
   RAISE NOTICE 'cmd = %', cmd;

   -- filter by a displayname
   IF dispname is not null THEN
      wherecls = wherecls || ' AND o.displayname like ' || quote_literal(dispname);
   END IF;

   -- filter by hasProjections
   IF hasProjections is True THEN
      wherecls = wherecls || 
      ' AND (SELECT count(*) FROM lm3.lm_fullprojection fp WHERE o.occurrencesetid = fp.occurrencesetid) > 0 ';
   END IF;
   
   -- filter by status
   IF stat is not null THEN
      wherecls = wherecls || ' AND status =  ' || quote_literal(stat);
   END IF;

   -- filter by 'in-pipeline', aka less than completed status
   IF completestat is not null THEN
      wherecls = wherecls || ' AND status <  ' || quote_literal(completestat);
   END IF;

   -- filter by layers modified before given time
   IF beforetime is not null THEN
      wherecls = wherecls || ' AND o.dateLastModified <=  ' || quote_literal(beforetime);
   END IF;

   -- filter by layers modified after given time
   IF aftertime is not null THEN
      wherecls = wherecls || 'AND o.dateLastModified >=  ' || quote_literal(aftertime);
   END IF;
   
   -- filter by epsgcode
   IF epsg is not null THEN
      wherecls = wherecls || ' AND  epsgcode =  ' || epsg;
   END IF;   

   RAISE NOTICE 'fromcls = %', fromcls;
   RAISE NOTICE 'wherecls = %', wherecls;
   RAISE NOTICE 'ordercls = %', ordercls;
   RAISE NOTICE 'limitcls = %', limitcls;
   RAISE NOTICE 'cmd = %', cmd;
   cmd := cmd || fromcls || wherecls || ordercls || limitcls;

   RAISE NOTICE 'cmd = %', cmd;

   -- run command
   FOR rec in EXECUTE cmd
      LOOP
         RETURN NEXT rec;
      END LOOP;
   RETURN;
END;
$$  LANGUAGE 'plpgsql' STABLE;

-- ----------------------------------------------------------------------------
-- ----------------------------------------------------------------------------
-- lm_listOccurrenceSets
-- hasProjections filters on OccurrenceSets with models and projections if True,
-- no filter if False 
CREATE OR REPLACE FUNCTION lm3.lm_listOccurrenceSetObjects(firstRecNum int, 
                                                 maxNum int, 
                                                 minOccCount int,
                                                 hasProjections boolean,
                                                 uid varchar,
                                                 dispname varchar,
                                                 beforetime double precision,
                                                 aftertime double precision,
                                                 epsg int, 
                                                 stat int, 
                                                 completestat int)
   RETURNS SETOF lm3.occurrenceset AS
$$
DECLARE
   cmd varchar;
   fromcls varchar;
   wherecls varchar;
   limitcls varchar;
   ordercls varchar;
   rec lm3.occurrenceset;
BEGIN
   cmd = 'SELECT * ';
   fromcls = ' FROM lm3.occurrenceset o ';
   wherecls = ' WHERE o.querycount >= ' || quote_literal(minOccCount) ||
                ' AND o.userid = ' || quote_literal(uid);
   -- ordercls = ' ORDER BY o.displayname ASC ';
   ordercls = ' ORDER BY o.dateLastModified DESC ';
   limitcls = ' LIMIT ' || quote_literal(maxNum) || ' OFFSET ' || quote_literal(firstRecNum);

   -- filter by a displayname
   IF dispname is not null THEN
      wherecls = wherecls || ' AND o.displayname like ' || quote_literal(dispname);
   END IF;

   -- filter by hasProjections
   IF hasProjections is True THEN
      wherecls = wherecls || 
      ' AND (SELECT count(*) FROM lm3.lm_fullprojection fp WHERE o.occurrencesetid = fp.occurrencesetid) > 0 ';
   END IF;

   -- filter by layers modified before given time
   IF beforetime is not null THEN
      wherecls = wherecls || ' AND o.dateLastModified <=  ' || quote_literal(beforetime);
   END IF;

   -- filter by layers modified after given time
   IF aftertime is not null THEN
      wherecls = wherecls || 'AND o.dateLastModified >=  ' || quote_literal(aftertime);
   END IF;
   
   -- filter by epsgcode
   IF epsg is not null THEN
      wherecls = wherecls || ' AND  epsgcode =  ' || epsg;
   END IF;   

   -- filter by status
   IF stat is not null THEN
      wherecls = wherecls || ' AND status =  ' || quote_literal(stat);
   END IF;

   -- filter by 'in-pipeline', aka less than completed status
   IF completestat is not null THEN
      wherecls = wherecls || ' AND status <  ' || quote_literal(completestat);
   END IF;

   cmd := cmd || fromcls || wherecls || ordercls || limitcls;

   RAISE NOTICE 'cmd = %', cmd;

   -- run command
   FOR rec in EXECUTE cmd
      LOOP
         RETURN NEXT rec;
      END LOOP;
   RETURN;
END;
$$  LANGUAGE 'plpgsql' STABLE;

-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lm3.lm_getFullOccurrenceSetsSubspecies(maxNum int, 
                                                                  uid varchar)
   RETURNS SETOF lm3.lm_fulloccurrenceset AS
$$
DECLARE
   rec lm3.lm_fulloccurrenceset;
BEGIN
   -- run command
   FOR rec in 
      SELECT * FROM lm3.lm_fulloccurrenceset
         WHERE userid = uid 
           AND scientificnameid is not null
           AND taxonomyKey != specieskey
           AND taxonomyKey != genuskey
           ORDER BY status ASC
         LIMIT maxNum
      LOOP
         RETURN NEXT rec;
      END LOOP;
   RETURN;
END;
$$  LANGUAGE 'plpgsql' STABLE;

-- ----------------------------------------------------------------------------
-- lm_getOccurrenceSet
CREATE OR REPLACE FUNCTION lm3.lm_getOccurrenceSet(id int)
   RETURNS lm3.occurrenceset AS
$$
DECLARE
   rec lm3.occurrenceset%ROWTYPE;
BEGIN
   BEGIN
      SELECT * INTO STRICT rec FROM lm3.occurrenceset 
         WHERE occurrencesetid = id;
         
      EXCEPTION
         WHEN NO_DATA_FOUND THEN
            RAISE NOTICE 'OccurrenceSet % not found', id;
         WHEN TOO_MANY_ROWS THEN
            RAISE EXCEPTION 'OccurrenceSet % not unique', id;
   END;
   RETURN rec;
END;
$$  LANGUAGE 'plpgsql' STABLE;

-- ----------------------------------------------------------------------------
-- SELECT INTO keystr lm3.lm_getScenarioKeywordString(scenid);
CREATE OR REPLACE FUNCTION lm3.lm_getScenarioKeywordString(id int)
   RETURNS varchar AS
$$
DECLARE
   lyrkeyword record;
   keystr varchar := '';
BEGIN
   FOR lyrkeyword in SELECT k.*
                     FROM lm3.keyword k, lm3.scenariokeywords sk
                     WHERE sk.scenarioid = id
                       AND k.keywordid = sk.keywordid
   LOOP
      IF keystr = '' THEN
         keystr := lyrkeyword.keyword;
      ELSE
         keystr := keystr || ',' || lyrkeyword.keyword;
      END IF;
   END LOOP;
   RETURN keystr;
END;
$$  LANGUAGE 'plpgsql' STABLE;    

-- ----------------------------------------------------------------------------
-- SELECT INTO keystr lm3.lm_getLayerTypeKeywordString(lyrtypeid);
CREATE OR REPLACE FUNCTION lm3.lm_getLayerTypeKeywordString(id int)
   RETURNS varchar AS
$$
DECLARE
   lyrkeyword record;
   keystr varchar := '';
BEGIN
   FOR lyrkeyword in SELECT k.*
                     FROM lm3.keyword k, lm3.layertypekeyword lk
                     WHERE lk.layertypeid = id
                       AND k.keywordid = lk.keywordid
   LOOP
      IF keystr = '' THEN
         keystr := lyrkeyword.keyword;
      ELSE
         keystr := keystr || ',' || lyrkeyword.keyword;
      END IF;
   END LOOP;
   RETURN keystr;
END;
$$  LANGUAGE 'plpgsql' STABLE;    


-- ----------------------------------------------------------------------------
-- Insert a statistics query and value 
CREATE OR REPLACE FUNCTION lm3.lm_insertStatistic(keyname varchar, 
                                              qry varchar, descr varchar)
   RETURNS int AS
$$
DECLARE
   id int := -1;
   cnt int;
BEGIN
   SELECT INTO cnt count(*) FROM lm3.Statistics WHERE key = keyname;
   IF cnt = 0 THEN
      BEGIN
         INSERT INTO lm3.Statistics (key, query, description)
            VALUES (keyname, qry, descr);
         IF FOUND THEN
            SELECT INTO id last_value FROM lm3.statistics_statisticsid_seq;
         END IF;
      END;
   END IF;
   RETURN id;
END;
$$  LANGUAGE 'plpgsql' VOLATILE;

-- ----------------------------------------------------------------------------
-- Return statistics row 
CREATE OR REPLACE FUNCTION lm3.lm_getStatistic(keyname varchar)
   RETURNS lm3.statistics AS
$$
DECLARE
   rec lm3.statistics%ROWTYPE;
BEGIN
   BEGIN
      SELECT * INTO STRICT rec FROM lm3.Statistics WHERE key = keyname;
      EXCEPTION
         WHEN NO_DATA_FOUND THEN
            RAISE NOTICE 'Statistic % not found', keyname;
         WHEN TOO_MANY_ROWS THEN
            RAISE EXCEPTION 'Statistic % not unique', keyname;
   END;
   RETURN rec;
END;
$$  LANGUAGE 'plpgsql' STABLE;


-- ----------------------------------------------------------------------------
-- Return all pre-computed statistics rows 
CREATE OR REPLACE FUNCTION lm3.lm_getAllStatistics()
   RETURNS SETOF lm3.statistics AS
$$
DECLARE
   rec lm3.statistics%ROWTYPE;
BEGIN
   FOR rec in SELECT * FROM lm3.statistics
      LOOP
         RETURN NEXT rec;
      END LOOP;
   RETURN;
END;
$$  LANGUAGE 'plpgsql' STABLE;


-- ----------------------------------------------------------------------------
-- Insert a statistics query and value 
CREATE OR REPLACE FUNCTION lm3.lm_returnStatisticQuery(keyname varchar)
   RETURNS varchar AS
$$
DECLARE
   qry varchar;
BEGIN
   BEGIN
      SELECT query INTO STRICT qry FROM lm3.Statistics WHERE key = keyname;
      EXCEPTION
         WHEN NO_DATA_FOUND THEN
            RAISE EXCEPTION 'Statistic % not found', keyname;
         WHEN TOO_MANY_ROWS THEN
            RAISE EXCEPTION 'Statistic % not unique', keyname;
   END;
   RETURN qry;
END;
$$  LANGUAGE 'plpgsql' STABLE;

-- ----------------------------------------------------------------------------
-- Insert a statistics query and value 
CREATE OR REPLACE FUNCTION lm3.lm_updateStatistic(keyname varchar, val int, modtime double precision)
   RETURNS int AS
$$
DECLARE
   success int := -1;
BEGIN
   UPDATE lm3.Statistics SET (value, dateLastModified) = (val, modtime) 
      WHERE key = keyname;
   IF FOUND THEN
      success := 0;
   END IF;
   RETURN success;
END;
$$  LANGUAGE 'plpgsql' VOLATILE;

-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lm3.lm_callStatisticQuery(keyname varchar)
   RETURNS int AS
$$
DECLARE
   qry varchar;
   resp int;
BEGIN
   BEGIN
      SELECT query INTO STRICT qry FROM lm3.Statistics WHERE key = keyname;
      EXCEPTION
         WHEN NO_DATA_FOUND THEN
            RAISE EXCEPTION 'Statistic % not found', keyname;
         WHEN TOO_MANY_ROWS THEN
            RAISE EXCEPTION 'Statistic % not unique', keyname;
   END;
   SELECT INTO resp qry;
   RETURN resp;
END;
$$  LANGUAGE 'plpgsql' STABLE;

-- ----------------------------------------------------------------------------
-- lm_countJobs
-- ----------------------------------------------------------------------------
-- TODO: check this for efficiency
CREATE OR REPLACE FUNCTION lm3.lm_getModelNumbers(usr varchar, completestat int)
   RETURNS SETOF lm3.lm_occStats AS
$$
DECLARE
   rec lm3.lm_occStats%rowtype;
   totalprojComplete int;
BEGIN
   FOR rec.occurrenceSetId, rec.displayname, rec.datelastmodified, rec.querycount, 
       rec.totalmodels, totalprojComplete IN
           SELECT occurrencesetid, displayname, datelastmodified, querycount, 
                  count(distinct(modelid)), 
                  SUM(CASE WHEN prjstatus = completestat THEN 1 ELSE 0 END)
              FROM lm3.lm_fullprojection
              WHERE mdlUserId = usr AND querycount > 0 and occstatus = completestat
              GROUP BY occurrencesetid, displayname, datelastmodified, querycount
      LOOP
         IF totalprojComplete = 0 THEN
            rec.totalmodels = 0;
         END IF;
         RETURN NEXT rec;
      END LOOP;
   RETURN;
END;
$$ LANGUAGE 'plpgsql' STABLE; 

-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lm3.lm_getOccurrenceWOModelNumbers(usr varchar)
   RETURNS SETOF lm3.lm_occStats AS
$$
DECLARE
   rec lm3.lm_occStats%rowtype;
   totalComplete int;
BEGIN
   FOR rec.occurrenceSetId, rec.displayname, rec.datelastmodified, rec.querycount, rec.totalmodels in 
        SELECT o.occurrencesetid, o.displayname, o.datelastmodified, o.querycount, count(distinct(m.modelid) )
              FROM lm3.occurrenceset o 
              LEFT JOIN lm3.model m ON o.occurrencesetid = m.occurrencesetid
              WHERE o.userid = usr and o.querycount > 0
              GROUP BY o.occurrenceSetId, o.displayname, o.datelastmodified, o.querycount
      LOOP
         if rec.totalmodels = 0 THEN
            RETURN NEXT rec;
         end if;
      END LOOP;
   RETURN;
END;
$$ LANGUAGE 'plpgsql' STABLE; 
              
-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lm3.lm_findOldUnmodeledOccurrencesets(usr varchar, 
                                                             expiretime double precision, 
                                                             lmt int, ofst int)
   RETURNS SETOF int AS
$$
DECLARE
   id int;
   mdltotal int;
BEGIN
   FOR id in SELECT occurrencesetid FROM lm3.occurrenceset 
              WHERE userId = usr AND fromgbif is true 
                 and querycount > 0 
                 and datelastmodified < expiretime
                 order by occurrencesetid 
                 limit lmt offset ofst
      LOOP
         SELECT count(*) into mdltotal FROM lm3.model 
         where occurrencesetid = id;
         IF mdltotal = 0 THEN
            RETURN NEXT id;
         END IF;
      END LOOP;
   RETURN;
END;
$$ LANGUAGE 'plpgsql' STABLE; 

-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lm3.lm_deleteExperiments(stat int)
   RETURNS void AS
$$
DECLARE
   success int = -1;
BEGIN
   DELETE FROM lm3.projection p USING model m WHERE p.modelid = m.modelid AND m.status = stat;
   DELETE FROM lm3.model WHERE status = stat;
END;
$$ LANGUAGE 'plpgsql' VOLATILE; 

-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lm3.lm_deleteOccAndDependentObjects(occid int, usr varchar)
   RETURNS int AS
$$
DECLARE
   success int = -1;
   mdlcount int = -1;
   prjcount int = -1;
   jbcount int = 0;
   rowcount int = 0;
BEGIN
   -- Message Jobs
   DELETE FROM lm3.lmjob j USING lm_msgJob mj
      WHERE j.lmjobid = mj.lmjobid 
        AND mj.occurrencesetid = occid AND mj.mdluserid = usr;
   GET DIAGNOSTICS rowcount = ROW_COUNT;  
   jbcount = jbcount + rowcount;
   
   -- Projection Jobs
   DELETE FROM lm3.lmjob j USING lm_prjJob pj
      WHERE j.lmjobid = pj.lmjobid 
        AND pj.occurrencesetid = occid AND pj.mdluserid = usr;
   GET DIAGNOSTICS rowcount = ROW_COUNT;     
   jbcount = jbcount + rowcount;
   
   -- Model Jobs
   DELETE FROM lm3.lmjob j USING lm_mdlJob mj
      WHERE j.lmjobid = mj.lmjobid 
        AND mj.occurrencesetid = occid AND mj.mdluserid = usr;
   GET DIAGNOSTICS rowcount = ROW_COUNT;    
   jbcount = jbcount + rowcount;
        
   -- Occ Jobs
   DELETE FROM lm3.lmjob j USING lm_occJob oj
      WHERE j.lmjobid = oj.lmjobid 
        AND oj.occurrencesetid = occid AND oj.occuserid = usr;
   GET DIAGNOSTICS rowcount = ROW_COUNT;     
   jbcount = jbcount + rowcount;

   -- Projections
   DELETE FROM lm3.projection p USING model m 
      WHERE p.modelid = m.modelid 
        AND m.occurrencesetid = occid
        AND m.userid = usr;
   GET DIAGNOSTICS prjcount = ROW_COUNT;

   DELETE FROM lm3.model WHERE occurrencesetid = occid AND userid = usr;
   GET DIAGNOSTICS mdlcount = ROW_COUNT;

   DELETE FROM lm3.occurrenceset WHERE occurrencesetid = occid AND userid = usr;
   GET DIAGNOSTICS rowcount = ROW_COUNT;
   IF FOUND THEN
      success = 0;
   END IF;

   RAISE NOTICE  'Deleted % models, % projections, % jobs', mdlcount, prjcount, jbcount;

   RETURN success;
END;
$$ LANGUAGE 'plpgsql' VOLATILE; 


-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lm3.lm_getScenarioByCode(code varchar)
   RETURNS lm3.lm_scenarioAndKeywords AS
$$
DECLARE
   rec lm3.lm_scenarioAndKeywords%ROWTYPE;
   keystr varchar;
BEGIN
   BEGIN
      SELECT s.scenarioid, s.scenariocode, s.metadataUrl, s.title, s.author, 
             s.description, s.startDate, s.endDate, s.units, s.resolution, 
             s.epsgcode, s.bbox, s.dateLastModified
      INTO rec
      FROM lm3.scenario s
      WHERE lower(s.scenariocode) = lower(code);
      
      EXCEPTION
         WHEN NO_DATA_FOUND THEN
            RAISE NOTICE 'Scenario code % not found', code;
         WHEN TOO_MANY_ROWS THEN
            RAISE EXCEPTION 'Scenario code % not unique', code;
   END;
   IF FOUND THEN
      SELECT INTO keystr lm3.lm_getScenarioKeywordString(rec.scenarioid);
      rec.keywords = keystr;
   END IF;
   RETURN rec;
END;
$$ LANGUAGE 'plpgsql' STABLE; 

-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lm3.lm_getScenariosByKeyword(kywd varchar)
   RETURNS SETOF lm3.lm_scenarioAndKeywords AS
$$
DECLARE
   rec lm3.lm_scenarioAndKeywords%ROWTYPE;
   keystr varchar;
BEGIN
   FOR rec in 
      SELECT s.scenarioid, s.scenariocode, s.metadataUrl, s.title, s.author, 
             s.description, s.startDate, s.endDate, s.units, s.resolution, 
             s.epsgcode, s.bbox, s.dateLastModified
      FROM lm3.scenario s, lm3.scenariokeywords sk, lm3.keyword k
      WHERE s.scenarioid = sk.scenarioid
        AND sk.keywordid = k.keywordid
        AND k.keyword = kywd
   LOOP
      SELECT INTO keystr lm3.lm_getScenarioKeywordString(rec.scenarioid);
      rec.keywords = keystr;
      RETURN NEXT rec;
   END LOOP;   
   RETURN;
END;
$$ LANGUAGE 'plpgsql' STABLE; 

-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lm3.lm_getMatchingScenarios(scenid int)
   RETURNS SETOF lm3.lm_scenarioAndKeywords AS
$$
DECLARE
   arrScenTypes int[];
   arrNewScenTypes int[];
   rec lm3.lm_scenarioAndKeywords%ROWTYPE;
   keystr varchar;
BEGIN
   SELECT INTO arrScenTypes lm3.lm_getScenarioTypes(scenid) ;
   FOR rec in 
      SELECT s.scenarioid, s.scenariocode, s.metadataUrl, s.title, s.author, 
             s.description, s.startDate, s.endDate, s.units, s.resolution, 
             s.epsgcode, s.bbox, s.dateLastModified
      FROM lm3.scenario s
      WHERE s.scenarioid <> scenid
   LOOP
      SELECT INTO arrNewScenTypes lm3.lm_getScenarioTypes(rec.scenarioid);
      IF arrScenTypes = arrNewScenTypes THEN
         BEGIN
            SELECT INTO keystr lm3.lm_getScenarioKeywordString(rec.scenarioid);
            rec.keywords = keystr;
            RETURN NEXT rec;
         END;
      END IF;
   END LOOP;   
   RETURN;
END;
$$ LANGUAGE 'plpgsql' STABLE; 

-- ----------------------------------------------------------------------------
-- lm_getScenarioById
CREATE OR REPLACE FUNCTION lm3.lm_getScenarioById(id int)
   RETURNS lm3.lm_scenarioAndKeywords AS
$$
DECLARE
   rec lm3.lm_scenarioAndKeywords%ROWTYPE;
   keystr varchar;
BEGIN
   BEGIN
      SELECT s.scenarioid, s.scenariocode, s.metadataUrl, s.title, s.author, 
             s.description, s.startDate, s.endDate, s.units, s.resolution, 
             s.epsgcode, s.bbox, s.dateLastModified
      INTO rec
      FROM lm3.scenario s
      WHERE s.scenarioid = id;
      
      EXCEPTION
         WHEN NO_DATA_FOUND THEN
            RAISE NOTICE 'Scenario id % not found', id;
         WHEN TOO_MANY_ROWS THEN
            RAISE EXCEPTION 'Scenario id % not unique', id;
   END;
   IF FOUND THEN
      SELECT INTO keystr lm3.lm_getScenarioKeywordString(rec.scenarioid);
      rec.keywords = keystr;
   END IF;
   RETURN rec;
END;
$$ LANGUAGE 'plpgsql' STABLE; 
 
-- ----------------------------------------------------------------------------
-- UPDATE lm3.a layer
CREATE OR REPLACE FUNCTION lm3.lm_updateLayer(lyrid int,
                                          lyrtype varchar,
                                          url varchar,
                                          ttl varchar,
                                          nm varchar,
                                          minv double precision,
                                          maxv double precision, 
                                          nodatav double precision,
                                          vunits varchar,
                                          fullpth varchar,
                                          gtype int,
                                          startdt  double precision,
                                          enddt  double precision,
                                          munits varchar,
                                          res double precision,
                                          -- thumb bytea, 
                                          epsg int,
                                          dsc varchar,
                                          usr varchar,
                                          modtime double precision, 
                                          bndsstring varchar, 
                                          bboxwkt varchar)
   RETURNS int AS
$$
DECLARE
   lyrtypeid INT = -1;
   lyrgeom geometry;
   success int = -1;
BEGIN
   SELECT layertypeid INTO lyrtypeid FROM lm3.layertype 
      WHERE code = lyrtype AND userid = usr;

   -- Default LM EPSG Code
   IF epsg = 4326 THEN 
      lyrgeom = ST_GeomFromText(bboxwkt, epsg);
   ELSE
      lyrgeom = null;
   END IF;
      
   UPDATE lm3.Layer SET 
      (layerTypeId, metadataUrl, title, name, minVal, maxVal, nodataVal, valUnits, 
       dlocation, gdalType, startDate, endDate, mapunits, resolution,
       epsgcode, description, userid, dateLastModified, bbox, geom) = 
      (lyrtypeid, url, ttl, nm, minv, maxv, nodatav, vunits, 
       fullpth, gtype, startdt, enddt, munits, res,  
       epsg, dsc, usr, modtime, bndsstring, lyrgeom)
   WHERE layerId = lyrid;

      
   IF FOUND THEN
      success := 0;
   END IF;
   RETURN success;
END;
$$  LANGUAGE 'plpgsql' VOLATILE;

-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lm3.lm_insertJobChain(usr varchar,
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
CREATE OR REPLACE FUNCTION lm3.lm_getJobChains(startstat int, 
                                               total int, 
                                               usr varchar)
RETURNS SETOF lm3.JobChain AS
$$
DECLARE
   cmd varchar;
   wherecls varchar;
   lastcls varchar;
   rec lm3.JobChain;
BEGIN
   cmd = 'SELECT * FROM lm3.JobChain ';
   wherecls = ' WHERE status = ' || quote_literal(startstat) ';
   lastcls = ' ORDER BY priority, datecreated ASC LIMIT ' || quote_literal(total);

   -- optional filter by userid
   IF usr is NOT NULL THEN
      wherecls = wherecls || ' AND userid = ' || quote_literal(usr);
   END IF;
   
   cmd := cmd || wherecls || lastcls;
   RAISE NOTICE 'cmd = %', cmd;
   FOR rec in EXECUTE cmd
      LOOP
         RETURN NEXT rec;
      END LOOP;
   RETURN;
END;
$$  LANGUAGE 'plpgsql' STABLE;

-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lm3.lm_moveAndReturnJobChains(startstat int, 
                                                         endstat int,
                                                         currtime double precision,
                                                         total int, 
                                                         usr varchar)
RETURNS SETOF lm3.JobChain AS
$$
DECLARE
   cmd varchar;
   wherecls varchar;
   lastcls varchar;
   rec lm3.JobChain;
BEGIN
   pullcmd = 'SELECT * FROM lm3.JobChain ';
   wherecls = ' WHERE status = ' || quote_literal(startstat) ';
   lastcls = ' ORDER BY priority, datecreated ASC LIMIT ' || quote_literal(total);

   -- optional filter by userid
   IF usr is NOT NULL THEN
      wherecls = wherecls || ' AND userid = ' || quote_literal(usr);
   END IF;
   
   cmd := cmd || wherecls || lastcls;
   RAISE NOTICE 'cmd = %', cmd;
   FOR rec in EXECUTE cmd
      LOOP
         UPDATE lm3.JobChain SET (status, statusmodtime) = (endstat, currtime)
            WHERE jobchainId = rec.jobchainId;
         rec.status = endstat;
         rec.statusmodtime = currtime;
         RETURN NEXT rec;
      END LOOP;
   RETURN;
END;
$$  LANGUAGE 'plpgsql' VOLATILE;

-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lm3.lm_deleteJobChain(jcid int)
RETURNS INT AS
$$
DECLARE
   success int = -1;
BEGIN
   DELETE FROM lm3.JobChain WHERE jobchainId = jcid; 

   IF FOUND THEN
      success := 0;
   END IF;

   RETURN success;
END;
$$  LANGUAGE 'plpgsql' VOLATILE;

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
            RAISE NOTICE 'JobId % not found', jobid;
   end;
   
   RETURN reftype;
END;
$$  LANGUAGE 'plpgsql' STABLE;

-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lm3.lm_getModelJob(jid int)
   RETURNS lm3.lm_mdlJob AS
$$
DECLARE
   rec lm3.lm_mdlJob;
BEGIN
   begin
      SELECT * INTO STRICT rec FROM lm3.lm_mdlJob WHERE lmjobid = jid;

      EXCEPTION
         WHEN NO_DATA_FOUND THEN
            RAISE NOTICE 'ModelJob not found for %', jid;
   end;
   RETURN rec;
END;
$$  LANGUAGE 'plpgsql' STABLE;

-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lm3.lm_getModelJobForId(mdlid int)
   RETURNS lm3.lm_mdlJob AS
$$
DECLARE
   rec lm3.lm_mdlJob;
BEGIN
   SELECT * INTO rec FROM lm3.lm_mdlJob WHERE modelid = mdlid;
   RETURN rec;
END;
$$  LANGUAGE 'plpgsql' STABLE;

-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lm3.lm_getOccurrenceJobForId(occid int)
   RETURNS lm3.lm_occJob AS
$$
DECLARE
   rec lm3.lm_occJob;
BEGIN
   SELECT * INTO rec FROM lm3.lm_occJob WHERE occurrencesetid = occid;
   RETURN rec;
END;
$$  LANGUAGE 'plpgsql' STABLE;

-- ----------------------------------------------------------------------------
DROP FUNCTION IF EXISTS lm3.lm_pullModelJobsForOcc(occid int,
                                                  processType int,
                                                  startStat int,
                                                  endStat int,
                                                  currtime double precision,
                                                  crid int);
CREATE OR REPLACE FUNCTION lm3.lm_getModelJobsForOcc(occid int)
   RETURNS SETOF lm3.lm_mdlJob AS
$$
DECLARE
   rec lm3.lm_mdlJob;
BEGIN
   FOR rec in SELECT * FROM lm3.lm_mdlJob 
      WHERE occurrenceSetId = occid 
   LOOP
      RETURN NEXT rec;
   END LOOP;   
   RETURN;
END;
$$  LANGUAGE 'plpgsql' STABLE;

-- ----------------------------------------------------------------------------
DROP FUNCTION IF EXISTS lm3.lm_getCompletedModelsForOcc(occid int);

-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lm3.lm_pullProjectionJobsForModel(mdlid int,
                                                  processType int,
                                                  startStat int,
                                                  endStat int,
                                                  currtime double precision,
                                                  crid int)
   RETURNS SETOF lm3.lm_prjJob AS
$$
DECLARE
   rec lm3.lm_prjJob;
BEGIN
   FOR rec in SELECT * FROM lm3.lm_prjJob 
      WHERE modelid = mdlid  AND jbstatus = startStat
   LOOP
      UPDATE lm3.LmJob SET (status, statusmodtime, lastheartbeat, computeResourceId) 
                         = (endStat, currtime, currtime, crid) 
         WHERE lmJobId = rec.lmJobId;
      UPDATE lm3.Projection SET (status, statusmodtime, computeResourceId) 
                                 = (endStat, currtime, crid) 
         WHERE modelid = rec.modelid;
   	rec.jbstatus = endstat; 
   	rec.jbstatusmodtime = currtime;
   	rec.lastheartbeat = currtime;
   	rec.jbcomputeresourceid = crid;
   	rec.prjstatus = endstat; 
   	rec.prjstatusmodtime = currtime;
   	rec.prjcomputeresourceid = crid;
      RETURN NEXT rec;
   END LOOP;   
   RETURN;
END;
$$  LANGUAGE 'plpgsql' VOLATILE;

-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lm3.lm_getProjectionJob(jid int)
   RETURNS lm3.lm_prjJob AS
$$
DECLARE
   rec lm3.lm_prjJob;
BEGIN
   begin
      SELECT * INTO STRICT rec FROM lm3.lm_prjJob WHERE lmjobid = jid;

      EXCEPTION
         WHEN NO_DATA_FOUND THEN
            RAISE NOTICE 'ProjectionJob not found for %', jid;
   end;
   RETURN rec;
END;
$$  LANGUAGE 'plpgsql' STABLE;

-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lm3.lm_getProjectionJobForId(prjid int)
   RETURNS lm3.lm_prjJob AS
$$
DECLARE
   rec lm3.lm_prjJob;
BEGIN
   SELECT * INTO rec FROM lm3.lm_prjJob WHERE projectionid = prjid;
   RETURN rec;
END;
$$  LANGUAGE 'plpgsql' STABLE;

-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lm3.lm_getOccurrenceJob(jid int)
   RETURNS lm3.lm_occJob AS
$$
DECLARE
   rec lm3.lm_occJob;
BEGIN
   begin
      SELECT * INTO STRICT rec FROM lm3.lm_occJob WHERE lmjobid = jid;

      EXCEPTION
         WHEN NO_DATA_FOUND THEN
            RAISE NOTICE 'OccurrenceJob not found for %', jid;
   end;
   RETURN rec;
END;
$$  LANGUAGE 'plpgsql' STABLE;

-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lm3.lm_getComputeId(crip varchar, msk varchar)
   RETURNS int AS
$$
DECLARE
   crid int := -1;
BEGIN
   begin
      -- Get computeresource id for requesting resource.
      SELECT computeResourceId INTO STRICT crid FROM lm3.ComputeResource
         WHERE ipaddress = crip AND ipmask = msk;
      EXCEPTION
         WHEN NO_DATA_FOUND THEN
            RAISE NOTICE 'ComputeResource not found for IP %, mask %', crip, msk;
   end;
   RETURN crid;
END;
$$  LANGUAGE 'plpgsql' STABLE;

-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lm3.lm_getComputeRec(crip int, msk varchar)
   RETURNS lm3.ComputeResource AS
$$
DECLARE
   rec lm3.ComputeResource;
BEGIN
   begin
      -- Get computeresource id for requesting resource.
      SELECT * INTO STRICT rec FROM lm3.ComputeResource
         WHERE ipaddress = crip AND ipmask = msk;
      EXCEPTION
         WHEN NO_DATA_FOUND THEN
            RAISE NOTICE 'ComputeResource not found for IP %, mask %', crip, msk;
   end;
   RETURN rec;
END;
$$  LANGUAGE 'plpgsql' STABLE;

-- ----------------------------------------------------------------------------
-- TODO: Change to also use reqsoftware (LmCommon.common.lmconstants.ProcessType)?
CREATE OR REPLACE FUNCTION lm3.lm_pullOccurrenceJobForId(occid int,
                                                  startStat int,
                                                  endStat int,
                                                  currtime double precision,
                                                  crid int)
   RETURNS lm3.lm_occJob AS
$$
DECLARE
   rec lm3.lm_occJob;
BEGIN
   SELECT * INTO rec FROM lm3.lm_occJob 
      WHERE occurrencesetid = occid AND jbstatus = startStat;
   
   IF FOUND THEN 
      UPDATE lm3.LmJob SET (status, statusmodtime, lastheartbeat, computeResourceId) 
                         = (endStat, currtime, currtime, crid) 
         WHERE lmJobId = rec.lmJobId;
      UPDATE lm3.OccurrenceSet SET (status, statusmodtime) 
                                 = (endStat, currtime) 
         WHERE occurrencesetid = rec.occurrencesetid;
   	rec.jbstatus = endstat; 
   	rec.jbstatusmodtime = currtime;
   	rec.lastheartbeat = currtime;
   	rec.jbcomputeresourceid = crid;
   	rec.occstatus = endstat; 
   	rec.occstatusmodtime = currtime;
   END IF;
   
   RETURN rec;
END;
$$  LANGUAGE 'plpgsql' VOLATILE;

-- ----------------------------------------------------------------------------
-- TODO: Change to also use reqsoftware (LmCommon.common.lmconstants.ProcessType)?
CREATE OR REPLACE FUNCTION lm3.lm_pullOccurrenceJob(occid int,
                                                  startStat int,
                                                  endStat int,
                                                  currtime double precision,
                                                  crid int)
   RETURNS lm3.lm_occJob AS
$$
DECLARE
   rec lm3.lm_occJob;
BEGIN
   SELECT * INTO rec FROM lm3.lm_occJob 
      WHERE occurrencesetid = occid AND jbstatus = startStat;
   
   IF FOUND THEN 
      UPDATE lm3.LmJob SET (status, statusmodtime, lastheartbeat, computeResourceId) 
                         = (endStat, currtime, currtime, crid) 
         WHERE lmJobId = rec.lmJobId;
      UPDATE lm3.OccurrenceSet SET (status, statusmodtime) 
                                 = (endStat, currtime) 
         WHERE occurrencesetid = rec.occurrencesetid;
   	rec.jbstatus = endstat; 
   	rec.jbstatusmodtime = currtime;
   	rec.lastheartbeat = currtime;
   	rec.jbcomputeresourceid = crid;
   	rec.occstatus = endstat; 
   	rec.occstatusmodtime = currtime;
   END IF;
   
   RETURN rec;
END;
$$  LANGUAGE 'plpgsql' VOLATILE;

-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lm3.lm_insertJob(jfam int,
                                        software int,
                                        reftype int,
                                        refid int,
                                        crid int, 
                                        notify boolean,
                                        prior int,
                                        stat int,
                                        stg int,
                                        currtime double precision, 
                                        endstat int,
                                        retries int)
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
      DELETE FROM lm3.LMJob WHERE lmJobId = rec.lmjobid;
   END IF;

   INSERT INTO lm3.LMJob (jobFamily, reqSoftware, referenceType, referenceId, 
                          computeResourceId, priority, progress, donotify, 
                          status, statusmodtime, stage, stagemodtime, 
                          datecreated, lastheartbeat, retrycount)
              VALUES (jfam, software, reftype, refid, crid, 
                      prior, 0, notify, stat, currtime, stg, currtime, 
                      currtime, currtime, retries);
   IF FOUND THEN 
      SELECT INTO jid last_value FROM lm3.lmjob_lmjobid_seq;
   END IF;

   RETURN jid;

END;
$$  LANGUAGE 'plpgsql' VOLATILE;

-- ----------------------------------------------------------------------------
-- lm3.lm_updateJob
-- Now in createCommonExtras.sql
-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lm3.lm_updateJobAndObjLite(jid int,
                                                      crip varchar,
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
   jsuccess int := -1;
   osuccess int := -1;
BEGIN
   SELECT INTO crid computeResourceId FROM lm3.computeResource WHERE ipaddress = crip;
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
   IF reftype = 101 THEN
      UPDATE lm3.Model SET (computeResourceId, status, statusModTime) 
                         = (crid, stat, currtime)
         WHERE modelId = refid;
   ELSEIF reftype = 102 THEN
      UPDATE lm3.Projection SET (computeResourceId, status, statusModTime) 
                              = (crid, stat, currtime)
         WHERE projectionId = refid;
   ELSEIF reftype = 104 THEN
      UPDATE lm3.OccurrenceSet SET (status, statusModTime) 
                              = (stat, currtime)
         WHERE occurrencesetId = refid;      
   END IF;
   IF FOUND THEN 
      osuccess := 0;
   END IF;
      
   RETURN osuccess + jsuccess;
END;
$$  LANGUAGE 'plpgsql' VOLATILE;

-- ----------------------------------------------------------------------------
-- select * from lm3.lm_updateAllModelDependentJobs(300,1000,0,1, 1002,56852);
CREATE OR REPLACE FUNCTION lm3.lm_updateAllModelDependentJobs(completestat int,
                                                           errorstat int,
                                                           depnotreadystat int,
                                                           depreadystat int,
                                                           deperrorstat int,
                                                           currtime double precision)
RETURNS int AS
$$
DECLARE
   prjid int;
   mdlid int;
   rowcount int;
   total int :=0;
BEGIN
   -- On successful model, move projections and their jobs to ready   
   FOR prjid IN SELECT projectionid FROM lm3.lm_fullprojection 
           WHERE mdlstatus = completestat 
             AND prjstatus = depnotreadystat
      LOOP
         UPDATE lm3.Projection SET (status, statusmodtime) 
                                 = (depreadystat, currtime)
            WHERE projectionid = prjid;
         
         UPDATE lm3.LMJob SET (status, statusmodtime) 
                             = (depreadystat, currtime)
            WHERE referencetype = 102
              AND referenceid = prjid;
         GET DIAGNOSTICS rowcount = ROW_COUNT;
         total := total + rowcount;
      END LOOP;

   -- On error model ... 
   FOR mdlid, prjid IN SELECT modelid, projectionid FROM lm3.lm_fullprojection 
          WHERE mdlstatus = errorstat AND prjstatus = depnotreadystat
      LOOP
         -- move projections to error
         UPDATE lm3.Projection SET (status, statusmodtime) 
                                 = (deperrorstat, currtime)
            WHERE projectionid = prjid;
            
         -- move notify jobs to ready
         UPDATE lm3.LMJob SET (status, statusmodtime) = (depreadystat, currtime)
            WHERE lmjobid in (SELECT lmjobid FROM lm3.lm_msgJob 
                                             WHERE modelid = mdlid);
         GET DIAGNOSTICS rowcount = ROW_COUNT;
         total := total + rowcount;
         
         -- delete project jobs
         DELETE FROM lm3.LMJob WHERE referencetype = 102
              AND referenceid = prjid;
      END LOOP;

   RETURN total;
END;
$$  LANGUAGE 'plpgsql' VOLATILE;

-- ----------------------------------------------------------------------------
-- select * from lm3.lm_updateAllOccsetDependentJobs(300,1000,0,1,56852);
-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lm3.lm_updateAllOccsetDependentJobs(completestat int,
                                                           errorstat int,
                                                           depnotreadystat int,
                                                           depreadystat int,
                                                           currtime double precision)
RETURNS int AS
$$
DECLARE
   mdlid int;
   rowcount int;
   total int :=0;
BEGIN
   -- On successful occurrence update, move models and their jobs to ready   
   FOR mdlid IN SELECT modelid FROM lm3.lm_fullmodel 
           WHERE occstatus = completestat 
             AND mdlstatus = depnotreadystat
      LOOP
         UPDATE lm3.Model SET (status, statusmodtime) 
                            = (depreadystat, currtime)
            WHERE modelid = mdlid;
         
         UPDATE lm3.LMJob SET (status, statusmodtime) 
                             = (depreadystat, currtime)
            WHERE referencetype = 101
              AND referenceid = mdlid;
         GET DIAGNOSTICS rowcount = ROW_COUNT;
         total := total + rowcount;
      END LOOP;

   -- On error occurrenceset, do nothing, leave model at status 0

   RETURN total;
END;
$$  LANGUAGE 'plpgsql' VOLATILE;

-- ----------------------------------------------------------------------------
-- select * from lm3.lm_updateOccurrenceDependentJobs(300,1000,0,1,56852);
-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lm3.lm_updateOccurrenceDependentJobs(occid int,
                                                           completestat int,
                                                           errorstat int,
                                                           depnotreadystat int,
                                                           depreadystat int,
                                                           currtime double precision)
RETURNS int AS
$$
DECLARE
   mdlid int;
   rowcount int;
   total int :=0;
BEGIN
   -- On successful occurrence update, move models and their jobs to ready   
   FOR mdlid IN SELECT modelid FROM lm3.lm_fullmodel 
           WHERE occurrencesetid = occid
             AND occstatus = completestat 
             AND mdlstatus = depnotreadystat
      LOOP
         UPDATE lm3.Model SET (status, statusmodtime) 
                             = (depreadystat, currtime)
            WHERE modelid = mdlid;
         
         UPDATE lm3.LMJob SET (status, statusmodtime) 
                             = (depreadystat, currtime)
            WHERE referencetype = 101
              AND referenceid = mdlid;
         GET DIAGNOSTICS rowcount = ROW_COUNT;
         total = total + rowcount;
      END LOOP;

   -- On error occurrenceset ... do nothing, leave model at 0 status

   RETURN 0;
END;
$$  LANGUAGE 'plpgsql' VOLATILE;

-- ----------------------------------------------------------------------------
-- select * from lm3.lm_updateModelDependentJobs(301917,300,1000,1002,0,1,56594.8859745);
CREATE OR REPLACE FUNCTION lm3.lm_updateModelDependentJobs(mdlid int,
                                                           completestat int,
                                                           errorstat int,
                                                           depnotreadystat int,
                                                           depreadystat int,
                                                           deperrorstat int,
                                                           currtime double precision)
RETURNS int AS
$$
DECLARE
   prjid int;
   rowcount int;
   total int :=0;
BEGIN
   -- On successful model, move projections and their jobs to ready   
   FOR prjid IN SELECT projectionid FROM lm3.lm_fullprojection 
           WHERE modelid = mdlid
             AND mdlstatus = completestat 
             AND prjstatus = depnotreadystat
      LOOP
         UPDATE lm3.Projection SET (status, statusmodtime) 
                                 = (depreadystat, currtime)
            WHERE projectionid = prjid;
         
         UPDATE lm3.LMJob SET (status, statusmodtime) 
                             = (depreadystat, currtime)
            WHERE referencetype = 102
              AND referenceid = prjid;
         GET DIAGNOSTICS rowcount = ROW_COUNT;
         total = total + rowcount;
      END LOOP;

   -- On error model ... 
   FOR prjid IN SELECT projectionid FROM lm3.lm_fullprojection 
          WHERE modelid = mdlid 
             AND mdlstatus = errorstat 
             AND prjstatus = depnotreadystat
      LOOP
         -- move projections to error
         UPDATE lm3.Projection SET (status, statusmodtime) 
                                 = (deperrorstat, currtime)
            WHERE projectionid = prjid;
         -- move notify jobs to ready
         UPDATE lm3.LMJob SET (status, statusmodtime) = (depreadystat, currtime)
            WHERE lmjobid in (SELECT lmjobid FROM lm3.lm_msgJob 
                                             WHERE modelid = mdlid);
         -- delete project jobs
         DELETE FROM lm3.LMJob WHERE referencetype = 102
              AND referenceid = prjid;
      END LOOP;

   RETURN 0;
END;
$$  LANGUAGE 'plpgsql' VOLATILE;

-- ----------------------------------------------------------------------------
-- select * from lm3.lm_updateAllExpDependentJobs(300,0,1,56604);
CREATE OR REPLACE FUNCTION lm3.lm_updateAllExpDependentJobs(completestat int,
                                                         depnotreadystat int,
                                                         depreadystat int,
                                                         currtime double precision)
RETURNS int AS
$$
DECLARE
   mdlid int;
   incomplete int;
   rowcount int;
   total int := 0;
BEGIN
   -- On successful model, check if all projections are complete, move notify jobs to ready   
   FOR mdlid IN SELECT modelid FROM lm3.lm_msgjob 
           WHERE modelid = mdlid
             AND mdlstatus >= completestat 
             AND jbstatus = depnotreadystat
      LOOP
         SELECT count(*) INTO incomplete FROM lm3.Projection 
            WHERE modelid = mdlid AND status >= completestat;
         IF incomplete = 0 THEN
            begin
               UPDATE lm3.LMJob SET (status, statusmodtime) 
                                  = (depreadystat, currtime)
                  WHERE referencetype = 103 AND referenceid = mdlid;
               GET DIAGNOSTICS rowcount = ROW_COUNT;
               total := total + rowcount;
            end;
         END IF;
      END LOOP;

   RETURN total;

END;
$$  LANGUAGE 'plpgsql' VOLATILE;

-- ----------------------------------------------------------------------------
-- select * from lm3.lm_updateExpDependentJobs(1628643,300,1000,0,1,56604.870159);
CREATE OR REPLACE FUNCTION lm3.lm_updateExpDependentJobs(prjid int,
                                                         completestat int,
                                                         errorstat int,
                                                         depnotreadystat int,
                                                         depreadystat int,
                                                         currtime double precision)
RETURNS int AS
$$
DECLARE
   mdlid int;
   incomplete int;
   total int := 0;
BEGIN
   SELECT modelid INTO mdlid FROM lm3.projection WHERE projectionid = prjid;
   SELECT count(*) INTO incomplete FROM lm3.projection 
      WHERE modelid = mdlid AND status < completestat;
      
   IF incomplete = 0 THEN
      begin
         UPDATE lm3.LMJob SET (status, statusmodtime) = (depreadystat, currtime)
            WHERE lmjobid IN (SELECT lmjobid FROM lm3.lm_msgJob 
                               WHERE referenceid = mdlid);
         IF FOUND THEN 
            total := 1;
         END IF;
      end;
   END IF;

   RETURN total;
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
CREATE OR REPLACE FUNCTION lm3.lm_getEmail(usr varchar)
RETURNS varchar AS
$$
DECLARE
   emale varchar;
BEGIN
   SELECT email INTO emale FROM lm3.LMUser 
      WHERE userId = usr;
   RETURN emale;

END;
$$  LANGUAGE 'plpgsql' STABLE;

-- ----------------------------------------------------------------------------
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
-- select * from lm3.lm_pullMessageJobs(2,500,1,90,NULL,56850,'129.237.201.119');
-- ----------------------------------------------------------------------------
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
      SELECT computeResourceId INTO STRICT crid FROM lm3.ComputeResource
         WHERE ipaddress = crip;
      EXCEPTION
         WHEN NO_DATA_FOUND THEN
            RAISE NOTICE 'ComputeResource not found for IP %', crip;
   end;
       
   -- LOCK rows with 'for update'
   -- Move continuing NotifyJobs to next status 
   start := 'SELECT * FROM lm3.lm_msgJob WHERE ';
   filters := ' jbstatus = ' || quote_literal(startStat);
   extra := ' ORDER BY priority DESC, mdlstatusmodtime ASC LIMIT ' 
            || quote_literal(total);

   IF usr is not null THEN 
      filters := filters || ' AND mdluserid = ' || quote_literal(usr) ;
   END IF;
   
   cmd := start || filters || extra;
   RAISE NOTICE 'Loop1: %', cmd;
   
   FOR rec in EXECUTE cmd
   LOOP
      RAISE NOTICE 'Pulling model/notify %, job %', rec.modelid, rec.lmJobId;
      IF crid IS NOT NULL THEN
         begin
            UPDATE lm3.LmJob SET (status, statusmodtime, computeResourceId) 
                               = (endStat, currtime, crid) 
               WHERE lmJobId = rec.lmJobId;
            UPDATE lm3.Model SET (status, statusmodtime, computeResourceId) 
                               = (endStat, currtime, crid) 
               WHERE modelId = rec.modelId;
         end;
      ELSE
         begin
            UPDATE lm3.LmJob SET (status, statusmodtime) = (endStat, currtime) 
               WHERE lmJobId = rec.lmJobId;
            UPDATE lm3.Model SET (status, statusmodtime) = (endStat, currtime) 
               WHERE modelId = rec.modelId;
         end;
      END IF;
      SELECT * FROM lm3.lm_msgJob INTO retrec WHERE lmJobId = rec.lmJobId;       
      RETURN NEXT retrec;
   END LOOP;   
   
   RETURN;
END;
$$  LANGUAGE 'plpgsql' VOLATILE;

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
   mrec lm3.lm_mdlJob;
   prec lm3.lm_prjJob;
   total int := 0;
BEGIN
   
   -- Can LOCK rows with 'for update'
   FOR mrec in 
      SELECT * FROM lm3.lm_mdljob 
         WHERE lastheartbeat < giveuptime AND jbstatus >= pulledStat AND jbstatus < completeStat
         ORDER BY priority DESC, mdlstatusmodtime ASC 
   LOOP
      RAISE NOTICE 'Reseting model %, job %', mrec.modelid, mrec.lmJobId;
      UPDATE lm3.LmJob SET (status, statusmodtime, lastheartbeat, computeResourceId) 
                         = (initStat, currtime, NULL, NULL) 
         WHERE lmJobId = mrec.lmJobId;
      UPDATE lm3.Model SET (status, statusmodtime, computeResourceId) 
                         = (initStat, currtime, NULL) 
         WHERE modelId = mrec.modelId;

      total = total + 1;
   END LOOP;   
   
   FOR prec in 
      SELECT * FROM lm3.lm_prjjob 
         WHERE lastheartbeat < giveuptime AND jbstatus >= pulledStat AND jbstatus < completeStat
         ORDER BY priority DESC, prjstatusmodtime ASC 
   LOOP
      RAISE NOTICE 'Reseting projection %, job %', prec.projectionId, prec.lmJobId;
      UPDATE lm3.LmJob SET (status, statusmodtime, lastheartbeat, computeResourceId) 
                         = (initStat, currtime, NULL, NULL) 
         WHERE lmJobId = prec.lmJobId;
      UPDATE lm3.Projection SET (status, statusmodtime, computeResourceId) 
                              = (initStat, currtime, NULL) 
         WHERE projectionId = prec.projectionId;
      total = total + 1;
   END LOOP;   

   RETURN total;
END;
$$  LANGUAGE 'plpgsql' VOLATILE;

-- ----------------------------------------------------------------------------
-- TODO: Change to also use reqsoftware (LmCommon.common.lmconstants.ProcessType) 
CREATE OR REPLACE FUNCTION lm3.lm_countOccJobs(usrid varchar(20), 
    	                                         stat int)
   RETURNS int AS
$$
DECLARE
   num int;
   cmd varchar;
   wherecls varchar;
BEGIN
   cmd = 'select count(*) FROM lm3.lm_occjob ';
   wherecls = ' WHERE jbstatus =  ' || quote_literal(stat);

   -- filter by user
   IF usrid is not null THEN
      wherecls = wherecls || ' AND occuserid = ' || quote_literal(usrid) ;
   END IF;

   cmd := cmd || wherecls;
   RAISE NOTICE 'cmd = %', cmd;

   EXECUTE cmd INTO num;
   RETURN num;
END;
$$  LANGUAGE 'plpgsql' STABLE;


-- ----------------------------------------------------------------------------
-- TODO: Change to also use reqsoftware (LmCommon.common.lmconstants.ProcessType) 
CREATE OR REPLACE FUNCTION lm3.lm_countMdlJobs(usrid varchar(20), 
    	                                         stat int)
   RETURNS int AS
$$
DECLARE
   num int;
   cmd varchar;
   wherecls varchar;
BEGIN
   cmd = 'select count(*) FROM lm3.lm_mdljob ';
   wherecls = ' WHERE jbstatus =  ' || quote_literal(stat);

   -- filter by user
   IF usrid is not null THEN
      wherecls = wherecls || ' AND mdluserid = ' || quote_literal(usrid) ;
   END IF;

   cmd := cmd || wherecls;
   RAISE NOTICE 'cmd = %', cmd;

   EXECUTE cmd INTO num;
   RETURN num;
END;
$$  LANGUAGE 'plpgsql' STABLE;

-- ----------------------------------------------------------------------------
-- TODO: Change to also use reqsoftware (LmCommon.common.lmconstants.ProcessType) 
CREATE OR REPLACE FUNCTION lm3.lm_countPrjJobs(usrid varchar(20), 
    	                                         stat int)
   RETURNS int AS
$$
DECLARE
   num int;
   cmd varchar;
   wherecls varchar;
BEGIN
   cmd = 'select count(*) FROM lm3.lm_prjjob ';
   wherecls = ' WHERE jbstatus =  ' || quote_literal(stat);

   -- filter by user
   IF usrid is not null THEN
      wherecls = wherecls || ' AND mdluserid = ' || quote_literal(usrid) ;
   END IF;

   cmd := cmd || wherecls;
   RAISE NOTICE 'cmd = %', cmd;

   EXECUTE cmd INTO num;
   RETURN num;
END;
$$  LANGUAGE 'plpgsql' STABLE;

-- ----------------------------------------------------------------------------
-- TODO: Change to also use reqsoftware (LmCommon.common.lmconstants.ProcessType) 
CREATE OR REPLACE FUNCTION lm3.lm_countMsgJobs(usrid varchar(20), 
    	                                         stat int)
   RETURNS int AS
$$
DECLARE
   num int;
   cmd varchar;
   wherecls varchar;
BEGIN
   cmd = 'select count(*) FROM lm3.lm_msgjob ';
   wherecls = ' WHERE jbstatus =  ' || quote_literal(stat);
   
   -- filter by user
   IF usrid is not null THEN
      wherecls = wherecls || ' AND mdluserid = ' || quote_literal(usrid) ;
   END IF;

   cmd := cmd || wherecls;
   RAISE NOTICE 'cmd = %', cmd;

   EXECUTE cmd INTO num;
   RETURN num;
END;
$$  LANGUAGE 'plpgsql' STABLE;

-- ----------------------------------------------------------------------------
-- select modelid, lmjobid, mdlstatus, jbstatus from lm_pullmodeljobs (4, 210, 1, 13, 'lm2', null, 56890, '129.237.201.119');
CREATE OR REPLACE FUNCTION lm3.lm_pullModelJobs(total int, 
                                                processType int,
                                                startStat int,
                                                endStat int,
                                                usr varchar,
                                                inputtype varchar,
                                                currtime double precision,
                                                crip varchar)
   RETURNS SETOF lm3.lm_mdlJob AS
$$
DECLARE
   multiplier int := 100;
   crid int;
   rec lm3.lm_mdlJob;
   retrec lm3.lm_mdlJob;
   cmd varchar;
   start varchar;
   extra varchar;
   filters varchar := '';
   trynumber int := total * multiplier;
   gotnumber int := 0;
   gotone boolean;
BEGIN
   begin
      -- Get computeresource id for requesting resource.
      SELECT computeResourceId INTO STRICT crid FROM lm3.ComputeResource
         WHERE ipaddress = crip;
      EXCEPTION
         WHEN NO_DATA_FOUND THEN
            RAISE NOTICE 'ComputeResource not found for IP %', crip;
   end;
   
   -- LOCK rows with 'for update'
   -- Move continuing SDMModelJobs to next status 
   start := 'SELECT * FROM lm3.lm_mdljob WHERE ';
   filters := ' jbstatus = ' || quote_literal(startStat);
   extra := ' ORDER BY priority DESC, mdlstatusmodtime ASC LIMIT ' 
            || quote_literal(trynumber);

   IF usr is not null THEN 
      filters := filters || ' AND mdluserid = ' || quote_literal(usr) ;
   END IF;
   IF inputtype is not null THEN 
      filters := filters || ' AND reqdata = ' || quote_literal(inputtype) ;
   END IF;
   
   cmd := start || filters || extra;
   RAISE NOTICE 'Loop1: %', cmd;
   
   FOR rec in EXECUTE cmd
   LOOP
      RAISE NOTICE 'Loop';
      IF gotnumber < total THEN
         begin
            RAISE NOTICE '  Try Update';
            gotone := FALSE;
            IF crid IS NOT NULL THEN
               begin
                  UPDATE lm3.LmJob SET (status, statusmodtime, lastheartbeat, computeResourceId) 
                                     = (endStat, currtime, currtime, crid) 
                     WHERE lmJobId = rec.lmJobId;
                  IF FOUND THEN 
                     gotone := TRUE;
                     UPDATE lm3.Model SET (status, statusmodtime, computeResourceId) 
                                        = (endStat, currtime, crid) 
                        WHERE modelId = rec.modelId;
                  END IF;
               end;
            ELSE
               begin
                  UPDATE lm3.LmJob SET (status, statusmodtime, lastheartbeat) = (endStat, currtime, currtime) 
                     WHERE lmJobId = rec.lmJobId;
                  IF FOUND THEN 
                     gotone := TRUE;
                     UPDATE lm3.Model SET (status, statusmodtime) = (endStat, currtime) 
                        WHERE modelId = rec.modelId;
                  END IF;
               end;
            END IF;
            IF gotone THEN
               RAISE NOTICE '  Gotone';
               SELECT * FROM lm3.lm_mdlJob INTO retrec WHERE lmJobId = rec.lmJobId;       
               gotnumber = gotnumber + 1;       
               RETURN NEXT retrec;
            END IF;
         end;
      END IF;
   END LOOP;   
   RETURN;
END;
$$  LANGUAGE 'plpgsql' VOLATILE;
   
-- ----------------------------------------------------------------------------
-- select projectionid, lmjobid, prjstatus, jbstatus from lm_pullprojectionjobs (4, 220, 1, 13, 'lm2', null, 56890, '129.237.201.119');
CREATE OR REPLACE FUNCTION lm3.lm_pullProjectionJobs(total int,
                                                     processType int,
                                                     startStat int,
                                                     endStat int,
                                                     usr varchar,
                                                     inputtype varchar,
                                                     currtime double precision,
                                                     crip varchar)
   RETURNS SETOF lm3.lm_prjJob AS
$$
DECLARE
   multiplier int := 100;
   crid int;
   rec  lm3.lm_prjjob;
   retrec lm3.lm_prjJob;
   cmd varchar;
   start varchar;
   filters varchar;
   extra varchar;
   trynumber int := total * multiplier;
   gotnumber int := 0;
   gotone boolean;
BEGIN
   begin
      -- Get computeresource id for requesting resource.
      SELECT computeResourceId INTO STRICT crid FROM lm3.ComputeResource
         WHERE ipaddress = crip;
      EXCEPTION
         WHEN NO_DATA_FOUND THEN
            RAISE NOTICE 'ComputeResource not found for IP %', crip;
   end;

   -- Can LOCK rows with 'for update'
   start := 'SELECT * FROM lm3.lm_prjJob WHERE ';
   filters := ' jbstatus = ' || quote_literal(startStat);
   extra := ' ORDER BY priority DESC, prjstatusmodtime ASC LIMIT ' 
            || quote_literal(trynumber);

   IF usr is not null THEN 
      filters := filters || ' AND mdluserid = ' || quote_literal(usr) ;
   END IF;
   IF inputtype is not null THEN 
      filters := filters || ' AND reqdata = ' || quote_literal(inputtype) ;
   END IF;
   cmd := start || filters || extra;
      
   -- NOW, Pull ready SDMProjectionJobs
   FOR rec in EXECUTE cmd
   LOOP
      RAISE NOTICE 'Loop';
      IF gotnumber < total THEN
         begin
            RAISE NOTICE '  Try Update';
            gotone := FALSE;
            IF crid IS NOT NULL THEN
               begin
                  UPDATE lm3.lmJob SET (status, statusmodtime, lastheartbeat, computeResourceId) 
                                     = (endStat, currtime, currtime, crid) 
                     WHERE lmJobId = rec.lmJobId;
                  IF FOUND THEN 
                     gotone := TRUE;
                     UPDATE lm3.Projection SET (status, statusmodtime, computeResourceId) 
                                             = (endStat, currtime, crid) 
                        WHERE projectionId = rec.projectionid;
                  END IF;
               end;
            ELSE
               begin
                  UPDATE lm3.lmJob SET (status, statusmodtime, lastheartbeat) 
                                     = (endStat, currtime, currtime) 
                     WHERE lmJobId = rec.lmJobId;
                  IF FOUND THEN 
                     gotone := TRUE;
                     UPDATE lm3.Projection SET (status, statusmodtime) 
                                             = (endStat, currtime) 
                        WHERE projectionId = rec.projectionid;
                  END IF;
               end;
            END IF;
            IF gotone THEN
               RAISE NOTICE '  Gotone';
               SELECT * FROM lm3.lm_prjJob INTO retrec WHERE lmJobId = rec.lmJobId;       
               gotnumber = gotnumber + 1;       
               RETURN NEXT retrec;
            END IF;
         end;
      END IF;
   END LOOP;   
   RETURN;
END;
$$  LANGUAGE 'plpgsql' VOLATILE;
   
   
-- ----------------------------------------------------------------------------
   
-- ----------------------------------------------------------------------------
--NEW
-- select occurrenceid, lmjobid, occstatus, jbstatus from lm_pulloccurrencejobs (4, 405, 1, 13, 'lm2', null, 56890, '129.237.201.119');
CREATE OR REPLACE FUNCTION lm3.lm_pullOccurrenceJobs(total int,
                                                     processType int,
                                                     startStat int,
                                                     endStat int,
                                                     usr varchar,
                                                     inputtype varchar,
                                                     currtime double precision,
                                                     crip varchar)
   RETURNS SETOF lm3.lm_occJob AS
$$
DECLARE
   crid int;
   rec  lm3.lm_occjob;
   retrec lm3.lm_occJob;
   cmd varchar;
   start varchar;
   filters varchar;
   extra varchar;
   trynumber int := total * 20;
   gotnumber int := 0;
   gotone boolean;
BEGIN
   begin
      -- Get computeresource id for requesting resource.
      SELECT computeResourceId INTO STRICT crid FROM lm3.ComputeResource
         WHERE ipaddress = crip;
      EXCEPTION
         WHEN NO_DATA_FOUND THEN
            RAISE NOTICE 'ComputeResource not found for IP %', crip;
   end;

   -- Can LOCK rows with 'for update'
   start := 'SELECT * FROM lm3.lm_occJob WHERE ';
   filters := ' jbstatus = ' || quote_literal(startStat);
   extra := ' ORDER BY jbstatusmodtime ASC LIMIT ' 
            || quote_literal(trynumber);

   IF usr is not null THEN 
      filters := filters || ' AND occuserid = ' || quote_literal(usr) ;
   END IF;
   cmd := start || filters || extra;
      
   -- NOW, Pull ready OccurrenceJobs
   FOR rec in EXECUTE cmd
   LOOP
      RAISE NOTICE 'Loop';
      IF gotnumber < total THEN
         begin
            RAISE NOTICE '  Try Update';
            gotone := FALSE;
            IF crid IS NOT NULL THEN
               begin
                  UPDATE lm3.lmJob SET (status, statusmodtime, lastheartbeat, computeResourceId) 
                                     = (endStat, currtime, currtime, crid) 
                     WHERE lmJobId = rec.lmJobId AND status = startStat;
                  IF FOUND THEN 
                     gotone := TRUE;
                     UPDATE lm3.OccurrenceSet SET (status, statusmodtime) 
                                                = (endStat, currtime) 
                        WHERE occurrencesetId = rec.occurrencesetId;
                  END IF;
               end;
            ELSE
               begin
                  UPDATE lm3.lmJob SET (status, statusmodtime, lastheartbeat) 
                                     = (endStat, currtime, currtime) 
                     WHERE lmJobId = rec.lmJobId AND status = startStat;
                  IF FOUND THEN 
                     gotone := TRUE;
                     UPDATE lm3.Occurrenceset SET (status, statusmodtime) 
                                                = (endStat, currtime) 
                        WHERE occurrencesetId = rec.occurrencesetId;
                  END IF;
               end;
            END IF;
            IF gotone THEN
               RAISE NOTICE '  Gotone';
               SELECT * FROM lm3.lm_occJob INTO retrec WHERE lmJobId = rec.lmJobId;
               gotnumber = gotnumber + 1;       
               RETURN NEXT retrec;
            END IF;
         end;
      END IF;
   END LOOP;   
   RETURN;
END;
$$  LANGUAGE 'plpgsql' VOLATILE;
    
-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lm3.lm_findOccurrenceJobs(occid int, stat int)
   RETURNS SETOF lm3.lm_occJob AS
$$
DECLARE
   rec lm3.lm_occjob;
   cmd varchar;
BEGIN
   IF stat IS NOT null THEN
      cmd = 'SELECT * FROM lm3.lm_occJob WHERE occurrencesetid = '  
            || quote_literal(occid) || ' AND jbstatus = ' || quote_literal(stat)
            || ' ORDER BY jbstatus ASC, jbstatusmodtime ASC';
   ELSE
      cmd = 'SELECT * FROM lm3.lm_occJob WHERE occurrencesetid = '  
            || quote_literal(occid) 
            || ' ORDER BY jbstatus ASC, jbstatusmodtime ASC';
   END IF;

   FOR rec IN EXECUTE cmd
   LOOP
      RETURN NEXT rec;
   END LOOP;   
   RETURN;
END;
$$  LANGUAGE 'plpgsql' STABLE;

-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lm3.lm_findModelJobs(mdlid int)
   RETURNS SETOF lm3.lm_mdljob AS
$$
DECLARE
   rec  lm3.lm_mdljob;
BEGIN
   FOR rec IN 
      -- order by highest to lowest job status, oldest to newest modtime 
      SELECT * FROM lm3.lm_mdljob WHERE modelid = mdlid
         ORDER BY jbstatus ASC, jbstatusmodtime ASC
   LOOP
      RETURN NEXT rec;
   END LOOP;
   RETURN;
END;
$$  LANGUAGE 'plpgsql' STABLE;

-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lm3.lm_findProjectionJobs(prjid int)
   RETURNS SETOF lm3.lm_prjjob AS
$$
DECLARE
   rec  lm3.lm_prjjob;
BEGIN
   FOR rec IN 
      SELECT * FROM lm3.lm_prjjob WHERE projectionid = prjid
         ORDER BY jbstatus ASC, jbstatusmodtime ASC
   LOOP
      RETURN NEXT rec;
   END LOOP;   
   RETURN;
END;
$$  LANGUAGE 'plpgsql' STABLE;

-- ----------------------------------------------------------------------------
-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lm3.lm_insertTaxon(tsourceid int,
                                              tkey int,
                                              king varchar,
                                              phyl varchar,
                                              clss varchar,
                                              ordr varchar,
                                              fam  varchar,
                                              gen  varchar,
                                              name varchar,
                                              can varchar,
                                              rnk varchar,
                                              gkey int,
                                              skey int,
                                              hierkey varchar,
                                              cnt  int,
                                              currtime double precision)
RETURNS int AS
$$
DECLARE
   rec lm3.ScientificName%ROWTYPE;
   id int := -1;
BEGIN
   SELECT * INTO rec FROM lm3.ScientificName
      WHERE taxonomysourceid = tsourceid and taxonomykey = tkey;

   IF NOT FOUND THEN
      begin
         INSERT INTO lm3.ScientificName (taxonomysourceid, taxonomykey, kingdom, 
                                         phylum, tx_class, tx_order, family, 
                                         genus, sciname, canonical, rank, 
                                         genuskey, specieskey, 
                                         keyHierarchy, lastcount, 
                                         datecreated, datelastmodified)
                 VALUES (tsourceid, tkey, king, phyl, clss, ordr, fam, gen, 
                         name, can, rnk, gkey, skey, 
                         hierkey, cnt, currtime, currtime);
         IF FOUND THEN 
            SELECT INTO id last_value FROM lm3.scientificname_scientificnameid_seq;
         END IF;
      end;
   ELSE
      begin
         RAISE EXCEPTION 'Duplicate taxonKey % for taxonomySource % ', tkey, tsourceid;
      end;
   END IF;
   RETURN id;
END;
$$  LANGUAGE 'plpgsql' VOLATILE;

-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lm3.lm_deleteTaxon(scinameid int)
RETURNS int AS
$$
DECLARE
   rec lm3.ScientificName%ROWTYPE;
   deps int := -1;
   success int := -1;
BEGIN
   SELECT count(*) INTO deps FROM lm3.Occurrenceset 
      WHERE scientificnameid = scinameid;

   IF deps = 0 THEN
      begin
         DELETE FROM lm3.ScientificName WHERE scientificnameid = scinameid;
         IF FOUND THEN 
            success := 0;
         END IF;
      end;
   END IF;
   RETURN success;
END;
$$  LANGUAGE 'plpgsql' VOLATILE;

-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lm3.lm_findTaxonSource(tsourcename varchar)
RETURNS lm3.TaxonomySource AS
$$
DECLARE
   rec lm3.TaxonomySource%ROWTYPE;
BEGIN
   begin
      SELECT * INTO STRICT rec FROM lm3.TaxonomySource
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
CREATE OR REPLACE FUNCTION lm3.lm_insertTaxonSource(name varchar,
                                                    taxsrcurl varchar,
                                                    modtime double precision)
RETURNS int AS
$$
DECLARE
   id int = -1;
BEGIN
   SELECT taxonomySourceId INTO id 
      FROM lm3.TaxonomySource
      WHERE datasetIdentifier = name or url = taxsrcurl;
   IF NOT FOUND THEN
      INSERT INTO lm3.TaxonomySource 
         (url, datasetIdentifier, dateCreated, dateLastModified)
          VALUES (taxsrcurl, name, modtime, modtime);
      IF FOUND THEN
         SELECT INTO id last_value FROM lm3.taxonomysource_taxonomysourceid_seq;
      END IF;
   END IF;
   RETURN id;
END;
$$  LANGUAGE 'plpgsql' VOLATILE;


-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lm3.lm_findOrUpdateTaxon(tsourceid int,
                                            tkey int,
                                            king varchar,
                                            phyl varchar,
                                            clss varchar,
                                            ordr varchar,
                                            fam  varchar,
                                            gen  varchar,
                                            name varchar,
                                            can varchar,
                                            rnk varchar,
                                            gkey int,
                                            skey int,
                                            hierkey varchar,
                                            cnt  int,
                                            currtime double precision)
RETURNS lm3.ScientificName AS
$$
DECLARE
   rec      lm3.ScientificName%ROWTYPE;
BEGIN
   SELECT * FROM lm3.ScientificName
      WHERE taxonomysourceid = tsourceid and taxonomykey = tkey INTO rec;

   IF FOUND THEN
      BEGIN
         IF (rec.kingdom != king OR rec.phylum != phyl OR rec.tx_class != clss
             OR rec.tx_order != ordr OR rec.family != fam OR rec.genus != gen
             OR rec.sciname != name 
             OR rec.canonical != can OR rec.rank != rnk
             OR rec.genuskey != gkey 
             OR rec.specieskey != skey OR rec.keyHierarchy != hierkey) THEN
            UPDATE lm3.ScientificName 
               SET (kingdom, phylum, tx_class, tx_order, family, genus, 
                    sciname, canonical, rank, genuskey, specieskey, keyHierarchy, 
                    lastcount, datelastmodified)
               =   (king, phyl, clss, ordr, fam, gen, name, can, rnk, gkey, skey, 
                    hierkey, cnt, currtime)
               WHERE taxonomysourceid = tsourceid AND taxonomykey = tkey
               RETURNING * INTO rec;
         ELSEIF rec.lastcount != cnt THEN
            UPDATE lm3.ScientificName 
               SET (lastcount, datelastmodified) = (cnt, currtime)
               WHERE taxonomysourceid = tsourceid AND taxonomykey = tkey
               RETURNING * INTO rec;
         END IF;
      END;
   END IF;
   RETURN rec;
END;
$$  LANGUAGE 'plpgsql' VOLATILE;


-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lm3.lm_findTaxon(tsourceid int,
                                            tkey int)
RETURNS lm3.ScientificName AS
$$
DECLARE
   rec lm3.ScientificName%ROWTYPE;
BEGIN
   SELECT * INTO rec FROM lm3.ScientificName
      WHERE taxonomysourceid = tsourceid and taxonomykey = tkey;
   RETURN rec;
END;
$$  LANGUAGE 'plpgsql' STABLE;

-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lm3.lm_updateTaxon(snid int,
                                              cnt  int,
                                              currtime double precision)
RETURNS int AS
$$
DECLARE
   success int = -1;
   rec lm3.ScientificName%ROWTYPE;
BEGIN
   SELECT * INTO rec FROM lm3.ScientificName
      WHERE scientificnameid = snid;

   IF NOT FOUND THEN
      begin
         RAISE EXCEPTION 'ScientificName % not found', snid;
      end;
   ELSE
      begin
         UPDATE lm3.ScientificName SET (lastcount, datelastmodified) 
                                     = (cnt, currtime) 
                                     WHERE scientificnameid = snid;
         IF FOUND THEN
            success := 0;
         END IF;
      end;
   END IF;
   RETURN success;
END;
$$  LANGUAGE 'plpgsql' VOLATILE;

-- ----------------------------------------------------------------------------
-- ----------------------------------------------------------------------------


-- ----------------------------------------------------------------------------
-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lm3.lm_updatePaths(olddir varchar, newdir varchar)
   RETURNS void AS
$$
DECLARE
   start int = 0;
BEGIN
   start = char_length(olddir) + 1;
   UPDATE lm3.OccurrenceSet SET dlocation = newdir || substr(dlocation, start)  
	   WHERE dlocation like olddir || '%';
	UPDATE lm3.Layer SET dlocation = newdir || substr(dlocation, start)  
	   WHERE dlocation like olddir || '%';
	UPDATE lm3.Model SET dlocation = newdir || substr(dlocation, start)  
	   WHERE dlocation like olddir || '%';
	UPDATE lm3.Projection SET dlocation = newdir || substr(dlocation, start)  
	   WHERE dlocation like olddir || '%';
	
END;
$$ LANGUAGE 'plpgsql' VOLATILE; 


-- ----------------------------------------------------------------------------
-- Uses reftype defined in LmServer.common.lmconstants ReferenceType 
--   and LmDbServer/dbsetup/createMALViews.sql
CREATE OR REPLACE FUNCTION lm3.lm_measureProgress(reftype int,
                                                  starttime double precision,
                                                  endtime double precision,
                                                  usr varchar)
   RETURNS SETOF lm3.lm_progress AS
$$
DECLARE
   rec lm3.lm_progress%ROWTYPE;
   statcol varchar;
   timecol varchar;
   usercol varchar;
   tblname varchar;
   cmd varchar;
   wherecls varchar := '';
   aggcls varchar;
BEGIN
   IF reftype in (104, 101) THEN
      begin
         statcol = 'status';
         timecol = 'statusmodtime';
         usercol = 'userid';
         IF reftype = 104 THEN
            tblname = 'lm3.occurrenceset';
         ELSEIF reftype = 101 THEN
            tblname = 'lm3.model';
         END IF;
      end;
   ELSEIF reftype = 102 THEN
      begin
         statcol = 'prjstatus';
         timecol = 'prjstatusmodtime';
         usercol = 'mdluserid';
         tblname = 'lm3.lm_fullprojection';
      end;
   END IF;
   
   cmd = 'SELECT ' || statcol || ', count(*) FROM ' || tblname || ' ';
   aggcls = ' group by ' || statcol || ' order by ' || statcol;
              
   IF usr IS NOT null THEN
      wherecls = ' WHERE ' || usercol || ' = ' || quote_literal(usr) ;
   END IF;
   IF starttime IS NOT null THEN
      begin
         IF char_length(wherecls) = 0 THEN
            wherecls = ' WHERE ' || timecol || ' >= ' || quote_literal(starttime);
         ELSE
            wherecls = wherecls || ' AND ' || timecol || ' >= ' || quote_literal(starttime) ;
         END IF;
      end;
   END IF;
   IF endtime IS NOT null THEN
      begin
         IF char_length(wherecls) = 0 THEN
            wherecls = ' WHERE ' || timecol || ' <= ' || quote_literal(endtime);
         ELSE
            wherecls = wherecls || ' AND ' || timecol || ' <= ' || quote_literal(endtime) ;
         END IF;
      end;
   END IF;
   
   cmd = cmd || wherecls || aggcls;
   RAISE NOTICE 'cmd= %', cmd;   
	
	FOR rec IN EXECUTE cmd
	LOOP
		RETURN NEXT rec;
	END LOOP;
	RETURN;
END;
$$ LANGUAGE 'plpgsql' STABLE; 


-- ----------------------------------------------------------------------------
-- uses 2-d array
-- SELECT * FROM lm3.lm_getJobObjIds(102, 1100, 'kubi');
CREATE OR REPLACE FUNCTION lm3.lm_getJobObjIds(reftype int,
                                               oldstat int, 
                                               usr varchar)
   RETURNS TABLE(jobid int, objid int) AS
$$
DECLARE
   cmd varchar;
BEGIN
   IF usr is null THEN
      cmd = 'SELECT lmjobid, referenceid FROM lm3.lmjob WHERE referencetype = ' 
             || quote_literal(reftype) || ' AND status = ' || quote_literal(oldstat);
   ELSEIF reftype = 104 THEN
      cmd = 'SELECT lmjobid, occurrencesetid FROM lm3.lm_occjob WHERE occuserid = ' 
            || quote_literal(usr) || ' AND jbstatus = ' || quote_literal(oldstat);
   ELSEIF reftype = 101 THEN
      cmd = 'SELECT lmjobid, modelid FROM lm3.lm_mdljob WHERE mdluserid = ' 
            || quote_literal(usr) || ' AND jbstatus = ' || quote_literal(oldstat);
   ELSEIF reftype = 102 THEN
      cmd = 'SELECT lmjobid, projectionid FROM lm3.lm_prjjob WHERE mdluserid = ' 
            || quote_literal(usr) || ' AND jbstatus = ' || quote_literal(oldstat);
   END IF;
   
   RETURN QUERY EXECUTE cmd;
END;
$$ LANGUAGE 'plpgsql' STABLE; 


-- ----------------------------------------------------------------------------
-- Uses reftype defined in LmServer.common.lmconstants ReferenceType 
--   and LmDbServer/dbsetup/createMALViews.sql 
CREATE OR REPLACE FUNCTION lm3.lm_resetObjectsJobsAtStatus(reftype int,
                                            oldstat int,
                                            newstat int,
                                            currtime double precision,
                                            usr varchar)
   RETURNS int AS
$$
DECLARE
   num int := 0;
   jobid int; 
   objid int;
   rec record ;
BEGIN
   FOR jobid, objid IN SELECT * FROM lm3.lm_getJobObjIds(reftype, oldstat, usr)
   LOOP
      IF reftype = 104 THEN
         UPDATE lm3.occurrenceset SET (status, statusmodtime) = (newstat, currtime) 
            WHERE occurrencesetid = objid;
      ELSEIF reftype = 101 THEN
         UPDATE lm3.model SET (status, statusmodtime) = (newstat, currtime) 
            WHERE modelid = objid;
      ELSEIF reftype = 102 THEN
         UPDATE lm3.projection SET (status, statusmodtime) = (newstat, currtime) 
            WHERE projectionid = objid;
      END IF;
      
      UPDATE lm3.lmjob SET (status, statusmodtime) = (newstat, currtime) 
         WHERE lmjobid = jobid;
      num = num + 1; 
   END LOOP;
   RETURN num;
END;
$$ LANGUAGE 'plpgsql' VOLATILE; 

-- ----------------------------------------------------------------------------
-- Uses reftype defined in LmServer.common.lmconstants ReferenceType 
--   and LmDbServer/dbsetup/createMALViews.sql  
CREATE OR REPLACE FUNCTION lm3.lm_resetObjectAndJob(reftype int,
                                                    objid int,
                                                    newstat int,
                                                    currtime double precision)
   RETURNS int AS
$$
DECLARE
   num int := 0;
BEGIN
   IF reftype = 104 THEN
      UPDATE lm3.occurrenceset SET (status, statusmodtime) = (newstat, currtime) 
         WHERE occurrencesetid = objid;
   ELSEIF reftype = 101 THEN
      UPDATE lm3.model SET (status, statusmodtime) = (newstat, currtime) 
         WHERE modelid = objid;
   ELSEIF reftype = 102 THEN
      UPDATE lm3.projection SET (status, statusmodtime) = (newstat, currtime) 
         WHERE projectionid = objid;
   END IF;
   IF FOUND THEN
      num = num + 1;
   END IF;
      
   UPDATE lm3.lmjob SET (status, statusmodtime) = (newstat, currtime) 
      WHERE referencetype = reftype AND referenceid = objid;
   IF FOUND THEN
      num = num + 1;
   END IF;
   RETURN num;
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
   UPDATE lm3.Occurrenceset SET metadataUrl = newbase || substr(metadataUrl, start)  
	   WHERE metadataurl like oldbase || '%';
   UPDATE lm3.Scenario SET metadataUrl = newbase || substr(metadataUrl, start)  
	   WHERE metadataurl like oldbase || '%';
   UPDATE lm3.Layer SET metadataUrl = newbase || substr(metadataUrl, start)  
	   WHERE metadataurl like oldbase || '%';
   UPDATE lm3.Projection SET metadataUrl = newbase || substr(metadataUrl, start)  
	   WHERE metadataurl like oldbase || '%';
END;
$$ LANGUAGE 'plpgsql' VOLATILE; 
  
-- ----------------------------------------------------------------------------
