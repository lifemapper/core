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
   rec lm_v3.v3.algorithm;
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
      BEGIN
         INSERT INTO lm_v3.Keyword (keyword) VALUES (kywd);
         IF FOUND THEN
            SELECT INTO wdid last_value FROM lm_v3.keyword_keywordid_seq;
         END IF;
      END;
   END IF;
   -- if found or inserted, join
   IF FOUND THEN
      BEGIN
         SELECT count(*) INTO total FROM lm_v3.LayerTypeKeyword 
            WHERE layerTypeId = typid AND keywordId = wdid;
         IF total = 0 THEN
            BEGIN
               INSERT INTO lm_v3.LayerTypeKeyword (layerTypeId, keywordId) 
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
CREATE OR REPLACE FUNCTION lm_v3.lm_insertEnvLayer(lyrverify varchar,
                                             lyrsquid varchar, 
                                             usr varchar,
                                             txid int,
                                             lyrname varchar,
                                             lyrtitle varchar,
                                             lyrauthor,
                                             descr text,
                                             dloc varchar,
                                             lyrurl varchar,
                                             mloc varchar,
                                             gtype int,
                                             iscat boolean,
                                             dformat varchar,
                                             epsg int,
                                             munits varchar,
                                             res double precision,
                                             stdt double precision,
                                             enddt double precision,
                                             mtime double precision,
                                             bboxstr varchar,
                                             bboxwkt varchar,
                                             vattr varchar,
                                             vnodata double precision,
                                             vmin double precision,
                                             vmax double precision,
                                             vunits varchar,
                                             lyrtypeid int)
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
   -- *** Will fail with legacy duplicate layernames of user changeThinking ***
   SELECT l.layerid INTO lyrid FROM lm_v3.Layer l 
      WHERE l.name = lyrname AND l.userid = usr;

   IF NOT FOUND THEN
      INSERT INTO lm_v3.Layer (verify, squid, userid, taxonid, name, title, author, 
                             description, dlocation, metalocation, 
                             gdalType, isCategorical, dataFormat, epsgcode, 
                             mapunits, resolution, startDate, endDate, modTime, 
                             bbox, valAttribute, nodataVal, minVal, maxVal, 
                             valUnits, layerTypeId)
            VALUES (lyrverify, lyrsquid, usr, txid, lyrname, lyrtitle, lyrauthor,
                    descr, dloc, mloc, gtype, iscat, dformat, epsg, munits, res,
                    stdt, enddt, mtime, bboxstr, vattr, vnodata, vmin, vmax, 
                    vunits, lyrtypeid);
      -- if successful      
      IF FOUND THEN
         SELECT INTO lyrid last_value FROM lm_v3.layer_layerid_seq;
         idstr = cast(lyrid as varchar);
         lyrmetadataUrl := replace(lyrurl, '#id#', idstr);
         IF epsg = 4326 THEN
            UPDATE lm_v3.layer SET (metadataUrl, geom) 
                             = (lyrmetadataUrl, ST_GeomFromText(bboxwkt, epsg)) 
               WHERE layerid = lyrid;         
         ELSE
            UPDATE lm_v3.layer SET metadataUrl = lyrmetadataUrl WHERE layerid = lyrid;
         END IF;
      END IF; -- end if successful insert
      
   -- layer exists 
   ELSE
      success := 0;
      RAISE NOTICE 'Layer with matching user/name or URL found with id %', lyrid;
   END IF;
      
   RETURN lyrid;
END;
$$  LANGUAGE 'plpgsql' VOLATILE;


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
