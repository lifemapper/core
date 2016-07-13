-- ----------------------------------------------------------------------------
-- From APP_DIR
-- psql -U admin -d borg --file=LmDbServer/dbsetup/createBorgLayerFunctions.sql
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
-- LayerType (EnvLayer)
-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lm_v3.lm_findLayerType(ltypeid int, ltype varchar, usr varchar)
   RETURNS lm_v3.lm_layerTypeAndKeywords AS
$$
DECLARE
   rec lm_v3.lm_layerTypeAndKeywords%rowtype;
   keystr varchar;
BEGIN
   IF ltypeid IS NOT NULL THEN
      SELECT layerTypeId, userid, code, title, description, modTime 
         INTO rec FROM lm_v3.LayerType WHERE layertypeid = ltypeid;
   ELSE
      SELECT layerTypeId, userid, code, title, description, modTime 
         INTO rec FROM lm_v3.LayerType WHERE code = ltype and userid = usr;
   END IF;

   IF FOUND THEN
      SELECT INTO keystr lm_v3.lm_getLayerTypeKeywordString(rec.layertypeid);
      rec.keywords = keystr;
   END IF;
   RETURN rec;
END;
$$  LANGUAGE 'plpgsql' STABLE; 

-- ----------------------------------------------------------------------------
-- Returns existing layerType with keywords, or newly inserted without
CREATE OR REPLACE FUNCTION lm_v3.lm_findOrInsertLayerType(usr varchar,
                                                  ltypeid int,
                                                  ltype varchar,
                                                  ltypetitle varchar,
                                                  ltypedesc varchar,
                                                  mtime double precision)
   RETURNS lm_v3.lm_layerTypeAndKeywords AS
$$
DECLARE
   tid int = -1;
   rec lm_v3.lm_layerTypeAndKeywords%rowtype;
   keystr varchar = '';
BEGIN
   IF ltypeid IS NOT NULL THEN
      SELECT layerTypeId, userid, code, title, description, modTime 
         INTO rec FROM lm_v3.LayerType WHERE layertypeid = ltypeid;
      RAISE NOTICE 'tried with ltypeid %', ltypeid;
   ELSE
      SELECT layerTypeId, userid, code, title, description, modTime 
         INTO rec FROM lm_v3.LayerType WHERE code = ltype and userid = usr;
      RAISE NOTICE 'tried with type, usr';
   END IF;      
      
   IF NOT FOUND THEN
      RAISE NOTICE 'not found';
      INSERT INTO lm_v3.LayerType (code, title, userid, description, modTime) 
         VALUES (ltype, ltypetitle, usr, ltypedesc, mtime);
      IF FOUND THEN
         RAISE NOTICE 'successful insert';
         SELECT INTO tid last_value FROM lm_v3.layertype_layertypeid_seq;
         SELECT * FROM  lm_v3.lm_findLayerType(tid, null, null) INTO rec;
      END IF;
   END IF;
   
   RETURN rec;
END;
$$  LANGUAGE 'plpgsql' VOLATILE; 

-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lm_v3.lm_joinLayerTypeKeyword(typid int, kywd varchar)
   RETURNS int AS
$$
DECLARE
   retval int := -1;
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
      IF total > 0 THEN
         retval := 0;
      ELSE
         INSERT INTO lm_v3.LayerTypeKeyword (layerTypeId, keywordId) 
            VALUES (typid, wdid);
         IF FOUND THEN 
            retval := 0;
         END IF;
      END IF;
   END IF;
   
   RETURN retval;
END;
$$  LANGUAGE 'plpgsql' VOLATILE;

-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lm_v3.lm_joinScenarioLayer(scenid int, lyrid int)
   RETURNS int AS
$$
DECLARE
   success int = -1;
   temp int;
BEGIN
   -- if layer is found
   SELECT count(*) INTO temp FROM lm_v3.scenario WHERE scenarioid = scenid;
   IF temp < 1 THEN
      RAISE EXCEPTION 'Scenario with id % does not exist', scenid;
   END IF;
   
   SELECT count(*) INTO temp FROM lm_v3.layer WHERE layerid = lyrid;
   IF temp < 1 THEN
      RAISE EXCEPTION 'Layer with id % does not exist', lyrid;
   END IF;
   
   SELECT count(*) INTO temp FROM lm_v3.ScenarioLayers WHERE scenarioId = scenid AND layerId = lyrid;
   IF temp < 1 THEN
      -- get or insert scenario x layer entry
      INSERT INTO lm_v3.ScenarioLayers (scenarioId, layerId) VALUES (scenid, lyrid);
      IF FOUND THEN
         success := 0;
      END IF;
   ELSE
      RAISE NOTICE 'Scenario % and Layer % are already joined', scenid, lyrid;
      success := 0;
   END IF;
   
   RETURN success;
END;
$$  LANGUAGE 'plpgsql' VOLATILE;



-- ----------------------------------------------------------------------------
DROP FUNCTION IF EXISTS lm_v3.lm_countTypeCodes(varchar, double precision, double precision);
CREATE OR REPLACE FUNCTION lm_v3.lm_countTypeCodes(usr varchar, 
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
   wherecls = ' WHERE userid =  ' || quote_literal(usr) ;

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
                                                usr varchar(20), 
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
   wherecls = ' WHERE userid =  ' || quote_literal(usr) ;
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
                                                usr varchar(20), 
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
   wherecls = ' WHERE userid =  ' || quote_literal(usr) ;
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
CREATE OR REPLACE FUNCTION lm_v3.lm_getLayerTypeKeywordString(ltid int)
   RETURNS varchar AS
$$
DECLARE
   lyrkeyword record;
   keystr varchar := '';
BEGIN
   FOR lyrkeyword in SELECT k.*
                     FROM lm_v3.keyword k, lm_v3.layertypekeyword lk
                     WHERE lk.layertypeid = ltid
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
-- EnvLayer
-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lm_v3.lm_findOrInsertEnvLayer(lyrverify varchar,
                                          lyrsquid varchar,
                                          usr varchar,
                                          --txid int,
                                          lyrname varchar, 
                                          lyrtitle varchar,
                                          lyrauthor varchar,
                                          lyrdesc varchar,
                                          dloc varchar,
                                          mloc varchar,
                                          vtype int,
                                          rtype int,
                                          iscat boolean,
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
                                          murlprefix varchar,
                                          ltype varchar,
                                          ltypetitle varchar,
                                          ltypedesc varchar)
RETURNS lm_v3.lm_envlayer AS
$$
DECLARE
   lyrid int;
   shpid int;
   reclyr lm_v3.layer%ROWTYPE;
   reclt lm_v3.lm_layerTypeAndKeywords%ROWTYPE;
   rec_envlyr lm_v3.lm_envlayer%ROWTYPE;
BEGIN
   -- get or insert layertype 
   SELECT *  INTO reclt FROM lm_v3.lm_findOrInsertLayerType(usr, lyrtypeid, ltype, ltypetitle, 
       ltypedesc, mtime);
   -- get or insert layer 
   SELECT * FROM lm_v3.lm_findOrInsertLayer(lyrverify, lyrsquid, usr, null, 
            lyrname, lyrtitle, lyrauthor, lyrdesc, dloc, mloc, vtype, rtype, 
            iscat, datafmt, epsg, munits, res, startdt, enddt, mtime, bboxstr, 
            bboxwkt, vattr, vnodata, vmin, vmax, vunits, reclt.layertypeid, 
            murlprefix) INTO reclyr;
         
   IF FOUND THEN
      SELECT * INTO rec_envlyr FROM lm_v3.lm_envlayer 
         WHERE layertypeid = reclt.layertypeid;
   ELSE
      RAISE EXCEPTION 'Unable to insert shapegrid';
   END IF;
   
   RETURN rec_envlyr;
END;
$$  LANGUAGE 'plpgsql' VOLATILE;

-- ----------------------------------------------------------------------------
-- ShapeGrid
-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lm_v3.lm_findOrInsertShapeGrid(lyrverify varchar,
                                          --lyrsquid varchar,
                                          usr varchar,
                                          --txid int,
                                          lyrname varchar, 
                                          lyrtitle varchar,
                                          lyrauthor varchar,
                                          lyrdesc varchar,
                                          dloc varchar,
                                          mloc varchar,
                                          vtype int,
                                          --rtype int,
                                          iscat boolean,
                                          datafmt varchar,
                                          epsg int,
                                          munits varchar,
                                          res double precision,
                                          --startdt double precision,
                                          --enddt double precision,
                                          mtime double precision,
                                          bboxstr varchar,
                                          bboxwkt varchar,
                                          --vattr varchar, 
                                          --vnodata double precision,
                                          --vmin double precision,
                                          --vmax double precision,
                                          --vunits varchar,
                                          --lyrtypeid int,
                                          murlprefix varchar,
                                          csides int,
                                          csize double precision,
                                          vsz int,
                                          idAttr varchar,
                                          xAttr varchar,
                                          yAttr varchar,
                                          stat int,
                                          stattime double precision)
RETURNS lm_v3.lm_shapegrid AS
$$
DECLARE
   lyrid int;
   shpid int;
   reclyr lm_v3.layer%ROWTYPE;
   recsgp lm_v3.shapegrid%ROWTYPE;
   recshpgrd lm_v3.lm_shapegrid%ROWTYPE;
BEGIN
   SELECT * INTO recshpgrd FROM lm_v3.lm_shapegrid 
      WHERE userid = usr AND name = lyrname AND epsgcode = epsg;
   IF NOT FOUND THEN
      begin
         -- get or insert layer 
         SELECT * FROM lm_v3.lm_findOrInsertLayer(lyrverify, null, usr, null, 
            lyrname, lyrtitle, lyrauthor, lyrdesc, dloc, mloc, vtype, null, 
            iscat, datafmt, epsg, munits, res, null, null, mtime, bboxstr, 
            bboxwkt, null, null, null, null, null, null, murlprefix) 
            INTO reclyr;
         IF NOT FOUND THEN
            RAISE EXCEPTION 'Unable to find or insert layer';
         ELSE
            SELECT * FROM lm_v3.lm_findOrInsertShapeGridParams (reclyr.layerid, csides, 
               csize, vsz, idAttr, xAttr, yAttr, stat, stattime) INTO recsgp;
         
            IF FOUND THEN
               SELECT * FROM lm_v3.lm_shapegrid 
                  WHERE shapeGridId = recsgp.shapeGridId INTO recshpgrd;
            ELSE
               RAISE EXCEPTION 'Unable to insert shapegridparams';
            END IF;
         END IF;
      end;
   END IF;
   
   RETURN recshpgrd;
END;
$$  LANGUAGE 'plpgsql' VOLATILE;
 

-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lm_v3.lm_findOrInsertShapeGridParams(lyrid int,
                                              csides int,
                                              csize double precision,
                                              vsz int,
                                              idAttr varchar,
                                              xAttr varchar,
                                              yAttr varchar,
                                              stat int,
                                              stattime double precision)
RETURNS lm_v3.shapegrid AS
$$
DECLARE
   shpid int = -1;
   rec lm_v3.shapegrid%ROWTYPE;
BEGIN
   SELECT * INTO rec FROM lm_v3.lm_shapegrid WHERE layerid = lyrid;
   IF NOT FOUND THEN
      INSERT INTO lm_v3.ShapeGrid (layerId, cellsides, cellsize, vsize, 
                     idAttribute, xAttribute, yAttribute, status, statusmodtime)
        values (lyrid, csides, csize, vsz, idAttr, xAttr, yAttr, stat, stattime);
   
      IF FOUND THEN
         SELECT * INTO rec FROM lm_v3.shapegrid WHERE layerid = lyrid;
      ELSE
         RAISE EXCEPTION 'Unable to insert shapegrid';
      END IF;
   END IF;
   
   RETURN rec;
END;
$$  LANGUAGE 'plpgsql' VOLATILE;
 
-- ----------------------------------------------------------------------------
-- LAYER
-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lm_v3.lm_findOrInsertLayer(lyrverify varchar,
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
                                          iscat boolean,
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
RETURNS lm_v3.Layer AS
$$
DECLARE
   lyrid int = -1;
   idstr varchar;
   murl varchar;
   rec lm_v3.Layer%rowtype;
BEGIN
   -- get or insert layer 
   SELECT * INTO rec FROM lm_v3.Layer WHERE userId = usr
                                        AND name = lyrname
                                        AND epsgcode = epsg;
   IF FOUND THEN
      RAISE NOTICE 'User/Name/EPSG Layer % / % / % found with id %', 
                    usr, lyrname, epsg, rec.layerid;
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
         idstr := cast(lyrid as varchar);
         murl := replace(murlprefix, '#id#', idstr);
         IF bboxwkt is NOT NULL THEN
            UPDATE lm_v3.Layer SET (metadataurl, geom) 
               = (murl, ST_GeomFromText(bboxwkt, epsg)) WHERE layerid = lyrid;
         ELSE
            UPDATE lm_v3.Layer SET metalocation = murl WHERE layerid = lyrid;
         END IF;
         SELECT * INTO rec FROM lm_v3.Layer WHERE layerid = lyrid;
      END IF; -- end if layer inserted
   END IF;  
      
   RETURN rec;
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

