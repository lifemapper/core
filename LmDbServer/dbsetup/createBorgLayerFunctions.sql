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
CREATE OR REPLACE FUNCTION lm_v3.lm_findEnvironmentalType(etypeid int, 
                                                          usr varchar, 
                                                          ecode varchar, 
                                                          gcode varchar, 
                                                          apcode varchar, 
                                                          dtcode varchar)
   RETURNS lm_v3.EnvironmentalType AS
$$
DECLARE
   rec lm_v3.EnvironmentalType%rowtype;
   keystr varchar;
BEGIN
   IF etypeid IS NOT NULL THEN
      SELECT * INTO rec FROM lm_v3.EnvironmentalType WHERE EnvironmentalTypeid = etypeid;
   ELSE
      SELECT * INTO rec FROM lm_v3.EnvironmentalType 
         WHERE userid = usr AND envcode = ecode AND gcmcode = gcode 
           AND altpredcode = apcode AND datecode = dtcode;
   END IF;

   RETURN rec;
END;
$$  LANGUAGE 'plpgsql' STABLE; 

-- ----------------------------------------------------------------------------
-- Returns existing EnvironmentalType with keywords, or newly inserted without
CREATE OR REPLACE FUNCTION lm_v3.lm_findOrInsertEnvironmentalType(etypeid int, 
                                                                  usr varchar, 
                                                                  ecode varchar, 
                                                                  gcode varchar, 
                                                                  apcode varchar, 
                                                                  dtcode varchar,
                                                                  ettitle varchar,
                                                                  etdesc varchar,
                                                                  etkeywords varchar,
                                                                  mtime double precision)
   RETURNS lm_v3.EnvironmentalType AS
$$
DECLARE
   tid int = -1;
   rec lm_v3.EnvironmentalType%rowtype;
   keystr varchar = '';
BEGIN
   SELECT * INTO rec FROM 
      lm_v3.lm_findEnvironmentalType(usr, ecode, gcode, apcode, dtcode);
   IF NOT FOUND THEN
      INSERT INTO lm_v3.EnvironmentalType (userid, envCode, gcmCode, altpredCode, 
                                dateCode, title, description, keywords, modTime) 
      VALUES (usr, ecode, gcode, apcode, 
                                dtcode, ettitle, etdesc, etkeywords, mtime);
      IF FOUND THEN
         RAISE NOTICE 'successful insert';
         SELECT INTO tid last_value FROM lm_v3.EnvironmentalType_EnvironmentalTypeid_seq;
         SELECT * FROM  lm_v3.lm_findEnvironmentalType(tid, null, null) INTO rec;
      END IF;
   END IF;
   
   RETURN rec;
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
   cmd = 'SELECT count(*) FROM lm_v3.EnvironmentalType ';
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
   cmd = 'SELECT EnvironmentalTypeId, code, description, datelastmodified, title
               FROM lm_v3.EnvironmentalType ';
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
   RETURNS SETOF lm_v3.lm_EnvironmentalTypeAndKeywords AS
$$
DECLARE
   rec lm_v3.lm_EnvironmentalTypeAndKeywords;
   keystr varchar;
   ltTitle varchar;
   cmd varchar;
   wherecls varchar;
   limitcls varchar;
   ordercls varchar;
BEGIN
   cmd = 'SELECT * FROM lm_v3.EnvironmentalType ';
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
         SELECT INTO keystr lm_v3.lm_getEnvironmentalTypeKeywordString(rec.EnvironmentalTypeid);
         rec.keywords = keystr;
         RETURN NEXT rec;
      END LOOP;
   RETURN;
END;
$$  LANGUAGE 'plpgsql' STABLE;



-- ----------------------------------------------------------------------------
-- EnvLayer
-- ----------------------------------------------------------------------------
DROP FUNCTION IF EXISTS lm_v3.lm_findOrInsertEnvLayer(lyrverify varchar,
lyrsquid varchar,usr varchar,lyrname varchar, lyrtitle varchar,lyrauthor varchar,
lyrdesc varchar, dloc varchar,mloc varchar,vtype int,rtype int,iscat boolean,
datafmt varchar,epsg int,munits varchar,res double precision,startdt double precision,
enddt double precision,mtime double precision,bboxstr varchar,bboxwkt varchar,
vattr varchar, vnodata double precision,vmin double precision,vmax double precision,
vunits varchar,lyrtypeid int,murlprefix varchar,ltype varchar,ltypetitle varchar,
ltypedesc varchar);
CREATE OR REPLACE FUNCTION lm_v3.lm_findOrInsertEnvLayer(lyrverify varchar,
                                          lyrsquid varchar,
                                          usr varchar,
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
   SELECT * INTO reclt FROM lm_v3.lm_findOrInsertEnvironmentalType(usr, env, 
                    gcm, altpred, tm, etypetitle, etypedesc, keywds, modtime);
   -- get or insert layer 
   SELECT * FROM lm_v3.lm_findOrInsertLayer(lyrverify, lyrsquid, usr, null, 
            lyrname, lyrtitle, lyrauthor, lyrdesc, dloc, mloc, vtype, rtype, 
            iscat, datafmt, epsg, munits, res, startdt, enddt, mtime, bboxstr, 
            bboxwkt, vnodata, vmin, vmax, vunits, reclt.layertypeid, 
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
CREATE OR REPLACE FUNCTION lm_v3.lm_findOrInsertEnvironmentalType(usr varchar,
                                                        env varchar,
                                                        gcm varchar,
                                                        altpred varchar,
                                                        tm varchar,
                                                        etypetitle varchar,
                                                        etypedesc varchar,
                                                        keywds text,
                                                        modtime double precision)
   RETURNS int AS
$$
DECLARE
   etypeid int;
BEGIN
   INSERT INTO lm_v3.EnvironmentalType 
      (userid, envCode, gcmCode, altpredCode, dateCode, title, description, keywords, modTime) 
      VALUES 
      (usr, env, gcm, altpred, tm, etypetitle, etypedesc, keywds, modtime);
   IF FOUND THEN
      SELECT INTO etypeid last_value FROM lm3.EnvironmentalType_EnvironmentalTypeid_seq;
   END IF;
   
   RETURN etypeid;
END;
$$  LANGUAGE 'plpgsql' VOLATILE; 

-- ----------------------------------------------------------------------------
-- ShapeGrid
-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lm_v3.lm_findOrInsertShapeGrid(usr varchar,
                                          lyrsquid varchar,
                                          lyrverify varchar,
                                          lyrname varchar, 
                                          lyrdloc varchar,
                                          lyrmurlprefix varchar,
                                          lyrmeta varchar,
                                          datafmt varchar,
                                          rtype int,
                                          vtype int,
                                          vunits varchar,
                                          vnodata double precision,
                                          vmin double precision,
                                          vmax double precision,
                                          epsg int,
                                          munits varchar,
                                          res double precision,
                                          bboxstr varchar,
                                          bboxwkt varchar,
                                          lyrmtime double precision,
                                          
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
         SELECT * FROM lm_v3.lm_findOrInsertLayer(usr, lyrsquid, lyrverify, 
            lyrname, lyrdloc, lyrmurlprefix, lyrmeta, datafmt, rtype, vtype, 
            vunits, vnodata, vmin, vmax, epsg, munits, res, bboxstr, bboxwkt, 
            lyrmtime)  INTO reclyr;
         IF NOT FOUND THEN
            RAISE EXCEPTION 'Unable to find or insert layer';
         ELSE
            SELECT * INTO rec FROM lm_v3.lm_shapegrid WHERE layerid = reclyr.layerid;
            IF NOT FOUND THEN
               INSERT INTO lm_v3.ShapeGrid (layerid, cellsides, cellsize, vsize, 
                  idAttribute, xAttribute, yAttribute, status, statusmodtime)
                  VALUES (reclyr.layerid, csides, csize, vsz, 
                  idAttr, xAttr, yAttr, stat, stattime);
               IF FOUND THEN
                  SELECT * INTO rec FROM lm_v3.shapegrid WHERE layerid = lyrid;
               ELSE
                  RAISE EXCEPTION 'Unable to insert shapegrid';
               END IF;
            END IF;
         END IF;
      end;
   END IF;
   
   RETURN recshpgrd;
END;
$$  LANGUAGE 'plpgsql' VOLATILE;
 
 
-- ----------------------------------------------------------------------------
-- LAYER
-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lm_v3.lm_findOrInsertLayer(usr varchar,
                                          lyrsquid varchar,
                                          lyrverify varchar,
                                          lyrname varchar, 
                                          lyrdloc varchar,
                                          lyrmurlprefix varchar,
                                          lyrmeta varchar,
                                          datafmt varchar,
                                          rtype int,
                                          vtype int,
                                          vunits varchar,
                                          vnodata double precision,
                                          vmin double precision,
                                          vmax double precision,
                                          epsg int,
                                          munits varchar,
                                          res double precision,
                                          bboxstr varchar,
                                          bboxwkt varchar,
                                          lyrmtime double precision)
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
      INSERT INTO lm_v3.Layer (userid, squid, verify, name, dlocation, metadata, 
           dataFormat, gdalType, ogrType, valUnits, nodataVal, minVal, maxVal, 
           epsgcode, mapunits, resolution, bbox, modTime)
         VALUES 
          (usr, lyrverify, lyrsquid, lyrname, dloc, datafmt, rtype, vtype, 
           vunits, vnodata, vmin, vmax, epsg, munits, res, bboxstr, lyrmtime);         
                  
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

