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
   cmd varchar;
   wherecls varchar;
BEGIN
   IF etypeid IS NOT NULL THEN
      SELECT * INTO rec FROM lm_v3.EnvironmentalType WHERE EnvironmentalTypeid = etypeid;
   ELSE
      begin
         cmd = 'SELECT * FROM lm_v3.EnvironmentalType ';
         wherecls = ' WHERE userid =  ' || quote_literal(usr) ;

         IF ecode is not null THEN
            wherecls = wherecls || ' AND envcode =  ' || quote_literal(ecode);
         ELSE
            wherecls = wherecls || ' AND envcode IS NULL ';
         END IF;

         IF gcode is not null THEN
            wherecls = wherecls || ' AND gcmcode =  ' || quote_literal(gcode);
         ELSE
            wherecls = wherecls || ' AND gcmcode IS NULL ';
         END IF;
         
         IF apcode is not null THEN
            wherecls = wherecls || ' AND altpredcode =  ' || quote_literal(apcode);
         ELSE
            wherecls = wherecls || ' AND altpredcode IS NULL ';
         END IF;
         
         IF dtcode is not null THEN
            wherecls = wherecls || ' AND datecode =  ' || quote_literal(dtcode);
         ELSE
            wherecls = wherecls || ' AND datecode IS NULL ';
         END IF;

         cmd := cmd || wherecls;
         RAISE NOTICE 'cmd = %', cmd;

         EXECUTE cmd INTO rec;
      end;
   END IF;

   RETURN rec;
END;
$$  LANGUAGE 'plpgsql' STABLE; 

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
-- EnvLayer
-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lm_v3.lm_findOrInsertEnvLayer(lyrid int,
                                          usr varchar,
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
                                          
                                          etypeid int, 
                                          env varchar,
                                          gcm varchar,
                                          altpred varchar,
                                          tm varchar,
                                          etypemeta text,
                                          etypemodtime double precision)
RETURNS lm_v3.lm_envlayer AS
$$
DECLARE
   joinid int;
   reclyr lm_v3.layer%ROWTYPE;
   rec_etype lm_v3.EnvironmentalType%ROWTYPE;
   rec_envlyr lm_v3.lm_envlayer%ROWTYPE;
BEGIN
   -- get or insert environmentalType 
   SELECT * INTO rec_etype FROM lm_v3.lm_findOrInsertEnvironmentalType(etypeid, 
                    usr, env, gcm, altpred, tm, etypemeta, etypemodtime);
   IF NOT FOUND THEN
      RAISE EXCEPTION 'Unable to findOrInsertEnvironmentalType';
   ELSE
      -- get or insert layer 
      SELECT * FROM lm_v3.lm_findOrInsertLayer(lyrid, usr, lyrsquid, lyrverify, 
         lyrname, lyrdloc, lyrmurlprefix, lyrmeta, datafmt, rtype, vtype, vunits, 
         vnodata, vmin, vmax, epsg, munits, res, bboxstr, bboxwkt, lyrmtime) INTO reclyr;
         
      IF NOT FOUND THEN
         RAISE EXCEPTION 'Unable to findOrInsertLayer';
      ELSE
         SELECT environmentalLayerId INTO joinid FROM lm_v3.EnvironmentalLayer 
            WHERE layerid  = reclyr.layerid AND environmentalTypeId = rec_etype.environmentalTypeId;
         IF NOT FOUND THEN
            INSERT INTO lm_v3.EnvironmentalLayer (layerId, environmentalTypeId)
                         VALUES (reclyr.layerId, rec_etype.environmentalTypeId);
            IF NOT FOUND THEN
               RAISE EXCEPTION 'Unable to insert/join EnvironmentalLayer';
            ELSE
               SELECT INTO joinid last_value FROM lm_v3.EnvironmentalLayer_EnvironmentalLayerid_seq;
            END IF;
         END IF;
         
         SELECT * INTO rec_envlyr FROM lm_v3.lm_envlayer WHERE environmentalLayerId = joinid;
      END IF;
   END IF;
   
   RETURN rec_envlyr;
END;
$$  LANGUAGE 'plpgsql' VOLATILE;

-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lm_v3.lm_findOrInsertEnvironmentalType(etypeid int, 
                                                        usr varchar,
                                                        env varchar,
                                                        gcm varchar,
                                                        altpred varchar,
                                                        tm varchar,
                                                        meta text,
                                                        modtime double precision)
   RETURNS lm_v3.EnvironmentalType AS
$$
DECLARE
   rec lm_v3.EnvironmentalType%ROWTYPE;
   newid int;
BEGIN
   SELECT * into rec FROM lm_v3.lm_findEnvironmentalType(etypeid, usr, env, gcm, 
                                                         altpred, tm);
   IF rec.environmentalTypeId IS NULL THEN
      INSERT INTO lm_v3.EnvironmentalType 
         (userid, envCode, gcmCode, altpredCode, dateCode, metadata, modTime) 
      VALUES (usr, env, gcm, altpred, tm, meta, modtime);
      RAISE NOTICE 'vals = %, %, %, %, %, %, %', usr, env, gcm, altpred, tm, meta, modtime;
      IF NOT FOUND THEN
         RAISE EXCEPTION 'Unable to insert EnvironmentalType';
      ELSE
         SELECT INTO newid last_value FROM lm_v3.EnvironmentalType_EnvironmentalTypeid_seq;
         SELECT * INTO rec FROM lm_v3.EnvironmentalType where environmentalTypeId = newid;
      END IF;
   END IF;
   
   RETURN rec;
END;
$$  LANGUAGE 'plpgsql' VOLATILE; 

-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lm_v3.lm_getEnvLayersForScenario(scenid int)
RETURNS SETOF lm_v3.lm_envlayer AS
$$
DECLARE
   rec lm_v3.lm_envlayer%ROWTYPE;
   elid int;
BEGIN
   FOR elid IN SELECT environmentalLayerId FROM lm_v3.ScenarioLayers 
         WHERE scenarioId = scenid
      LOOP
         SELECT * into rec FROM lm_v3.lm_envlayer WHERE environmentalLayerId = elid;
         RETURN NEXT rec;
      END LOOP;
   RETURN;
END;
$$  LANGUAGE 'plpgsql' STABLE;


-- ----------------------------------------------------------------------------
-- ShapeGrid
-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lm_v3.lm_findOrInsertShapeGrid(lid int,
                                          usr varchar,
                                          lsquid varchar,
                                          lverify varchar,
                                          lname varchar, 
                                          ldloc varchar,
                                          lmurlprefix varchar,
                                          lmeta varchar,
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
                                          lmtime double precision,
                                          
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
   recshpgrd lm_v3.lm_shapegrid%ROWTYPE;
BEGIN
   IF lyrid IS NOT NULL THEN
      SELECT * INTO recshpgrd FROM lm_v3.lm_shapegrid WHERE layerid = lid;
   ELSE
      SELECT * INTO recshpgrd FROM lm_v3.lm_shapegrid 
         WHERE userid = usr AND lyrname = lname AND epsgcode = epsg;
   END IF;
   
   IF NOT FOUND THEN
      begin
         -- get or insert layer 
         SELECT * FROM lm_v3.lm_findOrInsertLayer(lid, usr, lsquid, lverify, 
            lname, ldloc, lmurlprefix, lmeta, datafmt, rtype, vtype, vunits, 
            vnodata, vmin, vmax, epsg, munits, res, bboxstr, bboxwkt, lmtime) INTO reclyr;
         
         IF NOT FOUND THEN
            RAISE EXCEPTION 'Unable to find or insert layer';
         ELSE
            INSERT INTO lm_v3.ShapeGrid (layerid, cellsides, cellsize, vsize, 
               idAttribute, xAttribute, yAttribute, status, statusmodtime)
            VALUES (reclyr.layerid, csides, csize, vsz, 
               idAttr, xAttr, yAttr, stat, stattime);
            
            IF NOT FOUND THEN
               RAISE EXCEPTION 'Unable to insert shapegrid';
            ELSE
               SELECT * INTO recshpgrd FROM lm_v3.lm_shapegrid 
                 WHERE layerid = reclyr.layerid;
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
CREATE OR REPLACE FUNCTION lm_v3.lm_findOrInsertLayer(lyrid int,
                                          usr varchar,
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
   newid int = -1;
   idstr varchar;
   murl varchar;
   rec lm_v3.Layer%rowtype;
BEGIN
   -- get or insert layer 
   IF lyrid IS NOT NULL THEN
      SELECT * INTO rec FROM lm_v3.Layer WHERE layerid = lyrid;
   ELSE
      SELECT * INTO rec FROM lm_v3.Layer WHERE userId = usr AND name = lyrname
                                           AND epsgcode = epsg;
   END IF;
   
   IF FOUND THEN
      RAISE NOTICE 'User/Name/EPSG Layer % / % / % found with id %', 
                    usr, lyrname, epsg, rec.layerid;
   ELSE
      INSERT INTO lm_v3.Layer (userid, squid, verify, name, dlocation, metadata, 
           dataFormat, gdalType, ogrType, valUnits, nodataVal, minVal, maxVal, 
           epsgcode, mapunits, resolution, bbox, modTime)
         VALUES 
          (usr, lyrsquid, lyrverify, lyrname, lyrdloc, lyrmeta, 
           datafmt, rtype, vtype, vunits, vnodata, vmin, vmax, 
           epsg, munits, res, bboxstr, lyrmtime);         
                  
      IF FOUND THEN
         SELECT INTO newid last_value FROM lm_v3.layer_layerid_seq;
         idstr := cast(newid as varchar);
         murl := replace(lyrmurlprefix, '#id#', idstr);
         IF bboxwkt is NOT NULL THEN
            UPDATE lm_v3.Layer SET (metadataurl, geom) 
               = (murl, ST_GeomFromText(bboxwkt, epsg)) WHERE layerid = newid;
         ELSE
            UPDATE lm_v3.Layer SET metadataurl = murl WHERE layerid = newid;
         END IF;
         
         SELECT * INTO rec FROM lm_v3.Layer WHERE layerid = newid;
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

