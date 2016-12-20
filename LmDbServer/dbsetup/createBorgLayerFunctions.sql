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
CREATE OR REPLACE FUNCTION lm_v3.lm_findEnvType(etypeid int, 
                                                usr varchar, 
                                                ecode varchar, 
                                                gcode varchar, 
                                                apcode varchar, 
                                                dtcode varchar)
   RETURNS lm_v3.EnvType AS
$$
DECLARE
   rec lm_v3.EnvType%rowtype;
   cmd varchar;
   wherecls varchar;
BEGIN
   IF etypeid IS NOT NULL THEN
      SELECT * INTO rec FROM lm_v3.EnvType WHERE EnvTypeid = etypeid;
   ELSE
      begin
         cmd = 'SELECT * FROM lm_v3.EnvType ';
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
CREATE OR REPLACE FUNCTION lm_v3.lm_joinEnvLayer(lyrid int, etypeid int)
   RETURNS lm_v3.lm_envlayer AS
$$
DECLARE
   temp1 int;
   temp2 int;
   rec lm_v3.lm_envlayer%ROWTYPE;
BEGIN
   SELECT count(*) INTO temp1 FROM lm_v3.layer WHERE layerId = lyrid;
   SELECT count(*) INTO temp2 FROM lm_v3.envType WHERE envTypeId = etypeid;
   IF temp1 < 1 THEN
      RAISE EXCEPTION 'Layer with id % does not exist', lyrid;
   ELSIF temp2 < 1 THEN
      RAISE EXCEPTION 'EnvType with id % does not exist', etypeid;
   END IF;
   
   SELECT * INTO rec FROM lm_v3.lm_envlayer
      WHERE layerid = lyrid AND envTypeId = etypeid;
   IF FOUND THEN 
      RAISE NOTICE 'Layer % and EnvType % are already joined', lyrid, etypeid;
   ELSE   
      INSERT INTO EnvLayer (layerid, envTypeId) VALUES (lyrid, etypeid);
      IF NOT FOUND THEN
         RAISE EXCEPTION 'Unable to insert/join EnvLayer';
      ELSE
         SELECT * INTO rec FROM lm_v3.lm_envlayer WHERE layerid = lyrid 
                                                    AND envTypeId = etypeid;
      END IF;
   END IF;
   
   RETURN rec;
END;
$$  LANGUAGE 'plpgsql' VOLATILE;

-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lm_v3.lm_joinScenarioLayer(scenid int, lyrid int, etypeid int)
   RETURNS lm_v3.lm_scenlayer AS
$$
DECLARE
   temp int;
   elid int;
   rec_scenlyr lm_v3.lm_scenlayer%ROWTYPE;
BEGIN
   SELECT count(*) INTO temp FROM lm_v3.scenario WHERE scenarioid = scenid;
   IF temp < 1 THEN
      RAISE EXCEPTION 'Scenario with id % does not exist', scenid;
   END IF;
   
   SELECT envlayerid INTO elid FROM lm_v3.lm_joinEnvLayer(lyrid, etypeid);
   IF FOUND THEN    
      SELECT * INTO rec_scenlyr FROM lm_v3.lm_scenlayer 
         WHERE scenarioId = scenid AND envLayerId = elid;
      IF rec_scenlyr.scenariolayerid IS NULL THEN
         INSERT INTO lm_v3.ScenarioLayer (scenarioid, envlayerid) 
                                  VALUES (scenid, elid);
         IF NOT FOUND THEN
            RAISE EXCEPTION 'Unable to insert/join EnvLayer';
         ELSE
            SELECT * INTO rec_scenlyr FROM lm_v3.ScenarioLayer 
               WHERE scenarioId = scenid AND envLayerId = elid;
         END IF;
      END IF;
   END IF;
   
   RETURN rec_scenlyr;
END;
$$  LANGUAGE 'plpgsql' VOLATILE;


-- ----------------------------------------------------------------------------
-- EnvLayer
-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lm_v3.lm_findOrInsertScenLayer(scenid int,
                                          lyrid int,
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
RETURNS lm_v3.lm_scenlayer AS
$$
DECLARE
   reclyr lm_v3.layer%ROWTYPE;
   rec_etype lm_v3.EnvType%ROWTYPE;
   rec_envlyr lm_v3.lm_scenlayer%ROWTYPE;
BEGIN
   -- get or insert envType 
   SELECT * INTO rec_etype FROM lm_v3.lm_findOrInsertEnvType(etypeid, 
                    usr, env, gcm, altpred, tm, etypemeta, etypemodtime);
   IF NOT FOUND THEN
      RAISE EXCEPTION 'Unable to findOrInsertEnvType';
   ELSE
      -- get or insert layer 
      SELECT * FROM lm_v3.lm_findOrInsertLayer(lyrid, usr, lyrsquid, lyrverify, 
         lyrname, lyrdloc, lyrmurlprefix, lyrmeta, datafmt, rtype, vtype, vunits, 
         vnodata, vmin, vmax, epsg, munits, res, bboxstr, bboxwkt, lyrmtime) INTO reclyr;
         
      IF NOT FOUND THEN
         RAISE EXCEPTION 'Unable to findOrInsertLayer';
      ELSE
         SELECT * INTO rec_envlyr FROM lm_v3.lm_joinScenarioLayer(scenid, 
                                 reclyr.layerId, rec_etype.envTypeId);
      END IF;
   END IF;
   
   RETURN rec_envlyr;
END;
$$  LANGUAGE 'plpgsql' VOLATILE;

-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lm_v3.lm_findOrInsertEnvType(etypeid int, 
                                                        usr varchar,
                                                        env varchar,
                                                        gcm varchar,
                                                        altpred varchar,
                                                        tm varchar,
                                                        meta text,
                                                        modtime double precision)
   RETURNS lm_v3.EnvType AS
$$
DECLARE
   rec lm_v3.EnvType%ROWTYPE;
   newid int;
BEGIN
   SELECT * into rec FROM lm_findEnvType(etypeid, usr, env, gcm, altpred, tm);
   IF rec.envtypeid IS NOT NULL THEN
      RAISE NOTICE 'EnvType, id %, found for % % % % %', rec.envtypeid, 
                                                 usr, env, gcm, altpred, tm;
   ELSE
      RAISE NOTICE 'Inserting EnvType for % % % % %', usr, env, gcm, altpred, tm;
      INSERT INTO lm_v3.EnvType 
         (userid, envCode, gcmCode, altpredCode, dateCode, metadata, modTime) 
      VALUES (usr, env, gcm, altpred, tm, meta, modtime);
 
      IF NOT FOUND THEN
         RAISE EXCEPTION 'Unable to insert EnvType';
      ELSE
         SELECT INTO newid last_value FROM lm_v3.EnvType_EnvTypeid_seq;
         SELECT * INTO rec FROM lm_v3.EnvType where envTypeId = newid;
      END IF;
   END IF;
   
   RETURN rec;
END;
$$  LANGUAGE 'plpgsql' VOLATILE; 

-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lm_v3.lm_getEnvLayersForScenario(scenid int)
RETURNS SETOF lm_v3.lm_scenlayer AS
$$
DECLARE
   rec lm_v3.lm_scenlayer%ROWTYPE;
   elid int;
BEGIN
   FOR rec IN SELECT * FROM lm_v3.lm_scenlayer WHERE scenarioId = scenid
      LOOP
         RETURN NEXT rec;
      END LOOP;
   RETURN;
END;
$$  LANGUAGE 'plpgsql' STABLE;

-- ----------------------------------------------------------------------------
-- SDMProject
-- ----------------------------------------------------------------------------
-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lm_v3.lm_findOrInsertSDMProject(prjid int,
                                                           lyrid int,
                                                           usr varchar, 
                                                           occid int,
                                                           algcode varchar,
                                                           alghash text,
                                                           mdlscenid int,
                                                           mdlmskid int,
                                                           prjscenid int,
                                                           prjmskid int,
                                                           prjmeta text, 
                                                           ptype int, 
                                                           stat int, 
                                                           stattime double precision)
   RETURNS lm_v3.lm_sdmproject AS
$$
DECLARE
   rec lm_v3.lm_sdmproject%ROWTYPE;                             
BEGIN
   IF prjid IS NOT NULL then                     
      SELECT * INTO rec from lm_v3.lm_sdmproject WHERE sdmprojectId = prjid;
   ELSIF lyrid IS NOT NULL then                     
      SELECT * INTO rec from lm_v3.lm_sdmproject WHERE layerId = lyrid;
   ELSE
      SELECT * INTO rec from lm_v3.lm_sdmproject
             WHERE userid = usr 
               AND occurrenceSetId = occid 
               AND algorithmCode = algcode 
               AND algParamHash = alghash
               AND mdlscenarioId = mdlscenid
               AND mdlmaskId = mdlmskid
               AND prjscenarioId = prjscenid
               AND prjmaskId = prjmskid;
   END IF;
   
   IF NOT FOUND THEN 
   RETURN rec;                                              
END; 
$$ LANGUAGE 'plpgsql' STABLE; 
                                                                        
-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lm_v3.lm_findOrInsertSDMProject(prjid int, 
                                          lyrid int,
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
                                          
                                          occid int,
                                          algcode varchar,
                                          alghash text,
                                          mdlscenid int,
                                          mdlmskid int,
                                          prjscenid int,
                                          prjmskid int,
                                          prjmeta text,
                                          ptype int,
                                          stat int,
                                          stattime double precision)
RETURNS lm_v3.lm_sdmproject AS
$$
occurrenceSetId,
                                          algorithmCode,
                                          algParamHash,
                                          mdlscenarioId,
                                          mdlmaskId,
                                          prjscenarioId,
                                          prjmaskId,
                                          metadata,
                                          processType,
                                          status,
                                          statusModTime
DECLARE
   newid int = -1;
   idstr varchar;
   murl varchar;
   rec_lyr lm_v3.Layer%rowtype;
   rec_prj lm_v3.SDMProject%rowtype;
   rec_fullprj lm_v3.lm_sdmproject%rowtype;
BEGIN
   -- get or insert sdmproject 
   SELECT * INTO rec_prj FROM lm_v3.lm_findOrInsertSDMProject(prjid, lyrid, usr, occid, 
                   algcode, alghash, mdlscenid, mdlmskid, prjscenid, prjmskid, 
                   prjmeta, ptype, stat, stattime);
   IF NOT FOUND THEN
      RAISE EXCEPTION 'Unable to findOrInsertSDMProject';
   ELSE

   -- get or insert layer 
   SELECT * FROM lm_v3.lm_findOrInsertLayer(lyrid, usr, lyrsquid, lyrverify, 
      lyrname, lyrdloc, lyrmurlprefix, lyrmeta, datafmt, rtype, vtype, vunits, 
      vnodata, vmin, vmax, epsg, munits, res, bboxstr, bboxwkt, lyrmtime) INTO reclyr;


   -- get or insert envType 
   SELECT * INTO rec_etype FROM lm_v3.lm_findOrInsertEnvType(etypeid, 
                    usr, env, gcm, altpred, tm, etypemeta, etypemodtime);
   IF NOT FOUND THEN
      RAISE EXCEPTION 'Unable to findOrInsertEnvType';
   ELSE
         
      IF NOT FOUND THEN
         RAISE EXCEPTION 'Unable to findOrInsertLayer';
      ELSE
         SELECT * INTO rec_envlyr FROM lm_v3.lm_joinScenarioLayer(scenid, 
                                 reclyr.layerId, rec_etype.envTypeId);
      END IF;
   END IF;
   
   RETURN rec_envlyr;

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
CREATE OR REPLACE FUNCTION lm_v3.lm_updateShapeGrid(lyrid int,
                                          lyrverify varchar,
                                          lyrdloc varchar,
                                          lyrmeta varchar,
                                          lyrmtime double precision,
                                          vsz int,
                                          stat int,
                                          stattime double precision)
RETURNS lm_v3.lm_shapegrid AS
$$
DECLARE
   reclyr lm_v3.layer%ROWTYPE;
   recshpgrd lm_v3.lm_shapegrid%ROWTYPE;
BEGIN
   SELECT * INTO recshpgrd FROM lm_v3.lm_shapegrid WHERE layerid = lyrid;
   IF NOT FOUND THEN
      -- get or insert layer 
      SELECT * FROM lm_v3.lm_updateLayer(lyrid, null, lverify, 
         ldloc, lmeta, null, null, null, lmtime) INTO reclyr;
         
      IF NOT FOUND THEN
         RAISE EXCEPTION 'Unable to find or update layer';
      ELSE
         UPDATE lm_v3.ShapeGrid 
            SET (vsize, status, statusmodtime) = (vsz, stat, stattime);
            
         IF NOT FOUND THEN
            RAISE EXCEPTION 'Unable to update shapegrid';
         ELSE
            SELECT * INTO recshpgrd FROM lm_v3.lm_shapegrid 
              WHERE layerid = lyrid;
         END IF;
      END IF;
   END IF;
   
   RETURN recshpgrd;
END;
$$  LANGUAGE 'plpgsql' VOLATILE;

-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lm_v3.lm_getShapegrid(sgid int, 
                                                 lyrid int, 
                                                 usr varchar, 
                                                 nm varchar, 
                                                 epsg int)
RETURNS lm_v3.lm_shapegrid AS
$$
DECLARE
   rec lm_v3.lm_shapegrid%ROWTYPE;
BEGIN
   IF sgid IS NOT NULL THEN
      SELECT * INTO rec FROM lm_v3.lm_shapegrid WHERE shapeGridId = sgid;
   ELSIF lyrid IS NOT NULL THEN
      SELECT * INTO rec FROM lm_v3.lm_shapegrid WHERE layerId = lyrid;
   ELSE
      SELECT * INTO rec FROM lm_v3.lm_shapegrid WHERE userid = usr 
                                                  AND lyrname = nm 
                                                  AND epsgcode = epsg;
   END IF;
   RETURN rec;
END;
$$  LANGUAGE 'plpgsql' STABLE;


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
CREATE OR REPLACE FUNCTION lm_v3.lm_updateLayer(lyrid int,
                                          lyrsquid varchar,
                                          lyrverify varchar,
                                          lyrdloc varchar,
                                          lyrmeta varchar,
                                          vnodata double precision,
                                          vmin double precision,
                                          vmax double precision,
                                          lyrmtime double precision)
RETURNS lm_v3.Layer AS
$$
DECLARE
   newid int = -1;
   idstr varchar;
   murl varchar;
   rec lm_v3.Layer%rowtype;
BEGIN
   -- get layer 
   SELECT * INTO rec FROM lm_v3.Layer WHERE layerid = lyrid;
   
   IF NOT FOUND THEN
      RAISE EXCEPTION 'Layer % NOT found', lyrid; 
   ELSE
      UPDATE lm_v3.Layer SET (squid, verify, dlocation, metadata, nodataVal, 
                              minVal, maxVal, modTime)
         = (lyrsquid, lyrverify, lyrdloc, lyrmeta, vnodata, vmin, vmax, lyrmtime)
         WHERE layerid = lyrid;         
      IF NOT FOUND THEN
         RAISE EXCEPTION 'Layer % NOT updated', lyrid;
      ELSE 
         SELECT * INTO rec FROM lm_v3.Layer WHERE layerid = lyrid;
      END IF; -- end if layer updated
   END IF;  
      
   RETURN rec;
END;
$$  LANGUAGE 'plpgsql' VOLATILE;

-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lm_v3.lm_getLayer(lyrid int,
                                             lyrverify varchar,
                                             usr varchar,
                                             lyrname varchar,
                                             epsg int)
RETURNS lm_v3.Layer AS
$$
DECLARE
   rec lm_v3.Layer%rowtype;
BEGIN
   IF lyrid IS NOT NULL THEN
      SELECT * INTO rec FROM lm_v3.layer WHERE layerid = lyrid;
   ELSIF lyrverify IS NOT NULL THEN
      SELECT * INTO rec FROM lm_v3.layer WHERE verify = lyrverify;
   ELSE
      SELECT * INTO rec FROM lm_v3.layer WHERE userid = usr 
                                           AND name = lyrname 
                                           AND epsgcode = epsg;
   END IF;
   RETURN rec;
END;
$$  LANGUAGE 'plpgsql' STABLE;

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
