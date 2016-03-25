-- ----------------------------------------------------------------------------
-- Layers
-- Todo: REMOVE LM_SCHEMA = 'lm3' is in config.ini  
--       and LmServer.common.localconstants, put into lmconstants
-- psql -U admin -d speco --file=/share/apps/lm2/lm2hydra/config/createSPECOLayerFns.sql

-- ----------------------------------------------------------------------------
\c speco
-- ----------------------------------------------------------------------------

-- ----------------------------------------------------------------------------
-- ShapeGrids
-- ----------------------------------------------------------------------------
-- lm_countShapegrids
CREATE OR REPLACE FUNCTION lm3.lm_countShapegrids(usrid varchar(20), 
                                         beforetime double precision,
                                         aftertime double precision,
                                         epsg int,
                                         lyrid int,
                                         lyrname varchar,
                                         csides int)
   RETURNS int AS
$$
DECLARE
   num int;
   cmd varchar;
   wherecls varchar;
BEGIN
   cmd = 'select count(*) FROM lm3.lm_shapegrid ';
   wherecls = ' WHERE lyruserId =  ' || quote_literal(usrid) ;

   -- filter by modified before given time
   IF beforetime is not null THEN
      wherecls = wherecls || ' AND lyrdatelastmodified <=  ' || quote_literal(beforetime);
   END IF;

   -- filter by modified after given time
   IF aftertime is not null THEN
      wherecls = wherecls || ' AND lyrdatelastmodified >=  ' || quote_literal(aftertime);
   END IF;

   -- filter by epsg code
   IF epsg is not null THEN
      wherecls = wherecls || ' AND epsgcode =  ' || quote_literal(epsg);
   END IF;

   -- filter by layerId
   IF lyrid is not null THEN
      wherecls = wherecls || ' AND layerId =  ' || quote_literal(lyrid);
   END IF;

   -- filter by layerName
   IF lyrname is not null THEN
      wherecls = wherecls || ' AND layername like ' || quote_literal(lyrname);
   END IF;

   -- filter by cellsides
   IF csides is not null THEN
      wherecls = wherecls || ' AND cellsides =  ' || quote_literal(csides);
   END IF;

   cmd := cmd || wherecls;
   RAISE NOTICE 'cmd = %', cmd;

   EXECUTE cmd INTO num;
   RETURN num;
END;
$$  LANGUAGE 'plpgsql' STABLE;

-- ----------------------------------------------------------------------------
-- lm_listShapegrids
-- select * from lm3.lm_listShapegrids(0,100,'AfricaEPSCOR',NULL,NULL,NULL,NULL,NULL);
CREATE OR REPLACE FUNCTION lm3.lm_listShapegrids(firstRecNum int, maxNum int, 
                                         usrid varchar(20), 
                                         beforetime double precision,
                                         aftertime double precision,
                                         epsg int,
                                         lyrid int,
                                         lyrname varchar,
                                         csides int)
   RETURNS SETOF lm3.lm_atom AS
$$
DECLARE
   rec lm3.lm_atom;
   cmd varchar;
   wherecls varchar;
   limitcls varchar;
   ordercls varchar;
BEGIN
   cmd = 'SELECT layerId, layername, epsgcode, description, lyrdatelastmodified
               FROM lm3.lm_shapegrid ';
   wherecls = ' WHERE lyruserid =  ' || quote_literal(usrid) ;
   ordercls = ' ORDER BY lyrdatelastmodified DESC ';
   limitcls = ' LIMIT ' || quote_literal(maxNum) || ' OFFSET ' || quote_literal(firstRecNum);

   -- filter by modified before given time
   IF beforetime is not null THEN
      wherecls = wherecls || ' AND lyrdatelastmodified <=  ' || quote_literal(beforetime);
   END IF;

   -- filter by modified after given time
   IF aftertime is not null THEN
      wherecls = wherecls || ' AND lyrdatelastmodified >=  ' || quote_literal(aftertime);
   END IF;

   -- filter by epsg code
   IF epsg is not null THEN
      wherecls = wherecls || ' AND epsgcode =  ' || quote_literal(epsg);
   END IF;

   -- filter by layerId
   IF lyrid is not null THEN
      wherecls = wherecls || ' AND layerId =  ' || quote_literal(lyrid);
   END IF;

   -- filter by layerName
   IF lyrname is not null THEN
      wherecls = wherecls || ' AND layername like ' || quote_literal(lyrname);
   END IF;
   
   -- filter by cellsides
   IF csides is not null THEN
      wherecls = wherecls || ' AND cellsides =  ' || quote_literal(csides);
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
CREATE OR REPLACE FUNCTION lm3.lm_listShapegridObjects(firstRecNum int, maxNum int, 
                                         usrid varchar(20), 
                                         beforetime double precision,
                                         aftertime double precision,
                                         epsg int,
                                         lyrid int,
                                         lyrname varchar,
                                         csides int)
   RETURNS SETOF lm3.lm_shapegrid AS
$$
DECLARE
   rec lm3.lm_shapegrid;
   cmd varchar;
   wherecls varchar;
   limitcls varchar;
   ordercls varchar;
BEGIN
   cmd = 'SELECT * FROM lm3.lm_shapegrid ';
   wherecls = ' WHERE lyruserid =  ' || quote_literal(usrid) ;
   ordercls = ' ORDER BY lyrdatelastmodified DESC ';
   limitcls = ' LIMIT ' || quote_literal(maxNum) || ' OFFSET ' || quote_literal(firstRecNum);

   -- filter by modified before given time
   IF beforetime is not null THEN
      wherecls = wherecls || ' AND lyrdatelastmodified <=  ' || quote_literal(beforetime);
   END IF;

   -- filter by modified after given time
   IF aftertime is not null THEN
      wherecls = wherecls || ' AND lyrdatelastmodified >=  ' || quote_literal(aftertime);
   END IF;

   -- filter by epsg code
   IF epsg is not null THEN
      wherecls = wherecls || ' AND epsgcode =  ' || quote_literal(epsg);
   END IF;

   -- filter by layerId
   IF lyrid is not null THEN
      wherecls = wherecls || ' AND layerId =  ' || quote_literal(lyrid);
   END IF;

   -- filter by layerName
   IF lyrname is not null THEN
      wherecls = wherecls || ' AND layername like ' || quote_literal(lyrname);
   END IF;
   
   -- filter by cellsides
   IF csides is not null THEN
      wherecls = wherecls || ' AND cellsides =  ' || quote_literal(csides);
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
CREATE OR REPLACE FUNCTION lm3.lm_findShapeGrids(usr varchar,
                                             lyrname varchar,
                                             csides int,
                                             csize double precision,
                                             vsz int,
                                             epsg int,
                                             mpunits varchar(20),
                                             bboxstr varchar(60))
RETURNS SETOF lm3.lm_shapegrid AS
$$
DECLARE
   success int = -1;
   rec lm3.lm_shapegrid%ROWTYPE;
   cmd varchar;
   wherecls varchar;
BEGIN
   cmd = 'SELECT * FROM lm3.lm_shapegrid ';
   wherecls = 'WHERE lyruserid = ' || quote_literal(usr) || ' ';
   raise notice 'where = %', wherecls;
   
   IF lyrname is not null THEN
      wherecls = wherecls || ' AND layername = ' || quote_literal(lyrname);
   ELSE
      BEGIN
         wherecls = wherecls || ' AND cellsides = ' || csides || 
                                ' AND cellsize = ' || csize || 
                                ' AND mapunits = ' || quote_literal(mpunits) ||
                                ' AND vsize = ' || vsz || 
                                ' AND epsgcode = ' || epsg || 
                                ' AND bbox = ' || quote_literal(bboxstr);   
      END;
   END IF;
   raise notice 'where = %', wherecls;
   
   cmd = cmd || wherecls;

   FOR rec in EXECUTE cmd
      LOOP
         RETURN NEXT rec;
      END LOOP;
   RETURN;
END;
$$ LANGUAGE 'plpgsql' STABLE;

-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lm3.lm_getShapeGrid(usr varchar, sgid int)
RETURNS lm3.lm_shapegrid AS
$$
DECLARE
   rec lm3.lm_shapegrid%ROWTYPE;
BEGIN
   begin
      SELECT * INTO STRICT rec FROM lm3.lm_shapegrid 
         WHERE lyruserid = usr AND shapegridid = sgid;
   
      EXCEPTION
         WHEN NO_DATA_FOUND THEN
            RAISE NOTICE 'ShapeGrid id = % for user % not found', sgid, usr;
   end;
   RETURN rec;
END;
$$ LANGUAGE 'plpgsql' STABLE;

-- ----------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION lm3.lm_getShapeGridByLayerid(usr varchar, lyrid int)
RETURNS lm3.lm_shapegrid AS
$$
DECLARE
   rec lm3.lm_shapegrid%ROWTYPE;
BEGIN
   begin
      SELECT * INTO STRICT rec FROM lm3.lm_shapegrid 
         WHERE lyruserid = usr AND layerid = lyrid;
   
      EXCEPTION
         WHEN NO_DATA_FOUND THEN
            RAISE NOTICE 'ShapeGrid layerid = % for user % not found', lyrid, usr;
   end;
   RETURN rec;
END;
$$ LANGUAGE 'plpgsql' STABLE;

-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lm3.lm_getShapeGridByName(usr varchar, name varchar)
RETURNS lm3.lm_shapegrid AS
$$
DECLARE
   rec lm3.lm_shapegrid%ROWTYPE;
BEGIN
   begin
      SELECT * INTO STRICT rec FROM lm3.lm_shapegrid 
         WHERE lyruserid = usr AND layername = name;
   
      EXCEPTION
         WHEN NO_DATA_FOUND THEN
            RAISE NOTICE 'ShapeGrid (user, layername) = (% %) not found', 
                          usr, name;
   end;
   RETURN rec;
END;
$$ LANGUAGE 'plpgsql' STABLE;

-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lm3.lm_insertShapeGrid(usr varchar,
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
RETURNS lm3.lm_shapegrid AS
$$
DECLARE
   lyrid int;
   shpid int;
   rec lm3.lm_shapegrid%ROWTYPE;
BEGIN
   SELECT shapegridid INTO shpid
     FROM lm3.lm_shapegrid WHERE lyruserid = usr and layername = lyrname;
   IF NOT FOUND THEN
      begin
         -- get or insert layer 
         SELECT lm3.lm_insertLayer(usr, lyrname, lyrtitle, lyrdesc, dloc, vtype, 
                               null, datafmt, epsg, mpunits, null, null, null, 
                               metaloc, modtime, modtime, bboxstr, bboxwkt, murlprefix)  
                INTO lyrid;          
         IF lyrid = -1 THEN
            RAISE EXCEPTION 'Unable to insert layer';
         END IF;
         
         INSERT INTO lm3.ShapeGrid (layerId, cellsides, cellsize, vsize, 
                                idAttribute, xAttribute, yAttribute)
                       values (lyrid, csides, csize, vsz, idAttr, xAttr, yAttr);
   
         IF FOUND THEN
            SELECT INTO shpid last_value FROM lm3.shapegrid_shapegridid_seq;
            RAISE NOTICE 'Inserted shapegrid into %', shpid;
         ELSE
            RAISE EXCEPTION 'Unable to insert shapegrid';
         END IF;
      end;
   END IF;
   
   SELECT * INTO rec FROM lm3.lm_shapegrid WHERE shapegridid = shpid;    
   
   RETURN rec;
END;
$$  LANGUAGE 'plpgsql' VOLATILE;

-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lm3.lm_renameShapeGrid(shpid int, 
                                              usr varchar,
                                              name varchar,
                                              epsg int)
RETURNS int AS
$$
DECLARE
   lyrid int = -1;
   success int = -1;
   total int = -1;
BEGIN

   SELECT count(*) INTO total FROM lm3.lm_shapegrid 
          WHERE shapegridid = shpid AND lyruserid = usr;
   IF total = 1 THEN
      SELECT count(*) INTO total FROM lm3.lm_shapegrid 
             WHERE layername = name AND userid = usr AND epsgcode = epsg; 
      IF total = 0 THEN 
         begin
            SELECT INTO lyrid layerid FROM lm3.lm_shapegrid WHERE shapegridid = shpid; 
            SELECT lm3.lm_renameLayer(lyrid, usr, name, epsg) INTO success;
         end;
      ELSE
         RAISE NOTICE 'ShapeGrid % found for User/EPSG %', name, usr, epsg;
      END IF;
   ELSE
      RAISE NOTICE 'ShapeGrid id = % not found for User %', shpid, usr;
   END IF;
   RETURN success;
END;
$$  LANGUAGE 'plpgsql' VOLATILE;

-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lm3.lm_deleteShapeGrid(shpid int)
RETURNS int AS
$$
DECLARE
   usr varchar;
   lyrid int;
   total int;
   success int = -1;
BEGIN
   SELECT count(*) INTO total FROM lm3.Bucket WHERE shapegridid = shpid;
   IF total = 0 THEN
      begin
         SELECT lyruserid, layerid INTO usr, lyrid FROM lm3.lm_shapegrid 
           WHERE shapegridid = shpid;
         RAISE NOTICE '% %', usr, lyrid;
         DELETE FROM lm3.ShapeGrid WHERE shapegridid = shpid;
         SELECT INTO success lm_deleteOrphanedLayer(usr, lyrid);
      end;
   ELSE
      RAISE NOTICE 'ShapeGrid % still being used (0 rows deleted)', shpid;
   END IF;
   RETURN success;
END;
$$  LANGUAGE 'plpgsql' VOLATILE;


-- ----------------------------------------------------------------------------
-- lm_countLayers
CREATE OR REPLACE FUNCTION lm3.lm_countLayers(usrid varchar(20), 
                                         beforetime double precision,
                                         aftertime double precision,
                                         epsg int,
                                         lyrid int,
                                         lyrname varchar)
   RETURNS int AS
$$
DECLARE
   num int;
   cmd varchar;
   wherecls varchar;
BEGIN
   cmd = 'select count(*) FROM lm3.layer ';
   wherecls = ' WHERE userId =  ' || quote_literal(usrid) ;

   -- filter by modified before given time
   IF beforetime is not null THEN
      wherecls = wherecls || ' AND datelastmodified <=  ' || quote_literal(beforetime);
   END IF;

   -- filter by modified after given time
   IF aftertime is not null THEN
      wherecls = wherecls || ' AND datelastmodified >=  ' || quote_literal(aftertime);
   END IF;

   -- filter by epsg code
   IF epsg is not null THEN
      wherecls = wherecls || ' AND epsgcode =  ' || quote_literal(epsg);
   END IF;

   -- filter by layerId
   IF lyrid is not null THEN
      wherecls = wherecls || ' AND layerId =  ' || quote_literal(lyrid);
   END IF;

   -- filter by layerName
   IF lyrname is not null THEN
      wherecls = wherecls || ' AND layername like ' || quote_literal(lyrname);
   END IF;

   cmd := cmd || wherecls;
   RAISE NOTICE 'cmd = %', cmd;

   EXECUTE cmd INTO num;
   RETURN num;
END;
$$  LANGUAGE 'plpgsql' STABLE;

-- ----------------------------------------------------------------------------
-- lm_listLayers
CREATE OR REPLACE FUNCTION lm3.lm_listLayers(firstRecNum int, maxNum int, 
                                         usrid varchar(20), 
                                         beforetime double precision,
                                         aftertime double precision,
                                         epsg int,
                                         lyrid int,
                                         lyrname varchar)
   RETURNS SETOF lm3.lm_atom AS
$$
DECLARE
   rec lm3.lm_atom;
   cmd varchar;
   wherecls varchar;
   limitcls varchar;
   ordercls varchar;
BEGIN
   cmd = 'SELECT layerId, layername, epsgcode, description, datelastmodified
               FROM lm3.Layer ';
   wherecls = ' WHERE userid =  ' || quote_literal(usrid) ;
   ordercls = ' ORDER BY datelastmodified DESC ';
   limitcls = ' LIMIT ' || quote_literal(maxNum) || ' OFFSET ' || quote_literal(firstRecNum);

   -- filter by modified before given time
   IF beforetime is not null THEN
      wherecls = wherecls || ' AND datelastmodified <=  ' || quote_literal(beforetime);
   END IF;

   -- filter by modified after given time
   IF aftertime is not null THEN
      wherecls = wherecls || ' AND datelastmodified >=  ' || quote_literal(aftertime);
   END IF;

   -- filter by epsg code
   IF epsg is not null THEN
      wherecls = wherecls || ' AND epsgcode =  ' || quote_literal(epsg);
   END IF;

   -- filter by layerId
   IF lyrid is not null THEN
      wherecls = wherecls || ' AND layerId =  ' || quote_literal(lyrid);
   END IF;

   -- filter by layerName
   IF lyrname is not null THEN
      wherecls = wherecls || ' AND layername like ' || quote_literal(lyrname);
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
CREATE OR REPLACE FUNCTION lm3.lm_listLayerObjects(firstRecNum int, maxNum int, 
                                         usrid varchar(20), 
                                         beforetime double precision,
                                         aftertime double precision,
                                         epsg int,
                                         lyrid int,
                                         lyrname varchar)
   RETURNS SETOF lm3.Layer AS
$$
DECLARE
   rec lm3.Layer;
   cmd varchar;
   wherecls varchar;
   limitcls varchar;
   ordercls varchar;
BEGIN
   cmd = 'SELECT * FROM lm3.Layer ';
   wherecls = ' WHERE userid =  ' || quote_literal(usrid) ;
   ordercls = ' ORDER BY datelastmodified DESC ';
   limitcls = ' LIMIT ' || quote_literal(maxNum) || ' OFFSET ' || quote_literal(firstRecNum);

   -- filter by modified before given time
   IF beforetime is not null THEN
      wherecls = wherecls || ' AND datelastmodified <=  ' || quote_literal(beforetime);
   END IF;

   -- filter by modified after given time
   IF aftertime is not null THEN
      wherecls = wherecls || ' AND datelastmodified >=  ' || quote_literal(aftertime);
   END IF;

   -- filter by epsg code
   IF epsg is not null THEN
      wherecls = wherecls || ' AND epsgcode =  ' || quote_literal(epsg);
   END IF;

   -- filter by layerId
   IF lyrid is not null THEN
      wherecls = wherecls || ' AND layerId =  ' || quote_literal(lyrid);
   END IF;

   -- filter by layerName
   IF lyrname is not null THEN
      wherecls = wherecls || ' AND layername like ' || quote_literal(lyrname);
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
-- lm_getLayer
-- Returns one empty row if nothing found
-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lm3.lm_getLayer(lyrid int)
RETURNS lm3.layer AS
$$
DECLARE
   rec lm3.layer%ROWTYPE;
BEGIN
   BEGIN
      SELECT l.* INTO STRICT rec FROM lm3.layer l 
         WHERE layerid = lyrid;

      EXCEPTION
         WHEN NO_DATA_FOUND THEN
            RAISE NOTICE 'Layer id % not found', lyrid;
         WHEN TOO_MANY_ROWS THEN
            RAISE EXCEPTION 'Layer id % not unique', lyrid;
   END;
   return rec;
END;
$$ LANGUAGE 'plpgsql' STABLE;

-- ----------------------------------------------------------------------------
-- Find one layer with unique usr/name combination
CREATE OR REPLACE FUNCTION lm3.lm_findLayer(usr varchar, name varchar, epsg int)
RETURNS lm3.layer AS
$$
DECLARE
   rec lm3.layer%ROWTYPE;
BEGIN
   BEGIN
      SELECT l.* INTO STRICT rec FROM lm3.layer l 
         WHERE userid = usr AND layername = name AND epsgcode = epsg;

      EXCEPTION
         WHEN NO_DATA_FOUND THEN
            RAISE NOTICE 'Layer % not found for user %, epsg %', name, usr, epsg;
         WHEN TOO_MANY_ROWS THEN
            RAISE EXCEPTION 'Layer % not unique for user %, epsg %', name, usr, epsg;
   END;
   return rec;
END;
$$ LANGUAGE 'plpgsql' STABLE;


-- ----------------------------------------------------------------------------
-- Find layers matching parameters
CREATE OR REPLACE FUNCTION lm3.lm_findLayers(firstRecNum int, maxNum int, 
                                         name varchar,
                                         title varchar, 
                                         ogr int, 
                                         gdal int, 
                                         epsg int, 
                                         munits varchar,
                                         res double precision,
                                         startdt double precision,
                                         enddt double precision,
                                         bbcsv varchar,
                                         usr varchar, 
                                         beforetime double precision,
                                         aftertime double precision)
RETURNS SETOF lm3.layer AS
$$
DECLARE
   rec lm3.layer%ROWTYPE;
   cmd varchar;
   wherecls varchar;
   lyrWhere varchar;
   wherecount int;
   limitcls varchar;
   ordercls varchar;
   i int;
BEGIN
   cmd = 'SELECT * FROM lm3.layer ';
   wherecls = ' ' ;
   ordercls = ' ORDER BY layername ASC ';
   IF maxNum is not null AND firstRecNum is not null THEN
      limitcls = ' LIMIT ' || quote_literal(maxNum) || ' OFFSET ' || quote_literal(firstRecNum);
   ELSE
      limitcls = '';
   END IF;
   
   SELECT INTO lyrWhere lm3.lm_assembleLayerWhere(name, title, ogr, gdal, epsg, 
                                               munits, res, startdt, enddt, bbcsv,
                                               beforetime, aftertime) ;
   IF char_length(ancWhere) > 3 THEN
      wherecls = 'WHERE ' || lyrWhere;
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
-- Note: this is currently ignoring the bbox
-- TODO: change to a spatial query?
-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lm3.lm_assembleLayerWhere( 
                                            name varchar,
                                            ttl varchar, 
                                            ogr int, 
                                            gdal int, 
                                            epsg int, 
                                            munits varchar,
                                            res double precision,
                                            startdt double precision,
                                            enddt double precision,
                                            bbcsv varchar,
                                            beforetime double precision,
                                            aftertime double precision)
RETURNS varchar AS
$$
DECLARE
   currwhere varchar;
   arrWheres varchar[];
   wherecount int = 0;
   wherecls varchar = '';
BEGIN
   -- filter by name
   IF name is not null AND epsg is not null THEN
      currwhere = ' layername =  ' || quote_literal(name) ||
                  ' and epsgcode = ' || quote_literal(epsg);
      arrWheres = arrWheres || currwhere;

   ELSE
      begin
         -- filter by name
         IF name is not null THEN
            currwhere = ' layername =  ' || quote_literal(name);
            arrWheres = arrWheres || currwhere;
         END IF;
   
         -- filter by title
         IF ttl is not null THEN
            currwhere = ' title =  ' || quote_literal(ttl);
            arrWheres = arrWheres || currwhere;
         END IF;
   
         -- filter by ogrtype
         IF ogr is not null THEN
            BEGIN
               IF ogr = 0 THEN
                  currwhere = ' ogrtype IS NOT NULL ';
               ELSE
                  currwhere = ' ogrtype =  ' || quote_literal(ogr);   
               END IF;
               arrWheres = arrWheres || currwhere;
            END;
         END IF;
   
         -- filter by gdaltype
         IF gdal is not null THEN
            BEGIN
               IF gdal = 0 THEN
                  currwhere = ' gdaltype IS NOT NULL ';
               ELSE
                  currwhere = ' gdaltype =  ' || quote_literal(gdal);   
               END IF;
               arrWheres = arrWheres || currwhere;
            END;
         END IF;
   
         -- filter by epsg
         IF epsg is not null THEN
            currwhere = ' epsgcode =  ' || quote_literal(epsg);
            arrWheres = arrWheres || currwhere;
         END IF;
   
         -- filter by mapunits
         IF munits is not null THEN
            currwhere = ' mapunits =  ' || quote_literal(munits);
            arrWheres = arrWheres || currwhere;
         END IF;
   
         -- filter by resolution
         IF res is not null THEN
            currwhere = ' resolution =  ' || quote_literal(res);
            arrWheres = arrWheres || currwhere;
         END IF;
   
         -- filter by startDate
         IF startdt is not null THEN
            currwhere = ' startDate =  ' || quote_literal(startdt);
            arrWheres = arrWheres || currwhere;
         END IF;
   
         -- filter by endDate
         IF enddt is not null THEN
            currwhere = ' endDate =  ' || quote_literal(enddt);
            arrWheres = arrWheres || currwhere;
         END IF;
   
         -- filter by modified before given time
         IF beforetime is not null THEN
            currwhere = ' lyrdatelastmodified <=  ' || quote_literal(beforetime);
            arrWheres = arrWheres || currwhere;
         END IF;

         -- filter by modified after given time
         IF aftertime is not null THEN
            currwhere = ' lyrdatelastmodified >=  ' || quote_literal(aftertime);
            arrWheres = arrWheres || currwhere;
         END IF;
      end;
   END IF;
   
   SELECT INTO wherecount array_length(arrWheres, 1);
   IF wherecount > 0 THEN
      wherecls = arrWheres[1];
      FOR i in 2 .. wherecount LOOP
         wherecls = wherecls || ' AND ' || arrWheres[i];
      END LOOP;
   END IF;   

   RETURN wherecls;
END;
$$  LANGUAGE 'plpgsql' STABLE;


-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lm3.lm_insertLayer(usr varchar,
                                          lyrname varchar, 
                                          lyrtitle varchar,
                                          lyrdesc varchar,
                                          dloc varchar,
                                          vtype int,
                                          rtype int,
                                          datafmt varchar,
                                          epsg int,
                                          munits varchar,
                                          res double precision,
                                          startdt double precision,
                                          enddt double precision,
                                          metaloc varchar,
                                          createtime double precision,
                                          modtime double precision,
                                          bboxstr varchar,
                                          bboxwkt varchar,
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
      FROM lm3.Layer
      WHERE userId = usr
        AND layername = lyrname
        AND epsgcode = epsg;
                
   IF NOT FOUND THEN
      BEGIN
         RAISE NOTICE 'This layer not FOUND';
         IF epsg = 4326 THEN 
            INSERT INTO lm3.Layer (userId, layername, title, description, dlocation, 
                               ogrtype, gdaltype, dataformat, 
                               epsgcode, mapunits,
                                resolution, startDate, endDate, metalocation,
                                datecreated, datelastmodified, bbox, geom)
            VALUES (usr, lyrname, lyrtitle, lyrdesc, dloc, vtype, rtype,
                    datafmt, epsg, munits, res, startdt, enddt, metaloc, 
                    createtime, modtime, bboxstr, ST_GeomFromText(bboxwkt, epsg));
         ELSE  --non-epsg:4326
            INSERT INTO lm3.Layer (userId, layername, title, description, dlocation,
                               ogrtype, gdaltype, dataformat, 
                               epsgcode, mapunits,
                                resolution, startDate, endDate, metalocation,
                               datecreated, datelastmodified, bbox)
            VALUES (usr, lyrname, lyrtitle, lyrdesc, dloc, vtype, rtype,
                    datafmt, epsg, munits, res, startdt, enddt, metaloc, 
                    createtime, modtime, bboxstr);         
         END IF;   
                  
         IF FOUND THEN
            SELECT INTO lyrid last_value FROM lm3.layer_layerid_seq;
            RAISE NOTICE 'This layer inserted with id %', lyrid;
            idstr := cast(lyrid as varchar);
            murl := replace(murlprefix, '#id#', idstr);
            UPDATE lm3.Layer SET (metalocation, metadataurl, layerurl) = (murl, murl, murl) 
                WHERE layerid = lyrid;
         END IF;
      END; -- end if layer not found
      
   -- if layer is found 
   ELSE
      RAISE NOTICE 'User/Name/EPSG Layer % / % / % found with id %', 
                    usr, lyrname, epsg, lyrid;
   END IF;
      
   RETURN lyrid;
END;
$$  LANGUAGE 'plpgsql' VOLATILE;

-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lm3.lm_updateLayer(lyrid int,
                                          usr varchar,
                                          lyrtitle varchar,
                                          lyrdesc varchar,
                                          dloc varchar,
                                          vcttype int,
                                          rsttype int,
                                          dformat varchar,
                                          epsg int,
                                          munits varchar,
                                          res double precision,
                                          startdt double precision,
                                          enddt double precision,
                                          metaloc varchar,
                                          modtime double precision,
                                          bboxstr varchar,
                                          bboxwkt varchar)
RETURNS int AS
$$
DECLARE
   success int = -1;
BEGIN
   -- get or insert layer 
   SELECT count(*) FROM lm3.Layer 
      WHERE layerId = lyrid AND userId = usr;
   
   IF FOUND THEN
      BEGIN
         IF epsg = 4326 THEN 
            UPDATE lm3.Layer SET (title, description, dlocation, ogrtype, gdaltype, dataformat,
                                  epsgcode, mapunits, resolution, startDate, endDate, metalocation,
                                  datelastmodified, bbox, geom)
                               = (lyrtitle, lyrdesc, dloc, vcttype, rsttype, dformat,
                                  epsg, munits, res, startdt, enddt, metaloc, modtime, bboxstr, 
                                  ST_GeomFromText(bboxwkt, epsg))
                         WHERE layerId = lyrid AND userId = usr;
         ELSE
            UPDATE lm3.Layer SET (title, description, dlocation, ogrtype, gdaltype, dataformat,
                                  epsgcode, mapunits, resolution, startDate, endDate, metalocation,
                                  datelastmodified, bbox)
                               = (lyrtitle, lyrdesc, dloc, vcttype, rsttype, dformat,
                                  epsg, munits, res, startdt, enddt, metaloc, modtime, bboxstr)
                         WHERE layerId = lyrid AND userId = usr;
         END IF;
                  
         IF FOUND THEN
            success = 0;
         END IF;
      END; -- end if layer found
      
   -- if layer is NOT found 
   ELSE
      RAISE NOTICE 'Layer/User % / %  not found', lyrid, usr;
   END IF;
      
   RETURN success;
END;
$$  LANGUAGE 'plpgsql' VOLATILE;

-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lm3.lm_renameLayer(lyrid int,
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
   SELECT count(*) INTO total FROM lm3.layer 
          WHERE layerid = lyrid AND userid = usr;
   IF total = 1 THEN
      SELECT count(*) INTO total FROM lm3.layer 
             WHERE layername = lyrname AND userid = usr AND epsgcode = epsg; 
      IF total = 0 THEN 
         BEGIN
            UPDATE lm3.Layer SET layername = lyrname
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

-- ----------------------------------------------------------------------------
-- Deletes Layer (if orphaned)
CREATE OR REPLACE FUNCTION lm3.lm_deleteOrphanedLayer(usr varchar, lyrid int)
RETURNS int AS
$$
DECLARE
   total int = -1;
   patotal int = -1;
   anctotal int = -1;
   shptotal int = -1;
   lyrsdeleted int = 0;
   success int = -1;
BEGIN
   
   -- Does Layer info belong to Experiment user?
   SELECT count(*) INTO total FROM lm3.Layer WHERE layerid = lyrid AND userid = usr;
   IF total = 0 THEN
      RAISE NOTICE 'User % does not own Layer %', usr, lyrid;
   ELSE
      -- Delete Layer record if orphaned 
      SELECT count(*) INTO patotal FROM lm3.ExperimentPALayer WHERE layerId = lyrid;
      SELECT count(*) INTO anctotal FROM lm3.ExperimentAncLayer WHERE layerId = lyrid;
      SELECT count(*) INTO shptotal FROM lm3.lm_shapegrid WHERE layerId = lyrid;
      IF patotal = 0 AND anctotal = 0 AND shptotal = 0 THEN
         DELETE FROM lm3.Layer WHERE layerId = lyrid;
         GET DIAGNOSTICS lyrsdeleted = ROW_COUNT;
         IF lyrsdeleted > 0 THEN
            success := 0;
            RAISE NOTICE 'Layer % deleted (% rows deleted)', lyrid, lyrsdeleted;
         END IF;
      ELSE
         RAISE NOTICE 'Layer % is being used by an experiment (NOT deleted)', lyrid;
      END IF;
       
   END IF;

   RETURN success;
END;
$$  LANGUAGE 'plpgsql' VOLATILE;


-- ----------------------------------------------------------------------------
-- Ancillary Layers
-- ----------------------------------------------------------------------------
-- ----------------------------------------------------------------------------
-- lm_countAncillaryLayers
CREATE OR REPLACE FUNCTION lm3.lm_countAncLayers(usrid varchar(20), 
                                         beforetime double precision,
                                         aftertime double precision,
                                         epsg int,
                                         lyrid int,
                                         lyrname varchar,
                                         ancid int,
                                         eid int)
   RETURNS int AS
$$
DECLARE
   num int;
   cmd varchar;
   wherecls varchar;
BEGIN
   cmd = 'select count(*) FROM lm3.lm_anclayer ';
   wherecls = ' WHERE ancuserId =  ' || quote_literal(usrid) ;

   -- filter by modified before given time
   IF beforetime is not null THEN
      wherecls = wherecls || ' AND lyrdatelastmodified <=  ' || quote_literal(beforetime);
   END IF;

   -- filter by modified after given time
   IF aftertime is not null THEN
      wherecls = wherecls || ' AND lyrdatelastmodified >=  ' || quote_literal(aftertime);
   END IF;

   -- filter by epsgcode
   IF epsg is not null THEN
      wherecls = wherecls || ' AND  epsgcode =  ' || epsg;
   END IF;

   -- filter by layerId
   IF lyrid is not null THEN
      wherecls = wherecls || ' AND layerId =  ' || quote_literal(lyrid);
   END IF;

   -- filter by layerName
   IF lyrname is not null THEN
      wherecls = wherecls || ' AND layername =  ' || quote_literal(lyrname);
   END IF;

   -- filter by ancillaryValueId
   IF ancid is not null THEN
      wherecls = wherecls || ' AND ancillaryValueId =  ' || quote_literal(ancid);
   END IF;

   -- filter by ExperimentId
   IF eid is not null THEN
      wherecls = wherecls || ' AND experimentId =  ' || quote_literal(eid);
   END IF;

   cmd := cmd || wherecls;
   RAISE NOTICE 'cmd = %', cmd;

   EXECUTE cmd INTO num;
   RETURN num;
END;
$$  LANGUAGE 'plpgsql' STABLE;

-- ----------------------------------------------------------------------------
-- lm_listAncLayers
CREATE OR REPLACE FUNCTION lm3.lm_listAncLayers(firstRecNum int, maxNum int, 
                                         usrid varchar(20), 
                                         beforetime double precision,
                                         aftertime double precision,
                                         epsg int,
                                         lyrid int,
                                         lyrname varchar,
                                         ancid int,
                                         eid int)
   RETURNS SETOF lm3.lm_atom AS
$$
DECLARE
   rec lm3.lm_atom;
   avId int;
   cmd varchar;
   wherecls varchar;
   limitcls varchar;
   ordercls varchar;
BEGIN
   cmd = 'SELECT layerId, layername, epsgcode, description, lyrdatelastmodified,
                 ancillaryValueId FROM lm3.lm_anclayer ';
   wherecls = ' WHERE ancuserid =  ' || quote_literal(usrid) ;
   ordercls = ' ORDER BY lyrdatelastmodified DESC ';
   limitcls = ' LIMIT ' || quote_literal(maxNum) || ' OFFSET ' || quote_literal(firstRecNum);

   -- filter by modified before given time
   IF beforetime is not null THEN
      wherecls = wherecls || ' AND lyrdatelastmodified <=  ' || quote_literal(beforetime);
   END IF;

   -- filter by modified after given time
   IF aftertime is not null THEN
      wherecls = wherecls || ' AND lyrdatelastmodified >=  ' || quote_literal(aftertime);
   END IF;

   -- filter by epsgcode
   IF epsg is not null THEN
      wherecls = wherecls || ' AND  epsgcode =  ' || epsg;
   END IF;

   -- filter by layerId
   IF lyrid is not null THEN
      wherecls = wherecls || ' AND layerId =  ' || quote_literal(lyrid);
   END IF;

   -- filter by layerName
   IF lyrname is not null THEN
      wherecls = wherecls || ' AND layername =  ' || quote_literal(lyrname);
   END IF;

   -- filter by ancillaryValueId
   IF ancid is not null THEN
      wherecls = wherecls || ' AND ancillaryValueId =  ' || quote_literal(ancid);
   END IF;

   -- filter by ExperimentId
   IF eid is not null THEN
      wherecls = wherecls || ' AND experimentId =  ' || quote_literal(eid);
   END IF;

   cmd := cmd || wherecls || ordercls || limitcls;
   RAISE NOTICE 'cmd = %', cmd;

   FOR rec.id, rec.title, rec.epsgcode, rec.description, rec.modtime, avId in EXECUTE cmd
      LOOP
         rec.description = rec.description || ' with ancillaryValueId % values', avId;
         RETURN NEXT rec;
      END LOOP;
   RETURN;
END;
$$  LANGUAGE 'plpgsql' STABLE;

-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lm3.lm_listAncLayerObjects(firstRecNum int, maxNum int, 
                                         usrid varchar(20), 
                                         beforetime double precision,
                                         aftertime double precision,
                                         epsg int,
                                         lyrid int,
                                         lyrname varchar,
                                         ancid int,
                                         eid int)
   RETURNS SETOF lm3.lm_anclayer AS
$$
DECLARE
   rec lm3.lm_anclayer;
   cmd varchar;
   wherecls varchar;
   limitcls varchar;
   ordercls varchar;
BEGIN
   cmd = 'SELECT * FROM lm3.lm_anclayer ';
   wherecls = ' WHERE ancuserid =  ' || quote_literal(usrid) ;
   ordercls = ' ORDER BY lyrdatelastmodified DESC ';
   limitcls = ' LIMIT ' || quote_literal(maxNum) || ' OFFSET ' || quote_literal(firstRecNum);

   -- filter by modified before given time
   IF beforetime is not null THEN
      wherecls = wherecls || ' AND lyrdatelastmodified <=  ' || quote_literal(beforetime);
   END IF;

   -- filter by modified after given time
   IF aftertime is not null THEN
      wherecls = wherecls || ' AND lyrdatelastmodified >=  ' || quote_literal(aftertime);
   END IF;

   -- filter by epsgcode
   IF epsg is not null THEN
      wherecls = wherecls || ' AND  epsgcode =  ' || epsg;
   END IF;

   -- filter by layerId
   IF lyrid is not null THEN
      wherecls = wherecls || ' AND layerId =  ' || quote_literal(lyrid);
   END IF;

   -- filter by layerName
   IF lyrname is not null THEN
      wherecls = wherecls || ' AND layername =  ' || quote_literal(lyrname);
   END IF;

   -- filter by ancillaryValueId
   IF ancid is not null THEN
      wherecls = wherecls || ' AND ancillaryValueId =  ' || quote_literal(ancid);
   END IF;

   -- filter by ExperimentId
   IF eid is not null THEN
      wherecls = wherecls || ' AND experimentId =  ' || quote_literal(eid);
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
-- select * FROM lm3.lm_findAncLayers(NULL,NULL,E'nalandcov',E'NA Landcover 2005',NULL,1,2163,E'dd',0.00833333333333,36934.0,51544.0,NULL,E'astewart',E'pixel',TRUE,FALSE,1,NULL,NULL);
-- ----------------------------------------------------------------------------
-- Find layers already defined for an experiment
-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lm3.lm_assembleAncillaryWhere(usr varchar, 
                                                     nameval varchar,
                                                     wtmean boolean, 
                                                     lgclass boolean,
                                                     minpct int)
RETURNS varchar AS
$$
DECLARE
   currwhere varchar;
   arrWheres varchar[];
   wherecls varchar = '';
   wherecount int = 0;
BEGIN
   -- filter by User
   IF usr is not null THEN
      currwhere = ' ancuserid =  ' || quote_literal(usr);
      arrWheres = arrWheres || currwhere;
   END IF;

   -- filter by nameValue (field name of the attribute of interest)
   IF nameval is not null THEN
      currwhere = ' namevalue =  ' || quote_literal(nameval);
      arrWheres = arrWheres || currwhere;
   END IF;

   -- filter by weightedMean 
   IF wtmean is not null THEN
      currwhere = ' weightedmean =  ' || quote_literal(wtmean);
      arrWheres = arrWheres || currwhere;
   END IF;

   -- filter by largestClass
   IF lgclass is not null THEN
      currwhere = ' largestclass =  ' || quote_literal(lgclass);
      arrWheres = arrWheres || currwhere;
   END IF;

   -- filter by minPercent
   IF minpct is not null THEN
      currwhere = ' minpercent =  ' || quote_literal(minpct);
      arrWheres = arrWheres || currwhere;
   END IF;   
   
   SELECT INTO wherecount array_length(arrWheres, 1);
   IF wherecount > 0 THEN
      wherecls = arrWheres[1];
      FOR i in 2 .. wherecount LOOP
         wherecls = wherecls || ' AND ' || arrWheres[i];
      END LOOP;
   END IF;      
   
   RETURN wherecls;
END;
$$  LANGUAGE 'plpgsql' STABLE;

-- ----------------------------------------------------------------------------
-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lm3.lm_findAncLayers(firstRecNum int, maxNum int, 
                                            name varchar,
                                            title varchar, 
                                            ogr int, 
                                            gdal int, 
                                            epsg int, 
                                            munits varchar,
                                            res double precision,
                                            startdt double precision,
                                            enddt double precision,
                                            bbcsv varchar,
                                            usr varchar, 
                                            nameval varchar,
                                            wtmean boolean, 
                                            lgclass boolean,
                                            minpct int,
                                            beforetime double precision,
                                            aftertime double precision)
RETURNS SETOF lm3.lm_anclayer AS
$$
DECLARE
   rec lm3.lm_anclayer%ROWTYPE;
   cmd varchar;
   wherecls varchar;
   lyrWhere varchar;
   ancWhere varchar;
   limitcls varchar;
   ordercls varchar;
   i int;
BEGIN
   cmd = 'SELECT * FROM lm3.lm_anclayer ';
   wherecls = ' ' ;
   ordercls = ' ORDER BY layername ASC ';
   
   IF maxNum is not null AND firstRecNum is not null THEN
      limitcls = ' LIMIT ' || quote_literal(maxNum) || ' OFFSET ' || quote_literal(firstRecNum);
   ELSE
      limitcls = '';
   END IF;
   
   
   SELECT INTO lyrWhere lm3.lm_assembleLayerWhere(name, title, ogr, gdal, epsg, 
                                              munits, res, startdt, enddt, bbcsv,
                                              beforetime, aftertime) ;
   SELECT INTO ancWhere lm3.lm_assembleAncillaryWhere(usr, nameval, wtmean,
                                                  lgclass, minpct) ;
   IF char_length(lyrWhere) > 0 THEN
      begin
         wherecls = 'WHERE ' || lyrWhere;
         IF char_length(ancWhere) > 3 THEN
            wherecls = wherecls || ' AND ' || ancWhere;
         END IF;
      end;
   ELSE
      begin
         IF char_length(ancWhere) > 3 THEN
            wherecls = 'WHERE ' || ancWhere;
         END IF;
      end;
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
-- Find Ancillary Values already defined 
CREATE OR REPLACE FUNCTION lm3.lm_findAncValues(firstRecNum int, maxNum int, 
                                            usr varchar, 
                                            nameval varchar,
                                            wtmean boolean, 
                                            lgclass boolean,
                                            minpct int)
RETURNS SETOF lm3.AncillaryValue AS
$$
DECLARE
   rec lm3.AncillaryValue%ROWTYPE;
   cmd varchar;
   wherecls varchar = ' ';
   ancWhere varchar;
   limitcls varchar;
   ordercls varchar := ' ORDER BY ancillaryValueId ASC ';
   i int;
BEGIN
   cmd = 'SELECT * FROM lm3.AncillaryValue ';

   IF maxNum is not null AND firstRecNum is not null THEN
      limitcls = ' LIMIT ' || quote_literal(maxNum) || ' OFFSET ' || quote_literal(firstRecNum);
   ELSE
      limitcls = '';
   END IF;
   
   SELECT INTO ancWhere lm3.lm_assembleAncillaryWhere(usr, nameval, wtmean,
                                                  lgclass, minpct) ;
   IF char_length(ancWhere) > 3 THEN
      wherecls = ' WHERE ' || ancWhere;
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
-- lm_getAncLayer
-- Returns one empty row if nothing found
-- ----------------------------------------------------------------------------
-- select * FROM lm3.lm_getAncLayer(3,4,E'astewart');
-- ----------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION lm3.lm_getAncLayer(usr varchar, lyrid int, avid int)
RETURNS lm3.lm_anclayer AS
$$
DECLARE
   rec lm3.lm_anclayer%ROWTYPE;
BEGIN
   BEGIN
      SELECT * INTO STRICT rec FROM lm3.lm_anclayer WHERE layerid = lyrid
                                                  AND ancillaryvalueid = avid
                                                  AND ancuserid = usr;
         
      EXCEPTION
         WHEN NO_DATA_FOUND THEN
            RAISE EXCEPTION 'AncillaryValueLayer not found for layerid %, ancillaryValueId % and userid %', 
                             lyrid, avid, usr;
         WHEN TOO_MANY_ROWS THEN
            RAISE EXCEPTION 'AncillaryValueLayer not unique for layerid %, ancillaryValueid % and userid %', 
                             lyrid, avid, usr;
   END;
   return rec;
END;
$$ LANGUAGE 'plpgsql' STABLE;

-- ----------------------------------------------------------------------------
-- lm_getAncLayerById
-- Returns one empty row if nothing found
CREATE OR REPLACE FUNCTION lm3.lm_getAncLayerById(anclyrid int)
RETURNS lm3.lm_anclayer AS
$$
DECLARE
   rec lm3.lm_anclayer%ROWTYPE;
BEGIN
   BEGIN
      SELECT * INTO STRICT rec FROM lm3.lm_anclayer 
                      WHERE experimentAncLayerId = anclyrid LIMIT 1;
         
      EXCEPTION
         WHEN NO_DATA_FOUND THEN
            RAISE EXCEPTION 'AncillaryLayer not found for experimentAncLayerId %', 
                             anclyrid;
   END;
   return rec;
END;
$$ LANGUAGE 'plpgsql' STABLE;

-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lm3.lm_getAncValue(usrid varchar,
                                          nameval varchar,
                                          wtmean boolean,
                                          lgcls boolean,
                                          minpct int)
RETURNS int AS
$$
DECLARE
   avid int;
BEGIN
   -- find values if they exist 
   -- no fields will be null, so do not need to construct the where clause
   SELECT ancillaryvalueid INTO avid
      FROM lm3.AncillaryValue
      WHERE userid = usrid
        AND nameValue = nameval
        AND weightedMean = wtmean
        AND largestClass = lgcls
        AND minPercent = minpct;
        
   IF NOT FOUND THEN
      avid = -1;
   END IF; 
   
   RETURN avid;
END;
$$  LANGUAGE 'plpgsql' STABLE;

-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lm3.lm_insertAncValues(usrid varchar,
                                              nameval varchar,
                                              wtmean boolean,
                                              lgcls boolean,
                                              minpct int)
RETURNS int AS
$$
DECLARE
   avid int = -1;
BEGIN
   INSERT INTO lm3.AncillaryValue (userId, nameValue, weightedMean, 
                               largestClass, minPercent)
      VALUES (usrid, nameval, wtmean, lgcls, minpct);
   IF FOUND THEN
      SELECT INTO avid last_value FROM lm3.ancillaryvalue_ancillaryvalueid_seq;
   END IF;
   
   RETURN avid;
END;
$$  LANGUAGE 'plpgsql' VOLATILE;

-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lm3.lm_findOrInsertAncValues(usrid varchar,
                                              nameval varchar,
                                              wtmean boolean,
                                              lgcls boolean,
                                              minpct int)
RETURNS int AS
$$
DECLARE
   avid int = -1;
BEGIN
   -- find existing AncillaryValue record
   SELECT lm3.lm_getAncValue(usrid, nameval, wtmean, lgcls, minpct) INTO avid;
   RAISE NOTICE 'Got AncillaryValue %', avid;
        
   IF avid = -1 THEN
      SELECT * INTO avid FROM lm3.lm_insertAncValues(usrid, nameval, wtmean, lgcls, minpct);
      RAISE NOTICE 'Inserted AncillaryValue %', avid;
   END IF;
   
   RETURN avid;
END;
$$  LANGUAGE 'plpgsql' VOLATILE;

-- ----------------------------------------------------------------------------
-- 
-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lm3.lm_insertAncLayer(usr varchar,
                                             expid int,
                                             lyrname varchar, 
                                             lyrtitle varchar,
                                             lyrdesc varchar,
                                             dloc varchar, 
                                             vtype int, 
                                              rtype int,
                                              datafmt varchar,
                                             epsg int,
                                             munits varchar,
                                             res double precision,
                                             startdt double precision,
                                             enddt double precision,
                                             metaloc varchar,
                                             createtime double precision,
                                             modtime double precision,
                                             bboxstr varchar,
                                             bboxwkt varchar,
                                             nameval varchar,
                                             wtmean boolean,
                                             lgcls boolean,
                                             minpct int,
                                             murlprefix varchar)
RETURNS lm3.lm_anclayer AS
$$
DECLARE
   lyrid int = -1;
   avid int = -1;
   expcount int = -1;
   nextIdx int = -1;
   rec lm3.lm_anclayer%ROWTYPE;
   existingEntry lm3.ExperimentAncLayer%ROWTYPE;
BEGIN
   SELECT count(*) FROM lm3.Experiment INTO expcount 
      WHERE userid = usr AND experimentid = expid;      
   IF expcount != 1 THEN
      RAISE EXCEPTION 'Experiment % does not exist', expid;
   END IF;
   
   -- get or insert layer 
   SELECT lm3.lm_insertLayer(usr, lyrname, lyrtitle, lyrdesc, dloc, vtype, 
                         rtype, datafmt, epsg, munits, res, startdt, enddt, metaloc, 
                         createtime, modtime, bboxstr, bboxwkt, murlprefix)  INTO lyrid;          
   IF lyrid = -1 THEN
      RAISE EXCEPTION 'Unable to insert layer';
   END IF;
   
   SELECT lm3.lm_findOrInsertAncValues(usr, nameval, wtmean, lgcls, minpct) INTO avid;
   IF avid = -1 THEN
      RAISE EXCEPTION 'Unable to insert ancillary values';
   END IF;      
  
   -- Find existing entry for this layer in this experiment
   SELECT * FROM lm3.ExperimentAncLayer INTO existingEntry 
      WHERE experimentId = expid AND layerId = lyrid AND ancillaryValueId = avid;
   IF NOT FOUND THEN 
      begin
         -- Get Matrix Index for this layer
         SELECT lm3.lm_computeNextIndex(expid, False) INTO nextIdx;
         INSERT INTO lm3.ExperimentAncLayer (experimentId, layerId, ancillaryValueId, matrixIdx) 
                           VALUES (expid, lyrid, avid, nextIdx);
         IF NOT FOUND THEN
            RAISE EXCEPTION 'Unable to join Experiment %, Layer %, and AncValues %',
                 expid, lyrid, avid;
         END IF;
      end;
   END IF;
   
   SELECT * FROM lm3.lm_anclayer INTO rec 
      WHERE experimentId = expid AND layerId = lyrid AND ancillaryValueId = avid;
      
   RETURN rec;
END;
$$  LANGUAGE 'plpgsql' VOLATILE;


-- ----------------------------------------------------------------------------
-- Return all Ancillary layers used for an Experiment.  UserId is not 
-- checked. 
CREATE OR REPLACE FUNCTION lm3.lm_getAncLayersForExperiment(expid int)
RETURNS SETOF lm3.lm_anclayer AS
$$
DECLARE
   rec lm3.lm_anclayer%ROWTYPE;
BEGIN
   FOR rec IN 
      SELECT l.* FROM lm3.lm_anclayer al, ExperimentAncLayer eal 
         WHERE eal.experimentid = expid
           AND eal.ancillaryvalueid = al.ancillaryvalueid
   LOOP
      RETURN NEXT rec;
   END LOOP;
   RETURN;
END;
$$ LANGUAGE 'plpgsql' STABLE;

-- ----------------------------------------------------------------------------
-- Return all Ancillary Layers for a User
CREATE OR REPLACE FUNCTION lm3.lm_getAncLayersForUser(usr varchar)
RETURNS SETOF lm3.lm_anclayer AS
$$
DECLARE
   rec lm3.lm_anclayer%ROWTYPE;
BEGIN
   FOR rec IN SELECT * FROM lm3.lm_anclayer WHERE ancuserId = usr
      LOOP
         RETURN NEXT rec;
      END LOOP;
   
   RETURN;     
END;
$$  LANGUAGE 'plpgsql' STABLE;

-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lm3.lm_getAncLayersForUserAndLayerid(usr varchar, lyrid int)
RETURNS SETOF lm3.lm_anclayer AS
$$
DECLARE
   rec lm3.lm_anclayer%ROWTYPE;
BEGIN
   FOR rec IN  
      SELECT * FROM lm3.lm_anclayer 
         WHERE layerId = lyrid AND ancuserId = usr
   LOOP
      RETURN NEXT rec;
   END LOOP;
   RETURN;
END;
$$  LANGUAGE 'plpgsql' STABLE;

-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lm3.lm_getAncLayersForUserAndAncid(usr varchar, ancid int)
RETURNS SETOF lm3.lm_anclayer AS
$$
DECLARE
   rec lm3.lm_anclayer%ROWTYPE;
BEGIN
   FOR rec IN  
      SELECT * FROM lm3.lm_anclayer 
         WHERE ancuserId = usr AND ancillaryValueId = ancid
   LOOP
      RETURN NEXT rec;
   END LOOP;
   RETURN;
END;
$$  LANGUAGE 'plpgsql' STABLE;

-- ----------------------------------------------------------------------------
-- Return all Ancillary layers with lyrname that User usr has used in any
-- Experiments.  Only AncillaryValue UserId is checked, so Layers may be owned by 
-- this User or another (only Archive User's layers are available to everyone).
CREATE OR REPLACE FUNCTION lm3.lm_getAncLayersForUserAndLayername(usr varchar,
                                                              lyrname varchar)
RETURNS SETOF lm3.lm_anclayer AS
$$
DECLARE
   rec lm3.lm_anclayer%ROWTYPE;
BEGIN
   FOR rec IN
      SELECT al.* FROM lm3.lm_anclayer al
         WHERE al.ancuserid = usr
           AND al.layername = lyrname
   LOOP
      RETURN NEXT rec;
   END LOOP;
   RETURN;
END;
$$  LANGUAGE 'plpgsql' STABLE;

-- ----------------------------------------------------------------------------
-- Counts number of layers Ancillary values are connected to
CREATE OR REPLACE FUNCTION lm3.lm_countLayersForAncillaryVals(usrid varchar,
                                                          avid int)
RETURNS int AS
$$
DECLARE
   total int = -1;
BEGIN
   -- Find existing or add new AncillaryValue record
   SELECT INTO total count(*) FROM lm3.ExperimentAncLayer  
      WHERE ancillaryValueId = avid AND userId = usrid;
   
   RETURN total;
END;
$$  LANGUAGE 'plpgsql' STABLE;

-- ----------------------------------------------------------------------------
-- Inserts new Ancillary values and updates Join to Experiment and Layer
-- ----------------------------------------------------------------------------
-- select * FROM lm3.lm_updateAncLayerForExperiment(2,3,'astewart',4,'pixel',TRUE,TRUE,50);
-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lm3.lm_updateAncLayerForExperiment(expid int, 
                                                          lyrid int,
                                                          usrid varchar,
                                                          old_avid int,
                                                          nameval varchar,
                                                          wtmean boolean,
                                                          lgcls boolean,
                                                          minpct int)
RETURNS int AS
$$
DECLARE
   new_avid int = -1;
   total int = -1;
   success int = -1;
BEGIN
   -- Ensure AncillaryLayer to UPDATE lm3.actually exists
   SELECT count(*) INTO total FROM lm3.ExperimentAncLayer
     WHERE ancillaryValueId = old_avid AND experimentId = expid AND layerid = lyrid;
   IF total = 0 THEN  
      RAISE EXCEPTION 'Ancillary Layer (ids % %) does not exist for experiment %',
         lyrid, old_avid, expid;
   END IF;  

   -- find existing AncillaryValue record
   SELECT lm3.lm_getAncValue(usrid, nameval, wtmean, lgcls, minpct) INTO new_avid;

   -- If new AncillaryValues don't exist
   IF new_avid = -1 THEN
      begin
         -- and other layers use this AncillaryValue record, add a new one  
         IF total > 1 THEN
            begin
               SELECT lm3.lm_insertAncValues(usrid, nameval, wtmean, lgcls, minpct) INTO new_avid;
               IF new_avid = -1 THEN
                  RAISE EXCEPTION 'Unable to insert new AncillaryValues';
               END IF;
            end;
   
         -- or no other layers use this AncillaryValue record, UPDATE lm3.this one  
         ELSE
            begin
               UPDATE lm3.AncillaryValue 
                  SET (nameValue, weightedMean, largestClass, minPercent)
                    = (nameval, wtmean, lgcls, minpct)
                  WHERE ancillaryValueId = old_avid AND userId = usrid;
               IF FOUND THEN
                  new_avid := old_avid;
               ELSE
                  RAISE EXCEPTION 'Unable to UPDATE lm3.old AncillaryValues to new';
               END IF;
            end;
         END IF;
      end;
   END IF;
 
   SELECT count(*) into total FROM lm3.ExperimentAncLayer 
      WHERE ancillaryValueId = new_avid AND experimentId = expid AND layerid = lyrid;
   IF total > 0 THEN
      RAISE NOTICE 'Ancillary Layer (ids % %) with new values already exists for experiment %',
         lyrid, new_avid, expid;
   ELSE
      begin
         UPDATE lm3.ExperimentAncLayer SET ancillaryValueId = new_avid WHERE 
             experimentId = expid AND ancillaryValueId = old_avid AND layerid = lyrid;
         IF NOT FOUND THEN 
            RAISE EXCEPTION 'Unable to join new AncillaryValues to layer and experiment';
         END IF;
      end;
   END IF;
   
   RETURN new_avid;
END;
$$  LANGUAGE 'plpgsql' VOLATILE;

-- ----------------------------------------------------------------------------
-- Deletes PresenceAbsenceLayer join; Layer (if orphaned) and 
-- PresenceAbsence (if orphaned)
CREATE OR REPLACE FUNCTION lm3.lm_deleteAncLayerFromExperiment(expusr varchar,
                                                           expid int,
                                                           lyrid int,
                                                           ancid int)
RETURNS int AS
$$
DECLARE
   expusr varchar;
   ancusr varchar;
   total int = -1;
   lyrsdeleted int = 0;
   ancsdeleted int = 0;
   success int = -1;
BEGIN
   SELECT INTO ancusr ancuserid FROM lm3.lm_anclayer 
     WHERE experimentId = expid AND layerId = lyrid AND ancillaryValueId = ancid;

   -- Delete join of AncillaryValue, Layer, Experiment records
   DELETE FROM lm3.ExperimentAncLayer
      WHERE experimentId = expid AND layerId = lyrid AND ancillaryValueId = ancid;

   IF FOUND THEN
      success = 0;
      -- If Layer belongs to Experiment user, and it is not being used, delete
      SELECT * INTO lyrsdeleted FROM lm3.lm_deleteOrphanedLayer(expusr, lyrid);
   ELSE
      RAISE EXCEPTION 'Unable to delete Ancillary % Layer % FROM lm3.Experiment %',
                      ancid, lyrid, expid;
   END IF;
   
   -- Delete Ancillary record if orphaned 
   SELECT count(*) INTO total FROM lm3.ExperimentAncLayer 
      WHERE ancillaryValueId = ancid;
   IF total = 0 THEN
      DELETE FROM lm3.AncillaryValue WHERE ancillaryValueId = ancid;
      GET DIAGNOSTICS ancsdeleted = ROW_COUNT;
      IF ancsdeleted > 0 THEN
         RAISE NOTICE 'Ancillary % deleted (% rows deleted)', ancid, ancsdeleted;
      END IF;
   END IF;

   RETURN success;
END;
$$  LANGUAGE 'plpgsql' VOLATILE;

-- SELECT count(*) INTO total FROM lm3.ExperimentAncLayer WHERE ancillaryValueId = 7;
-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lm3.lm_addAncLayerToExperiment(lyrid int,
                                                      avid  int, 
                                                      expid int,
                                                      usrid varchar, defusr varchar)
RETURNS int AS
$$
DECLARE
   tmpcount int;
   nextidx int = -1;
BEGIN
   -- check Experiment for User exists
   SELECT count(*) into tmpcount FROM lm3.experiment 
      WHERE experimentid = expid AND userid = usrid;
   IF tmpcount != 1 THEN
      RAISE EXCEPTION 'Experiment with id % does not exist', expid;
   END IF;
   
   -- check Layer for User or Default User exists
   SELECT count(*) into tmpcount FROM lm3.Layer 
      WHERE layerId = lyrid AND (userId = usrid OR userId = defusr);
   IF tmpcount != 1 THEN
      RAISE EXCEPTION 'Layer with id % does not exist', lyrid;
   END IF;
   
   SELECT count(*) INTO tmpcount FROM lm3.ExperimentAncLayer
      WHERE experimentid = expId AND ancillaryValueId = avid AND layerId = lyrid;
   IF tmpcount = 0 THEN      
      BEGIN
         -- get matrixIndex
         SELECT lm3.lm_computeNextIndex(expid, True) INTO nextidx;

         -- get or insert scenario x layer entry
         INSERT INTO lm3.ExperimentAncLayer (experimentId, layerId, 
                                         ancillaryValueId, matrixidx) 
                     VALUES (expid, lyrid, avid, nextidx);
         IF NOT FOUND THEN
            nextidx := -1;
            RAISE NOTICE 'Unable to add AncillaryValue % / Layer % to Experiment %', 
                    avid, lyrid, expid;
         END IF;
      END;
   ELSE
      RAISE NOTICE 'Experiment % already contains AncillaryValue % / Layer %', 
                    expid, avid, lyrid;
   END IF;
   
   RETURN nextidx;
END;
$$  LANGUAGE 'plpgsql' VOLATILE;

-- ----------------------------------------------------------------------------
-- Presence/Absence Layers
-- ----------------------------------------------------------------------------
-- ----------------------------------------------------------------------------
-- lm_countPALayers
CREATE OR REPLACE FUNCTION lm3.lm_countPALayers(usrid varchar(20), 
                                         beforetime double precision,
                                         aftertime double precision,
                                         epsg int,
                                         lyrid int,
                                         lyrname varchar,
                                         paid int,
                                         eid int)
   RETURNS int AS
$$
DECLARE
   num int;
   cmd varchar;
   wherecls varchar;
BEGIN
   cmd = 'select count(*) FROM lm3.lm_palayer ';
   wherecls = ' WHERE pauserId =  ' || quote_literal(usrid) ;

   -- filter by modified before given time
   IF beforetime is not null THEN
      wherecls = wherecls || ' AND lyrdatelastmodified <=  ' || quote_literal(beforetime);
   END IF;

   -- filter by modified after given time
   IF aftertime is not null THEN
      wherecls = wherecls || ' AND lyrdatelastmodified >=  ' || quote_literal(aftertime);
   END IF;

   -- filter by epsgcode
   IF epsg is not null THEN
      wherecls = wherecls || ' AND  epsgcode =  ' || epsg;
   END IF;
   
   -- filter by layerId
   IF lyrid is not null THEN
      wherecls = wherecls || ' AND layerId =  ' || quote_literal(lyrid);
   END IF;

   -- filter by layerName
   IF lyrname is not null THEN
      wherecls = wherecls || ' AND layername =  ' || quote_literal(lyrname);
   END IF;

   -- filter by presenceAbsenceId
   IF paid is not null THEN
      wherecls = wherecls || ' AND presenceAbsenceId =  ' || quote_literal(paid);
   END IF;

   -- filter by ExperimentId
   IF eid is not null THEN
      wherecls = wherecls || ' AND experimentId =  ' || quote_literal(eid);
   END IF;

   cmd := cmd || wherecls;
   RAISE NOTICE 'cmd = %', cmd;

   EXECUTE cmd INTO num;
   RETURN num;
END;
$$  LANGUAGE 'plpgsql' STABLE;

-- ----------------------------------------------------------------------------
-- lm_listPALayers
CREATE OR REPLACE FUNCTION lm3.lm_listPALayers(firstRecNum int, maxNum int, 
                                         usrid varchar(20), 
                                         beforetime double precision,
                                         aftertime double precision,
                                         epsg int,
                                         lyrid int,
                                         lyrname varchar,
                                         paid int,
                                         eid int)
   RETURNS SETOF lm3.lm_atom AS
$$
DECLARE
   rec lm3.lm_atom;
   cmd varchar;
   wherecls varchar;
   limitcls varchar;
   ordercls varchar;
BEGIN
   cmd = 'SELECT layerId, layername, epsgcode, description, lyrdatelastmodified,
                 presenceAbsenceId
               FROM lm3.lm_palayer ';
   wherecls = ' WHERE pauserid =  ' || quote_literal(usrid) ;
   ordercls = ' ORDER BY lyrdatelastmodified DESC ';
   limitcls = ' LIMIT ' || quote_literal(maxNum) || ' OFFSET ' || quote_literal(firstRecNum);

   -- filter by modified before given time
   IF beforetime is not null THEN
      wherecls = wherecls || ' AND lyrdatelastmodified <=  ' || quote_literal(beforetime);
   END IF;

   -- filter by modified after given time
   IF aftertime is not null THEN
      wherecls = wherecls || ' AND lyrdatelastmodified >=  ' || quote_literal(aftertime);
   END IF;

   -- filter by epsgcode
   IF epsg is not null THEN
      wherecls = wherecls || ' AND  epsgcode =  ' || epsg;
   END IF;
   
   -- filter by layerId
   IF lyrid is not null THEN
      wherecls = wherecls || ' AND layerId =  ' || quote_literal(lyrid);
   END IF;

   -- filter by layerName
   IF lyrname is not null THEN
      wherecls = wherecls || ' AND layername =  ' || quote_literal(lyrname);
   END IF;

   -- filter by presenceAbsenceId
   IF paid is not null THEN
      wherecls = wherecls || ' AND presenceAbsenceId =  ' || quote_literal(paid);
   END IF;

   -- filter by ExperimentId
   IF eid is not null THEN
      wherecls = wherecls || ' AND experimentId =  ' || quote_literal(eid);
   END IF;

   cmd := cmd || wherecls || ordercls || limitcls;
   RAISE NOTICE 'cmd = %', cmd;

   FOR rec.id, rec.title, rec.epsgcode, rec.description, rec.modtime, paId in EXECUTE cmd
      LOOP
         rec.description = rec.description || 'with presenceAbsenceId % values', paId;
         RETURN NEXT rec;
      END LOOP;
   RETURN;
END;
$$  LANGUAGE 'plpgsql' STABLE;

-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lm3.lm_listPALayerObjects(firstRecNum int, maxNum int, 
                                         usrid varchar(20), 
                                         beforetime double precision,
                                         aftertime double precision,
                                         epsg int,
                                         lyrid int,
                                         lyrname varchar,
                                         paid int,
                                         eid int)
   RETURNS SETOF lm3.lm_palayer AS
$$
DECLARE
   rec lm3.lm_palayer;
   cmd varchar;
   wherecls varchar;
   limitcls varchar;
   ordercls varchar;
BEGIN
   cmd = 'SELECT * FROM lm3.lm_palayer ';
   wherecls = ' WHERE pauserid =  ' || quote_literal(usrid) ;
   ordercls = ' ORDER BY lyrdatelastmodified DESC ';
   limitcls = ' LIMIT ' || quote_literal(maxNum) || ' OFFSET ' || quote_literal(firstRecNum);

   -- filter by modified before given time
   IF beforetime is not null THEN
      wherecls = wherecls || ' AND lyrdatelastmodified <=  ' || quote_literal(beforetime);
   END IF;

   -- filter by modified after given time
   IF aftertime is not null THEN
      wherecls = wherecls || ' AND lyrdatelastmodified >=  ' || quote_literal(aftertime);
   END IF;

   -- filter by epsgcode
   IF epsg is not null THEN
      wherecls = wherecls || ' AND  epsgcode =  ' || epsg;
   END IF;
   
   -- filter by layerId
   IF lyrid is not null THEN
      wherecls = wherecls || ' AND layerId =  ' || quote_literal(lyrid);
   END IF;

   -- filter by layerName
   IF lyrname is not null THEN
      wherecls = wherecls || ' AND layername =  ' || quote_literal(lyrname);
   END IF;

   -- filter by presenceAbsenceId
   IF paid is not null THEN
      wherecls = wherecls || ' AND presenceAbsenceId =  ' || quote_literal(paid);
   END IF;

   -- filter by ExperimentId
   IF eid is not null THEN
      wherecls = wherecls || ' AND experimentId =  ' || quote_literal(eid);
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
CREATE OR REPLACE FUNCTION lm3.lm_assemblePAWhere(usr varchar, 
                                              namePres varchar,
                                              minPres double precision,
                                              maxPres double precision,
                                              pctPres int,
                                              nameAbs varchar,
                                              minAbs double precision,
                                              maxAbs double precision,
                                              pctAbs int)
RETURNS varchar AS
$$
DECLARE
   currwhere varchar;
   arrWheres varchar[];
   wherecls varchar = '';
   wherecount int = 0;
BEGIN
   IF usr is not null THEN
      currwhere = ' pauserid =  ' || quote_literal(usr);
      arrWheres = arrWheres || currwhere;
   END IF;

   IF namePres is not null THEN
      currwhere = ' namePresence =  ' || quote_literal(namePres);
      arrWheres = arrWheres || currwhere;
   END IF;

   IF minPres is not null THEN
      currwhere = ' minPresence =  ' || quote_literal(minPres);
      arrWheres = arrWheres || currwhere;
   END IF;

   IF maxPres is not null THEN
      currwhere = ' maxPresence =  ' || quote_literal(maxPres);
      arrWheres = arrWheres || currwhere;
   END IF;

   IF pctPres is not null THEN
      currwhere = ' percentPresence =  ' || quote_literal(pctPres);
      arrWheres = arrWheres || currwhere;
   END IF;   
   
   IF nameAbs is not null THEN
      currwhere = ' nameAbsence =  ' || quote_literal(nameAbs);
      arrWheres = arrWheres || currwhere;
   END IF;

   IF minAbs is not null THEN
      currwhere = ' minAbsence =  ' || quote_literal(minAbs);
      arrWheres = arrWheres || currwhere;
   END IF;

   IF maxAbs is not null THEN
      currwhere = ' maxAbsence =  ' || quote_literal(minAbs);
      arrWheres = arrWheres || currwhere;
   END IF;

   IF pctAbs is not null THEN
      currwhere = ' percentAbsence =  ' || quote_literal(pctAbs);
      arrWheres = arrWheres || currwhere;
   END IF;   

   SELECT array_length(arrWheres, 1) INTO wherecount;
   IF wherecount > 0 THEN
      wherecls = arrWheres[1];
      FOR i in 2 .. wherecount LOOP
         wherecls = wherecls || ' AND ' || arrWheres[i];
      END LOOP;
   END IF;      
   
   RETURN wherecls;
END;
$$  LANGUAGE 'plpgsql' STABLE;

-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lm3.lm_findPALayers(firstRecNum int, maxNum int, 
                                           name varchar,
                                           title varchar, 
                                           ogr int, 
                                           gdal int, 
                                           epsg int, 
                                           munits varchar,
                                           res double precision,
                                           startdt double precision,
                                           enddt double precision,
                                           bbcsv varchar,
                                           usr varchar, 
                                           namePres varchar,
                                           minPres double precision,
                                           maxPres double precision,
                                           pctPres int,
                                           nameAbs varchar,
                                           minAbs double precision,
                                           maxAbs double precision,
                                           pctAbs int,
                                           beforetime double precision,
                                           aftertime double precision)
RETURNS SETOF lm3.lm_palayer AS
$$
DECLARE
   rec lm3.lm_palayer%ROWTYPE;
   cmd varchar;
   wherecls varchar;
   lyrWhere varchar;
   paWhere varchar;
   limitcls varchar;
   ordercls varchar;
   i int;
BEGIN
   cmd = 'SELECT * FROM lm3.lm_palayer ';
   wherecls = ' ' ;
   ordercls = ' ORDER BY layername ASC ';
   IF maxNum is not null AND firstRecNum is not null THEN
      limitcls = ' LIMIT ' || quote_literal(maxNum) || ' OFFSET ' || quote_literal(firstRecNum);
   ELSE
      limitcls = '';
   END IF;
      
   SELECT INTO lyrWhere lm3.lm_assembleLayerWhere(name, title, ogr, gdal, epsg, 
                      munits, res, startdt, enddt, bbcsv,beforetime, aftertime);
   SELECT INTO paWhere  lm3.lm_assemblePAWhere(usr, namePres, minPres, maxPres, 
                                      pctPres, nameAbs, minAbs, maxAbs, pctAbs);
   IF char_length(lyrWhere) > 0 THEN
      begin
         wherecls = 'WHERE ' || lyrWhere;
         IF char_length(paWhere) > 3 THEN
            wherecls = wherecls || ' AND ' || paWhere;
         END IF;
      end;
   ELSE
      begin
         IF char_length(paWhere) > 3 THEN
            wherecls = 'WHERE ' || paWhere;
         END IF;
      end;
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
CREATE OR REPLACE FUNCTION lm3.lm_findPAValues(firstRecNum int, maxNum int, 
                                           usr varchar, 
                                           namePres varchar,
                                           minPres double precision,
                                           maxPres double precision,
                                           pctPres int,
                                           nameAbs varchar,
                                           minAbs double precision,
                                           maxAbs double precision,
                                           pctAbs int)
RETURNS SETOF lm3.PresenceAbsence AS
$$
DECLARE
   rec lm3.PresenceAbsence%ROWTYPE;
   cmd varchar;
   wherecls varchar := ' ';
   lyrWhere varchar;
   paWhere varchar;
   limitcls varchar;
   ordercls varchar := ' ORDER BY presenceAbsenceId ASC ';
   i int;
BEGIN
   cmd = 'SELECT * FROM lm3.PresenceAbsence ';
   IF maxNum is not null AND firstRecNum is not null THEN
      limitcls = ' LIMIT ' || quote_literal(maxNum) || ' OFFSET ' || quote_literal(firstRecNum);
   ELSE
      limitcls = '';
   END IF;
      
   SELECT INTO paWhere  lm3.lm_assemblePAWhere(usr, namePres, minPres, maxPres, 
                                      pctPres, nameAbs, minAbs, maxAbs, pctAbs);
   IF char_length(paWhere) > 3 THEN
      wherecls = ' WHERE ' || paWhere;
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
CREATE OR REPLACE FUNCTION lm3.lm_findSimilarPALayers(firstRecNum int, maxNum int, 
                                           name varchar,
                                           epsg int, 
                                           usr varchar, 
                                           namePres varchar,
                                           minPres double precision,
                                           maxPres double precision,
                                           pctPres int,
                                           nameAbs varchar,
                                           minAbs double precision,
                                           maxAbs double precision,
                                           pctAbs int,
                                           beforetime double precision,
                                           aftertime double precision)
RETURNS SETOF lm3.lm_palayer AS
$$
DECLARE
   rec lm3.lm_palayer%ROWTYPE;
   cmd varchar;
   wherecls varchar;
   lyrWhere varchar;
   paWhere varchar;
   limitcls varchar;
   ordercls varchar;
   i int;
BEGIN
   cmd = 'SELECT * FROM lm3.lm_palayer ';
   wherecls = ' ' ;
   ordercls = ' ORDER BY layername ASC ';
   IF maxNum is not null AND firstRecNum is not null THEN
      limitcls = ' LIMIT ' || quote_literal(maxNum) || ' OFFSET ' || quote_literal(firstRecNum);
   ELSE
      limitcls = '';
   END IF;
      
   SELECT INTO lyrWhere lm3.lm_assembleLayerWhere(name, title, ogr, gdal, epsg, 
                      munits, res, startdt, enddt, bbcsv,beforetime, aftertime);
   SELECT INTO paWhere  lm3.lm_assemblePAWhere(usr, namePres, minPres, maxPres, 
                                      pctPres, nameAbs, minAbs, maxAbs, pctAbs);
   IF char_length(lyrWhere) > 0 THEN
      begin
         wherecls = 'WHERE ' || lyrWhere;
         IF char_length(paWhere) > 3 THEN
            wherecls = wherecls || ' AND ' || paWhere;
         END IF;
      end;
   ELSE
      begin
         IF char_length(paWhere) > 3 THEN
            wherecls = 'WHERE ' || paWhere;
         END IF;
      end;
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

CREATE OR REPLACE FUNCTION lm3.lm_getPAValue(usrid varchar,
                                           namePres varchar,
                                           minPres double precision,
                                           maxPres double precision,
                                           pctPres int,
                                           nameAbs varchar,
                                           minAbs double precision,
                                           maxAbs double precision,
                                           pctAbs int)
RETURNS int AS
$$
DECLARE
   paid int;
   cmd varchar;
   wherecls varchar;
BEGIN
   cmd = 'select presenceAbsenceid FROM lm3.PresenceAbsence ';
   wherecls = ' WHERE userid =  ' || quote_literal(usrid) ;
   
   IF namePres is not null THEN
      wherecls = wherecls || ' AND namePresence = ' || quote_literal(namePres);
   END IF;

   IF minPres is not null THEN
      wherecls = wherecls || ' AND minPresence = ' || quote_literal(minPres);
   END IF;

   IF maxPres is not null THEN
      wherecls = wherecls || ' AND maxPresence = ' || quote_literal(maxPres);
   END IF;

   IF pctPres is not null THEN
      wherecls = wherecls || ' AND percentPresence = ' || quote_literal(pctPres);
   END IF;

   IF nameAbs is not null THEN
      wherecls = wherecls || ' AND nameAbsence = ' || quote_literal(nameAbs);
   END IF;

   IF minAbs is not null THEN
      wherecls = wherecls || ' AND minAbsence = ' || quote_literal(minAbs);
   END IF;

   IF maxAbs is not null THEN
      wherecls = wherecls || ' AND maxAbsence = ' || quote_literal(maxAbs);
   END IF;

   IF pctAbs is not null THEN
      wherecls = wherecls || ' AND percentAbsence = ' || quote_literal(pctAbs);
   END IF;
   
   cmd := cmd || wherecls;
   RAISE NOTICE 'cmd = %', cmd;

   EXECUTE cmd INTO paid; 
   IF paid is null THEN
      paid = -1;
   END IF;
   RAISE NOTICE 'paid = %', paid;
        
   RETURN paid;
END;
$$  LANGUAGE 'plpgsql' STABLE;

-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lm3.lm_insertPAValues(usrid varchar,
                                             namePres varchar,
                                             minPres double precision,
                                             maxPres double precision,
                                             pctPres int,
                                             nameAbs varchar,
                                             minAbs double precision,
                                             maxAbs double precision,
                                             pctAbs int)
RETURNS int AS
$$
DECLARE
   paid int = -1;
BEGIN
   INSERT INTO lm3.PresenceAbsence (userId, namePresence, minPresence, 
                                maxPresence, percentPresence, nameAbsence, 
                                minAbsence, maxAbsence, percentAbsence)
          VALUES (usrid, namePres, minPres, maxPres, pctPres, nameAbs, 
                  minAbs, maxAbs, pctAbs);
   IF FOUND THEN
      SELECT INTO paid last_value FROM lm3.presenceabsence_presenceabsenceid_seq;
   END IF;

   RETURN paid;
END;
$$  LANGUAGE 'plpgsql' VOLATILE;


-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lm3.lm_findOrInsertPAValues(usrid varchar,
                                             namePres varchar,
                                             minPres double precision,
                                             maxPres double precision,
                                             pctPres int,
                                             nameAbs varchar,
                                             minAbs double precision,
                                             maxAbs double precision,
                                             pctAbs int)
RETURNS int AS
$$
DECLARE
   paid int = -1;
BEGIN
   -- get or insert layer 
   SELECT lm3.lm_getPAValue(usrid, namePres, minPres, maxPres, pctPres, nameAbs, 
                          minAbs, maxAbs, pctAbs) INTO paid;
        
   IF paid = -1 THEN
      SELECT * INTO paid FROM lm3.lm_insertPAValues(usrid, namePres, minPres, 
                              maxPres, pctPres, nameAbs, minAbs, maxAbs, pctAbs);
   END IF;
   
   RETURN paid;
END;
$$  LANGUAGE 'plpgsql' VOLATILE;

-- ----------------------------------------------------------------------------
-- select * from lm3.lm_insertPALayer('HuwPrice',14,'Acabaria_Projection_2374610',NULL,NULL,'/share/data/archive/HuwPrice/4326/Acabaria_Projection_2374610.tif','http://sporks.nhm.ku.edu/ogc?map=usr_HuwPrice_4326&layers=Acabaria_Projection_2374610',NULL,1,'GTiff',4326,'dd',0.25,NULL,NULL,NULL,57008.9540555,57008.9540555,'-180.00,-90.00,180.00,90.00','POLYGON((-180.0 -90.0,-180.0 90.0,180.0 90.0,180.0 -90.0,-180.0 -90.0))','pixel','30','254','30',NULL,NULL,NULL,NULL,'http://sporks.nhm.ku.edu/services/rad/layers/19');
-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lm3.lm_insertPALayer(usr varchar,
                                            expid int,
                                            lyrname varchar, 
                                            lyrtitle varchar,
                                            lyrdesc varchar,
                                            dloc varchar,
                                            vtype int,
                                            rtype int,
                                            datafmt varchar,
                                            epsg int,
                                            munits varchar,
                                            res double precision,
                                            startdt double precision,
                                            enddt double precision,
                                            metaloc varchar,
                                            createtime double precision,
                                            modtime double precision,
                                            bboxstr varchar,
                                            bboxwkt varchar,
                                            namePres varchar,
                                            minPres double precision,
                                            maxPres double precision,
                                            pctPres int,
                                            nameAbs varchar,
                                            minAbs double precision,
                                            maxAbs double precision,
                                            pctAbs int,
                                            murlprefix varchar)
RETURNS lm3.lm_palayer AS
$$
DECLARE
   lyrid int = -1;
   paid int = -1;
   expcount int = -1;
   nextIdx int = -1;
   rec lm3.lm_palayer%ROWTYPE;
   existingEntry lm3.ExperimentPALayer%ROWTYPE;
BEGIN
   -- Make sure experiment exists
   SELECT count(*) FROM lm3.Experiment INTO expcount 
      WHERE userid = usr AND experimentid = expid;      
   IF expcount != 1 THEN
      RAISE EXCEPTION 'Experiment % does not exist', expid;
   END IF;

   -- get or insert layer 
   SELECT lm3.lm_insertLayer(usr, lyrname, lyrtitle, lyrdesc, dloc, vtype, 
                         rtype, datafmt, epsg, munits, res, startdt, enddt, metaloc, 
                         createtime, modtime, bboxstr, bboxwkt, murlprefix) INTO lyrid;   
   IF lyrid = -1 THEN
      RAISE EXCEPTION 'Unable to insert layer';
   END IF;
   
   -- get or insert PA values
   SELECT lm3.lm_findOrInsertPAValues(usr, namePres, minPres, maxPres, pctPres, 
                                  nameAbs, minAbs, maxAbs, pctAbs) INTO paid;
   IF paid = -1 THEN
      RAISE EXCEPTION 'Unable to insert PresenceAbsence values';
   END IF;
   
   -- Find existing entry for this layer in this experiment
   SELECT * FROM lm3.ExperimentPALayer INTO existingEntry 
      WHERE experimentId = expid AND layerId = lyrid AND presenceAbsenceId = paid;
   IF NOT FOUND THEN 
      begin
         SELECT lm3.lm_computeNextIndex(expid, True) INTO nextIdx;
         INSERT INTO lm3.ExperimentPALayer (experimentId, layerId, presenceAbsenceId, matrixIdx) 
                           VALUES (expid, lyrid, paid, nextIdx);
         IF NOT FOUND THEN
            RAISE EXCEPTION 'Unable to join Experiment %, Layer %, and PAValues %',
                 expid, lyrid, paid;
         END IF;
      end;
   END IF;
   
   SELECT * FROM lm3.lm_palayer INTO rec 
      WHERE experimentId = expid AND layerId = lyrid AND presenceAbsenceId = paid;
   RETURN rec;
END;
$$  LANGUAGE 'plpgsql' VOLATILE;

-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lm3.lm_computeNextIndex(expid int,
                                               ispresenceabsence boolean)
RETURNS int AS
$$
DECLARE
   midx int;
   idx int := 0;
   lyr lm3.lm_layeridx%ROWTYPE;
BEGIN
   FOR lyr IN SELECT * FROM lm3.lm_getLayerIndices(expid, ispresenceabsence)
      LOOP
         RAISE NOTICE 'Found % % with index %', lyr.layerid, lyr.layername, lyr.matrixidx;
         -- missing index
         IF lyr.matrixidx != idx THEN
            RETURN idx;
         ELSE
            idx = idx + 1;
         END IF;
      END LOOP;
      
      -- If we made it this far, index is already incremented  
      RETURN idx;

END;
$$  LANGUAGE 'plpgsql' STABLE;

-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lm3.lm_getLayerIndices(expid int,
                                              ispresenceabsence boolean)
   RETURNS SETOF lm3.lm_layeridx AS
$$
DECLARE
   rec lm3.lm_layeridx%ROWTYPE;
   i int := 0;
BEGIN
   IF ispresenceabsence THEN
      FOR rec in SELECT l.layerId, l.verify, l.squid, l.userId, l.layername, l.metadataurl, l.layerurl,
                        epl.presenceAbsenceId, epl.matrixidx, epl.experimentid
                    FROM lm3.Layer l, lm3.ExperimentPALayer epl
                    WHERE epl.experimentid = expid AND epl.layerId = l.layerId 
                    ORDER BY epl.matrixidx ASC
      LOOP
         RETURN NEXT rec;
      END LOOP;
   ELSE
      FOR rec in SELECT l.layerId, l.verify, l.squid, l.userId, l.layername, l.metadataurl, l.layerurl,
                        eal.ancillaryValueId, eal.matrixidx, eal.experimentid
                    FROM lm3.Layer l, lm3.ExperimentAncLayer eal
                    WHERE eal.experimentid = expid AND eal.layerId = l.layerId 
                    ORDER BY eal.matrixidx ASC
      LOOP
         RETURN NEXT rec;
      END LOOP;
   END IF;   
   RETURN;
END;
$$ LANGUAGE 'plpgsql' STABLE; 

-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lm3.lm_getPALayersForExperiment(expid int)
   RETURNS SETOF lm3.lm_palayer AS
$$
DECLARE
   rec lm3.lm_palayer%ROWTYPE;
BEGIN
   FOR rec in SELECT * FROM lm3.lm_palayer WHERE experimentid = expid  
              ORDER BY matrixidx ASC
      LOOP
         RETURN NEXT rec;
      END LOOP;
   RETURN;
END;
$$ LANGUAGE 'plpgsql' STABLE; 

-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lm3.lm_getAncLayersForExperiment(expid int)
   RETURNS SETOF lm3.lm_anclayer AS
$$
DECLARE
   rec lm3.lm_anclayer%ROWTYPE;
BEGIN
   FOR rec in SELECT * FROM lm3.lm_anclayer WHERE experimentid = expid
              ORDER BY matrixidx ASC
   LOOP
      RETURN NEXT rec;
   END LOOP;
   RETURN;
END;
$$ LANGUAGE 'plpgsql' STABLE; 

-- ----------------------------------------------------------------------------
-- Returns the MatrixIndx of the new layer
CREATE OR REPLACE FUNCTION lm3.lm_addPALayerToExperiment(lyrid int,
                                                     paid  int, 
                                                     expid int,
                                                     usrid varchar, defusr varchar)
RETURNS int AS
$$
DECLARE
   tmpcount int;
   nextidx int = -1;
BEGIN
   -- check Experiment for User exists
   SELECT count(*) into tmpcount FROM lm3.experiment 
      WHERE experimentid = expid AND userid = usrid;
   IF tmpcount != 1 THEN
      RAISE EXCEPTION 'Experiment with id % does not exist', expid;
   END IF;
   
   -- check Layer for User or Default User exists
   SELECT count(*) into tmpcount FROM lm3.Layer 
      WHERE layerId = lyrid AND (userId = usrid OR userId = defusr);
   IF tmpcount != 1 THEN
      RAISE EXCEPTION 'Layer with id % does not exist', orgid;
   END IF;
   
   SELECT count(*) INTO tmpcount FROM lm3.ExperimentPALayer
      WHERE experimentid = expId AND presenceAbsenceId = paid AND layerId = lyrid;
   IF tmpcount = 0 THEN      
      BEGIN
         -- get matrixIndex
         SELECT lm3.lm_computeNextIndex(expid, True) INTO nextidx;

         -- get or insert experiment x layer x presenceAbsence entry
         INSERT INTO lm3.ExperimentPALayer (experimentId, layerId, 
                                        presenceAbsenceId, matrixidx) 
                     VALUES (expid, lyrid, paid, nextidx);
         IF NOT FOUND THEN
            nextidx := -1;
            RAISE NOTICE 'Unable to add PresenceAbsence % / Layer % to Experiment %', 
                    paid, lyrid, expid;
         END IF;         
      END;
   ELSE
      RAISE NOTICE 'Experiment % already contains PresenceAbsence % / Layer %', 
                    expid, paid, lyrid;
   END IF;
   
   RETURN nextidx;
END;
$$  LANGUAGE 'plpgsql' VOLATILE;

-- ----------------------------------------------------------------------------
-- Inserts new PresenceAbsence values and updates join table
CREATE OR REPLACE FUNCTION lm3.lm_updatePALayerForExperiment(expid int, 
                                                         lyrid int,
                                                         usrid varchar,
                                                         old_paid int,
                                                         namePres varchar,
                                                         minPres double precision,
                                                         maxPres double precision,
                                                         pctPres int,
                                                         nameAbs varchar,
                                                         minAbs double precision,
                                                         maxAbs double precision,
                                                         pctAbs int)
RETURNS int AS
$$
DECLARE
   new_paid int = -1;
   total int = -1;
   success int = -1;
BEGIN
   -- Ensure PresenceAbsence to UPDATE lm3.actually exists
   SELECT count(*) INTO total FROM lm3.ExperimentPALayer
     WHERE presenceAbsenceId = old_paid AND experimentId = expid AND layerid = lyrid;
   IF total = 0 THEN  
      RAISE EXCEPTION 'PresenceAbsence Layer (ids % %) does not exist for experiment %',
         lyrid, old_paid, expid;
   END IF;  

   -- find existing AncillaryValue record
   SELECT lm3.lm_getPAValue(usrid, namePres, minPres, maxPres, pctPres,
                        nameAbs, minAbs, maxAbs, pctAbs) INTO new_paid;

   -- If new PresenceAbsence values don't exist
   IF new_paid = -1 THEN
      begin
         -- and other layers use this PresenceAbsence record, add a new one  
         IF total > 1 THEN
            begin
               SELECT lm3.lm_insertPAValues(usrid, namePres, minPres, maxPres, pctPres,
                                        nameAbs, minAbs, maxAbs, pctAbs) INTO new_paid;
               IF new_paid = -1 THEN
                  RAISE EXCEPTION 'Unable to insert new PresenceAbsence';
               END IF;
            end;
   
         -- or no other layers use this PresenceAbsence record, UPDATE lm3.this one  
         ELSE
            begin
               UPDATE lm3.PresenceAbsence
                  SET (namePresence, minPresence, maxPresence, percentPresence,
                       nameAbsence, minAbsence, maxAbsence, percentAbsence)
                    = (namePres, minPres, maxPres, pctPres,
                       nameAbs, minAbs, maxAbs, pctAbs)
                  WHERE presenceAbsenceId = old_paid AND userId = usrid;
               IF FOUND THEN
                  new_paid := old_paid;
               ELSE
                  RAISE EXCEPTION 'Unable to UPDATE lm3.old PresenceAbsence to new';
               END IF;
            end;
         END IF;
      end;
   END IF;
 
   SELECT count(*) into total FROM lm3.ExperimentPALayer 
      WHERE presenceAbsenceId = new_paid AND experimentId = expid AND layerid = lyrid;
   IF total > 0 THEN
      RAISE EXCEPTION 'PresenceAbsence Layer (ids % %) with new values already exists for experiment %',
         lyrid, new_paid, expid;
   END IF;
   
   UPDATE lm3.ExperimentPALayer SET presenceAbsenceId = new_paid WHERE 
       experimentId = expid AND presenceAbsenceId = old_paid AND layerid = lyrid;
   IF NOT FOUND THEN 
      RAISE EXCEPTION 'Unable to join new PresenceAbsence to layer and experiment';
   END IF;
   
   RETURN new_paid;
END;
$$  LANGUAGE 'plpgsql' VOLATILE;

-- ----------------------------------------------------------------------------
-- Deletes PresenceAbsenceLayer join; Layer (if orphaned) and 
-- PresenceAbsence (if orphaned)
CREATE OR REPLACE FUNCTION lm3.lm_deletePALayerFromExperiment(expusr varchar, 
                                                          expid int,
                                                          lyrid int,
                                                          paid int)
RETURNS int AS
$$
DECLARE
   expusr varchar;
   pausr varchar;
   total int = -1;
   lyrsdeleted int = 0;
   pasdeleted int = 0;
   success int = -1;
BEGIN
   SELECT INTO pausr pauserid FROM lm3.lm_palayer
     WHERE experimentId = expid AND layerId = lyrid AND presenceAbsenceId = paid;

   -- Delete join of PresenceAbsence, Layer, Experiment records
   DELETE FROM lm3.ExperimentPALayer
      WHERE experimentId = expid AND layerId = lyrid AND presenceAbsenceId = paid;

   IF FOUND THEN
      success = 0;
      -- If Layer belongs to Experiment user, and it is not being used, delete
      SELECT * INTO lyrsdeleted FROM lm3.lm_deleteOrphanedLayer(expusr, lyrid);
   ELSE
      RAISE EXCEPTION 'Unable to delete PresenceAbsence % Layer % FROM lm3.Experiment %',
                      paid, lyrid, expid;
   END IF;
   
   -- Delete PresenceAbsence record if orphaned 
   SELECT count(*) INTO total FROM lm3.ExperimentPALayer WHERE presenceAbsenceId = paid;
   IF total = 0 THEN
      DELETE FROM lm3.PresenceAbsence WHERE presenceAbsenceId = paid;
      GET DIAGNOSTICS pasdeleted = ROW_COUNT;
      IF pasdeleted > 0 THEN
         RAISE NOTICE 'PresenceAbsence % deleted (% rows deleted)', paid, pasdeleted;
      END IF;
   END IF;

   RETURN success;
END;
$$  LANGUAGE 'plpgsql' VOLATILE;

-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lm3.lm_getPALayer(usr varchar, lyrid int, paid int)
RETURNS lm3.lm_palayer AS
$$
DECLARE
   rec lm3.lm_palayer%ROWTYPE;
BEGIN
   begin
      SELECT * INTO STRICT rec FROM lm3.lm_palayer WHERE layerid = lyrid
                                                 AND presenceAbsenceId = paid 
                                                 AND pauserId = usr;
      EXCEPTION
         WHEN NO_DATA_FOUND THEN
            RAISE EXCEPTION 'PresenceAbsenceLayer not found for layerid %, presenceAbsenceId % and userid %', 
                             lyrid, paid, usr;
         WHEN TOO_MANY_ROWS THEN
            RAISE EXCEPTION 'PresenceAbsenceLayer not unique for layerid %, organismid % and userid %', 
                             lyrid, paid, usr;
   end;
   RETURN rec;
END;
$$  LANGUAGE 'plpgsql' STABLE;

-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lm3.lm_getPALayerById(palyrid int)
RETURNS lm3.lm_palayer AS
$$
DECLARE
   rec lm3.lm_palayer%ROWTYPE;
BEGIN
   begin
      SELECT * INTO STRICT rec FROM lm3.lm_palayer 
          WHERE experimentPALayerId = palyrid LIMIT 1;
      EXCEPTION
         WHEN NO_DATA_FOUND THEN
            RAISE EXCEPTION 'PresenceAbsenceLayer not found for experimentPALayerId %', 
                             palyrid;
   end;
   RETURN rec;
END;
$$  LANGUAGE 'plpgsql' STABLE;

-- ----------------------------------------------------------------------------
-- Return all PALayers for a User
CREATE OR REPLACE FUNCTION lm3.lm_getPALayersForUser(usr varchar)
RETURNS SETOF lm3.lm_palayer AS
$$
DECLARE
   rec lm3.lm_palayer%ROWTYPE;
BEGIN
   FOR rec IN SELECT * FROM lm3.lm_palayer WHERE pauserId = usr
      LOOP
         RETURN NEXT rec;
      END LOOP;
   
   RETURN;     
END;
$$  LANGUAGE 'plpgsql' STABLE;


-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lm3.lm_getPALayersForUserAndLayerid(usr varchar,lyrid int)
RETURNS SETOF lm3.lm_palayer AS
$$
DECLARE
   rec lm3.lm_palayer%ROWTYPE;
BEGIN
   FOR rec IN  
      SELECT * FROM lm3.lm_palayer 
         WHERE layerId = lyrid AND pauserid = usr
   LOOP
      RETURN NEXT rec;
   END LOOP;
   RETURN;
END;
$$  LANGUAGE 'plpgsql' STABLE;

-- ----------------------------------------------------------------------------
-- Return all PresenceAbsence layers with lyrname that User usr has used in any
-- Experiments.  Only PresenceAbsence UserId is checked, so Layers may be owned by 
-- this User or another (only Default User's layers are available to everyone).
CREATE OR REPLACE FUNCTION lm3.lm_getPALayersForUserAndLayername(usr varchar, 
                                                          lyrname varchar)
RETURNS SETOF lm3.lm_palayer AS
$$
DECLARE
   rec lm3.lm_palayer%ROWTYPE;
BEGIN
   FOR rec IN
      SELECT * FROM lm3.lm_palayer 
         WHERE pauserid = usr
           AND layername = lyrname
   LOOP
      RETURN NEXT rec;
   END LOOP;
   RETURN;
END;
$$  LANGUAGE 'plpgsql' STABLE;


-- ----------------------------------------------------------------------------
-- Return all PresenceAbsence layer indices used for an Experiment.  UserId is not 
-- checked. 
CREATE OR REPLACE FUNCTION lm3.lm_getPALayerIndicesForExperiment(expid int)
RETURNS SETOF lm3.lm_palayer AS
$$
DECLARE
   rec lm3.lm_palayer%ROWTYPE;
BEGIN
   FOR rec IN 
      SELECT l.* FROM lm3.lm_palayer pa, lm3.ExperimentPALayer epl 
         WHERE epl.experimentid = expid
           AND epl.presenceAbsenceId = pa.presenceAbsenceId
   LOOP
      RETURN NEXT rec;
   END LOOP;
   RETURN;
END;
$$ LANGUAGE 'plpgsql' STABLE;

-- ----------------------------------------------------------------------------
-- Ancillary Layers
-- ----------------------------------------------------------------------------
-- ----------------------------------------------------------------------------
