-- ----------------------------------------------------------------------------
\c borg
-- ----------------------------------------------------------------------------
-- lm_envlayer
DROP VIEW IF EXISTS lm_v3.lm_envlayer CASCADE;
CREATE OR REPLACE VIEW lm_v3.lm_envlayer (
   -- Layer.* 
   layerId,
   userid,
   lyrsquid,
   lyrverify,
   lyrname,
   lyrdlocation,
   lyrmetadataUrl,
   lyrmetadata,
   dataFormat,
   gdalType,
   ogrType,
   valUnits,
   valAttribute,
   nodataVal,
   minVal,
   maxVal,
   epsgcode,
   mapunits,
   resolution,
   bbox,
   lyrmodtime,
   -- environmentalType
   envTypeId,
   envCode,
   gcmcode,
   altpredCode,
   dateCode,
   envMetadata, 
   envModtime, 
   -- EnvLayer
   envLayerId
   ) AS
      SELECT l.layerId, l.userid, l.squid, l.verify, l.name, l.dlocation,
             l.metadataUrl, l.metadata, l.dataFormat, l.gdalType, l.ogrType, 
             l.valUnits, l.valAttribute, l.nodataVal, l.minVal, l.maxVal, 
             l.epsgcode, l.mapunits, l.resolution, l.bbox, l.modTime,
             et.envTypeId, et.envCode, et.gcmcode, et.altpredCode, et.dateCode, 
             et.metadata, et.modtime,
             el.envLayerId
        FROM lm_v3.EnvLayer el, lm_v3.layer l, lm_v3.EnvType et
        WHERE el.layerid = l.layerid
          AND el.envTypeid = et.envTypeid
        ORDER BY l.layerid ASC;

-- ----------------------------------------------------------------------------
-- lm_scenlayer
DROP VIEW IF EXISTS lm_v3.lm_scenlayer CASCADE;
CREATE OR REPLACE VIEW lm_v3.lm_scenlayer (
   -- scenario
   scenarioId, 
   scenarioCode,
   -- lm_envlayer.* 
   layerId,
   userid,
   lyrsquid,
   lyrverify,
   lyrname,
   lyrdlocation,
   lyrmetadataUrl,
   lyrmetadata,
   dataFormat,
   gdalType,
   ogrType,
   valUnits,
   valAttribute,
   nodataVal,
   minVal,
   maxVal,
   epsgcode,
   mapunits,
   resolution,
   bbox,
   lyrmodtime,
   envTypeId,
   envCode,
   gcmcode,
   altpredCode,
   dateCode,
   envMetadata, 
   envModtime,
   envLayerId,
   -- ScenarioLayer
   scenarioLayerId
   ) AS
      SELECT s.scenarioId, s.scenarioCode, 
             lel.layerId, lel.userid, lel.lyrsquid, lel.lyrverify, lel.lyrname, 
             lel.lyrdlocation, lel.lyrmetadataUrl, lel.lyrmetadata, lel.dataFormat, 
             lel.gdalType, lel.ogrType, lel.valUnits, lel.valAttribute, 
             lel.nodataVal, lel.minVal, lel.maxVal, lel.epsgcode, lel.mapunits, 
             lel.resolution, lel.bbox, lel.lyrmodtime, lel.envTypeId, lel.envCode,
             lel.gcmcode, lel.altpredCode, lel.dateCode, lel.envMetadata,  
             lel.envModtime, lel.envLayerId, 
             sl.scenarioLayerId
        FROM lm_v3.ScenarioLayer sl, lm_v3.lm_envlayer lel, lm_v3.Scenario s
        WHERE sl.envLayerId = lel.envLayerId
          AND sl.scenarioid = s.scenarioid
        ORDER BY sl.scenarioLayerId ASC;

-- ----------------------------------------------------------------------------
-- lm_shapegrid
DROP VIEW IF EXISTS lm_v3.lm_shapegrid CASCADE;
CREATE OR REPLACE VIEW lm_v3.lm_shapegrid (
   -- ShapeGrid.*
   layerId,
   cellsides,
   cellsize,
   vsize,
   idAttribute,
   xAttribute,
   yAttribute,
   shpgrdstatus,
   shpgrdstatusmodtime,
   -- Layer.* 
   userid,
   lyrsquid,
   lyrverify,
   lyrname,
   lyrdlocation,
   lyrmetadataUrl,
   lyrmetadata,
   dataFormat,
   gdalType,
   ogrType,
   valUnits,
   valAttribute,
   nodataVal,
   minVal,
   maxVal,
   epsgcode,
   mapunits,
   resolution,
   bbox,
   lyrmodtime
) AS
      SELECT sg.layerId, sg.cellsides, sg.cellsize, sg.vsize, sg.idAttribute,
             sg.xAttribute, sg.yAttribute, sg.status, sg.statusmodtime,
             l.userid, l.squid, l.verify, l.name, l.dlocation,
             l.metadataUrl, l.metadata, l.dataFormat, l.gdalType, l.ogrType, 
             l.valUnits, l.valAttribute, l.nodataVal, l.minVal, l.maxVal, 
             l.epsgcode, l.mapunits, l.resolution, l.bbox, l.modTime
        FROM lm_v3.layer l, lm_v3.shapegrid sg
        WHERE l.layerid = sg.layerid;

-- ----------------------------------------------------------------------------
-- lm_matrixlayer
DROP VIEW IF EXISTS lm_v3.lm_matrixlayer CASCADE;
CREATE OR REPLACE VIEW lm_v3.lm_matrixlayer
(
   -- Layer.* 
   layerId,
   userid,
   lyrsquid,
   lyrverify,
   lyrname,
   lyrdlocation,
   lyrmetadataUrl,
   lyrmetadata,
   dataFormat,
   gdalType,
   ogrType,
   valUnits,
   valAttribute,
   nodataVal,
   minVal,
   maxVal,
   epsgcode,
   mapunits,
   resolution,
   bbox,
   lyrmodtime,

   -- MatrixColumn.*
   matrixColumnId,
   matrixId,
   matrixIndex,
   mtxcolsquid,
   mtxcolident,
   mtxcoldlocation,
   metadata, 
   intersectParams,
   mtxcolstatus,
   mtxcolstatusmodtime,
   
   -- Matrix.*
   matrixType,
   gridsetId,
   matrixDlocation,
   siteLayerIndices,
   mtxmetadata
) AS 
      SELECT l.layerId, l.userid, l.squid, l.verify, l.name, l.dlocation,
             l.metadataUrl, l.metadata, l.dataFormat, l.gdalType, l.ogrType, 
             l.valUnits, l.valAttribute, l.nodataVal, l.minVal, l.maxVal, 
             l.epsgcode, l.mapunits, l.resolution, l.bbox, l.modTime,
             mc.matrixColumnId, mc.matrixId, mc.matrixIndex, 
             mc.squid, mc.ident, mc.dlocation, mc.metadata,
             mc.intersectParams, mc.status, mc.statusmodtime,
             m.matrixType, m.gridsetId, m.matrixDlocation, m.siteLayerIndices, 
             m.metadata
        FROM lm_v3.layer l, lm_v3.MatrixColumn mc, lm_v3.Matrix m
        WHERE mc.matrixId = m.matrixId
          AND mc.layerid = l.layerid;

-- ----------------
-- lm_sdmproject 
DROP VIEW IF EXISTS lm_v3.lm_sdmproject CASCADE;
CREATE OR REPLACE VIEW lm_v3.lm_sdmproject (
   -- sdmproject.*
   sdmprojectid,
   layerid,
   userid,
   occurrenceSetId,
   algorithmCode,
   algParamHash,
   mdlscenarioId,
   mdlmaskId,
   prjscenarioId,
   prjmaskId,
   prjmetadata,
   prjstatus,
   prjstatusModTime,
   
   -- Layer.* 
   squid,
   lyrverify,
   name,
   lyrdlocation,
   lyrmetadataUrl,
   lyrmetadata,
   dataFormat,
   gdalType,
   ogrType,
   valUnits,
   valAttribute,
   nodataVal,
   minVal,
   maxVal,
   epsgcode,
   mapunits,
   resolution,
   lyrbbox,
   lyrmodtime,

   -- occurrenceSet
   occverify,
   displayName,
   occmetadataUrl,
   occdlocation,
   queryCount,
   occbbox,
   occmetadata,
   occstatus,
   occstatusModTime,

   -- model scenario
   mdlscenarioCode,
   
   -- project scenario
   prjscenarioCode,
   prjscengcmCode,
   prjscenaltpredCode,
   prjscendateCode
   ) AS
      SELECT p.sdmprojectid, p.layerid, p.userid, p.occurrenceSetId, 
             p.algorithmCode, p.algParamHash, 
             p.mdlscenarioId, p.mdlmaskId, p.prjscenarioId, p.prjmaskId, 
             p.metadata, p.layerid, p.status, p.statusModTime,
             l.squid, l.verify, l.name, l.dlocation, l.metadataUrl, l.metadata, 
             l.dataFormat, l.gdalType, l.ogrType, l.valUnits, l.valAttribute, 
             l.nodataVal, l.minVal, l.maxVal, 
             l.epsgcode, l.mapunits, l.resolution, l.bbox, l.modTime,
             o.verify, o.displayName, o.metadataUrl, o.dlocation, o.queryCount, 
             o.bbox, o.metadata, o.status, o.statusModTime,
             ms.scenarioCode, 
             ps.scenarioCode, ps.gcmCode, ps.altpredCode, ps.dateCode
      FROM lm_v3.sdmproject p, lm_v3.layer l, lm_v3.occurrenceSet o, 
           lm_v3.scenario ms, lm_v3.scenario ps
      WHERE p.layerid = l.layerid
        AND p.prjscenarioId = ps.scenarioid
        AND p.mdlscenarioId = ms.scenarioid
        AND p.occurrencesetid = o.occurrencesetid;

       
-- ----------------------------------------------------------------------------
DROP VIEW IF EXISTS lm_v3.lm_Occurrenceset CASCADE;
CREATE OR REPLACE VIEW lm_v3.lm_occurrenceset (
   -- occurrenceset.*
   occurrenceSetId,
   userId,
   squid,
   verify,
   displayName,
   metadataUrl,
   dlocation,
   rawDlocation,
   queryCount,
   occbbox,
   epsgcode,
   occmetadata,
   occstatus,
   occstatusModTime,
   -- Taxon
   taxonId,
   taxonomySourceId,
   taxonomyKey,
   kingdom,
   phylum,
   tx_class,
   tx_order,
   family,
   genus,
   rank,
   canonical,
   sciname,
   genuskey,
   specieskey,
   keyHierarchy,
   lastcount,
   taxmodTime,
   -- TaxonomySource.*
   url,
   datasetIdentifier
   ) AS
   SELECT o.occurrenceSetId, o.userId, o.verify, o.squid, o.displayName, 
          o.metadataUrl, o.dlocation, o.rawDlocation, o.queryCount, 
          o.bbox, o.epsgcode, o.metadata, o.status, o.statusModTime,
          t.taxonId, t.taxonomySourceId, t.taxonomyKey, t.kingdom, t.phylum, 
          t.tx_class, t.tx_order, t.family, t.genus, t.rank, 
          t.canonical, t.sciname,  t.genuskey, t.specieskey, t.keyHierarchy, 
          t.lastcount, t.modtime,
          ts.url, ts.datasetIdentifier
   FROM lm_v3.occurrenceset o, lm_v3.taxon t, lm_v3.taxonomysource ts
   WHERE o.squid = t.squid 
     AND t.taxonomysourceid = ts.taxonomysourceid;

        
-- ----------------------------------------------------------------------------
-- lm_bloat
-- Shows bloated indicies
CREATE OR REPLACE VIEW lm_v3.lm_bloat AS
      SELECT
        schemaname, tablename, reltuples::bigint, relpages::bigint, otta,
        ROUND(CASE WHEN otta=0 THEN 0.0 ELSE sml.relpages/otta::numeric END,1) AS tbloat,
        relpages::bigint - otta AS wastedpages,
        bs*(sml.relpages-otta)::bigint AS wastedbytes,
        pg_size_pretty((bs*(relpages-otta))::bigint) AS wastedsize,
        iname, ituples::bigint, ipages::bigint, iotta,
        ROUND(CASE WHEN iotta=0 OR ipages=0 THEN 0.0 ELSE ipages/iotta::numeric END,1) AS ibloat,
        CASE WHEN ipages < iotta THEN 0 ELSE ipages::bigint - iotta END AS wastedipages,
        CASE WHEN ipages < iotta THEN 0 ELSE bs*(ipages-iotta) END AS wastedibytes,
        CASE WHEN ipages < iotta THEN pg_size_pretty(0) ELSE pg_size_pretty((bs*(ipages-iotta))::bigint) END AS wastedisize
      FROM (
        SELECT
          schemaname, tablename, cc.reltuples, cc.relpages, bs,
          CEIL((cc.reltuples*((datahdr+ma-
            (CASE WHEN datahdr%ma=0 THEN ma ELSE datahdr%ma END))+nullhdr2+4))/(bs-20::float)) AS otta,
          COALESCE(c2.relname,'?') AS iname, COALESCE(c2.reltuples,0) AS ituples, COALESCE(c2.relpages,0) AS ipages,
          COALESCE(CEIL((c2.reltuples*(datahdr-12))/(bs-20::float)),0) AS iotta -- very rough approximation, assumes all cols
        FROM (
          SELECT
            ma,bs,schemaname,tablename,
            (datawidth+(hdr+ma-(case when hdr%ma=0 THEN ma ELSE hdr%ma END)))::numeric AS datahdr,
            (maxfracsum*(nullhdr+ma-(case when nullhdr%ma=0 THEN ma ELSE nullhdr%ma END))) AS nullhdr2
          FROM (
            SELECT
              schemaname, tablename, hdr, ma, bs,
              SUM((1-null_frac)*avg_width) AS datawidth,
              MAX(null_frac) AS maxfracsum,
              hdr+(
                SELECT 1+count(*)/8
                FROM pg_stats s2
                WHERE null_frac<>0 AND s2.schemaname = s.schemaname AND s2.tablename = s.tablename
              ) AS nullhdr
            FROM pg_stats s, (
              SELECT
                (SELECT current_setting('block_size')::numeric) AS bs,
                CASE WHEN substring(v,12,3) IN ('8.0','8.1','8.2') THEN 27 ELSE 23 END AS hdr,
                CASE WHEN v ~ 'mingw32' THEN 8 ELSE 4 END AS ma
              FROM (SELECT version() AS v) AS foo
            ) AS constants
            GROUP BY 1,2,3,4,5
          ) AS foo
        ) AS rs
        JOIN pg_class cc ON cc.relname = rs.tablename
        JOIN pg_namespace nn ON cc.relnamespace = nn.oid AND nn.nspname = rs.schemaname
        LEFT JOIN pg_index i ON indrelid = cc.oid
        LEFT JOIN pg_class c2 ON c2.oid = i.indexrelid
      ) AS sml
      WHERE sml.relpages - otta > 0 OR ipages - iotta > 10
      ORDER BY wastedbytes DESC, wastedibytes DESC;

 
-- ----------------------------------------------------------------------------
-- ----------------------------------------------------------------------------
-- ----------------------------------------------------------------------------
GRANT SELECT ON TABLE 
lm_v3.lm_envlayer,
lm_v3.lm_scenlayer,
lm_v3.lm_shapegrid,
lm_v3.lm_occurrenceset, 
lm_v3.lm_matrixlayer,
lm_v3.lm_sdmProject, 
lm_v3.lm_bloat
TO GROUP reader;

GRANT SELECT ON TABLE 
lm_v3.lm_envlayer,
lm_v3.lm_scenlayer,
lm_v3.lm_shapegrid,
lm_v3.lm_occurrenceset, 
lm_v3.lm_matrixlayer,
lm_v3.lm_sdmProject, 
lm_v3.lm_bloat
TO GROUP writer;


-- ----------------------------------------------------------------------------
-- ----------------------------------------------------------------------------

-- ----------------------------------------------------------------------------
-- DATA TYPES (used on multiple tables)
-- Note: All column names are returned in lower case
-- ----------------------------------------------------------------------------
-- ----------------------------------------------------------------------------
-- lm_atom returns only an a few object attributes
DROP TYPE IF EXISTS lm_v3.lm_atom CASCADE;
CREATE TYPE lm_v3.lm_atom AS (
  id int,
  title varchar,
  epsgcode int,
  description text,
  modtime double precision
  );   
        
-- ----------------------------------------------------------------------------
DROP TYPE IF EXISTS lm_v3.lm_occStats CASCADE;
CREATE TYPE lm_v3.lm_occStats AS
(
    occurrenceSetId int,
    displayname varchar,
    statusmodtime double precision,
    querycount int,
    totalmodels int
   );
        
-- ----------------------------------------------------------------------------
-- lm_atom returns only an a few object attributes
DROP TYPE IF EXISTS lm_v3.lm_progress CASCADE;
CREATE TYPE lm_v3.lm_progress AS (
  status int,
  total int);
   