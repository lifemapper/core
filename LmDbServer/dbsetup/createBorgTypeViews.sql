-- ----------------------------------------------------------------------------
\c borg
-- ----------------------------------------------------------------------------
-- lm_envlayer
DROP VIEW IF EXISTS lm_v3.lm_envlayer CASCADE;
CREATE OR REPLACE VIEW lm_v3.lm_scenlayer (
   -- scenario
   scenarioId, 
   scenarioCode,
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
   nodataVal,
   minVal,
   maxVal,
   epsgcode,
   mapunits,
   resolution,
   bbox,
   lyrmodtime,
   -- environmentalType
   environmentalTypeId,
   envCode,
   gcmcode,
   altpredCode,
   dateCode,
   envMetadata, 
   envModtime
   ) AS
      SELECT s.scenarioId, s.scenarioCode, 
             l.layerId, l.userid, l.squid, l.verify, l.name, l.dlocation,
             l.metadataUrl, l.metadata, l.dataFormat, l.gdalType, l.ogrType, 
             l.valUnits, l.nodataVal, l.minVal, l.maxVal, 
             l.epsgcode, l.mapunits, l.resolution, l.bbox, l.modTime,
             et.environmentalTypeId, et.envCode, et.gcmcode, et.altpredCode, et.dateCode, 
             et.metadata, et.modtime
        FROM lm_v3.ScenarioLayer sl, lm_v3.layer l, lm_v3.EnvironmentalType et, lm_v3.Scenario s
        WHERE sl.layerid = l.layerid
          AND sl.scenarioid = s.scenarioid
          AND sl.environmentalTypeid = et.environmentalTypeid
        ORDER BY l.layerid ASC;

-- ----------------------------------------------------------------------------
-- lm_shapegrid
DROP VIEW IF EXISTS lm_v3.lm_shapegrid CASCADE;
CREATE OR REPLACE VIEW lm_v3.lm_shapegrid (
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
   nodataVal,
   minVal,
   maxVal,
   epsgcode,
   mapunits,
   resolution,
   bbox,
   lyrmodtime,
   -- ShapeGrid.*
   shapeGridId,
   cellsides,
   cellsize,
   vsize,
   idAttribute,
   xAttribute,
   yAttribute,
   status,
   statusmodtime
) AS
      SELECT l.layerId, l.userid, l.squid, l.verify, l.name, l.dlocation,
             l.metadataUrl, l.metadata, l.dataFormat, l.gdalType, l.ogrType, 
             l.valUnits, l.nodataVal, l.minVal, l.maxVal, 
             l.epsgcode, l.mapunits, l.resolution, l.bbox, l.modTime,
             sg.shapeGridId, sg.cellsides, sg.cellsize, sg.vsize, sg.idAttribute,
             sg.xAttribute, sg.yAttribute, sg.status, sg.statusmodtime
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
   bucketId, 
   mtxlyrsquid,
   mtxlyrident,
   matrixId,
   matrixIndex,
   intersectParams,
   mtxlyrmetadata,  
   mtxlyrstatus,
   mtxlyrstatusmodtime
) AS 
      SELECT l.layerId, l.userid, l.squid, l.verify, l.name, l.dlocation,
             l.metadataUrl, l.metadata, l.dataFormat, l.gdalType, l.ogrType, 
             l.valUnits, l.nodataVal, l.minVal, l.maxVal, 
             l.epsgcode, l.mapunits, l.resolution, l.bbox, l.modTime,
             mc.matrixColumnId, mc.bucketId, mc.squid, mc.ident, mc.matrixId, 
             mc.matrixIndex, mc.intersectParams, mc.metadata, 
             mc.status, mc.statusmodtime
        FROM lm_v3.layer l, lm_v3.MatrixColumn mc
        WHERE l.layerid = mc.layerid;

-- ----------------------------------------------------------------------------
DROP VIEW IF EXISTS lm_v3.lm_sdmmodel CASCADE;
CREATE OR REPLACE VIEW lm_v3.lm_sdmmodel (
  -- model.*
   sdmmodelid,
   mdluserId,
   occurrenceSetId,
   mdlscenarioId,
   mdlscenarioCode,
   mdlmaskId,
   mdlstatus,
   mdlstatusModTime,
   mdldlocation,
   email, 
   algorithmParams,
   algorithmCode,
   -- OccurrenceSet
   squid,
   verify,
   displayName,
   occmetadataUrl,
   occdlocation,
   rawDlocation,
   queryCount,
   occbbox,
   epsgcode,
   occstatus,
   occstatusmodtime
) AS
      SELECT m.sdmmodelId, m.userId, m.occurrenceSetId, 
      m.scenarioId, m.scenarioCode, m.maskId, m.status, m.statusModTime, 
      m.dlocation, m.email, m.algorithmParams, m.algorithmCode, 
      o.squid, o.verify, o.displayName, o.metadataUrl, 
      o.dlocation, o.rawDlocation, o.queryCount, o.bbox, o.epsgcode, 
      o.status, o.statusmodtime
      FROM lm_v3.sdmmodel m, lm_v3.occurrenceSet o
      WHERE m.occurrencesetid = o.occurrencesetid;
      
-- ----------------
-- ----------------------------------------------------------------------------
-- lm_sdmprojection 
DROP VIEW IF EXISTS lm_v3.lm_sdmprojection CASCADE;
CREATE OR REPLACE VIEW lm_v3.lm_sdmprojection (
   -- projection.*
   sdmprojectionId,
   layerid,
   sdmmodelid,
   prjscenarioId,
   prjscenarioCode,
   prjmaskId,
   prjstatus,
   prjstatusModTime,
   
   -- projection scenario
   scenmetadata,
   gcmCode,
   altpredCode,
   dateCode,   

   -- Layer.* 
   userid,
   squid,
   verify,
   name,
   prjdlocation,
   prjmetadataUrl,
   prjmetadata,
   dataFormat,
   gdalType,
   ogrType,
   valUnits,
   nodataVal,
   minVal,
   maxVal,
   epsgcode,
   mapunits,
   resolution,
   prjbbox,
   prjmodtime,
   
   -- SDMModel
   occurrenceSetId,
   mdlscenarioId,
   mdlscenarioCode,
   mdlmaskId,
   mdlstatus,
   mdlstatusModTime,
   mdldlocation,
   email, 
   algorithmParams,
   algorithmCode,

   -- occurrenceSet
   occverify,
   displayName,
   occmetadataUrl,
   occdlocation,
   queryCount,
   occbbox,
   occmetadata,
   occstatus,
   occstatusmodtime
   ) AS
      SELECT p.sdmprojectionId, p.layerid, p.sdmmodelid, p.scenarioId, p.scenarioCode,
             p.maskId, p.status, p.statusModTime,
             ps.metadata, ps.gcmCode, ps.altpredCode, ps.dateCode,
             l.userid, l.squid, l.verify, l.name, l.dlocation,
             l.metadataUrl, l.metadata, l.dataFormat, l.gdalType, l.ogrType, 
             l.valUnits, l.nodataVal, l.minVal, l.maxVal, 
             l.epsgcode, l.mapunits, l.resolution, l.bbox, l.modTime,
             m.occurrenceSetId, m.scenarioCode,  m.scenarioId, m.scenarioCode,
             m.maskId, m.status, m.statusModTime, m.dlocation, m.email,  
             m.algorithmParams, m.algorithmCode,
             o.verify, o.displayName, o.metadataUrl, o.dlocation, o.queryCount, 
             o.bbox, o.metadata, o.status, o.statusmodtime
      FROM lm_v3.sdmprojection p, lm_v3.scenario ps, lm_v3.layer l, 
           lm_v3.sdmmodel m, lm_v3.occurrenceSet o
      WHERE p.layerid = l.layerid
        AND p.scenarioid = ps.scenarioid
        AND p.sdmmodelid = m.sdmmodelid 
        AND m.occurrencesetid = o.occurrencesetid;

       
-- ----------------------------------------------------------------------------
DROP VIEW IF EXISTS lm_v3.lm_Occurrenceset CASCADE;
CREATE OR REPLACE VIEW lm_v3.lm_occurrenceset (
   -- occurrenceset.*
   occurrenceSetId,
   squid,
   verify,
   userId,
   displayName,
   metadataUrl,
   dlocation,
   rawDlocation,
   queryCount,
   bbox,
   epsgcode,
   metadata,
   status,
   statusmodtime,
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
   SELECT o.occurrenceSetId, o.verify, o.squid, o.userId, o.displayName, 
          o.metadataUrl, o.dlocation, o.rawDlocation, o.queryCount, 
          o.bbox, o.epsgcode, o.metadata, o.status, o.statusmodtime, 
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
lm_v3.lm_scenlayer,
lm_v3.lm_shapegrid,
lm_v3.lm_occurrenceset, 
lm_v3.lm_matrixlayer,
lm_v3.lm_sdmmodel, lm_v3.lm_sdmProjection, 
lm_v3.lm_bloat
TO GROUP reader;

GRANT SELECT ON TABLE 
lm_v3.lm_scenlayer,
lm_v3.lm_shapegrid,
lm_v3.lm_occurrenceset, 
lm_v3.lm_matrixlayer,
lm_v3.lm_sdmmodel, lm_v3.lm_sdmProjection, 
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
   
