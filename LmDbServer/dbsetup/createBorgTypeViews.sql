-- ----------------------------------------------------------------------------
\c borg
-- ----------------------------------------------------------------------------
-- lm_envlayer
DROP VIEW IF EXISTS lm_v3.lm_envlayer CASCADE;
CREATE OR REPLACE VIEW lm_v3.lm_envlayer (
   -- Layer.* 
   layerId,
   userid,
   squid,
   verify,
   name,
   dlocation,
   metadataUrl,
   metadata,
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
   modTime,
   bbox,
   keywords,
   -- EnvironmentalLayer
   environmentalLayerId,
   startDate,
   endDate,
   -- LayerType
   layerTypeId,
   typecode,
   typetitle,
   typedescription,
   typekeywords
) AS
      SELECT l.layerId, l.userid, l.squid, l.verify, l.name, l.dlocation,
             l.metadataUrl, l.metadata, l.dataFormat,
             l.gdalType, l.ogrType, l.valUnits, l.nodataVal, l.minVal, l.maxVal, 
             l.epsgcode, l.mapunits, l.resolution, l.modTime, l.bbox, l.keywords,
             el.environmentalLayerId, el.startDate, el.endDate,
             lt.layerTypeId, lt.code, lt.title, lt.description, lt.keywords
        FROM lm_v3.layer l, lm_v3.EnvironmentalLayer el, lm_v3.layertype lt
        WHERE l.layerid = el.layerid
          AND el.layertypeid = lt.layertypeid
        ORDER BY l.layerid ASC;

-- ----------------------------------------------------------------------------
-- lm_shapegrid
DROP VIEW IF EXISTS lm_v3.lm_shapegrid CASCADE;
CREATE OR REPLACE VIEW lm_v3.lm_shapegrid (
   -- Layer.* 
   layerId,
   userid,
   squid,
   verify,
   name,
   dlocation,
   metadataUrl,
   metadata,
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
   modTime,
   bbox,
   keywords,
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
             l.metadataUrl, l.metadata, l.dataFormat,
             l.gdalType, l.ogrType, l.valUnits, l.nodataVal, l.minVal, l.maxVal, 
             l.epsgcode, l.mapunits, l.resolution, l.modTime, l.bbox, l.keywords,
             sg.shapeGridId, sg.cellsides, sg.cellsize, sg.vsize, sg.idAttribute,
             sg.xAttribute, sg.yAttribute, sg.status, sg.statusmodtime
        FROM lm_v3.layer l, lm_v3.shapegrid sg
        WHERE l.layerid = sg.layerid;

-- ----------------------------------------------------------------------------
-- lm_anclayer
DROP VIEW IF EXISTS lm_v3.lm_anclayer CASCADE;
CREATE OR REPLACE VIEW lm_v3.lm_anclayer (
   -- BucketAncLayer.*
   bucketAncLayerId,
   bucketId, 
   matrixIndex,
   ident,
   status,
   statusmodtime,
   -- Bucket
   userId,
   -- Layer.* 
   layerId,
   squid,
   verify,
   name,
   dlocation,
   metadataUrl,
   metadata,
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
   modTime,
   bbox,
   keywords,
   -- AncillaryValue.*
   ancillaryValueId,
   nameValue,
   weightedMean,
   largestClass,
   minPercent,
   nameFilter,
   valueFilter
) AS
   SELECT bal.bucketAncLayerId, bal.bucketId, bal.matrixIndex, bal.ident,
             bal.status, bal.statusmodtime,
             b.userid, 
             l.layerId, l.squid, l.verify, l.name, l.dlocation,
             l.metadataUrl, l.metadata, l.dataFormat,
             l.gdalType, l.ogrType, l.valUnits, l.nodataVal, l.minVal, l.maxVal, 
             l.epsgcode, l.mapunits, l.resolution, l.modTime, l.bbox, l.keywords,
             a.ancillaryValueId, a.nameValue, a.weightedMean, a.largestClass, 
             a.minPercent, a.nameFilter, a.valueFilter
      FROM lm_v3.BucketAncLayer bal, lm_v3.Bucket b
      LEFT JOIN lm_v3.Layer l, lm_v3.AncillaryValue a 
      ON bal.bucketid = b.bucketid
        AND bal.layerId = l.layerId
        AND bal.ancillaryValueId = a.ancillaryValueId;
        
-- ----------------------------------------------------------------------------
-- lm_palayer
DROP VIEW IF EXISTS lm_v3.lm_palayer CASCADE;
CREATE OR REPLACE VIEW lm_v3.lm_palayer (
   -- BucketPALayer
   bucketPALayerId,
   bucketId, 
   matrixIndex,
   squid,
   status,
   statusmodtime,
   -- Bucket
   userId,
   -- Layer.* 
   layerId,
   squid,
   verify,
   name,
   dlocation,
   metadataUrl,
   metadata,
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
   modTime,
   bbox,
   keywords,
   -- PresenceAbsence.*
   presenceAbsenceId,
   nameFilter,
   valueFilter,
   namePresence,
   minPresence,
   maxPresence,
   percentPresence
) AS
      SELECT bpal.bucketPALayerId, bpal.bucketId, bpal.matrixIndex, bpal.squid,
             bpal.status, bpal.statusModTime,
             b.userid, 
             l.layerId, l.squid, l.verify, l.name, l.dlocation,
             l.metadataUrl, l.metadata, l.dataFormat,
             l.gdalType, l.ogrType, l.valUnits, l.nodataVal, l.minVal, l.maxVal, 
             l.epsgcode, l.mapunits, l.resolution, l.modTime, l.bbox, l.keywords,
             pa.presenceAbsenceId, pa.nameFilter, pa.valueFilter, pa.namePresence, 
             pa.minPresence, pa.maxPresence, pa.percentPresence
      FROM lm_v3.BucketPALayer bpal, lm_v3.Bucket b
      LEFT JOIN lm_v3.Layer l, lm_v3.PresenceAbsence pa
      ON bpal.bucketid = b.bucketid
        AND bpal.layerId = l.layerId
        AND bpal.presenceAbsenceId = pa.presenceAbsenceId;
        

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
   primaryEnv,
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
      o.squid, o.verify, o.displayName, o.primaryEnv, o.metadataUrl, 
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
   prjstatusModTime

   -- Layer.* 
   layerId,
   userid,
   squid,
   verify,
   name,
   prjdlocation,
   prjmetadataUrl,
   prjmetadata,
   prjdataFormat,
   prjgdalType,
   valUnits,
   nodataVal,
   minVal,
   maxVal,
   epsgcode,
   mapunits,
   resolution,
   prjbbox,
   prjkeywords,
   
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
   occstatus,
   occstatusmodtime
   ) AS
      SELECT p.sdmprojectionId, p.layerid, p.sdmmodelid, p.prjscenarioId, p.prjscenarioCode,
             p.prjmaskId, p.prjstatus, p.prjstatusModTime,
             l.layerId, l.userid, l.squid, l.verify, l.name, l.dlocation, 
             l.metadataUrl, l.metadata, l.dataFormat, l.gdalType, l.valUnits, 
             l.nodataVal, l.minVal, l.maxVal, l.epsgcode, l.mapunits, 
             l.resolution, l.bbox, l.keywords,
             m.occurrenceSetId, m.scenarioCode,  m.scenarioId, m.scenarioCode,
             m.maskId, m.status, m.statusModTime, m.dlocation, m.email,  
             m.algorithmParams, m.algorithmCode
             o.verify, o.displayName, o.metadataUrl, o.dlocation, o.queryCount, 
             o.bbox, o.status, o.statusmodtime
      FROM lm_v3.sdmprojection p, lm_v3.layer l, lm_v3.sdmmodel m, lm_v3.occurrenceSet o
      WHERE p.layerid = l.layerid
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
   status,
   statusmodtime
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
   datasetIdentifier,
   ) AS
   SELECT o.occurrenceSetId, o.verify, o.squid, o.userId, o.displayName, 
          o.primaryEnv, o.metadataUrl, o.dlocation, o.rawDlocation,
          o.queryCount, o.bbox, o.epsgcode, o.status, o.statusmodtime, 
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
lm_v3.lm_shapegrid,
lm_v3.lm_anclayer,  
lm_v3.lm_palayer, 
lm_v3.lm_occurrenceset, 
lm_v3.lm_sdmmodel, lm_v3.lm_sdmProjection, 
lm_v3.lm_bloat
TO GROUP reader;

GRANT SELECT ON TABLE 
lm_v3.lm_envlayer,
lm_v3.lm_shapegrid,
lm_v3.lm_anclayer,  
lm_v3.lm_palayer, 
lm_v3.lm_occurrenceset, 
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
-- lm_palayeridx OR lm_anclayeridx
DROP TYPE IF EXISTS lm_v3.lm_layeridx CASCADE;
CREATE TYPE lm_v3.lm_layeridx AS (
   -- Layer
   layerid int,
   userid varchar,
   squid varchar,
   verify varchar,
   name varchar,
   dlocation varchar,
   metadataurl varchar,
   -- BucketPALayer OR BucketAncLayer
   bktpanclayerid int,
   bucketid int,
   matrixIndex int);
   
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
   
