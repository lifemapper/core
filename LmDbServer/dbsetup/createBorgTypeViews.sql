-- ----------------------------------------------------------------------------
\c borg
-- ----------------------------------------------------------------------------
-- lm_envlayer
DROP VIEW IF EXISTS lm_v3.lm_envlayer CASCADE;
CREATE OR REPLACE VIEW lm_v3.lm_envlayer (
   -- Layer.* 
   layerId,
   verify,
   squid,
   userid,
   name,
   title,
   author,
   description,
   dlocation,
   metadataUrl,
   metalocation,
   gdalType,
   ogrType,
   isCategorical,
   dataFormat,
   epsgcode,
   mapunits,
   resolution,
   startDate,
   endDate,
   modTime,
   bbox,
   nodataVal,
   minVal,
   maxVal,
   valUnits,
   layerTypeId,
   -- LayerType
   typecode,
   typetitle,
   typedescription,
   typemodtime) AS
      SELECT l.layerId, l.verify, l.squid, l.userid, l.name, l.title,
             l.author, l.description, l.dlocation, l.metadataUrl, l.metalocation,
             l.gdalType, l.ogrType, l.isCategorical, l.dataFormat, l.epsgcode,
             l.mapunits, l.resolution, l.startDate, l.endDate, l.modTime, l.bbox, 
             l.nodataVal, l.minVal, l.maxVal, l.valUnits, 
             l.layerTypeId, 
             lt.code, lt.title, lt.description, lt.modtime
        FROM lm_v3.layer l, lm_v3.layertype lt
        WHERE l.layertypeid = lt.layertypeid
        ORDER BY l.layertypeid ASC;

-- ----------------------------------------------------------------------------
-- lm_shapegrid
DROP VIEW IF EXISTS lm_v3.lm_shapegrid CASCADE;
CREATE OR REPLACE VIEW lm_v3.lm_shapegrid (
   -- Layer.* 
   layerId,
   verify,
   squid,
   userid,
   taxonId,
   name,
   title,
   author,
   description,
   dlocation,
   metadataUrl,
   metalocation,
   gdalType,
   ogrType,
   isCategorical,
   dataFormat,
   epsgcode,
   mapunits,
   resolution,
   startDate,
   endDate,
   modTime,
   bbox,
   nodataVal,
   minVal,
   maxVal,
   valUnits,
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
      SELECT l.layerId, l.verify, l.squid, l.userid, l.taxonId, l.name, l.title,
             l.author, l.description, l.dlocation, l.metadataUrl, l.metalocation,
             l.gdalType, l.ogrType, l.isCategorical, l.dataFormat, l.epsgcode,
             l.mapunits, l.resolution, l.startDate, l.endDate, 
             l.modtime, l.bbox, l.nodataVal, l.minVal, l.maxVal, l.valUnits,
             sg.shapeGridId, sg.cellsides, sg.cellsize, sg.vsize, sg.idAttribute,
             sg.xAttribute, sg.yAttribute, sg.status, sg.statusmodtime
        FROM lm_v3.layer l, lm_v3.shapegrid sg
        WHERE l.layerid = sg.layerid;

-- ----------------------------------------------------------------------------
-- lm_anclayer
DROP VIEW IF EXISTS lm_v3.lm_anclayer CASCADE;
CREATE OR REPLACE VIEW lm_v3.lm_anclayer (
   -- Layer.* 
   layerId,
   verify,
   squid,
   userid,
   taxonId,
   name,
   title,
   author,
   description,
   dlocation,
   metadataUrl,
   metalocation,
   gdalType,
   ogrType,
   isCategorical,
   dataFormat,
   epsgcode,
   mapunits,
   resolution,
   startDate,
   endDate,
   modTime,
   bbox,
   nodataVal,
   minVal,
   maxVal,
   valUnits,
   -- AncillaryValue.*
   ancillaryValueId,
   nameValue,
   weightedMean,
   largestClass,
   minPercent,
   nameFilter,
   valueFilter,
   -- BucketAncLayer.*
   bucketAncLayerId,
   bucketId
) AS
      SELECT l.layerId, l.verify, l.squid, l.userid, l.taxonId, l.name, l.title,
             l.author, l.description, l.dlocation, l.metadataUrl, l.metalocation,
             l.gdalType, l.ogrType, l.isCategorical, l.dataFormat, l.epsgcode,
             l.mapunits, l.resolution, l.startDate, l.endDate, l.modTime, l.bbox, 
             l.nodataVal, l.minVal, l.maxVal, l.valUnits,
             a.ancillaryValueId, a.nameValue, a.weightedMean, a.largestClass, 
             a.minPercent, a.nameFilter, a.valueFilter,
             bal.bucketAncLayerId, bal.bucketId
      FROM lm_v3.Layer l, lm_v3.AncillaryValue a, lm_v3.BucketAncLayer bal
      WHERE l.layerId = bal.layerId 
        AND bal.ancillaryValueId = a.ancillaryValueId;
        
-- ----------------------------------------------------------------------------
-- lm_palayer
DROP VIEW IF EXISTS lm_v3.lm_palayer CASCADE;
CREATE OR REPLACE VIEW lm_v3.lm_palayer (
   -- Layer.* 
   layerId,
   verify,
   squid,
   userid,
   taxonId,
   name,
   title,
   author,
   description,
   dlocation,
   metadataUrl,
   metalocation,
   gdalType,
   ogrType,
   isCategorical,
   dataFormat,
   epsgcode,
   mapunits,
   resolution,
   startDate,
   endDate,
   modTime,
   bbox,
   nodataVal,
   minVal,
   maxVal,
   valUnits,
   -- PresenceAbsence.*
   presenceAbsenceId,
   nameFilter,
   valueFilter,
   namePresence,
   minPresence,
   maxPresence,
   percentPresence,
   nameAbsence,
   minAbsence,
   maxAbsence,
   percentAbsence,
   -- BucketPALayer.*
   bucketPALayerId,
   bucketId
) AS
      SELECT l.layerId, l.verify, l.squid, l.userid, l.taxonId, l.name, l.title,
             l.author, l.description, l.dlocation, l.metadataUrl, l.metalocation,
             l.gdalType, l.ogrType, l.isCategorical, l.dataFormat, l.epsgcode,
             l.mapunits, l.resolution, l.startDate, l.endDate, l.modTime, l.bbox, 
             l.nodataVal, l.minVal, l.maxVal, l.valUnits,
             pa.presenceAbsenceId, pa.nameFilter, pa.valueFilter, pa.namePresence, 
             pa.minPresence, pa.maxPresence, pa.percentPresence, pa.nameAbsence, 
             pa.minAbsence, pa.maxAbsence, pa.percentAbsence,
             bpal.bucketPALayerId, bpal.bucketId
      FROM lm_v3.Layer l, lm_v3.PresenceAbsence pa, lm_v3.BucketPALayer bpal
      WHERE l.layerId = bpal.layerId 
        AND bpal.presenceAbsenceId = pa.presenceAbsenceId;
        

-- ----------------------------------------------------------------------------
DROP VIEW IF EXISTS lm_v3.lm_sdmmodel CASCADE;
CREATE OR REPLACE VIEW lm_v3.lm_sdmmodel (
  -- model.*
   sdmmodelId,
   userId,
   name,
   description,
   occurrenceSetId,
   scenarioCode,
   maskId,
   mdlstatus,
   mdlstatusModTime,
   priority,
   mdldlocation,
   email, 
   algorithmParams,
   algorithmCode,
   -- OccurrenceSet
   verify,
   squid,
   displayName,
   taxonId,
   primaryEnv,
   occmetadataUrl,
   occdlocation,
   rawDlocation,
   queryCount,
   bbox,
   epsgcode,
   occstatus,
   occstatusmodtime
) AS
      SELECT m.sdmmodelId, m.userId, m.name, m.description, m.occurrenceSetId, 
      m.scenarioCode, m.maskId, m.status, m.statusModTime, m.priority, 
      m.dlocation, m.email, m.algorithmParams, m.algorithmCode, 
      o.verify, o.squid, o.displayName, o.taxonId, o.primaryEnv, o.metadataUrl, 
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
   prjverify,
   prjsquid,
   prjmetadataUrl,
   prjmetalocation,
   prjtaxonId,
   sdmmodelid,
   scenarioCode,
   prjmaskId,
   prjstatus,
   prjstatusModTime,
   units,
   resolution,
   epsgcode,
   prjbbox,
   prjdlocation,
   dataType,
   -- SDMModel
   userId,
   name,
   description,
   occurrenceSetId,
   mdlscenarioCode,
   mdlmaskId,
   mdlstatus,
   mdlstatusModTime,
   priority,
   mdldlocation,
   email, 
   algorithmParams,
   algorithmCode,
   -- occurrenceSet
   occverify,
   displayName,
   primaryEnv,
   occmetadataUrl,
   occdlocation,
   queryCount,
   occbbox,
   occstatus,
   occstatusmodtime
   ) AS
      SELECT p.sdmprojectionId, p.verify, p.squid, p.metadataUrl, p.metalocation, 
             p.taxonId, p.sdmmodelid, p.scenarioCode, p.maskId,
             p.status, p.statusModTime, p.units, p.resolution, p.epsgcode, 
             p.bbox, p.dlocation, p.dataType, 
             m.userId, m.name, m.description, m.occurrenceSetId, m.scenarioCode, 
             m.maskId, m.status, m.statusModTime, m.priority, 
             m.dlocation, m.email, m.algorithmParams, m.algorithmCode, 
             o.verify, o.squid, o.displayName, o.primaryEnv,
             o.metadataUrl, o.dlocation, o.queryCount, o.bbox, o.status, o.statusmodtime
      FROM lm_v3.sdmprojection p, lm_v3.sdmmodel m, lm_v3.occurrenceSet o
      WHERE p.sdmmodelid = m.sdmmodelid 
        AND m.occurrencesetid = o.occurrencesetid;

       
-- ----------------------------------------------------------------------------
DROP VIEW IF EXISTS lm_v3.lm_Occurrenceset CASCADE;
CREATE OR REPLACE VIEW lm_v3.lm_occurrenceset (
   -- occurrenceset.* (without geom, geompts)
   occurrenceSetId,
   verify,
   squid,
   userId,
   displayName,
   taxonId,
   primaryEnv,
   metadataUrl,
   dlocation,
   rawDlocation,
   queryCount,
   bbox,
   epsgcode,
   status,
   statusmodtime,
   -- Taxon.*
   taxonomySourceId,
   taxonomyKey,
   taxsquid,
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
   taxmodtime, 
   -- TaxonomySource.*
   url,
   datasetIdentifier,
   taxsrcmodtime
   ) AS
   SELECT o.occurrenceSetId, o.verify, o.squid, o.userId, o.displayName, 
          o.taxonId, o.primaryEnv, o.metadataUrl, o.dlocation, o.rawDlocation,
          o.queryCount, o.bbox, o.epsgcode, o.status, o.statusmodtime, 
          t.taxonomySourceId, t.taxonomyKey, t.squid, t.kingdom, t.phylum, t.tx_class,
          t.tx_order, t.family, t.genus, t.rank, t.canonical, t.sciname, 
          t.genuskey, t.specieskey, t.keyHierarchy, t.lastcount, t.modtime,
          ts.url, ts.datasetIdentifier, ts.modtime
   FROM lm_v3.occurrenceset o, lm_v3.taxon t, lm_v3.taxonomysource ts
   WHERE o.taxonid = t.taxonid 
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
   verify varchar,
   squid varchar,
   lyruserid varchar,
   layername varchar,
   metadataurl varchar,
   layerurl varchar,
   -- BucketPALayer OR BucketAncLayer
   bucketlayerid int,
   bucketid int);

-- ----------------------------------------------------------------------------
-- Type returning scenario with comma delimited list of keywords as string field
DROP TYPE IF EXISTS lm_v3.lm_scenarioAndKeywords CASCADE;
CREATE TYPE lm_v3.lm_scenarioAndKeywords AS
(
    scenarioId int,
    userid varchar,
    scenarioCode varchar,
    metadataUrl text,
    title varchar,
    author varchar,
    description text,
    startDate double precision,
    endDate double precision,
    units varchar,
    resolution double precision,
    epsgcode int,
    bbox varchar,
    modTime double precision,
    keywords varchar
   );
   
-- ----------------------------------------------------------------------------
-- lm_envlayer
DROP TYPE IF EXISTS lm_v3.lm_envlayerAndKeywords CASCADE;
CREATE TYPE lm_v3.lm_envlayerAndKeywords AS
(
             -- lm_envlayer.* + layertype keywords 
             layerId int,
             verify varchar,
             userid varchar,
             name varchar,
             title varchar,
             author varchar,
             description text,
             dlocation varchar,
             metadataUrl varchar,
             metalocation varchar,
             gdalType int,
             ogrType int,
             isCategorical boolean,
             dataFormat varchar,
             epsgcode int,
             mapunits varchar,
             resolution double precision,
             startDate double precision,
             endDate double precision,
             modTime double precision,
             bbox varchar,
             nodataVal double precision,
             minVal double precision,
             maxVal double precision,
             valUnits varchar,
             layerTypeId int,
             typecode varchar,
             typetitle varchar,
             typedescription text,
             typemodtime double precision,
             keywords varchar
);

-- ----------------------------------------------------------------------------
DROP TYPE IF EXISTS lm_v3.lm_layerTypeAndKeywords CASCADE;
CREATE TYPE lm_v3.lm_layerTypeAndKeywords AS
(
             layerTypeId int,
             userid varchar,
             typecode varchar,
             typetitle varchar,
             typedescription text,
             typemodtime double precision,
             keywords varchar
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
   
