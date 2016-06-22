-- ----------------------------------------------------------------------------
\c mal
-- ----------------------------------------------------------------------------
-- lm_envlayer
DROP VIEW IF EXISTS lm_v3.lm_envlayer CASCADE;
CREATE OR REPLACE VIEW lm_v3.lm_envlayer (
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
   valAttribute,
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
      SELECT l.layerId, l.verify, l.squid, l.userid, l.taxonId, l.name, l.title,
             l.author, l.description, l.dlocation, l.metadataUrl, l.metalocation,
             l.gdalType, l.ogrType, l.isCategorical, l.dataFormat, l.epsgcode,
             l.mapunits, l.resolution, l.startDate, l.endDate, l.modTime, l.bbox, 
             l.valAttribute, l.nodataVal, l.minVal, l.maxVal, l.valUnits, 
             l.layerTypeId, 
             lt.code, lt.title, lt.description, lt.datelastmodified
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
   valAttribute,
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
   statusmodtime,
   computeResourceId
) AS
      SELECT l.layerId, l.verify, l.squid, l.userid, l.taxonId, l.name, l.title,
             l.author, l.description, l.dlocation, l.metadataUrl, l.metalocation,
             l.gdalType, l.ogrType, l.isCategorical, l.dataFormat, l.epsgcode,
             l.mapunits, l.resolution, l.startDate, l.endDate, l.datecreated,
             l.dateLastModified, l.bbox, l.valAttribute, l.nodataVal, l.minVal,
             l.maxVal, l.valUnits,
             sg.shapeGridId, sg.cellsides, sg.cellsize, sg.vsize, sg.idAttribute,
             sg.xAttribute, sg.yAttribute, sg.status, sg.statusmodtime, sg.computeResourceId
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
   valAttribute,
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
   -- BoomAncLayer.*
   boomAncLayerId,
   boomId,
   boomAncLayerName,
   matrixIdx
) AS
      SELECT l.layerId, l.verify, l.squid, l.userid, l.taxonId, l.name, l.title,
             l.author, l.description, l.dlocation, l.metadataUrl, l.metalocation,
             l.gdalType, l.ogrType, l.isCategorical, l.dataFormat, l.epsgcode,
             l.mapunits, l.resolution, l.startDate, l.endDate, l.modTime, l.bbox, 
             l.valAttribute, l.nodataVal, l.minVal, l.maxVal, l.valUnits,
             a.ancillaryValueId, a.nameValue, a.weightedMean, a.largestClass, 
             a.minPercent, a.nameFilter, a.valueFilter,
             bal.boomAncLayerId, bal.boomId, bal.name, bal.matrixIdx,
      FROM lm3.Layer l, lm3.AncillaryValue a, lm3.BoomAncLayer bal
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
   valAttribute,
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
   -- BoomPALayer.*
   boomPALayerId,
   boomId,
   boomPALayerName,
   matrixIdx
) AS
      SELECT l.layerId, l.verify, l.squid, l.userid, l.taxonId, l.name, l.title,
             l.author, l.description, l.dlocation, l.metadataUrl, l.metalocation,
             l.gdalType, l.ogrType, l.isCategorical, l.dataFormat, l.epsgcode,
             l.mapunits, l.resolution, l.startDate, l.endDate, l.modTime, l.bbox, 
             l.valAttribute, l.nodataVal, l.minVal, l.maxVal, l.valUnits,
             pa.presenceAbsenceId, pa.nameFilter, pa.valueFilter, pa.namePresence, 
             pa.minPresence, pa.maxPresence, pa.percentPresence, pa.nameAbsence, 
             pa.minAbsence, pa.maxAbsence, pa.percentAbsence,
             bpal.boomPALayerId, bpal.boomId, bpal.name, bpal.matrixIdx
      FROM lm3.Layer l, lm3.PresenceAbsence pa, lm3.BoomPALayer bpal
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
   scenarioId,
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
   displayName text,
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
      m.scenarioId, m.scenarioCode, m.maskId, m.createTime, m.mdlstatus, 
      m.mdlstatusModTime, m.priority, m.mdldlocation, m.email, m.algorithmParams,
      m.algorithmCode, 
      o.verify, o.squid, o.displayName, o.taxonId, o.primaryEnv, o.occmetadataUrl, 
      o.occdlocation, o.rawDlocation, o.queryCount, o.bbox, o.epsgcode, 
      o.occstatus, o.occstatusmodtime
      FROM lm_v3.sdmmodel m, lm_v3.occurrenceSet o
      WHERE m.occurrencesetid = o.occurrencesetid;
      
-- ----------------
-- ----------------------------------------------------------------------------
-- lm_fullprojection 
DROP VIEW IF EXISTS lm_v3.lm_sdmprojection CASCADE;
CREATE OR REPLACE VIEW lm_v3.lm_sdmprojection (
   -- projection.*
   sdmprojectionId,
   prjverify,
   squid,
   prjmetadataUrl,
   prjmetalocation,
   taxonId,
   sdmmodelid,
   scenarioCode,
   scenarioId,
   maskId,
   prjstatus,
   prjstatusModTime,
   units,
   resolution double precision,
   epsgcode,
   prjbbox,
   prjdlocation,
   dataType,
   -- SDMModel
   userId,
   name,
   description,
   occurrenceSetId,
   scenarioId,
   scenarioCode,
   maskId,
   status,
   statusModTime,
   priority,
   dlocation,
   email, 
   algorithmParams,
   algorithmCode,
   -- occurrenceSet
   verify,
   squid,
   displayName,
   taxonId,
   primaryEnv,
   metadataUrl,
   dlocation,
   queryCount,
   bbox,
   status,
   statusmodtime
   ) AS
      SELECT p.sdmprojectionId, p.verify, p.squid, p.metadataUrl, p.sdmmodelid, p.scenarioCode, p.scenarioId, p.maskId,
             p.createTime, p.status, p.statusModTime, p.priority, p.units, 
             p.resolution, p.epsgcode, p.bbox, p.dlocation, p.dataType, 
             p.jobId, p.computeResourceId, p.geom, 
             m.userId, m.name, m.description, m.occurrenceSetId, m.scenarioCode, 
             m.scenarioId, m.maskId, m.algorithmCode, m.algorithmParams, 
             m.createTime, m.status, m.statusModTime, m.priority, 
             m.dlocation, m.qc, m.jobId, m.email, m.computeResourceId,
             o.verify, o.squid, o.userId, o.fromGbif, o.displayName, o.scientificNameId, 
             o.status, o.statusmodtime, o.metadataUrl, o.dlocation, 
             o.queryCount, o.dateLastModified, o.dateLastChecked, o.epsgcode, o.bbox, o.geom
      FROM lm_v3.projection p, lm_v3.sdmmodel m, lm_v3.occurrenceSet o
      WHERE p.sdmmodelid = m.sdmmodelid 
        AND m.occurrencesetid = o.occurrencesetid;

-- ----------------------------------------------------------------------------
-- referenceType defined in LmServer.common.lmconstants ReferenceType 
DROP VIEW IF EXISTS lm_v3.lm_prjJob CASCADE;
CREATE OR REPLACE VIEW lm_v3.lm_prjJob (
   -- lm_fullprojection.*
   sdmprojectionId, 
   prjverify,
   prjsquid,
   prjMetadataUrl, 
   sdmmodelid, 
   prjscenarioCode, 
   prjscenarioId, 
   prjMaskId, 
   prjcreateTime, 
   prjstatus, 
   prjstatusModTime, 
   prjpriority, 
   units, 
   resolution, 
   prjepsgcode, 
   prjbbox, 
   prjdlocation, 
   dataType, 
   prjjobId, 
   prjcomputeResourceId,
   prjgeom, 
   mdlUserId, 
   mdlname,
   mdldescription,
   occurrenceSetId, 
   mdlScenarioCode, 
   mdlScenarioId, 
   mdlMaskId, 
   algorithmCode, 
   algorithmParams, 
   mdlCreateTime, 
   mdlStatus, 
   mdlStatusModTime, 
   mdlPriority, 
   mdldlocation, 
   qc, 
   mdlJobId, 
   email, 
   mdlcomputeresourceid,
   occverify,
   occsquid,
   occUserId, 
   fromGbif, 
   displayName, 
   scientificNameId,
   occstatus, 
   occstatusmodtime,
   occMetadataUrl, 
   occdlocation, 
   queryCount, 
   dateLastModified, 
   dateLastChecked, 
   occepsgcode, 
   occbbox, 
   occgeom,
      
   -- LmJob.*
   lmJobId,
   jobFamily,
   referenceType,
   referenceId,
   jbcomputeResourceId,
   priority,
   progress,
   jbstatus,
   jbstatusmodtime,
   jbstage,
   jbstagemodtime,
   donotify,
   reqData,
   reqSoftware,
   jbdatecreated,
   lastheartbeat,
   retrycount
    ) AS
      SELECT p.sdmprojectionId, p.prjverify, p.prjsquid, p.prjMetadataUrl, p.sdmmodelid, p.prjscenarioCode, 
             p.prjscenarioId, p.prjMaskId, p.prjcreateTime, p.prjstatus, 
             p.prjstatusModTime, p.prjpriority, p.units, p.resolution, 
             p.prjepsgcode, p.prjbbox, p.prjdlocation, p.dataType, 
             p.prjjobId, p.prjcomputeResourceId, p.prjgeom, p.mdlUserId, p.mdlname, 
             p.mdldescription, p.occurrenceSetId, 
             p.mdlScenarioCode, p.mdlScenarioId, p.mdlMaskId, p.algorithmCode, 
             p.algorithmParams, p.mdlCreateTime, p.mdlStatus, p.mdlStatusModTime, 
             p.mdlPriority, p.mdldlocation, p.qc, p.mdlJobId, p.email, 
             p.mdlcomputeresourceid,
             p.occverify, p.occsquid, p.occUserId, p.fromGbif, p.displayName, p.scientificNameId, 
             p.occstatus, p.occstatusmodtime, p.occMetadataUrl, p.occdlocation, 
             p.queryCount, p.dateLastModified, p.dateLastChecked, p.occepsgcode, p.occbbox, 
             p.occgeom, 
             j.lmJobId, j.jobFamily, j.referenceType, j.referenceId, 
             j.computeResourceId, j.priority, j.progress, 
             j.status, j.statusmodtime, j.stage, j.stagemodtime, j.donotify,
             j.reqdata, j.reqsoftware, j.datecreated, j.lastheartbeat, j.retrycount
      FROM lm_v3.LmJob j, lm_v3.lm_fullprojection p 
      WHERE j.referenceType = 102
        AND j.referenceid = p.sdmprojectionId;
       
-- ----------------------------------------------------------------------------
DROP VIEW IF EXISTS lm_v3.lm_fullOccurrenceset CASCADE;
CREATE OR REPLACE VIEW lm_v3.lm_fullOccurrenceset (
   -- occurrenceset.* (without geom, geompts)
   occurrenceSetId,
   verify,
   squid,
   userId,
   fromGbif,
   displayName,
   scientificNameId,
   primaryEnv,
   metadataUrl,
   dlocation,
   queryCount,
   dateLastModified,
   dateLastChecked,
   bbox,
   epsgcode,
   status,
   statusmodtime,
   rawDlocation,
   -- ScientificName.*
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
   scidatecreated,
   scidatelastmodified, 
   -- TaxonomySource.*
   url,
   datasetIdentifier,
   taxdateCreated,
   taxdateLastModified
   ) AS
   SELECT o.occurrenceSetId, o.verify, o.squid, o.userId, o.fromGbif, o.displayName, 
          o.scientificNameId, o.primaryEnv, o.metadataUrl, o.dlocation,
          o.queryCount, o.dateLastModified, o.dateLastChecked, o.bbox, 
          o.epsgcode, o.status, o.statusmodtime, o.rawDlocation,
          n.taxonomySourceId, n.taxonomyKey, n.kingdom, n.phylum, n.tx_class,
          n.tx_order, n.family, n.genus, n.rank, n.canonical, n.sciname, 
          n.genuskey, n.specieskey,
          n.keyHierarchy, n.lastcount, n.datecreated, n.datelastmodified,
          t.url, t.datasetIdentifier, t.dateCreated, t.dateLastModified
   FROM lm_v3.occurrenceset o, lm_v3.scientificname n, lm_v3.taxonomysource t
   WHERE o.scientificnameid = n.scientificnameid 
     AND n.taxonomysourceid = t.taxonomysourceid;

-- ----------------------------------------------------------------------------
-- referenceType defined in LmServer.common.lmconstants ReferenceType 
DROP VIEW IF EXISTS lm_v3.lm_occJob CASCADE;
CREATE OR REPLACE VIEW lm_v3.lm_occJob (
   -- occurrenceset.* (without geom, geompts)
   occurrenceSetId,
   verify,
   squid,
   occuserId,
   fromGbif,
   displayName,
   scientificNameId,
   primaryEnv,
   occmetadataUrl,
   occdlocation,
   queryCount,
   dateLastModified,
   dateLastChecked,
   occbbox,
   occepsgcode, 
   occstatus,
   occstatusModTime,
   rawdlocation,
         
   -- LmJob.*
   lmJobId,
   jobFamily,
   referenceType,
   referenceId,
   jbcomputeResourceId,
   priority,
   progress,
   jbstatus,
   jbstatusmodtime,
   jbstage,
   jbstagemodtime,
   donotify,
   reqData,
   reqSoftware,
   jbdatecreated,
   lastheartbeat,
   retrycount
   ) AS
     SELECT o.occurrenceSetId, o.verify, o.squid, o.userid, o.fromgbif, o.displayname, 
            o.scientificNameId,
            o.primaryenv, o.metadataUrl, o.dlocation, o.querycount, o.datelastmodified, 
            o.datelastchecked, o.bbox, o.epsgcode, o.status, o.statusmodtime, 
            o.rawdlocation,
            j.lmJobId, j.jobFamily, j.referenceType, j.referenceId, 
            j.computeResourceId, j.priority, j.progress, 
            j.status, j.statusmodtime, j.stage, j.stagemodtime, j.donotify,
            j.reqdata, j.reqsoftware, j.datecreated, j.lastheartbeat, 
            j.retrycount  
      FROM lm_v3.LmJob j, lm_v3.occurrenceset o 
      WHERE j.referenceType = 104
        AND j.referenceid = o.occurrencesetid; 
        
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
  modtime double precision);

-- ----------------------------------------------------------------------------
-- Type returning scenario with comma delimited list of keywords as string field
DROP TYPE IF EXISTS lm_v3.lm_scenarioAndKeywords CASCADE;
CREATE TYPE lm_v3.lm_scenarioAndKeywords AS
(
    scenarioId int,
    scenarioCode varchar,
    -- todo: change to metadataUrl varchar(256)
    metadataUrl varchar,
    title varchar,
    author varchar,
    description text,
    startDate double precision,
    endDate double precision,
    units varchar,
    resolution double precision,
    epsgcode int,
    bbox varchar,
    dateLastModified double precision,
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
             valAttribute varchar,
             startDate double precision,
             endDate double precision,
             dateLastModified double precision,
             bbox varchar,
             thumbnail bytea,
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
             typecode varchar,
             typetitle varchar,
             userid varchar,
             typedescription text,
             typemodtime double precision,
             keywords varchar
);

-- ----------------------------------------------------------------------------
-- Type for creating a mapservice from scenario layers.  Eventually replaces 
-- SDL mapservice and maplayers.
DROP TYPE IF EXISTS lm_v3.lm_scenarioMapLayer;
CREATE TYPE lm_v3.lm_scenarioMapLayer AS
(
   layerid      int, 
   verify     varchar,
   metadataUrl  varchar,
   layername    varchar,
   layertitle   varchar,
   startdate    double precision,
   enddate      double precision,
   mapunits     varchar,
   resolution   double precision, 
   bbox         varchar,
   dlocation     varchar,
   gdaltype     int,
   scenarioMetadataUrl varchar,
   scenariocode varchar,
   scenariotitle varchar
);
        
-- ----------------------------------------------------------------------------
DROP TYPE IF EXISTS lm_v3.lm_mpids CASCADE;
CREATE TYPE lm_v3.lm_mpids AS (
  sdmmodelid int,
  sdmprojectionId int); 

-- ----------------------------------------------------------------------------
DROP TYPE IF EXISTS lm_v3.lm_occStats CASCADE;
CREATE TYPE lm_v3.lm_occStats AS
(
    occurrenceSetId int,
    displayname varchar,
    datelastmodified double precision,
    querycount int,
    totalmodels int
   );
        
-- ----------------------------------------------------------------------------
-- lm_atom returns only an a few object attributes
DROP TYPE IF EXISTS lm_v3.lm_progress CASCADE;
CREATE TYPE lm_v3.lm_progress AS (
  status int,
  total int);
 
-- ----------------------------------------------------------------------------
-- ----------------------------------------------------------------------------
-- ----------------------------------------------------------------------------
GRANT SELECT ON TABLE 
lm_v3.lm_envlayer,
lm3.lm_shapegrid,
lm3.lm_anclayer,  
lm3.lm_palayer, 

lm_v3.lm_fullOccurrenceset, 
lm_v3.lm_fullmodel, lm_v3.lm_fullProjection, 
lm_v3.lm_mdlJob, lm_v3.lm_prjJob, lm_v3.lm_msgJob, lm_v3.lm_occJob, lm_v3.lm_bloat
TO GROUP reader;

GRANT SELECT ON TABLE 
lm_v3.lm_envlayer, lm_v3.lm_fullOccurrenceset,  
lm_v3.lm_fullmodel, lm_v3.lm_fullProjection, 
lm_v3.lm_mdlJob, lm_v3.lm_prjJob, lm_v3.lm_msgJob, lm_v3.lm_occJob, lm_v3.lm_bloat
TO GROUP writer;

GRANT UPDATE ON TABLE 
lm_v3.lm_mdlJob, lm_v3.lm_prjJob, lm_v3.lm_msgJob, lm_v3.lm_occJob
TO GROUP writer;

-- ----------------------------------------------------------------------------
-- ----------------------------------------------------------------------------

