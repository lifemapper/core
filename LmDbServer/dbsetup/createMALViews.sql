-- ----------------------------------------------------------------------------
\c mal
-- ----------------------------------------------------------------------------
-- lm_envlayer
DROP VIEW IF EXISTS lm3.lm_envlayer CASCADE;
CREATE OR REPLACE VIEW lm3.lm_envlayer (
             -- Layer.* 
             layerId,
             userid,
             name,
             title,
             author,
             description,
             -- dataPath = dlocation
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
             valAttribute,
             startDate,
             endDate,
             dateLastModified,
             bbox,
             thumbnail,
             nodataVal,
             minVal,
             maxVal,
             valUnits,
             layerTypeId,
             -- LayerType.*
             typecode,
             typetitle,
             typedescription,
             typemodtime) AS
      SELECT l.layerId, l.userid, l.name, l.title, l.author, l.description, l.dlocation, 
             l.metadataUrl, l.metalocation, l.gdalType, l.ogrType, l.isCategorical, l.dataFormat, 
             l.epsgcode, l.mapunits, l.resolution, l.valAttribute, l.startDate, 
             l.endDate, l.dateLastModified, l.bbox, l.thumbnail, l.nodataVal, 
             l.minVal, l.maxVal, l.valUnits, l.layerTypeId, 
             lt.code, lt.title, lt.description, lt.datelastmodified
        FROM lm3.layer l, lm3.layertype lt
        WHERE l.layertypeid = lt.layertypeid
        ORDER BY l.layertypeid ASC;

        
-- ----------------------------------------------------------------------------
DROP VIEW IF EXISTS lm3.lm_fullmodel CASCADE;
CREATE OR REPLACE VIEW lm3.lm_fullmodel (
  -- model.*
   modelId, 
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
   mdlstatus, 
   mdlstatusModTime, 
   mdlpriority, 
   --rulesetFile, 
   mdldlocation,
   qc, 
   mdljobId, 
   email,
   mdlComputeResourceId,
  -- occurrenceSet.* 
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
   epsgcode, 
   occbbox, 
   occgeom) AS
      SELECT m.modelId, m.userId, m.name, m.description, m.occurrenceSetId, 
             m.scenarioCode, m.scenarioId, m.maskId, m.algorithmCode, 
             m.algorithmParams, m.createTime, m.status, 
             m.statusModTime, m.priority, m.dlocation, m.qc, m.jobId, m.email,
             m.computeResourceId,
             o.userid, o.fromGbif, o.displayName, o.scientificNameId, 
             o.status, o.statusmodtime,
             o.metadataUrl, o.dlocation, o.queryCount, 
             o.dateLastModified, o.dateLastChecked, o.epsgcode, o.bbox, o.geom
      FROM lm3.model m, lm3.occurrenceSet o
      WHERE m.occurrencesetid = o.occurrencesetid;
      
-- ----------------------------------------------------------------------------
-- lm_mdlJob
DROP VIEW IF EXISTS lm3.lm_mdlJob CASCADE;
CREATE OR REPLACE VIEW lm3.lm_mdlJob (
   -- lm_fullmodel.*
   modelId, 
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
   mdlstatus, 
   mdlstatusModTime, 
   mdlpriority, 
   mdldlocation, 
   qc, 
   mdljobId, 
   email,
   mdlcomputeResourceId,
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
   epsgcode, 
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
      SELECT m.modelId, m.mdlUserId, m.mdlname, m.mdldescription, m.occurrenceSetId, 
             m.mdlScenarioCode, 
             m.mdlScenarioId, m.mdlMaskId, m.algorithmCode, m.algorithmParams, 
             m.mdlCreateTime, m.mdlstatus, m.mdlstatusModTime, m.mdlpriority, 
             m.mdldlocation, m.qc, m.mdljobId, m.email, m.mdlcomputeResourceId,
             m.occUserId, m.fromGbif, 
             m.displayName, m.scientificNameId, m.occstatus, m.occstatusmodtime,
             m.occMetadataUrl, m.occdlocation, m.queryCount, m.dateLastModified, 
             m.dateLastChecked, m.epsgcode, m.occbbox, m.occgeom, 

             j.lmJobId, j.jobFamily, j.referenceType, j.referenceId, 
             j.computeResourceId, j.priority, j.progress, 
             j.status, j.statusmodtime, j.stage, j.stagemodtime, 
             j.donotify, j.reqdata, j.reqsoftware, j.datecreated, 
             j.lastheartbeat, retrycount
      FROM lm3.LmJob j, lm3.lm_fullmodel m 
      WHERE j.referenceType = 101
        AND j.referenceid = m.modelid;
              
-- ----------------------------------------------------------------------------
-- lm_msgJob
DROP VIEW IF EXISTS lm3.lm_msgJob CASCADE;
CREATE OR REPLACE VIEW lm3.lm_msgJob (
   -- lm_fullmodel.*
   modelId, 
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
   mdlstatus, 
   mdlstatusModTime, 
   mdlpriority, 
   mdldlocation, 
   qc, 
   mdljobId, 
   email,
   mdlcomputeResourceId,
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
   epsgcode, 
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
      SELECT m.modelId, m.mdlUserId, m.mdlname, m.mdldescription, m.occurrenceSetId, 
             m.mdlScenarioCode, 
             m.mdlScenarioId, m.mdlMaskId, m.algorithmCode, m.algorithmParams, 
             m.mdlCreateTime, m.mdlstatus, m.mdlstatusModTime, m.mdlpriority, 
             m.mdldlocation, m.qc, m.mdljobId, m.email, m.mdlcomputeResourceId,
             m.occUserId, m.fromGbif, 
             m.displayName, m.scientificNameId, m.occstatus, m.occstatusmodtime,
             m.occMetadataUrl, m.occdlocation, m.queryCount, m.dateLastModified, 
             m.dateLastChecked, m.epsgcode, m.occbbox, m.occgeom, 

             j.lmJobId, j.jobFamily, j.referenceType, j.referenceId, 
             j.computeResourceId, j.priority, j.progress, 
             j.status, j.statusmodtime, j.stage, j.stagemodtime, 
             j.donotify, j.reqdata, j.reqsoftware, j.datecreated, 
             j.lastheartbeat, retrycount
      FROM lm3.LmJob j, lm3.lm_fullmodel m
      WHERE j.referenceType = 103
        AND j.reqSoftware = 510
        AND j.referenceid = m.modelid;

-- ----------------------------------------------------------------------------
-- lm_fullprojection 
DROP VIEW IF EXISTS lm3.lm_fullprojection CASCADE;
CREATE OR REPLACE VIEW lm3.lm_fullprojection (
   -- projection.*
   projectionId, 
   prjMetadataUrl, 
   modelId, 
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
   dataPath, 
   dataType, 
   prjjobId,
   prjcomputeResourceId, 
   prjgeom, 
   -- model.*
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
   -- occurrenceSet
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
   occgeom
   ) AS
      SELECT p.projectionId, p.metadataUrl, p.modelId, p.scenarioCode, p.scenarioId, p.maskId,
             p.createTime, p.status, p.statusModTime, p.priority, p.units, 
             p.resolution, p.epsgcode, p.bbox, p.dlocation, p.dataPath, p.dataType, 
             p.jobId, p.computeResourceId, p.geom, 
             m.userId, m.name, m.description, m.occurrenceSetId, m.scenarioCode, 
             m.scenarioId, m.maskId, m.algorithmCode, m.algorithmParams, 
             m.createTime, m.status, m.statusModTime, m.priority, 
             m.dlocation, m.qc, m.jobId, m.email, m.computeResourceId,
             o.userId, o.fromGbif, o.displayName, o.scientificNameId, 
             o.status, o.statusmodtime, o.metadataUrl, o.dlocation, 
             o.queryCount, o.dateLastModified, o.dateLastChecked, o.epsgcode, o.bbox, o.geom
      FROM lm3.projection p, lm3.model m, lm3.occurrenceSet o
      WHERE p.modelid = m.modelid 
        AND m.occurrencesetid = o.occurrencesetid;

-- ----------------------------------------------------------------------------
DROP VIEW IF EXISTS lm3.lm_prjJob CASCADE;
CREATE OR REPLACE VIEW lm3.lm_prjJob (
   -- lm_fullprojection.*
   projectionId, 
   prjMetadataUrl, 
   modelId, 
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
   dataPath, 
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
      SELECT p.projectionId, p.prjMetadataUrl, p.modelId, p.prjscenarioCode, 
             p.prjscenarioId, p.prjMaskId, p.prjcreateTime, p.prjstatus, 
             p.prjstatusModTime, p.prjpriority, p.units, p.resolution, 
             p.prjepsgcode, p.prjbbox, p.prjdlocation, p.dataPath, p.dataType, 
             p.prjjobId, p.prjcomputeResourceId, p.prjgeom, p.mdlUserId, p.mdlname, 
             p.mdldescription, p.occurrenceSetId, 
             p.mdlScenarioCode, p.mdlScenarioId, p.mdlMaskId, p.algorithmCode, 
             p.algorithmParams, p.mdlCreateTime, p.mdlStatus, p.mdlStatusModTime, 
             p.mdlPriority, p.mdldlocation, p.qc, p.mdlJobId, p.email, 
             p.mdlcomputeresourceid,
             p.occUserId, p.fromGbif, p.displayName, p.scientificNameId, 
             p.occstatus, p.occstatusmodtime, p.occMetadataUrl, p.occdlocation, 
             p.queryCount, p.dateLastModified, p.dateLastChecked, p.occepsgcode, p.occbbox, 
             p.occgeom, 
             j.lmJobId, j.jobFamily, j.referenceType, j.referenceId, 
             j.computeResourceId, j.priority, j.progress, 
             j.status, j.statusmodtime, j.stage, j.stagemodtime, j.donotify,
             j.reqdata, j.reqsoftware, j.datecreated, j.lastheartbeat, j.retrycount
      FROM lm3.LmJob j, lm3.lm_fullprojection p 
      WHERE j.referenceType = 102
        AND j.referenceid = p.projectionid;
       
-- ----------------------------------------------------------------------------
DROP VIEW IF EXISTS lm3.lm_fullOccurrenceset CASCADE;
CREATE OR REPLACE VIEW lm3.lm_fullOccurrenceset (
   -- occurrenceset.* (without geom, geompts)
   occurrenceSetId,
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
   SELECT o.occurrenceSetId, o.userId, o.fromGbif, o.displayName, 
          o.scientificNameId, o.primaryEnv, o.metadataUrl, o.dlocation,
          o.queryCount, o.dateLastModified, o.dateLastChecked, o.bbox, 
          o.epsgcode, o.status, o.statusmodtime, o.rawDlocation,
          n.taxonomySourceId, n.taxonomyKey, n.kingdom, n.phylum, n.tx_class,
          n.tx_order, n.family, n.genus, n.sciname, n.genuskey, n.specieskey,
          n.keyHierarchy, n.lastcount, n.datecreated, n.datelastmodified,
          t.url, t.datasetIdentifier, t.dateCreated, t.dateLastModified
   FROM lm3.occurrenceset o, lm3.scientificname n, lm3.taxonomysource t
   WHERE o.scientificnameid = n.scientificnameid 
     AND n.taxonomysourceid = t.taxonomysourceid;

-- ----------------------------------------------------------------------------
DROP VIEW IF EXISTS lm3.lm_occJob CASCADE;
CREATE OR REPLACE VIEW lm3.lm_occJob (
   -- occurrenceset.* (without geom, geompts)
   occurrenceSetId,
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
     SELECT o.occurrenceSetId, o.userid, o.fromgbif, o.displayname, 
            o.scientificNameId,
            o.primaryenv, o.metadataUrl, o.dlocation, o.querycount, o.datelastmodified, 
            o.datelastchecked, o.bbox, o.epsgcode, o.status, o.statusmodtime, 
            o.rawdlocation,
            j.lmJobId, j.jobFamily, j.referenceType, j.referenceId, 
            j.computeResourceId, j.priority, j.progress, 
            j.status, j.statusmodtime, j.stage, j.stagemodtime, j.donotify,
            j.reqdata, j.reqsoftware, j.datecreated, j.lastheartbeat, 
            j.retrycount  
      FROM lm3.LmJob j, lm3.occurrenceset o 
      WHERE j.referenceType = 104
        AND j.referenceid = o.occurrencesetid; 
        
-- ----------------------------------------------------------------------------
-- lm_bloat
-- Shows bloated indicies
CREATE OR REPLACE VIEW lm3.lm_bloat AS
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
GRANT SELECT ON TABLE 
lm3.lm_envlayer, lm3.lm_fullmodel, lm3.lm_fullProjection, 
lm3.lm_mdlJob, lm3.lm_prjJob, lm3.lm_msgJob, lm3.lm_occJob, lm3.lm_bloat
TO GROUP reader;

GRANT SELECT ON TABLE 
lm3.lm_envlayer, lm3.lm_fullmodel, lm3.lm_fullProjection, 
lm3.lm_mdlJob, lm3.lm_prjJob, lm3.lm_msgJob, lm3.lm_occJob, lm3.lm_bloat
TO GROUP writer;
     
-- ----------------------------------------------------------------------------
-- DATA TYPES (used on multiple tables)
-- Note: All column names are returned in lower case
-- ----------------------------------------------------------------------------
-- ----------------------------------------------------------------------------
-- lm_atom returns only an a few object attributes
DROP TYPE IF EXISTS lm3.lm_atom CASCADE;
CREATE TYPE lm3.lm_atom AS (
  id int,
  title varchar,
  epsgcode int,
  description text,
  modtime double precision);

-- ----------------------------------------------------------------------------
-- Type returning scenario with comma delimited list of keywords as string field
DROP TYPE IF EXISTS lm3.lm_scenarioAndKeywords CASCADE;
CREATE TYPE lm3.lm_scenarioAndKeywords AS
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
DROP TYPE IF EXISTS lm3.lm_envlayerAndKeywords CASCADE;
CREATE TYPE lm3.lm_envlayerAndKeywords AS
(
             -- lm_envlayer.* + layertype keywords 
             layerId int,
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
DROP TYPE IF EXISTS lm3.lm_layerTypeAndKeywords CASCADE;
CREATE TYPE lm3.lm_layerTypeAndKeywords AS
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
DROP TYPE IF EXISTS lm3.lm_scenarioMapLayer;
CREATE TYPE lm3.lm_scenarioMapLayer AS
(
   layerid      int, 
   metadataUrl     varchar,
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
DROP TYPE IF EXISTS lm3.lm_mpids CASCADE;
CREATE TYPE lm3.lm_mpids AS (
  modelid int,
  projectionid int); 

-- ----------------------------------------------------------------------------
DROP TYPE IF EXISTS lm3.lm_occStats CASCADE;
CREATE TYPE lm3.lm_occStats AS
(
    occurrenceSetId int,
    displayname varchar,
    datelastmodified double precision,
    querycount int,
    totalmodels int
   );
        
        
-- ----------------------------------------------------------------------------
-- ----------------------------------------------------------------------------
GRANT SELECT ON TABLE 
lm3.lm_envlayer, lm3.lm_fullOccurrenceset, 
lm3.lm_fullmodel, lm3.lm_fullProjection, 
lm3.lm_mdlJob, lm3.lm_prjJob, lm3.lm_msgJob, lm3.lm_occJob, lm3.lm_bloat
TO GROUP reader;

GRANT SELECT ON TABLE 
lm3.lm_envlayer, lm3.lm_fullOccurrenceset,  
lm3.lm_fullmodel, lm3.lm_fullProjection, 
lm3.lm_mdlJob, lm3.lm_prjJob, lm3.lm_msgJob, lm3.lm_occJob, lm3.lm_bloat
TO GROUP writer;

GRANT UPDATE ON TABLE 
lm3.lm_mdlJob, lm3.lm_prjJob, lm3.lm_msgJob, lm3.lm_occJob
TO GROUP writer;

-- ----------------------------------------------------------------------------
-- ----------------------------------------------------------------------------
