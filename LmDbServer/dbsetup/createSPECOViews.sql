-- ----------------------------------------------------------------------------
-- from APP_DIR
-- psql -U admin -d template1 --file=LmDbServer/dbsetup/createSPECOViews.sql
-- ----------------------------------------------------------------------------
\c speco
-- ----------------------------------------------------------------------------
-- lm_fullradbucket contains experiment and shapegrid
DROP VIEW IF EXISTS lm3.lm_fullradbucket CASCADE;
CREATE OR REPLACE VIEW lm3.lm_fullradbucket (
   -- Experiment.*
   experimentId,
   expuserId,
   expname,
   attrMatrixDlocation,
   attrTreeDlocation,
   email,
   expepsgcode,
   expkeywords,
   expmetadataurl,
   expdescription,
   expdatelastmodified,
   expdatecreated,
      
   -- Bucket.*
   bucketId,
   slIndicesDlocation,   
   pamDlocation,
   grimDlocation,
   bktstatus,
   bktstatusmodtime,
   bktstage,
   bktstagemodtime,
   bktdatecreated,
   bktkeywords,
   bktmetadataurl,
   bktcomputeresourceid,

   -- Layer.* except geom
   layerId,
   verify,
   lyruserId,
   layername,
   title,
   description,
   lyrdlocation,
   lyrmetadataurl,
   layerurl,
   ogrType,
   gdalType,
   dataFormat,
   epsgcode,
   mapunits,
   resolution,
   startDate,
   endDate,
   metalocation,
   lyrdatecreated,
   lyrdatelastmodified,
   bbox,
   
   -- ShapeGrid.*
   shapeGridId,
   cellsides,
   cellsize,
   vsize,
   idAttribute,
   xAttribute,
   yAttribute,
   shpstatus,
   shpstatusmodtime,
   
   -- PamSum (Original, not Randomized)
   pamsumId,
   randomMethod,
   pspamDlocation,
   pssumDlocation,
   psmetadataurl,
   psstatus,
   psstatusmodtime,
   psstage,
   psstagemodtime,
   psdatecreated,
   pscomputeresourceid
   
   ) AS
      SELECT e.experimentId, e.userId, e.expname, 
             e.attrMatrixDlocation, e.attrTreeDlocation, 
             e.email, e.epsgcode, 
             e.keywords, e.metadataurl, e.description, 
             e.datelastmodified, e.datecreated,
             b.bucketid, b.slIndicesDlocation, 
             b.pamDlocation, b.grimDlocation,  
             b.status, b.statusmodtime, b.stage, b.stagemodtime,             
             b.datecreated, b.keywords, b.metadataurl, b.computeresourceid,
             l.layerId, l.verify, l.userId, l.layername, l.title, l.description, l.dlocation, 
             l.metadataurl, l.layerurl, l.ogrType, l.gdalType, l.dataFormat, 
             l.epsgcode, l.mapunits, 
             l.resolution, l.startDate, l.endDate, l.metalocation, 
             l.datecreated, l.datelastmodified, l.bbox,
             s.shapeGridId, s.cellsides, s.cellsize, s.vsize, 
             s.idAttribute, s.xAttribute, s.yAttribute, s.status, s.statusmodtime,
             ps.pamsumid, ps.randommethod, ps.pamdlocation, ps.sumdlocation, 
             ps.metadataurl, ps.status, ps.statusmodtime, ps.stage, 
             ps.stagemodtime, ps.datecreated, ps.computeresourceid
      FROM lm3.Experiment e
      RIGHT JOIN lm3.Bucket b ON e.experimentId = b.experimentId
      RIGHT JOIN lm3.ShapeGrid s ON b.shapegridid = s.shapegridid 
      RIGHT JOIN lm3.Layer l ON l.layerid = s.layerid
      LEFT JOIN lm3.PamSum ps ON b.bucketid = ps.bucketid AND ps.randommethod = 0;

-- ----------------------------------------------------------------------------
-- lm_shapegrid
DROP VIEW IF EXISTS lm3.lm_shapegrid CASCADE;
CREATE OR REPLACE VIEW lm3.lm_shapegrid (
   -- Layer.* except geom
   layerId,
   verify,
   lyruserId,
   layername,
   title,
   description,
   lyrdlocation,
   lyrmetadataurl,
   layerurl,
   ogrType,
   gdalType,
   dataFormat,
   epsgcode,
   mapunits,
   resolution,
   startDate,
   endDate,
   metalocation,
   lyrdatecreated,
   lyrdatelastmodified,
   bbox,
   -- ShapeGrid.*
   shapeGridId, 
   cellsides,
   cellsize,
   vsize,
   idAttribute,
   xAttribute,
   yAttribute,
   shpstatus,
   shpstatusmodtime) 
   AS SELECT l.layerId, l.verify, l.userId, l.layername, l.title, l.description, 
             l.dlocation, l.metadataurl, l.layerurl, l.ogrType, l.gdalType, 
             l.dataFormat, l.epsgcode, l.mapunits, 
             l.resolution, l.startDate, l.endDate, l.metalocation, 
             l.datecreated, l.datelastmodified, l.bbox, 
             s.shapeGridId, s.cellsides, s.cellsize, s.vsize, s.idAttribute,
             s.xAttribute, s.yAttribute, s.status, s.statusmodtime
      FROM lm3.Layer l, lm3.ShapeGrid s  WHERE l.layerid = s.layerid;

-- ----------------------------------------------------------------------------
-- lm_pamsum 
DROP VIEW IF EXISTS lm3.lm_pamsum CASCADE;
CREATE OR REPLACE VIEW lm3.lm_pamsum (
   -- Experiment
   experimentId,
   expuserId,
   expname,
   expepsgcode, 
   expmetadataurl,
   
   -- PamSum.*
   pamSumId,
   bucketId,
   randomMethod,
   randomParams,
   splotchPamDlocation,
   splotchSitesDlocation,
   pspamDlocation,
   pssumDlocation, 
   psmetadataurl,
   psstatus,
   psstatusmodtime,
   psstage,
   psstagemodtime,
   psdatecreated,
   computeResourceId
   
   ) AS
      SELECT e.experimentId, e.userId, e.expname, e.epsgcode, e.metadataurl,
             ps.pamsumid, ps.bucketid, ps.randommethod, ps.randomparams,
             ps.splotchpamdlocation, ps.splotchSitesDlocation, 
             ps.pamdlocation, ps.sumDlocation, 
             ps.metadataurl, ps.status, ps.statusmodtime, ps.stage, 
             ps.stagemodtime, ps.datecreated, ps.computeResourceId
      FROM lm3.Experiment e, lm3.Bucket b, lm3.PamSum ps
      WHERE e.experimentid = b.experimentid 
        AND b.bucketid = ps.bucketid;

-- ----------------------------------------------------------------------------
-- lm_anclayer
DROP VIEW IF EXISTS lm3.lm_anclayer CASCADE;
CREATE OR REPLACE VIEW lm3.lm_anclayer (
   -- Layer.* except geom
   layerId,
   verify, 
   squid,
   lyruserId,
   layername,
   title,
   description,
   lyrdlocation,
   lyrmetadataurl,
   layerurl,
   ogrType,
   gdalType,
   dataFormat,
   epsgcode,
   mapunits,
   resolution,
   startDate,
   endDate,
   metalocation,
   lyrdatecreated,
   lyrdatelastmodified,
   bbox,
   -- AncillaryValue.*
   ancillaryValueId,
   ancuserid,
   nameFilter, 
   valueFilter,
   nameValue,
   weightedMean,
   largestClass,
   minPercent,
   -- ExperimentAncLayer
   experimentAncLayerId, 
   expAncLayername,
   experimentId, 
   matrixidx
   ) AS
      SELECT l.layerId, l.verify, l.squid, l.userId, l.layername, l.title, l.description, 
             l.dlocation, l.metadataurl, l.layerurl, l.ogrType, l.gdalType, 
             l.dataFormat, l.epsgcode, l.mapunits, 
             l.resolution, l.startDate, l.endDate, l.metalocation, 
             l.datecreated, l.datelastmodified, l.bbox, 
             a.ancillaryValueId, a.userid, a.nameFilter, a. valueFilter, 
             a.nameValue, a.weightedMean, a.largestClass, a.minPercent,
             eal.experimentAncLayerId, eal.expAncLayername, eal.experimentId, eal.matrixidx
      FROM lm3.Layer l, lm3.AncillaryValue a, lm3.ExperimentAncLayer eal
      WHERE l.layerId = eal.layerId 
        AND eal.ancillaryValueId = a.ancillaryValueId;

-- lm_bktanclayer
DROP VIEW IF EXISTS lm3.lm_bktanclayer CASCADE;
CREATE OR REPLACE VIEW lm3.lm_bktanclayer (
   -- lm_anclayer.* 
   layerId,
   verify,
   squid,
   lyruserId,
   layername,
   title,
   description,
   lyrdlocation,
   lyrmetadataurl,
   layerurl,
   ogrType,
   gdalType,
   dataFormat,
   epsgcode,
   mapunits,
   resolution,
   startDate,
   endDate,
   metalocation,
   lyrdatecreated,
   lyrdatelastmodified,
   bbox,
   ancillaryValueId,
   ancuserid,
   nameFilter, 
   valueFilter,
   nameValue,
   weightedMean,
   largestClass,
   minPercent,
   experimentAncLayerId, 
   expAncLayername,
   experimentId, 
   matrixidx,
   bucketId,
   status,
   statusmodtime
   ) AS
      SELECT al.layerId, al.verify, al.squid, al.lyruserId, al.layername, al.title, al.description,
             al.lyrdlocation, al.lyrmetadataurl, al.layerurl, al.ogrType,
             al.gdalType, al.dataFormat, al.epsgcode, al.mapunits, al.resolution, 
             al.startDate, al.endDate, al.metalocation, al.lyrdatecreated,  
             al.lyrdatelastmodified, al.bbox, al.ancillaryValueId, al.ancuserid, 
             al.nameFilter, al.valueFilter, al.nameValue, al.weightedMean, 
             al.largestClass, al.minPercent, al.experimentAncLayerId, al.expAncLayername,
             al.experimentId, al.matrixidx, bal.bucketId, bal.status, bal.statusmodtime
      FROM lm3.lm_anclayer al, lm3.BucketAncLayer bal 
      WHERE al.experimentAncLayerId = bal.experimentAncLayerId;

-- ----------------------------------------------------------------------------
-- lm_palayer 
DROP VIEW IF EXISTS lm3.lm_palayer CASCADE;
CREATE OR REPLACE VIEW lm3.lm_palayer (
   -- Layer.* (except geom)
   layerId,
   verify, 
   squid,
   lyruserId,
   layername,
   title,
   description,
   lyrdlocation,
   lyrmetadataurl,
   layerurl,
   ogrType,
   gdalType,
   dataFormat,
   epsgcode,
   mapunits,
   resolution,
   startDate,
   endDate,
   metalocation,
   lyrdatecreated,
   lyrdatelastmodified,
   bbox,
   -- PresenceAbsence.*
   presenceAbsenceId,
   pauserId,
   nameFilter, valueFilter,
   namePresence,
   minPresence,
   maxPresence,
   percentPresence,
   nameAbsence,
   minAbsence,
   maxAbsence,
   percentAbsence,
   -- ExperimentPALayer
   experimentPALayerId, expPALayername,
   experimentId, 
   matrixidx
   ) AS
      SELECT l.layerId, l.verify, l.squid, l.userId, l.layername, l.title, l.description, l.dlocation, 
             l.metadataurl, l.layerurl, l.ogrType, l.gdalType, l.dataFormat, 
             l.epsgcode, l.mapunits, 
             l.resolution, l.startDate, l.endDate, l.metalocation, 
             l.datecreated, l.datelastmodified, l.bbox, 
             pa.presenceAbsenceId, pa.userId, pa.nameFilter, pa.valueFilter,
             pa.namePresence, pa.minPresence, pa.maxPresence, pa.percentPresence, 
             pa.nameAbsence, pa.minAbsence, pa.maxAbsence, pa.percentAbsence,
             epl.experimentPALayerId, epl.expPALayername, epl.experimentId, epl.matrixidx
      FROM lm3.Layer l, lm3.PresenceAbsence pa, lm3.ExperimentPALayer epl
      WHERE l.layerId = epl.layerId 
        AND pa.presenceAbsenceId = epl.presenceAbsenceId;

-- ----------------------------------------------------------------------------
-- lm_bktpalayer 
DROP VIEW IF EXISTS lm3.lm_bktpalayer CASCADE;
CREATE OR REPLACE VIEW lm3.lm_bktpalayer (
   -- lm_palayer.*
   layerId,
   verify, 
   squid,
   lyruserId,
   layername,
   title,
   description,
   lyrdlocation,
   lyrmetadataurl,
   layerurl,
   ogrType,
   gdalType,
   dataFormat,
   epsgcode,
   mapunits,
   resolution,
   startDate,
   endDate,
   metalocation,
   lyrdatecreated,
   lyrdatelastmodified,
   bbox,
   presenceAbsenceId,
   pauserId,
   nameFilter, valueFilter,
   namePresence,
   minPresence,
   maxPresence,
   percentPresence,
   nameAbsence,
   minAbsence,
   maxAbsence,
   percentAbsence,
   experimentPALayerId, expPALayername,
   experimentId, 
   matrixidx, 
   bucketId,
   status,
   statusmodtime
   ) AS
      SELECT pal.layerId, pal.verify, pal.squid, pal.lyruserId, pal.layername, pal.title, pal.description, 
             pal.lyrdlocation, pal.lyrmetadataurl, pal.layerurl, pal.ogrType, 
             pal.gdalType, pal.dataFormat, pal.epsgcode, pal.mapunits, pal.resolution, 
             pal.startDate, pal.endDate, pal.metalocation, pal.lyrdatecreated, 
             pal.lyrdatelastmodified, pal.bbox, pal.presenceAbsenceId, pal.pauserId, 
             pal.nameFilter, pal.valueFilter, pal.namePresence, pal.minPresence, 
             pal.maxPresence, pal.percentPresence, pal.nameAbsence, pal.minAbsence, 
             pal.maxAbsence, pal.percentAbsence, pal.experimentPALayerId, 
             pal.expPALayername, pal.experimentId, pal.matrixidx, bpal.bucketId,
             bpal.status, bpal.statusmodtime
      FROM lm3.lm_palayer pal, lm3.BucketPALayer bpal
      WHERE pal.experimentPALayerId = bpal.experimentPALayerId;
     
-- ----------------------------------------------------------------------------
-- lm_shpjob
DROP VIEW IF EXISTS lm3.lm_grdjob CASCADE;
CREATE OR REPLACE VIEW lm3.lm_grdjob (
   -- lm_shapegrid
   layerId,
   verify,
   lyruserId,
   layername,
   lyrdlocation,
   lyrmetadataurl,
   ogrType,
   dataFormat,
   epsgcode,
   mapunits,
   bbox,
   shapeGridId, 
   cellsides,
   cellsize,
   vsize,
   idAttribute,
   xAttribute,
   yAttribute,
   shpstatus,
   shpstatusModTime,
   -- lmjob
   lmJobId,
   jobFamily,
   referenceType,
   referenceId,
   computeResourceId,
   priority,
   progress,
   status,
   statusmodtime,
   stage,
   stagemodtime,
   doNotify,
   reqData,
   reqSoftware,
   datecreated,
   lastheartbeat,
   retryCount)
   AS SELECT s.layerId, s.verify, s.lyruserId, s.layername, s.lyrdlocation, s.lyrmetadataurl, 
             s.ogrType, s.dataFormat, s.epsgcode, s.mapunits, s.bbox, 
             s.shapeGridId, s.cellsides, s.cellsize, s.vsize, s.idAttribute,
             s.xAttribute, s.yAttribute, s.shpstatus, s.shpstatusmodtime,
             j.lmJobId, j.jobFamily, j.referenceType, j.referenceId, 
             j.computeResourceId, j.priority, j.progress, 
             j.status, j.statusmodtime, j.stage, j.stagemodtime, 
             j.donotify, j.reqdata, j.reqsoftware, j.datecreated, 
             j.lastheartbeat, j.retrycount
      FROM lm3.lm_shapegrid s, lm3.lmjob j
      -- referenceType == LmServer.common.lmconstants.ReferenceType
      -- reqSoftware == LmCommon.common.lmconstants.ProcessType
      WHERE j.referenceType = 205 
        AND j.reqSoftware = 310 
        AND j.referenceId = s.shapeGridId;

-- ----------------------------------------------------------------------------
-- lm_intJob
-- Note: Shapegrid for INTERSECT; Layerset must be pulled separately
DROP VIEW IF EXISTS lm3.lm_intJob CASCADE;
CREATE OR REPLACE VIEW lm3.lm_intJob (
   -- experiment
   experimentId,
   userId,
   expname,
   email,
   epsgcode,   
   expmetadataurl,
   
   -- bucket
   bucketId,
   bktmetadataurl,
   bktstage,
   bktstatus,
   
   -- lm_shapegrid
   layerId,
   verify,
   layername,
   lyrdlocation,
   lyrmetadataurl,
   ogrType,
   dataFormat,
   mapunits,
   bbox,
   shapeGridId, 
   cellsides,
   cellsize,
   vsize,
   idAttribute,
   xAttribute,
   yAttribute,
   shpstatus,
   shpstatusModTime,
   
   -- lmjob
   lmJobId,
   jobFamily,
   referenceType,
   referenceId,
   computeResourceId,
   priority,
   progress,
   status,
   statusmodtime,
   stage,
   stagemodtime,
   doNotify,
   reqData,
   reqSoftware,
   datecreated,
   lastheartbeat,
   retryCount)
   AS SELECT e.experimentId, e.userId, e.expname, e.email, e.epsgcode, 
             e.metadataurl, b.bucketId, b.metadataurl, b.stage, b.status, 
             s.layerId, s.verify, s.layername, s.lyrdlocation, s.lyrmetadataurl, s.ogrType,
             s.dataFormat,  s.mapunits, s.bbox, s.shapeGridId, 
             s.cellsides, s.cellsize, s.vsize, s.idAttribute, s.xAttribute, s.yAttribute,
             s.shpstatus, s.shpstatusmodtime,
             j.lmJobId, j.jobFamily, j.referenceType, j.referenceId, 
             j.computeResourceId, j.priority, j.progress, 
             j.status, j.statusmodtime, j.stage, j.stagemodtime, 
             j.donotify, j.reqdata, j.reqsoftware, j.datecreated, 
             j.lastheartbeat, j.retrycount
   FROM lm3.experiment e, lm3.bucket b, lm3.lm_shapegrid s, lm3.lmjob j
   WHERE j.referenceType = 202 AND j.reqSoftware = 310
     AND j.referenceId = b.bucketId AND b.experimentId = e.experimentId
     AND b.shapegridId = s.shapegridId;
   
-- ----------------------------------------------------------------------------
-- lm_mtxJob
-- Note: Original or Splotch PAM for COMPRESS or Original PAM for SPLOTCH; 
--       Compressed Original or Random PAM for CALCULATE or Compressed Original for SWAP
DROP VIEW IF EXISTS lm3.lm_mtxJob CASCADE;
CREATE OR REPLACE VIEW lm3.lm_mtxJob (
   -- experiment
   experimentId,
   userId,
   expname, 
   email, 
   epsgcode,
   expmetadataurl,
   
   -- bucket.fullPam (full)
   bucketId,
   bktmetadataurl,
   bktstage,
   bktstatus,
   fullPamDlocation,
   
      -- lm_shapegrid
   layerId,
   verify,
   layername,
   lyrdlocation,
   lyrmetadataurl,
   ogrType,
   dataFormat,
   mapunits,
   bbox,
   shapeGridId, 
   cellsides,
   cellsize,
   vsize,
   idAttribute,
   xAttribute,
   yAttribute,
   shpstatus,
   shpstatusModTime,
   
   -- ops.pam (original, compressed, could be the same as pamsum)
   opsPamsumId,
   opsPamDlocation,
   opsstage,
   opsstatus,
   
   -- pamsum (reference object, pamsumId = referenceId)
   pamsumId,
   randomMethod,
   randomParams,
   psmetadataurl,
   -- pamsum.pam (compressed)
   psPamDlocation,
   -- pamsum.splotch (full)
   splotchPamDlocation,
   psstage,
   psstatus,
   
   -- lmjob
   lmJobId,
   jobFamily,
   referenceType,
   referenceId,
   computeResourceId,
   priority,
   progress,
   status,
   statusmodtime,
   stage,
   stagemodtime,
   doNotify,
   reqData,
   reqSoftware,
   datecreated,
   lastheartbeat,
   retryCount)
   AS SELECT e.experimentId, e.userId, e.expname, e.email, e.epsgcode, 
             e.metadataurl, 
             b.bucketId, b.metadataurl, b.stage, b.status, b.pamDlocation,  
             s.layerId, s.verify, s.layername, s.lyrdlocation, s.lyrmetadataurl, s.ogrType,
             s.dataFormat,  s.mapunits, s.bbox, s.shapeGridId, 
             s.cellsides, s.cellsize, s.vsize, s.idAttribute, s.xAttribute, s.yAttribute,
             s.shpstatus, s.shpstatusModTime,
             ops.pamsumid, ops.pamDlocation, ops.stage, ops.status, 
             ps.pamsumId, ps.randomMethod, ps.randomParams, ps.metadataurl, ps.pamDlocation, 
             ps.splotchPamDlocation, ps.stage, ps.status,
             j.lmJobId, j.jobFamily, j.referenceType, j.referenceId, 
             j.computeResourceId, j.priority, j.progress, 
             j.status, j.statusmodtime, j.stage, j.stagemodtime, 
             j.donotify, j.reqdata, j.reqsoftware, j.datecreated, 
             j.lastheartbeat, j.retrycount
   FROM lm3.experiment e, lm3.bucket b, lm3.lm_shapegrid s, lm3.pamsum ops, lm3.pamsum ps, lm3.lmjob j 
   WHERE (j.referenceType = 203 OR j.referenceType = 204) 
     AND j.referenceId = ps.pamsumId 
     AND ops.bucketId = b.bucketId AND ops.randomMethod = 0 
     AND ps.bucketId = b.bucketId
     AND b.shapegridId = s.shapegridId
     AND b.experimentId = e.experimentId;
     
-- ----------------------------------------------------------------------------
-- lm_msgJob
-- Note: Notifications will be sent out when all requested jobs for a single 
-- bucket are complete
DROP VIEW IF EXISTS lm3.lm_msgJob CASCADE;
CREATE OR REPLACE VIEW lm3.lm_msgJob (
   -- experiment
   experimentId,
   userId,
   expname, 
   email, 
   epsgcode,
   expmetadataurl,
   
   -- bucket
   bucketId,
   bktmetadataurl,
   bktstage,
   bktstatus,
   
   -- lmjob
   lmJobId,
   jobFamily,
   referenceType,
   referenceId,
   computeResourceId,
   priority,
   progress,
   status,
   statusmodtime,
   stage,
   stagemodtime,
   doNotify,
   reqData,
   reqSoftware,
   datecreated,
   lastheartbeat,
   retryCount)
   AS SELECT e.experimentId, e.userId, e.expname, e.email, e.epsgcode, 
             e.metadataurl, b.bucketid, b.metadataurl, b.stage, b.status,
             j.lmJobId, j.jobFamily, j.referenceType, j.referenceId, 
             j.computeResourceId, j.priority, j.progress, 
             j.status, j.statusmodtime, j.stage, j.stagemodtime, 
             j.donotify, j.reqdata, j.reqsoftware, j.datecreated, 
             j.lastheartbeat, j.retrycount
   FROM lm3.experiment e, lm3.bucket b, lm3.lmjob j 
   WHERE j.referenceType = 202 AND j.reqSoftware = 510
     AND j.referenceId = b.bucketId
     AND b.experimentId = e.experimentId
     AND j.donotify is True;

-- ----------------------------------------------------------------------------
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
-- ----------------------------------------------------------------------------
GRANT SELECT ON TABLE 
lm3.lm_pamsum, lm3.lm_fullradbucket, 
lm3.lm_palayer, lm3.lm_anclayer, lm3.lm_shapegrid,
lm3.lm_intJob, lm3.lm_mtxJob, lm3.lm_msgJob, lm3.lm_bloat
TO GROUP reader;

GRANT SELECT ON TABLE 
lm3.lm_pamsum, lm3.lm_fullradbucket, 
lm3.lm_palayer, lm3.lm_anclayer, lm3.lm_shapegrid,
lm3.lm_intJob, lm3.lm_mtxJob, lm3.lm_msgJob, lm3.lm_bloat
TO GROUP writer;


-- ----------------------------------------------------------------------------
-- lm_palayeridx OR lm_anclayeridx
DROP TYPE IF EXISTS lm3.lm_layeridx CASCADE;
CREATE TYPE lm3.lm_layeridx AS (
   -- Layer
   layerid int,
   verify varchar,
   squid varchar,
   lyruserid varchar,
   layername varchar,
   metadataurl varchar,
   layerurl varchar,
   -- ExperimentPALayer OR ExperimentAncLayer
   paramid int,
   matrixidx int, 
   experimentid int);

-- ----------------------------------------------------------------------------
-- lm_atom returns only an a few object attributes
DROP TYPE IF EXISTS lm3.lm_atom CASCADE;
CREATE TYPE lm3.lm_atom AS (
  id int,
  title varchar,
  epsgcode int,
  description text,
  modtime double precision
  );

-- ----------------------------------------------------------------------------
-- ----------------------------------------------------------------------------
GRANT SELECT ON TABLE 
lm3.lm_pamsum, lm3.lm_fullradbucket, 
lm3.lm_palayer, lm3.lm_anclayer, lm3.lm_shapegrid,
lm3.lm_bktanclayer, lm_bktpalayer,
lm3.lm_intJob, lm3.lm_mtxJob, lm3.lm_msgJob, lm3.lm_grdjob
TO GROUP reader;

GRANT SELECT ON TABLE 
lm3.lm_pamsum, lm3.lm_fullradbucket, 
lm3.lm_palayer, lm3.lm_anclayer, lm3.lm_shapegrid,
lm3.lm_bktanclayer, lm_bktpalayer,
lm3.lm_intJob, lm3.lm_mtxJob, lm3.lm_msgJob, lm3.lm_grdjob
TO GROUP writer;

GRANT UPDATE ON TABLE 
lm3.lm_intJob, lm3.lm_mtxJob, lm3.lm_msgJob, lm3.lm_grdjob
TO GROUP writer;
-- ----------------------------------------------------------------------------
-- ----------------------------------------------------------------------------
