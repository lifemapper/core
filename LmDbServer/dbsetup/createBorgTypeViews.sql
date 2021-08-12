-- ----------------------------------------------------------------------------
-- @license: gpl2
-- @copyright: Copyright (C) 2017, University of Kansas Center for Research
 
--           Lifemapper Project, lifemapper [at] ku [dot] edu, 
--           Biodiversity Institute,
--           1345 Jayhawk Boulevard, Lawrence, Kansas, 66045, USA
--     
--           This program is free software; you can redistribute it and/or  
--           modify it under the terms of the GNU General Public License as 
--           published by the Free Software Foundation; either version 2 of the 
--           License, or (at your option) any later version.
--    
--           This program is distributed in the hope that it will be useful, but 
--           WITHOUT ANY WARRANTY; without even the implied warranty of 
--           MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU 
--           General Public License for more details.
--    
--           You should have received a copy of the GNU General Public License 
--           along with this program; if not, write to the Free Software 
--           Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 
--           02110-1301, USA.
-- ----------------------------------------------------------------------------
-- ----------------------------------------------------------------------------
\c borg
-- ----------------------------------------------------------------------------
-- ----------------------------------------------------------------------------
-- DROP everything first so don't have to ensure objects are created in order
DROP VIEW IF EXISTS lm_v3.lm_envlayer CASCADE;
DROP VIEW IF EXISTS lm_v3.lm_scenlayer CASCADE;
DROP VIEW IF EXISTS lm_v3.lm_scenPackageScenario CASCADE;
DROP VIEW IF EXISTS lm_v3.lm_shapegrid CASCADE;
DROP VIEW IF EXISTS lm_v3.lm_gridset CASCADE;
DROP VIEW IF EXISTS lm_v3.lm_gridset_tree CASCADE;
DROP VIEW IF EXISTS lm_v3.lm_fullmatrix CASCADE;
DROP VIEW IF EXISTS lm_v3.lm_matrix CASCADE;
DROP VIEW IF EXISTS lm_v3.lm_sdmproject CASCADE;
DROP VIEW IF EXISTS lm_v3.lm_occurrenceset CASCADE;
DROP VIEW IF EXISTS lm_v3.lm_matrixcolumn CASCADE;
DROP VIEW IF EXISTS lm_v3.lm_occMatrixcolumn CASCADE;
DROP VIEW IF EXISTS lm_v3.lm_sdmMatrixcolumn CASCADE;
DROP VIEW IF EXISTS lm_v3.lm_sdmMatrixcolumn_matrix CASCADE;
DROP VIEW IF EXISTS lm_v3.lm_lyrMatrixcolumn CASCADE;
DROP VIEW IF EXISTS lm_v3.lm_mfprocess CASCADE;


-- ----------------------------------------------------------------------------
-- ----------------------------------------------------------------------------
-- lm_envlayer (Layer + EnvType)
CREATE OR REPLACE VIEW lm_v3.lm_envlayer (
   -- Layer.* 
   layerId,
   userid,
   lyrsquid,
   lyrverify,
   lyrname,
   lyrdlocation,
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
             l.metadata, l.dataFormat, l.gdalType, l.ogrType, 
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
-- lm_scenlayer (Scenario + lm_envlayer)
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
             lel.lyrdlocation, lel.lyrmetadata, lel.dataFormat, 
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

-- -------------------------------
-- lm_scenPackageScenario (ScenPackage + Scenario)
CREATE OR REPLACE VIEW lm_v3.lm_scenPackageScenario (
   -- Scenario.*
    scenarioId,
    userid,
    scenarioCode,
    scenmetadata,
    gcmCode,
    altpredCode,
    dateCode,
    scenunits,
    resolution,
    scenepsgcode,
    scenbbox,
    scenmodTime,
    -- ScenPackage.*
    scenPackageId,
    pkgname,
    pkgmetadata,
    pkgunits,
    pkgepsgcode,
    pkgbbox,
    pkgmodTime
   ) AS
      SELECT s.scenarioId, s.userid, s.scenarioCode, s.metadata, 
      s.gcmCode, s.altpredCode, s.dateCode, s.units, s.resolution, s.epsgcode, 
      s.bbox, s.modTime,
      p.scenPackageId, p.name, p.metadata, p.units, 
      p.epsgcode, p.bbox, p.modTime
        FROM lm_v3.Scenario s, lm_v3.ScenPackage p, lm_v3.ScenPackageScenario sps
        WHERE sps.scenarioId = s.scenarioId
          AND sps.scenPackageId = p.scenPackageId
        ORDER BY s.modTime ASC;

-- ----------------------------------------------------------------------------
-- lm_shapegrid (ShapeGrid + Layer)
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
             l.metadata, l.dataFormat, l.gdalType, l.ogrType, 
             l.valUnits, l.valAttribute, l.nodataVal, l.minVal, l.maxVal, 
             l.epsgcode, l.mapunits, l.resolution, l.bbox, l.modTime
        FROM lm_v3.layer l, lm_v3.shapegrid sg
        WHERE l.layerid = sg.layerid;


-- ----------------------------------------------------------------------------
-- ----------------
-- lm_gridset  (Gridset + lm_shapegrid)
CREATE OR REPLACE VIEW lm_v3.lm_gridset (
   gridsetId,
   userId,
   grdname,
   treeId,
   layerId,
   grddlocation,
   grdepsgcode,
   grdmetadata,
   grdmodTime,
   -- lm_shapegrid.*
   cellsides,
   cellsize,
   vsize,
   idAttribute,
   xAttribute,
   yAttribute,
   shpgrdstatus,
   shpgrdstatusmodtime,
   lyrsquid,
   lyrverify,
   lyrname,
   lyrdlocation,
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
   lyrmodtime) AS
   SELECT g.gridsetId, g.userId, g.name, g.treeId, g.layerId, 
          g.dlocation, g.epsgcode, g.metadata, g.modTime,
          lsg.cellsides, lsg.cellsize, lsg.vsize, lsg.idAttribute,
          lsg.xAttribute, lsg.yAttribute, lsg.shpgrdstatus, 
          lsg.shpgrdstatusmodtime, lsg.lyrsquid, lsg.lyrverify, lsg.lyrname, 
          lsg.lyrdlocation, lsg.lyrmetadata, lsg.dataFormat, 
          lsg.gdalType, lsg.ogrType, lsg.valUnits, lsg.valAttribute, 
          lsg.nodataVal, lsg.minVal, lsg.maxVal, lsg.epsgcode, lsg.mapunits, 
          lsg.resolution, lsg.bbox, lsg.lyrmodtime
   FROM lm_v3.gridset g
   LEFT JOIN lm_v3.lm_shapegrid lsg ON g.layerid = lsg.layerid; 

-- ----------------------------------------------------------------------------
-- ----------------
-- lm_gridset_tree  (lm_gridset + tree)
CREATE OR REPLACE VIEW lm_v3.lm_gridset_tree (
   -- lm_gridset.*
   gridsetId,
   userId,
   grdname,
   treeId,
   layerId,
   grddlocation,
   grdepsgcode,
   grdmetadata,
   grdmodTime,
   cellsides,
   cellsize,
   vsize,
   idAttribute,
   xAttribute,
   yAttribute,
   shpgrdstatus,
   shpgrdstatusmodtime,
   lyrsquid,
   lyrverify,
   lyrname,
   lyrdlocation,
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
   -- Tree
   treename,
   treedlocation,
   isBinary,
   isUltrametric,
   hasBranchLengths,
   treemetadata,
   treemodTime
) AS
   SELECT lg.gridsetId, lg.userId, lg.grdname, lg.treeId, lg.layerId, 
          lg.grddlocation, lg.grdepsgcode, lg.grdmetadata, lg.grdmodTime, 
          lg.cellsides, lg.cellsize, lg.vsize, lg.idAttribute, lg.xAttribute, 
          lg.yAttribute, lg.shpgrdstatus, lg.shpgrdstatusmodtime, lg.lyrsquid, 
          lg.lyrverify, lg.lyrname, lg.lyrdlocation, lg.lyrmetadata, 
          lg.dataFormat, lg.gdalType, lg.ogrType, lg.valUnits, lg.valAttribute, 
          lg.nodataVal, lg.minVal, lg.maxVal, lg.epsgcode, lg.mapunits, 
          lg.resolution, lg.bbox, lg.lyrmodtime,
          t.name, t.dlocation, t.isBinary, t.isUltrametric, 
          t.hasBranchLengths, t.metadata, t.modTime
   FROM lm_v3.lm_gridset lg
   LEFT JOIN lm_v3.tree t ON lg.treeid = t.treeid; 

-- ----------------
-- lm_fullmatrix (Matrix + lm_gridset (Gridset + lm_shapegrid))
CREATE OR REPLACE VIEW lm_v3.lm_fullmatrix (
   -- Matrix
   matrixId,
   matrixType,
   gridsetId,
   -- TODO: replace 3 Codes with scenarioId
   -- scenarioId,
   gcmCode,
   altpredCode,
   dateCode,
   algorithmCode,
   matrixDlocation,
   metadata,
   status,
   statusmodtime, 
   -- lm_gridset
   userId,
   grdname,
   layerId,
   grddlocation,
   grdepsgcode,
   grdmetadata,
   grdmodTime,
   cellsides,
   cellsize,
   vsize,
   idAttribute,
   xAttribute,
   yAttribute,
   shpgrdstatus,
   shpgrdstatusmodtime,
   lyrsquid,
   lyrverify,
   lyrname,
   lyrdlocation,
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
   lyrmodtime) AS
   SELECT m.matrixId, m.matrixType, m.gridsetId, 
         -- TODO: replace 3 Codes with scenarioID
         -- m.scenarioid,
         m.gcmCode, m.altpredCode, m.dateCode, 
         m.algorithmCode, m.matrixDlocation, m.metadata, m.status, 
   		 m.statusmodtime, 
          g.userId, g.grdname, g.layerId, 
          g.grddlocation, g.grdepsgcode, g.grdmetadata, 
          g.grdmodTime, g.cellsides, g.cellsize, g.vsize, g.idAttribute, 
          g.xAttribute, g.yAttribute, g.shpgrdstatus, 
          g.shpgrdstatusmodtime, g.lyrsquid, g.lyrverify, g.lyrname, 
          g.lyrdlocation, g.lyrmetadata, g.dataFormat, 
          g.gdalType, g.ogrType, g.valUnits, g.valAttribute, g.nodataVal, 
          g.minVal, g.maxVal, g.epsgcode, g.mapunits, g.resolution, g.bbox, 
          g.lyrmodtime
   FROM lm_v3.matrix m, lm_v3.lm_gridset g
   WHERE m.gridsetid = g.gridsetid;


-- ----------------
-- lm_matrix (Matrix + Gridset)
CREATE OR REPLACE VIEW lm_v3.lm_matrix (
   -- Matrix
   matrixId,
   matrixType,
   gridsetId,
   -- TODO: replace 3 Codes with scenarioId
   -- scenarioId,
   gcmCode,
   altpredCode,
   dateCode,
   algorithmCode,
   matrixDlocation,
   metadata,
   status,
   statusmodtime, 
   -- Gridset
   userId,
   grdname,
   layerId,
   grddlocation,
   grdepsgcode,
   grdmetadata,
   grdmodTime) AS
   SELECT m.matrixId, m.matrixType, m.gridsetId, 
         -- TODO: replace 3 Codes with scenarioId
         -- m.scenarioId,
          m.gcmCode, m.altpredCode, m.dateCode, 
          m.algorithmCode, m.matrixDlocation, m.metadata, m.status, 
          m.statusmodtime, 
          g.userId, g.name, g.layerId, 
          g.dlocation, g.epsgcode, g.metadata, g.modTime
   FROM lm_v3.matrix m, lm_v3.gridset g
   WHERE m.gridsetid = g.gridsetid;

-- ----------------
-- lm_sdmproject (SDMProject + Layer + Occurrenceset + Model scenarioCode 
--                       + Project scenarioCode, gcmCode, altPredCode, dateCode)
CREATE OR REPLACE VIEW lm_v3.lm_sdmproject (
   -- sdmproject.*
   sdmprojectid,
   layerid,
   userid,
   occurrenceSetId,
   algorithmCode,
   algParams,
   mdlscenarioId,
   prjscenarioId,
   prjmetadata,
   prjstatus,
   prjstatusModTime,
   
   -- Layer.* 
   squid,
   lyrverify,
   name,
   lyrdlocation,
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
             p.algorithmCode, p.algParams, 
             p.mdlscenarioId, p.prjscenarioId, 
             p.metadata, p.status, p.statusModTime,
             l.squid, l.verify, l.name, l.dlocation, l.metadata, 
             l.dataFormat, l.gdalType, l.ogrType, l.valUnits, l.valAttribute, 
             l.nodataVal, l.minVal, l.maxVal, 
             l.epsgcode, l.mapunits, l.resolution, l.bbox, l.modTime,
             o.verify, o.displayName, o.dlocation, o.queryCount, 
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
-- lm_occurrenceset (Occurrenceset + Taxon + TaxonomySource) 
CREATE OR REPLACE VIEW lm_v3.lm_occurrenceset (
   -- occurrenceset.*
   occurrenceSetId,
   userId,
   squid,
   verify,
   displayName,
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
          o.dlocation, o.rawDlocation, o.queryCount, 
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
-- lm_matrixcolumn (MatrixColumn + Matrix + Gridset user/shapegrid ids)
CREATE OR REPLACE VIEW lm_v3.lm_matrixcolumn
(
   -- MatrixColumn.*
   matrixColumnId,
   matrixId,
   matrixIndex,
   squid,
   ident,
   mtxcolmetadata, 
   layerId,
   intersectParams,
   mtxcolstatus,
   mtxcolstatusmodtime,
   
   -- Matrix.*
   matrixType,
   gridsetId,
   -- TODO: replace 3 Codes with scenarioId
   -- scenarioId,
   gcmCode,
   altpredCode,
   dateCode,
   algorithmCode,
   matrixDlocation,
   mtxmetadata,
   mtxstatus,
   mtxstatusmodtime,
   
   -- Gridset userid and shapegrid-layerid
   userid, 
   shplayerid
) AS 
      SELECT mc.matrixColumnId, mc.matrixId, mc.matrixIndex, 
             mc.squid, mc.ident, mc.metadata, mc.layerId,
             mc.intersectParams, mc.status, mc.statusmodtime,
             m.matrixType, m.gridsetId, m.gcmCode, m.altpredCode, m.dateCode, 
             m.algorithmCode, m.matrixDlocation, m.metadata, m.status, 
             m.statusmodtime,
             g.userid, g.layerid
        FROM lm_v3.MatrixColumn mc, lm_v3.Matrix m, lm_v3.Gridset g
        WHERE mc.matrixId = m.matrixId AND m.gridsetid = g.gridsetid;

-- ----------------------------------------------------------------------------
-- lm_v3.lm_occMatrixcolumn (MatrixColumn + Matrix + SDMProject + OccurrenceSet)
CREATE OR REPLACE VIEW lm_v3.lm_occMatrixcolumn (
   -- MatrixColumn.*
   matrixColumnId,
   matrixId,
   matrixIndex,
   mtxcolsquid,
   mtxcolident,
   mtxcolmetadata, 
   layerId,
   intersectParams,
   mtxcolstatus,
   mtxcolstatusmodtime,
   
   -- Matrix.*
   matrixType,
   gridsetId,
   gcmCode,
   altpredCode,
   dateCode,
   --use algorithmCode from sdmproject 
   matrixDlocation,
   mtxmetadata,
   mtxstatus,
   mtxstatusmodtime,
   
   -- SDMProject.*
   sdmprojectid,
   userid,
   occurrenceSetId,
   algorithmCode,
   algParams,
   mdlscenarioId,
   prjscenarioId,
   prjmetadata,
   prjstatus,
   prjstatusModTime,
   
   -- OccurrenceSet.* 
   occsquid,
   occverify,
   displayName,
   occdlocation,
   queryCount,
   occbbox,
   epsgcode,
   occmetadata,
   occstatus,
   occstatusModTime
   ) AS
      SELECT mc.matrixColumnId, mc.matrixId, mc.matrixIndex, 
             mc.squid, mc.ident, mc.metadata, mc.layerId,
             mc.intersectParams, mc.status, mc.statusmodtime,
             m.matrixType, m.gridsetId, m.gcmCode, m.altpredCode, m.dateCode, 
             m.matrixDlocation, m.metadata, m.status, 
             m.statusmodtime,
             p.sdmprojectid, p.userid, p.occurrenceSetId, 
             p.algorithmCode, p.algParams, 
             p.mdlscenarioId, p.prjscenarioId, 
             p.metadata, p.status, p.statusModTime,
             o.squid, o.verify, o.displayName, o.dlocation, o.queryCount, 
             o.bbox, o.epsgcode, o.metadata, o.status, o.statusModTime
        FROM lm_v3.MatrixColumn mc, lm_v3.Matrix m, lm_v3.sdmproject p, 
             lm_v3.occurrenceSet o
        WHERE mc.matrixId = m.matrixId AND 
              mc.layerid = p.layerid AND
              p.occurrencesetid = o.occurrencesetid;

-- ----------------------------------------------------------------------------
-- lm_sdmMatrixcolumn (MatrixColumn + SDMProject + Layer)
-- ----------------
CREATE OR REPLACE VIEW lm_v3.lm_sdmMatrixcolumn
(
   -- MatrixColumn.*
   matrixColumnId,
   matrixId,
   matrixIndex,
   mtxcolsquid,
   mtxcolident,
   mtxcolmetadata, 
   layerId,
   intersectParams,
   mtxcolstatus,
   mtxcolstatusmodtime,

   -- sdmproject.*
   sdmprojectid,
   userid,
   occurrenceSetId,
   algorithmCode,
   algParams,
   mdlscenarioId,
   prjscenarioId,
   prjmetadata,
   prjstatus,
   prjstatusModTime,
   
   -- Layer.* 
   lyrverify,
   lyrname,
   lyrdlocation,
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
   lyrmodtime) AS
      SELECT mc.matrixColumnId, mc.matrixId, mc.matrixIndex, 
             mc.squid, mc.ident, mc.metadata, mc.layerId,
             mc.intersectParams, mc.status, mc.statusmodtime,
             p.sdmprojectid, p.userid, p.occurrenceSetId, 
             p.algorithmCode, p.algParams, 
             p.mdlscenarioId, p.prjscenarioId, 
             p.metadata, p.status, p.statusModTime,
             l.verify, l.name, l.dlocation, l.metadata, 
             l.dataFormat, l.gdalType, l.ogrType, l.valUnits, l.valAttribute, 
             l.nodataVal, l.minVal, l.maxVal, 
             l.epsgcode, l.mapunits, l.resolution, l.bbox, l.modTime
        FROM lm_v3.MatrixColumn mc, lm_v3.sdmproject p, lm_v3.Layer l
        WHERE mc.layerid = p.layerid 
          AND mc.layerid = l.layerid;

-- ----------------
-- lm_sdmMatrixcolumn_matrix (lm_sdmMatrixcolumn + Matrix)
CREATE OR REPLACE VIEW lm_v3.lm_sdmMatrixcolumn_matrix (
   -- lm_sdmMatrixcolumn.*
   matrixColumnId,
   matrixId,
   matrixIndex,
   mtxcolsquid,
   mtxcolident,
   mtxcolmetadata, 
   layerId,
   intersectParams,
   mtxcolstatus,
   mtxcolstatusmodtime,
   sdmprojectid,
   userid,
   occurrenceSetId,
   algorithmCode,
   algParams,
   mdlscenarioId,
   prjscenarioId,
   prjmetadata,
   prjstatus,
   prjstatusModTime,
   lyrverify,
   lyrname,
   lyrdlocation,
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

   -- Matrix
   matrixType,
   gridsetId,
   gcmCode,
   altpredCode,
   dateCode,
   -- use algorithmCode above
   matrixDlocation,
   mtxmetadata,
   mtxstatus,
   mtxstatusmodtime
   ) AS
   SELECT smc.matrixColumnId, smc.matrixId, smc.matrixIndex, smc.mtxcolsquid, 
      smc.mtxcolident, smc.mtxcolmetadata,  smc.layerId, smc.intersectParams, 
      smc.mtxcolstatus, smc.mtxcolstatusmodtime, smc.sdmprojectid, smc.userid, 
      smc.occurrenceSetId, smc.algorithmCode, smc.algParams, smc.mdlscenarioId, 
      smc.prjscenarioId, smc.prjmetadata, 
      smc.prjstatus, smc.prjstatusModTime, smc.lyrverify, smc.lyrname, 
      smc.lyrdlocation, smc.lyrmetadata, smc.dataFormat, smc.gdalType, smc.ogrType, 
      smc.valUnits, smc.valAttribute, smc.nodataVal, smc.minVal, smc.maxVal, 
      smc.epsgcode, smc.mapunits, smc.resolution, smc.lyrbbox, smc.lyrmodtime,
      m.matrixType, m.gridsetId, m.gcmCode, m.altpredCode, m.dateCode, 
      m.matrixDlocation, m.metadata, m.status, m.statusmodtime
      FROM lm_v3.lm_sdmMatrixcolumn smc, lm_v3.matrix m
      WHERE smc.matrixId = m.matrixId;

-- ----------------------------------------------------------------------------
-- lm_lyrMatrixcolumn (MatrixColumn + Layer)
CREATE OR REPLACE VIEW lm_v3.lm_lyrMatrixcolumn
(
   -- MatrixColumn.*
   matrixColumnId,
   matrixId,
   matrixIndex,
   mtxcolsquid,
   mtxcolident,
   mtxcolmetadata, 
   layerId,
   intersectParams,
   mtxcolstatus,
   mtxcolstatusmodtime,
   -- Layer
   userid,
   --squid,
   lyrverify,
   lyrname,
   lyrdlocation,
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
   lyrmodTime) AS
      SELECT mc.matrixColumnId, mc.matrixId, mc.matrixIndex, 
             mc.squid, mc.ident, mc.metadata, mc.layerId,
             mc.intersectParams, mc.status, mc.statusmodtime,
             l.userid, l.verify, l.name, l.dlocation, l.metadata, 
             l.dataFormat, l.gdalType, l.ogrType, l.valUnits, 
             l.valAttribute, l.nodataVal, l.minVal, l.maxVal, l.epsgcode,
             l.mapunits, l.resolution, l.bbox, l.modtime
        FROM lm_v3.MatrixColumn mc, lm_v3.Layer l
        WHERE mc.layerid = l.layerId;

-- ----------------------------------------------------------------------------
-- lm_mfprocess (MFProcess + Gridset)
CREATE OR REPLACE VIEW lm_v3.lm_mfprocess
(
   -- MFProces.*
   mfProcessId ,
   userid ,
   gridsetId ,
   mfpdlocation ,
   priority ,
   mfpmetadata ,
   mfpstatus ,
   mfpstatusmodtime ,

   -- Gridset.*
   grdname,
   treeId,
   layerId,
   grddlocation,
   grdepsgcode,
   grdmetadata,
   grdmodTime) AS
      SELECT mfp.mfProcessId, mfp.userid, mfp.gridsetId, mfp.dlocation,
             mfp.priority, mfp.metadata , mfp.status, mfp.statusmodtime,
             g.name, g.treeId, g.layerId, g.dlocation, g.epsgcode, 
             g.metadata, g.modTime
        FROM lm_v3.MFProcess mfp, lm_v3.Gridset g
        WHERE mfp.gridsetid = g.gridsetid;
-- ----------------------------------------------------------------------------
-- lm_taxon_solr
DROP VIEW IF EXISTS lm_v3.lm_taxon_solr CASCADE;
CREATE OR REPLACE VIEW lm_v3.lm_taxon_solr
(
	-- Solr fields for taxon information
	id, 
	taxon_source_id, 
	taxon_key, 
	squid, 
	taxon_kingdom, 
	taxon_phylum, 
	taxon_class, 
	taxon_order, 
	taxon_family, 
	taxon_genus, 
	taxon_rank, 
	canonical_name, 
	scientific_name) AS
		SELECT tax.taxonid, tax.taxonomysourceid, tax.taxonomykey, tax.squid, tax.kingdom, 
			   tax.phylum, tax.tx_class, tax.tx_order, tax.family, tax.genus, tax.rank, 
			   tax.canonical, tax.sciname 
		FROM lm_v3.Taxon tax;
		
-- ----------------------------------------------------------------------------
        
-- ----------------------------------------------------------------------------
-- lm_bloat
-- Shows bloated indicies
-- CREATE OR REPLACE VIEW lm_v3.lm_bloat AS
--       SELECT
--         schemaname, tablename, reltuples::bigint, relpages::bigint, otta,
--         ROUND(CASE WHEN otta=0 THEN 0.0 ELSE sml.relpages/otta::numeric END,1) AS tbloat,
--         relpages::bigint - otta AS wastedpages,
--         bs*(sml.relpages-otta)::bigint AS wastedbytes,
--         pg_size_pretty((bs*(relpages-otta))::bigint) AS wastedsize,
--         iname, ituples::bigint, ipages::bigint, iotta,
--         ROUND(CASE WHEN iotta=0 OR ipages=0 THEN 0.0 ELSE ipages/iotta::numeric END,1) AS ibloat,
--         CASE WHEN ipages < iotta THEN 0 ELSE ipages::bigint - iotta END AS wastedipages,
--         CASE WHEN ipages < iotta THEN 0 ELSE bs*(ipages-iotta) END AS wastedibytes,
--         CASE WHEN ipages < iotta THEN pg_size_pretty(0) ELSE pg_size_pretty((bs*(ipages-iotta))::bigint) END AS wastedisize
--       FROM (
--         SELECT
--           schemaname, tablename, cc.reltuples, cc.relpages, bs,
--           CEIL((cc.reltuples*((datahdr+ma-
--             (CASE WHEN datahdr%ma=0 THEN ma ELSE datahdr%ma END))+nullhdr2+4))/(bs-20::float)) AS otta,
--           COALESCE(c2.relname,'?') AS iname, COALESCE(c2.reltuples,0) AS ituples, COALESCE(c2.relpages,0) AS ipages,
--           COALESCE(CEIL((c2.reltuples*(datahdr-12))/(bs-20::float)),0) AS iotta -- very rough approximation, assumes all cols
--         FROM (
--           SELECT
--             ma,bs,schemaname,tablename,
--             (datawidth+(hdr+ma-(case when hdr%ma=0 THEN ma ELSE hdr%ma END)))::numeric AS datahdr,
--             (maxfracsum*(nullhdr+ma-(case when nullhdr%ma=0 THEN ma ELSE nullhdr%ma END))) AS nullhdr2
--           FROM (
--             SELECT
--               schemaname, tablename, hdr, ma, bs,
--               SUM((1-null_frac)*avg_width) AS datawidth,
--               MAX(null_frac) AS maxfracsum,
--               hdr+(
--                 SELECT 1+count(*)/8
--                 FROM pg_stats s2
--                 WHERE null_frac<>0 AND s2.schemaname = s.schemaname AND s2.tablename = s.tablename
--               ) AS nullhdr
--             FROM pg_stats s, (
--               SELECT
--                 (SELECT current_setting('block_size')::numeric) AS bs,
--                 CASE WHEN substring(v,12,3) IN ('8.0','8.1','8.2') THEN 27 ELSE 23 END AS hdr,
--                 CASE WHEN v ~ 'mingw32' THEN 8 ELSE 4 END AS ma
--               FROM (SELECT version() AS v) AS foo
--             ) AS constants
--             GROUP BY 1,2,3,4,5
--           ) AS foo
--         ) AS rs
--         JOIN pg_class cc ON cc.relname = rs.tablename
--         JOIN pg_namespace nn ON cc.relnamespace = nn.oid AND nn.nspname = rs.schemaname
--         LEFT JOIN pg_index i ON indrelid = cc.oid
--         LEFT JOIN pg_class c2 ON c2.oid = i.indexrelid
--       ) AS sml
--       WHERE sml.relpages - otta > 0 OR ipages - iotta > 10
--       ORDER BY wastedbytes DESC, wastedibytes DESC;

 
-- ----------------------------------------------------------------------------
-- ----------------------------------------------------------------------------
-- ----------------------------------------------------------------------------
GRANT SELECT ON TABLE 
lm_v3.lm_envlayer,
lm_v3.lm_scenlayer,
lm_v3.lm_scenPackageScenario,
lm_v3.lm_shapegrid,
lm_v3.lm_gridset,
lm_v3.lm_gridset_tree,
lm_v3.lm_fullmatrix,
lm_v3.lm_matrix,
lm_v3.lm_sdmProject, 
lm_v3.lm_occurrenceset, 
lm_v3.lm_matrixcolumn,
lm_v3.lm_occMatrixcolumn,
lm_v3.lm_sdmMatrixcolumn,
lm_v3.lm_sdmMatrixcolumn_matrix,
lm_v3.lm_lyrMatrixcolumn,
lm_v3.lm_mfprocess
-- lm_v3.lm_bloat
TO GROUP reader;


GRANT SELECT ON TABLE 
lm_v3.lm_envlayer,
lm_v3.lm_scenlayer,
lm_v3.lm_scenPackageScenario,
lm_v3.lm_shapegrid,
lm_v3.lm_gridset,
lm_v3.lm_gridset_tree,
lm_v3.lm_fullmatrix,
lm_v3.lm_matrix,
lm_v3.lm_sdmProject, 
lm_v3.lm_occurrenceset, 
lm_v3.lm_matrixcolumn,
lm_v3.lm_occMatrixcolumn,
lm_v3.lm_sdmMatrixcolumn,
lm_v3.lm_sdmMatrixcolumn_matrix,
lm_v3.lm_lyrMatrixcolumn,
lm_v3.lm_mfprocess
-- lm_v3.lm_bloat
TO GROUP writer;


-- ----------------------------------------------------------------------------
-- ----------------------------------------------------------------------------

-- ----------------------------------------------------------------------------
-- DATA TYPES (used on multiple tables)
-- Note: All column names are returned in lower case
-- ----------------------------------------------------------------------------
DROP TYPE IF EXISTS lm_v3.lm_atom CASCADE;
DROP TYPE IF EXISTS lm_v3.lm_occStats CASCADE;
DROP TYPE IF EXISTS lm_v3.lm_progress CASCADE;
-- ----------------------------------------------------------------------------
-- lm_atom returns only an a few object attributes
CREATE TYPE lm_v3.lm_atom AS (
  id int,
  name varchar,
  epsgcode int,
  modtime double precision);

        
-- ----------------------------------------------------------------------------
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
CREATE TYPE lm_v3.lm_progress AS (
  status int,
  total int);
   
   