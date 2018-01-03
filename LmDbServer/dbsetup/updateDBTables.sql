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
-- ----------------------------------------------------------------------------
-- psql -U admin -d template1 --file=LmDbServer/dbsetup/updateDBTables.sql
-- ----------------------------------------------------------------------------
-- These functions will do NOTHING if changes have already been performed.
-- ----------------------------------------------------------------------------
-- ----------------------------------------------------------------------------
\c borg
ALTER TABLE lm_v3.SDMProject DROP COLUMN IF EXISTS mdlmaskId CASCADE;
ALTER TABLE lm_v3.SDMProject DROP COLUMN IF EXISTS prjmaskId CASCADE;
CREATE INDEX idx_layerid ON lm_v3.SDMProject(layerid);

DROP FUNCTION IF EXISTS lm_v3.lm_insertSDMProject(prjid int,
                                                           lyrid int,
                                                           usr varchar, 
                                                           occid int,
                                                           algcode varchar,
                                                           algstr text,
                                                           mdlscenid int,
                                                           mdlmskid int,
                                                           prjscenid int,
                                                           prjmskid int,
                                                           prjmeta text, 
                                                           ptype int, 
                                                           stat int, 
                                                           stattime double precision);
DROP FUNCTION IF EXISTS lm_v3.lm_findOrInsertSDMProjectLayer(prjid int, 
                                          lyrid int,
                                          usr varchar,
                                          lyrsquid varchar,
                                          lyrverify varchar,
                                          lyrname varchar, 
                                          lyrdloc varchar,
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
                                          -- sdmproject
                                          occid int,
                                          algcode varchar,
                                          algstr text,
                                          mdlscenid int,
                                          mdlmskid int,
                                          prjscenid int,
                                          prjmskid int,
                                          prjmeta text,
                                          ptype int,
                                          stat int,
                                          stattime double precision);


-- -------------------------------
