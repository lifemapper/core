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



-- \c borg
DROP FUNCTION lm_v3.lm_findOrInsertMatrix(mtxid int, mtxtype int,
                                                       grdid int,
                                                       gcm varchar,
                                                       altpred varchar,
                                                       dt varchar,
                                                       dloc text,
                                                       meta varchar, 
                                                       stat int,
                                                   	 stattime double precision);
                                                   	 
DROP  FUNCTION lm_v3.lm_getMatrix(mtxid int, mtxtype int, 
                                              gsid int,
                                              gcm varchar,
                                              altpred varchar,
                                              dt varchar,
                                              gsname varchar,
                                              usr varchar); 
                                              
DROP  FUNCTION lm_v3.lm_getFilterMtx(usr varchar, mtxtype int,
                                                    gcm varchar,
                                                    altpred varchar,
                                                    tm varchar,
                                                    meta varchar, 
                                                    grdid int,
                                                    aftertime double precision,
                                                    beforetime double precision,
                                                    epsg int,
                                                    afterstat int,
                                                    beforestat int); 
                                                    
DROP  FUNCTION lm_v3.lm_countMatrices(usr varchar, mtxtype int,
                                                    gcm varchar,
                                                    altpred varchar,
                                                    tm varchar,
                                                    meta varchar, 
                                                    grdid int,
                                                    aftertime double precision,
                                                    beforetime double precision,
                                                    epsg int,
                                                    afterstat int,
                                                    beforestat int);
                                                    
DROP  FUNCTION lm_v3.lm_listMatrixAtoms(firstRecNum int, maxNum int, 
                                                    usr varchar,
                                                    mtxtype int,
                                                    gcm varchar,
                                                    altpred varchar,
                                                    tm varchar,
                                                    meta varchar, 
                                                    grdid int,
                                                    aftertime double precision,
                                                    beforetime double precision,
                                                    epsg int,
                                                    afterstat int,
                                                    beforestat int); 
                                                    
DROP FUNCTION lm_v3.lm_listMatrixObjects(firstRecNum int, maxNum int, 
                                                    usr varchar,
                                                    mtxtype int,
                                                    gcm varchar,
                                                    altpred varchar,
                                                    tm varchar,
                                                    meta varchar, 
                                                    grdid int,
                                                    aftertime double precision,
                                                    beforetime double precision,
                                                    epsg int,
                                                    afterstat int,
                                                    beforestat int);
                                                    
ALTER TABLE lm_v3.Matrix DROP CONSTRAINT IF EXISTS matrix_gridsetid_matrixtype_gcmcode_altpredcode_datecode_key;
ALTER TABLE lm_v3.Matrix ADD COLUMN algorithmCode varchar(30) REFERENCES lm_v3.Algorithm(algorithmCode);
ALTER TABLE lm_v3.Matrix ADD UNIQUE (gridsetId, matrixType, gcmCode, altpredCode, dateCode, algorithmCode);

ALTER TABLE lm_v3.MatrixColumn ADD UNIQUE (matrixId, squid);
