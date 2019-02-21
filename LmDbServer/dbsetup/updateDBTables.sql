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
DROP FUNCTION IF EXISTS lm_v3.lm_findOrInsertMatrix(mtxid int, mtxtype int,
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

/*
                                                    

ALTER TABLE lm_v3.MatrixColumn ADD UNIQUE (matrixId, squid);

ALTER TABLE lm_v3.SDMProject DROP CONSTRAINT IF EXISTS sdmproject_occurrencesetid_fkey;
    
        
DROP INDEX IF EXISTS idx_layerid;
DROP INDEX IF EXISTS idx_prjstatus;
DROP INDEX IF EXISTS idx_prjstatusmodtime;
CREATE INDEX idx_layerid ON lm_v3.SDMProject(layerid);
CREATE INDEX idx_prjStatus ON lm_v3.SDMProject(status);
CREATE INDEX idx_prjStatusModTime ON lm_v3.SDMProject(statusModTime);


ALTER TABLE lm_v3.MatrixColumn DROP CONSTRAINT IF EXISTS matrixcolumn_matrixid_layerid_intersectparams_key;
ALTER TABLE lm_v3.MatrixColumn DROP CONSTRAINT IF EXISTS matrixcolumn_layerid_fkey;


delete  from layer where modtime < 58362 and layerid in 
   (select layerid from sdmproject where  userid = 'kubi' and statusmodtime < 58362.0);





ALTER TABLE lm_v3.MatrixColumn ADD CONSTRAINT matrixcolumn_layerid_fkey
    FOREIGN KEY (layerid) REFERENCES lm_v3.Layer(layerid);
ALTER TABLE lm_v3.MatrixColumn ADD CONSTRAINT matrixcolumn_matrixid_layerid_intersectparams_key 
    UNIQUE (matrixId, layerid, intersectParams);
    
ALTER TABLE lm_v3.SDMProject ADD CONSTRAINT sdmproject_occurrencesetid_fkey 
    FOREIGN KEY (occurrenceSetId) REFERENCES lm_v3.OccurrenceSet (occurrenceSetId);
    
    
delete from layer where layerid in 
    (select layerid from sdmproject p
               left join occurrenceset o 
               on p.occurrencesetid = o.occurrencesetid
     where o.occurrencesetid is null);




select count(*) from sdmproject p
where not exists (
    select 1
    from occurrenceset o
    where p.occurrencesetid = o.occurrencesetid
)
    
     
*/