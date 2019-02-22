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

/*
-- ----------------------------------------------------------------------------                                                    
-- For deleting large amounts of data, drop indices and constraints first

ALTER TABLE lm_v3.SDMProject DROP CONSTRAINT IF EXISTS sdmproject_occurrencesetid_fkey;
DROP INDEX IF EXISTS idx_layerid;
DROP INDEX IF EXISTS idx_prjstatus;
DROP INDEX IF EXISTS idx_prjstatusmodtime;

-- MatrixColumn
ALTER TABLE lm_v3.MatrixColumn DROP CONSTRAINT IF EXISTS matrixcolumn_matrixid_layerid_intersectparams_key;
ALTER TABLE lm_v3.MatrixColumn DROP CONSTRAINT IF EXISTS matrixcolumn_layerid_fkey;


-- ----------------------------------------------------------------------------                                                    
-- Delete boomed projection-layers
-- need userid, gridsetid, cutofftime

-- returns files for deletion
select * from lm_deleteGridset(124);

-- write function to return projection files for deletion
delete  from layer where layerid in 
   (select layerid from sdmproject where  userid = 'kubi' and statusmodtime < 58535);





    
-- ----------------------------------------------------------------------------
-- Add indices and constraints after clearing data

-- SDMProject 
ALTER TABLE lm_v3.SDMProject ADD CONSTRAINT sdmproject_occurrencesetid_fkey 
    FOREIGN KEY (occurrenceSetId) REFERENCES lm_v3.OccurrenceSet (occurrenceSetId);
CREATE INDEX idx_layerid ON lm_v3.SDMProject(layerid);
CREATE INDEX idx_prjStatus ON lm_v3.SDMProject(status);
CREATE INDEX idx_prjStatusModTime ON lm_v3.SDMProject(statusModTime);

-- MatrixColumn
ALTER TABLE lm_v3.MatrixColumn ADD CONSTRAINT matrixcolumn_layerid_fkey
    FOREIGN KEY (layerid) REFERENCES lm_v3.Layer(layerid);
ALTER TABLE lm_v3.MatrixColumn ADD CONSTRAINT matrixcolumn_matrixid_layerid_intersectparams_key 
    UNIQUE (matrixId, layerid, intersectParams);
    
    
-- ----------------------------------------------------------------------------
-- ----------------------------------------------------------------------------
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