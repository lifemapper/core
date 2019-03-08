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

ALTER TABLE lm_v3.SDMProject DROP CONSTRAINT IF EXISTS 
   sdmproject_userid_occurrencesetid_algorithmcode_algparams_m_key;
   
ALTER TABLE lm_v3.SDMProject ADD CONSTRAINT 
   sdmproject_userid_occurrencesetid_algorithmcode_algparams_m_key 
   UNIQUE (userId, occurrenceSetId, algorithmCode, mdlscenarioId, prjscenarioId);

CREATE OR REPLACE FUNCTION lm_v3.lm_findOrInsertSDMProjectLayer(prjid int, 
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
                                          prjscenid int,
                                          prjmeta text,
                                          ptype int,
                                          stat int,
                                          stattime double precision)
RETURNS lm_v3.lm_sdmproject AS
$$
DECLARE
   cmd varchar;
   wherecls varchar = '';
   newlyrid int = -1;
   idstr varchar;
   rec_lyr lm_v3.Layer%rowtype;
   rec_fullprj lm_v3.lm_sdmproject%rowtype;
BEGIN
   -- Find existing
   IF prjid IS NOT NULL then                     
      cmd = 'SELECT * from lm_v3.lm_sdmproject WHERE sdmprojectId = ' || 
             quote_literal(prjid);
   ELSIF lyrid IS NOT NULL then                     
      cmd = 'SELECT * from lm_v3.lm_sdmproject WHERE layerId = ' || 
             quote_literal(lyrid);
   ELSE
      begin
         cmd = 'SELECT * from lm_v3.lm_sdmproject ';
         wherecls = ' WHERE userid =  ' || quote_literal(usr) ||
                    '   AND occurrenceSetId =  ' || quote_literal(occid) || 
                    '   AND algorithmCode =  ' || quote_literal(algcode) ||
                    '   AND mdlscenarioId =  ' || quote_literal(mdlscenid) ||
                    '   AND prjscenarioId =  ' || quote_literal(prjscenid);
      end;
   END IF;
   
   cmd := cmd || wherecls;
   RAISE NOTICE 'cmd = %', cmd;   
   EXECUTE cmd INTO rec_fullprj;
   RAISE NOTICE 'Results layerid = %, gdaltype = %', rec_fullprj.layerid, rec_fullprj.gdaltype;   
   
   -- Add new
   IF rec_fullprj.layerid IS NULL THEN
      RAISE NOTICE 'Unable to find existing lm_sdmProject for user: %', usr;
      -- get or insert layer 
      SELECT * INTO rec_lyr FROM lm_v3.lm_findOrInsertLayer(lyrid, usr, lyrsquid, 
         lyrverify, lyrname, lyrdloc, lyrmeta, datafmt, rtype, vtype, 
         vunits, vnodata, vmin, vmax, epsg, munits, res, bboxstr, bboxwkt, lyrmtime);
      
      IF NOT FOUND THEN
         RAISE EXCEPTION 'Unable to findOrInsertLayer';
      ELSE
         newlyrid = rec_lyr.layerid;
         RAISE NOTICE 'newlyrid = %', newlyrid;
      
         -- get or insert sdmproject 
         SELECT * INTO rec_fullprj FROM lm_v3.lm_insertSDMProject(prjid, newlyrid, 
                   usr, occid, algcode, algstr, mdlscenid, prjscenid, 
                   prjmeta, ptype, stat, stattime);
         RAISE NOTICE 'Returned rec_fullprj % / %', 
                       rec_fullprj.layerid, rec_fullprj.sdmprojectId;

         IF NOT FOUND THEN
            RAISE EXCEPTION 'Unable to insertSDMProject';
         ELSE
            -- URL and geometry are updated on Layer insert 
            RAISE NOTICE 'Successfully inserted SDMProject';
         END IF;
      END IF;
   END IF;
   
   RETURN rec_fullprj;
END;
$$  LANGUAGE 'plpgsql' VOLATILE;


/*
-- ----------------------------------------------------------------------------                                                    
-- ----------------------------------------------------------------------------                                                    
-- ----------------------------------------------------------------------------                                                    
-- ----------------------------------------------------------------------------                                                    
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
-- Add back indices and constraints after clearing data

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
    
-- ----------------------------------------------------------------------------                                                    
-- ----------------------------------------------------------------------------                                                    
-- ----------------------------------------------------------------------------                                                    
-- ----------------------------------------------------------------------------                                                    
-- ----------------------------------------------------------------------------                                                    
     
*/