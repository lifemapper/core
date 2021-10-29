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
-- Commands should do NOTHING if changes have already been performed.
-- ----------------------------------------------------------------------------
-- ----------------------------------------------------------------------------



\c borg

-- ----------------------------------------------------------------------------                                                    
-- ----------------------------------------------------------------------------                                                    

/*
-- ----------------------------------------------------------------------------                                                    
-- For deleting large amounts of data, drop indices and constraints first

-- ----------------------------------------------------------------------------
-- delete from layer where layerid in 
--     (select layerid from sdmproject p
--                left join occurrenceset o 
--                on p.occurrencesetid = o.occurrencesetid
--      where o.occurrencesetid is null);


-- select count(*) from sdmproject p
-- where not exists (
--     select 1
--     from occurrenceset o
--     where p.occurrencesetid = o.occurrencesetid
-- )
    
-- ----------------------------------------------------------------------------                                                         
*/