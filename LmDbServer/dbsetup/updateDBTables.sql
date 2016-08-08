-- ----------------------------------------------------------------------------
-- ----------------------------------------------------------------------------
-- psql -U admin -d template1 --file=LmDbServer/dbsetup/updateDBTables.sql
-- ----------------------------------------------------------------------------
-- These functions should change NOTHING if columns / indices already exist.
-- ----------------------------------------------------------------------------
\c mal
-- -------------------------------


-- ----------------------------------------------------------------------------
\c speco
-- -------------------------------

-- ----------------------------------------------------------------------------
\c template1
-- -------------------------------
DROP DATABASE borg IF EXISTS;
\i /opt/lifemapper/LmDbServer/dbsetup/createBorg.sql
