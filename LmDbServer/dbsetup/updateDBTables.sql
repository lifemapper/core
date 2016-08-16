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
\c borg
-- -------------------------------
ALTER TABLE Taxon ADD COLUMN userid varchar(20) REFERENCES lm_v3.LMUser ON DELETE CASCADE;
ALTER TABLE Taxon ADD CONSTRAINT taxon_userid_squid_key UNIQUE (userid, squid);
