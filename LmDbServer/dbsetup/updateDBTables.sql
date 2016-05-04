-- ----------------------------------------------------------------------------
-- ----------------------------------------------------------------------------
-- psql -U admin -d template1 --file=LmDbServer/dbsetup/updateDBTables.sql
-- ----------------------------------------------------------------------------
-- These functions should change NOTHING if columns / indices already exist.
-- ----------------------------------------------------------------------------
\c mal

alter table lm3.layer add column verify varchar(64);
alter table lm3.layer add column squid varchar(64);
CREATE INDEX idx_lyrVerify on lm3.Layer(verify);
CREATE INDEX idx_lyrSquid on lm3.Layer(squid);

alter table lm3.occurrenceset add column verify varchar(64);
alter table lm3.occurrenceset add column squid varchar(64);
CREATE INDEX idx_occSquid on lm3.OccurrenceSet(squid);

alter table lm3.projection add column verify varchar(64);
alter table lm3.projection add column squid varchar(64);
CREATE INDEX idx_prjSquid on lm3.Projection(squid);

alter table lm3.ScientificName add column rank varchar(20);
alter table lm3.ScientificName add column canonical text;


-- ----------------------------------------------------------------------------
\c speco

alter table lm3.layer add column verify varchar(64);
alter table lm3.layer add column squid varchar(64);
CREATE INDEX idx_lyrVerify on lm3.Layer(verify);
CREATE INDEX idx_lyrSquid on lm3.Layer(squid);

-- ----------------------------------------------------------------------------
