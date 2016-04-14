-- ----------------------------------------------------------------------------
-- -- psql -U admin -d template1 --file=LmDbServer/dbsetup/updateDBTables.sql
-- ----------------------------------------------------------------------------
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

-- ----------------------------------------------------------------------------
\c speco
alter table lm3.layer add column verify varchar(64);
alter table lm3.layer add column squid varchar(64);
CREATE INDEX idx_lyrVerify on lm3.Layer(verify);
CREATE INDEX idx_lyrSquid on lm3.Layer(squid);

-- ----------------------------------------------------------------------------
