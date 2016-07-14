-- ----------------------------------------------------------------------------
-- ----------------------------------------------------------------------------
-- psql -U admin -d template1 --file=LmDbServer/dbsetup/updateDBTables.sql
-- ----------------------------------------------------------------------------
-- These functions should change NOTHING if columns / indices already exist.
-- ----------------------------------------------------------------------------
\c mal
-- -------------------------------
-- 5-20-2016: Not yet updated on production/Yeti
-- -------------------------------
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

-- -------------------------------
-- 5-20-2016
-- -------------------------------
DROP TABLE IF EXISTS lm3.Experiment CASCADE;

create table lm3.JobChain
(
   jobchainId serial UNIQUE PRIMARY KEY,
   userid varchar(20) NOT NULL REFERENCES lm3.LMUser ON DELETE CASCADE,
   dlocation text,
   priority int,
   progress int,
   status int,
   statusmodtime double precision,
   datecreated double precision
);
GRANT SELECT ON TABLE lm3.jobchain, lm3.jobchain_jobchainid_seq TO GROUP reader;
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE lm3.jobchain TO GROUP writer;
GRANT SELECT, UPDATE ON TABLE lm3.jobchain_jobchainid_seq TO GROUP writer;


-- ----------------------------------------------------------------------------
\c speco
-- -------------------------------
-- 5-20-2016: Not yet updated on production/Yeti
-- -------------------------------
alter table lm3.layer add column verify varchar(64);
alter table lm3.layer add column squid varchar(64);
CREATE INDEX idx_lyrVerify on lm3.Layer(verify);
CREATE INDEX idx_lyrSquid on lm3.Layer(squid);

-- ----------------------------------------------------------------------------
\c borg
-- -------------------------------
