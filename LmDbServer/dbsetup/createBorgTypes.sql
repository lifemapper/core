
-- ----------------------------------------------------------------------------
-- lm_atom returns only an a few object attributes
DROP TYPE IF EXISTS lm3.lm_atom CASCADE;
CREATE TYPE lm3.lm_atom AS (
  id int,
  title varchar,
  epsgcode int,
  description text,
  modtime double precision
  );

-- ----------------------------------------------------------------------------
-- lm_palayeridx OR lm_anclayeridx
DROP TYPE IF EXISTS lm3.lm_layeridx CASCADE;
CREATE TYPE lm3.lm_layeridx AS (
   -- Layer
   layerid int,
   verify varchar,
   squid varchar,
   lyruserid varchar,
   layername varchar,
   metadataurl varchar,
   layerurl varchar,
   -- ExperimentPALayer OR ExperimentAncLayer
   paramid int,
   matrixidx int, 
   experimentid int);
