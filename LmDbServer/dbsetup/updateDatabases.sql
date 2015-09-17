-- ----------------------------------------------------------------------------
\c mal

ALTER TABLE lm3.computeresource ADD COLUMN ipmask varchar(2);
ALTER TABLE lm3.computeresource ADD CONSTRAINT computeresource_ipaddress_ipmask_key UNIQUE (ipaddress, ipmask);
DROP FUNCTION lm3.lm_getCompute(ip varchar);
DROP FUNCTION lm3.lm_insertCompute(cmpname varchar,
                                                ip varchar,
                                                domname text,
                                                usr varchar,
                                                createtime double precision);
-- ----------------------------------------------------------------------------
\c speco
ALTER TABLE lm3.computeresource ADD COLUMN ipmask varchar(2);
ALTER TABLE lm3.computeresource ADD CONSTRAINT computeresource_ipaddress_ipmask_key UNIQUE (ipaddress, ipmask);
DROP FUNCTION lm3.lm_getCompute(ip varchar);
DROP FUNCTION lm3.lm_insertCompute(cmpname varchar,
                                                ip varchar,
                                                domname text,
                                                usr varchar,
                                                createtime double precision);

-- ----------------------------------------------------------------------------
