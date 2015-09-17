-- ----------------------------------------------------------------------------
-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lm3.lm_updateJob(jid int,
                                            crid int,
                                            stat int,
                                            currtime double precision, 
                                            newpull boolean)
RETURNS int AS
$$
DECLARE
   jsuccess int = -1;
   retries int;
BEGIN
   IF newpull THEN
      begin
         SELECT INTO retries retryCount FROM lm3.lmjob WHERE lmjobid = jid;
         retries = retries + 1;
      end;
   END IF;
         
   -- ComputeResource, Status, Heartbeat, Retries
   IF crid IS NOT NULL AND stat IS NOT NULL THEN
      UPDATE lm3.LMJob SET
        (computeResourceId, status, statusmodtime, lastheartbeat, retrycount) 
        = (crid, stat, currtime, currtime, retries)
         WHERE lmjobid = jid;
   -- Status, Heartbeat, Retries
   ELSEIF stat IS NOT NULL THEN
      UPDATE lm3.LMJob SET (status, statusmodtime, lastheartbeat, retrycount) 
                     = (stat, currtime, currtime, retries) WHERE lmjobid = jid;
   -- Heartbeat
   ELSE
      UPDATE lm3.LMJob SET lastheartbeat = currtime WHERE lmjobid = jid;
   END IF;
   
   IF FOUND THEN
      jsuccess := 0;
   END IF;
      
   RETURN jsuccess;
END;
$$  LANGUAGE 'plpgsql' VOLATILE;


-- ----------------------------------------------------------------------------
-- ----------------------------------------------------------------------------

-- ----------------------------------------------------------------------------
-- Find or insert ComputeResource and return id.  Return -1 on failure.
CREATE OR REPLACE FUNCTION lm3.lm_insertCompute(cmpname varchar,
                                                ip varchar,
                                                mask varchar,
                                                domname text,
                                                usr varchar,
                                                createtime double precision)
   RETURNS int AS
$$
DECLARE
   id int = -1;
   rec lm3.ComputeResource%ROWTYPE;
BEGIN
   SELECT computeresource INTO id FROM lm3.computeresource 
      WHERE ipaddress = ip;
   IF NOT FOUND THEN
      BEGIN
         INSERT INTO lm3.ComputeResource 
            (name, ipaddress, ipmask, fqdn, userId, datecreated, datelastmodified)
         VALUES 
            (cmpname, ip, mask, domname, usr, createtime, createtime);

         IF FOUND THEN
            SELECT INTO id last_value FROM lm3.computeresource_computeresourceid_seq;
         END IF;         
      END;
   END IF;  -- end if not found
   RETURN id;
END;
$$  LANGUAGE 'plpgsql' VOLATILE;

-- ----------------------------------------------------------------------------
-- lm_getCompute
CREATE OR REPLACE FUNCTION lm3.lm_getCompute(ip varchar, mask varchar)
   RETURNS lm3.computeresource AS
$$
DECLARE
   rec lm3.computeresource%ROWTYPE;
BEGIN
   BEGIN
      SELECT * INTO STRICT rec FROM lm3.computeresource 
         WHERE ipaddress = ip AND ipmask = mask;

      EXCEPTION
         WHEN NO_DATA_FOUND THEN
            RAISE NOTICE 'IP Address % (mask %) not found', ip, mask;
         WHEN TOO_MANY_ROWS THEN
            RAISE EXCEPTION 'IP Address % not unique', ip;
   END;
   RETURN rec;
END;
$$  LANGUAGE 'plpgsql' STABLE;

-- ----------------------------------------------------------------------------
-- lm_getCompute
CREATE OR REPLACE FUNCTION lm3.lm_getAllComputes()
   RETURNS SETOF lm3.computeresource AS
$$
DECLARE
   rec lm3.computeresource%ROWTYPE;
BEGIN
   FOR rec IN 
      SELECT * FROM lm3.lm3.computeresource
   LOOP
      RETURN NEXT rec;
   END LOOP;   
   RETURN;
END;
$$  LANGUAGE 'plpgsql' STABLE;

