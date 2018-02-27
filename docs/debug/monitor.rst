
Get the number of running jobs on cluster nodes
-----------------------------------------------

  $ qstat -u lmwriter | grep compute | wc -l

Run a qstat call and then limit to just running jobs, then count

Get an idea of the output of the workers
----------------------------------------

  $ tail -f /share/lm/sge/worker.log
  
This will likely scroll very quickly.  Just look for obvious errors.  Inspect
the file manually if anything seems suspicious.


Use Ganglia to get an idea about the health of the system
---------------------------------------------------------

  Go to http://{FQDN}/ganglia/
  
Load_one is a good metric to see cluster utilization.  Free disk is another 
good one to see if the nodes are running out of space.  Various other metrics
may be useful for different things.


