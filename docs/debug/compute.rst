
Jobs stuck in qw status
------------------------
Check SGE (sgemaster.<FQDN>) on Frontend, restart if not "active (running)"::
	systemctl status sgemaster.notyeti-194
	systemctl restart sgemaster.notyeti-194

Check SGE (sgeexecd.<FQDN>) on the nodes, restart if not "active (running)".::
	rocks run host compute "systemctl status sgeexecd.notyeti-194"
	rocks run host compute "systemctl restart sgeexecd.notyeti-194"

Matt Daemon exits after one iteration
-------------------------------------
I have seen this happen if there is an issue connecting to the catalog server
or the work queue factory.  I believe that the code has been changed so that it
won't even start up if it cannot connect to these processes on startup.  It is 
also possible that there was an error thrown in the looping mechanism.  So 
check to see if there is an error message in the log file (in 
/share/lmserver/lmscratch/log/mattDaemon.log) indicating if there is a code error or
a database error.

Matt Daemon reports that the catalog server is not running
----------------------------------------------------------
This can happen if there is a problem with the catalog server command. However,
I have seen this much more frequently when an instance of Matt Daemon failed
and was not able to stop the running catalog server.  So to debug, first check
to see if there is an instance of the catalog server already running (as Matt
Daemon tries to start one and it fails if it is already running).  Run 
`ps aux | grep catalog` and if there is a running instance, kill it.  If the 
catalog server is not running, attempt to start it manually with the same 
command that Matt Daemon uses.  This command is constructed in the Matt Daemon,
but it is roughly a combination of the CATALOG_SERVER_BIN and CS_OPTIONS 
constants in LmServer.common.lmconstants.  At the time of this writing, the 
command is: 
   `/opt/lifemapper/bin/catalog_server -n {server FQDN} -B /share/lmserver/lmscratch/run/catalog_server.pid -p 9097 -m 100 -o /share/lmserver/lmscratch/log/catalog_server.log -O 100M -H /share/lmserver/lmsractch/catalog.history`
If an error message is not immediately clear, you can also check the catalog
server log file at: /share/lmserver/lmscratch/log/catalog_server.log

Matt Daemon reports that the work queue factory is not running
--------------------------------------------------------------
Debugging a non-running work queue factory is the same as when the catalog
server is not running.  Most likely, this happens because it was not shut down
properly and the Matt Daemon can't keep track of it.  Look for a running
instance with `ps aux | grep factory`.  Otherwise, try running it manually to 
see if there is a configuration problem (look at 
LmServer.common.lmconstants.(WORKER_FACTORY_BIN | WORKER_FACTORY_OPTIONS).  
There may also be information in /share/lm/data/worker.log which is the log 
file created by the factory when running work queue worker instances.

Nothing is completing / workers cycle quickly
---------------------------------------------
This problem presents multiple symptoms, first, no (remote) work is being 
completed (local jobs may still complete).  The easiest way to see this is if
workflows complete occurrence jobs but never progress through models and 
projections.  

It may be that there is a problem with the workers.  A better way
to tell if this is the problem is if you notice that the worker factory submits
new workers at every iteration.  Though, it is possible that this symptom will
not show up either if SGE is holding the submitted cluster jobs in the error 
queue.  If you notice any of these symptoms, the first thing to do is to check
to see if worker jobs are running.  To do this, run a qstat command with the 
lmwriter user, `qstat -U lmwriter`.  If the results show a status of `Eqw` then
the scheduler will likely lock up and you will need to delete those jobs with
`qdel -j {job id}`, but before you do that, check to see if there is any error
information on the job.  Do this with `qstat -j {job id}`.  Hopefully, there 
will be some information that can be used to debug the problem.  
** CJ: /share/lm/data/worker.log  is not currently being modified **
If not, look 
at /share/lm/data/worker.log to see if there is any debugging information there.
Most likely, the error will be related to file permissions / directory setup or
potentially networking issues.

My workflows are failing unexpectedly
-------------------------------------
The Matt Daemon logs the outputs of each makeflow but it can be difficult to 
discern what message belongs to what workflow when the output is streaming.
Until we produce individual log files or some other mechanism for looking at 
individual makeflow logs, the easiest way to debug it will be to run the 
workflow outside of the Matt Daemon.  There are two ways to do this, either run
the makeflow locally, or run it using work queue.  In general, it is best to
run it using work queue because that will distribute jobs to the workers on the
nodes and will run it in basically the same environment as that of Matt Daemon.
As of now, that process is not as simple as just running it locally.  You will
need the catalog server and the worker factory to be running.  If Matt Daemon 
is running, then you already have those and can just use the instances that the
Matt Daemon started.  Run the following command from the 
`/share/lmserver/lmscratch/worker/` directory:
   `/opt/lifemapper/bin/makeflow -T wq -t 600 -u 600 -X /share/lmserver/lmscratch/worker/ -a -C {server FQDN}:9097 -N lifemapper-test {path to makeflow DAG document}`
This should show you any error message that are thrown by any of the workers
and hopefully the problem will be obvious.

If you want to run the makeflow locally, you can use makeflow as the job queue
instead of work queue.  The command will look like this:
   `/opt/lifemapper/cctools/bin/makeflow -X /state/partition1/lmscratch/worker/ {path to makeflow DAG document}`

The command is simpler, but does not test as many components as the remote 
version.  I have even seen the workflow work correctly when run locally but
fail when run remotely.  There may be certain situations when running locally
is more appropriate, such as testing to see if a DAG is correctly formatted.
But in general, I do not recommend it.

How can I tell what is currently(ish) running?
----------------------------------------------
You can use the work queue status process to query the catalog server to see
a, more or less, up to date listing of the running makeflows and workers.  
There is some caching that takes place so it is not absolutely correct, but it
can give you a general idea if things are working or not.  To run this:
   `work_queue_status -C {server FQDN}:9097`
The results will show which makeflow instances have connected to the catalog 
server and what their current status is (as far as number of tasks, number 
completed, number waiting, etc).  It also shows the number of workers connected
to each makeflow instance, but I do not think that number is very reliable.

I am getting the message: "Unable to run job: denied: host "my machine name" is no submit host
----------------------------------------------------------------------------------------------
This message indicates that the machine you are submitting a job from (either 
with qsub or with worker factory) is not set up as a submit host.  Try:
   `qconf -as sge-qmaster`
If it does not work with "sge-qmaster" change to the name of the machine you are 
trying to submit from and update this document.

Qstat is reporting a bunch of old jobs with status like 'dt' or 'dr' that won't go away
---------------------------------------------------------------------------------------

  This seems to happen when nodes are shut off while jobs are running, but there
  could be other reasons.  To clear these, you need to use `qdel -f`
  
  To remove all jobs for user lmwriter, use root and:
    `# qdel -f -u lmwriter`

How can I tell what types of processes are running?
---------------------------------------------------
You can check what the catalog server says is running with:
   `work_queue_status -C {server FQDN}:9097`
you can also see what makeflow processes are running with:
   `ps aux | grep makeflow`
then check the contents of the makeflows with cat or something:
   `cat {path to running makeflow}`

Things look stuck, what do I do now?
------------------------------------
If things are going through (makeflows / projections / whatever), check first to see if there are any makeflows running.
   `ps aux | grep makeflow`
If there are makeflows, check when they started.  If they have been running for a while, they may not have 
workers or they may be stuck.  Check to see if there are connected workers.
   `work_queue_status -C {server FQDN}:9097`
If there are workers, tail the logs to see if anything is happening (check both .out and .err)
   `tail -f {log file}`
If there weren't any workers, run qstat to see if they are any running:
   `qstat | grep -v qw`
In either case, if logs aren't moving or no workers, try restarting matt daemon
   `$PYTHON /opt/lifemapper/LmServer/tools/mattDaemon.py stop
    $PYTHON /opt/lifemapper/LmServer/tools/mattDaemon.py start`
