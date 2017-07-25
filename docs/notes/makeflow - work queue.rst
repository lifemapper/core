########
Makeflow
########

A simple way to think of Makeflow (according to the Makeflow folks) is 
Make + workflow.  The structure of a makeflow file looks like a Makefile.  
Each entry looks like:

   {files defined}  : {resources required}
      {command}

For example:

   # Projection 949895 
   $OUT_DIR/120-949895.tif.zip : $OUT_DIR/120-949895.tif 
      zip -j $OUT_DIR/120-949895.tif.zip $OUT_DIR/120-949895.tif 

This entry defines a projection zip file.  It takes the projection tiff as 
input.  The command just uses zip.

Makeflow allows you to define variables that can be used throughout the 
document.  It also allows you to specify options for each of the processes, 
such as to run the job locally.

All targets must be defined.  This is so Makeflow can issue a clean up command 
and know what to remove.  Sources should be defined in a MAKEFLOW_INPUTS 
variable so that they will not be deleted on clean up.  The final targets 
should be added to a MAKEFLOW_OUTPUTS variable so that they are not deleted.  
Intermediate files will be cleaned up.


##########
Work Queue
##########
“Work Queue is a framework for building large master-worker applications...” 
We have been using it in conjunction with Makeflow.  Work queue workers sit on 
a core and continue to pull work until there is nothing left to do, or they are 
told to stop.  We have been running Makeflow so that it runs as a Work Queue 
master process registered with the master work queue catalog.  This allows Work 
Queue workers running anywhere to query the catalog, find a running master 
process and connect to it.  This lets us run workers in multiple locations or 
even arbitrary locations.  

There is an option to interact with Work Queue workers outside of Makeflow.  
This is described in their documentation and they have Python bindings for 
doing so.  When I spoke with the Notre Dame folks (Creators of CC Tools) they 
suggested that we may want to go this route for our larger workflows.  

######################
Current Implementation
######################
After much experimentation with configurations, the current implementation, at
Stampede, is a cluster job request is submitted asking for 1024 cores (64 
machines).  When these resources are allocated, the first core (labeled as core
zero, but somewhat arbitrary) is used to run Makeflow.  The provided workflow
contains processes for 1023 species.  This results in more than 30,000 processes
to run.  The rest of the cores are used to start work queue workers.  These 
workers connect to the makeflow process and pull available work until 
everything has completed.  We rely on a shared file system among all of the 
nodes and therefore we can prevent the transfer of the intermediate job results.
This greatly reduces the overhead associated with running makeflow.  Each 
experiment is packaged in the workflow in a format that Charlie can consume and
the result is a single zip archive for each species.  Charlie then pulls these
outputs using rsync from the Harvard Lustre storage machine where he will then
process them using Harvard resources.

In my experiments, I have found that the number of concurrent jobs directly
impacts system performance.  If we run on 160 cores at a time, each of the main
jobs, models and projections, completes in approximately 8 minutes.  As we 
scale up the number of cores, performance degrades.  At 1024 cores, the result
is about 15 minutes per job.  I have attempted multiple times to scale up to 
4096 cores but have found that to cause other problems with the system.  This 
will be an area to experiment with going forward.

#############
Going forward
#############

Going forward, we will drop the job mediator and replace the job mule with a 
new, long-running, daemon process.  This process will manage multiple makeflow
subprocesses.  Each makeflow process will be responsible for a single workflow
that will consist of one or more experiments.  As each of these makeflows 
completes, it will be replaced by a new makeflow instance.  This will be done 
because makeflow requires a static workflow and cannot be appended while it is 
running.  The current method uses one, very large, workflow with makeflow.  This
works for now because all of Charlie's jobs are known up front.  This does not
work for the dynamic system that is created by incoming user jobs and the 
addition of new computational content by the pipeline.  Running multiple
makeflow instances instead of a single instance allows us to reduce the size of
each workflow for greater flexibility and shorter overhead times.  This also
allows for more jobs to be available at a time compared to the scenario where
a single makeflow is used and only a small number of jobs remains.  In the time
required for a makeflow to finish off a single task before starting a new set,
workers remain idle or stop.  Having more work available than workers to do it 
ensures that the stream does not run dry at any time.  This could all be set up
faily easily, but there will be research that needs to take place in order to
optimize the process.  Future versions should also allow for scaling up and 
down for the quantity of workers and number of makeflow processes to handle 
bursts in computational load and / or availability of computational resources.

We will still need to install the LmCompute package, its dependancies, and 
CC Tools on our computational machines but controlling computations will be 
simplified.  By moving the makeflow, or master, process to an external machine,
the computational machines only need to run workers.  There are existing scripts
to submit workers to a number of compute schedulers (SLURM, SGE, Torque) that
are provided in the CC Tools package.  The default scripts are a good place to
start for initializing workers on these various compute environments but we may
find that we want greater control or additional customization.  If we find that
to be true, these scripts can be easily recreated using our own code.  I have 
already written an alternate version for TACC's Stampede machine that uses MPI
and CPU offsets to start workers on various cores.

Using one of the provided scripts, or one of our own, we will start multiple
work queue workers for the compute environment (which can be a single machine, 
a hard cluster, a virtual cluster, or other resource).  These workers will 
connect to a specified makeflow instance, or pool of instances, and pull the 
work that is available.  They will continue to pull available work until they 
are stopped (manually or by cluster kill signal) or if there is no additional 
work available for some period of time.

##################
Potential Pitfalls
##################

Catalog server
--------------
Until now, I have relied on the work queue catalog server from Notre Dame to 
register our Makeflow processes so that the workers can connect to them.  If we
run several makeflow processes, we may have problems with their server.  We are
also reliant on their server being operational.  As an alternative, we can 
connect to the master processes directly.  This method may not provide the 
necessary flexibility so that workers can connect to multiple makeflow 
instances.  An option may be to run our own catalog server.  As of yet, I have 
not found any documentation for doing so.  The Notre Dame group seems to be 
very helpful though and I would guess that they would be enthusiastic about
helping us set that up.

Data Transfer
-------------
For this next implementation, we will rely on a shared file system between the
workers and the Lifemapper controlling process.  This way we can prevent data
transfer of intermediate results and reduce overhead.  As long as the shared
file system can keep up with this method, this should be a viable approach for
the immediate future.  In the future, we may want to use something like iRODS 
to share data between the controller and (multiple) compute environments.  For 
environments such as Stampede, we will still rely on the internal shared file 
system, but we will need to transfer the results to the controlling process at 
some point, or use iRODS to do so.  If the intermediate results can stay on a
shared file system, we can avoid those data transfer steps.

