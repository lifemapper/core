###########
Matt Daemon
###########
The mattDaemon is a Lifemapper daemon process that provides a management layer
for computations.  Matt Daemon starts (and stops when finished) instances of
a catalog server and work queue factory and mangages a pool of Makeflow 
processes.  Each Makeflow process is responsible for an individual workflow and
when it completes, the Matt Daemon replaces it with a new one.  The Makeflow
processes advertise their work availability through the catalog server and the
work queue factory submits worker processes through the job sceduler.

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

##############
Catalog Server
##############
The catalog server process acts as a mediator between the Makeflow processes,
the worker factory, and the workers.  Each Makeflow connects to it and 
advertises the available work it has.  The worker factory monitors the total 
amount of available work and scales up or down the number of workers in 
response.  And the workers use it to connect to an appropriate makeflow process.
Notre Dame provides a public instance of the catalog server that can be used by 
default but we will run our own due to the volatility and number of makeflows
we run.

##################
Work Queue Factory
##################
The work queue factory process interfaces with the job scheduler.  In our case,
we will use SGE but other options are available if we ever want to expand to 
another environment.  This factory is configured with a minimum and maximum
number of workers to keep alive to perform the work available, as determined
from the catalog server.  It handles the interface between itself and the 
selected job scheduler and runs work queue workers as needed.

###########
Other notes
###########

Data movement
-------------
Data is moved to and from the workers using makeflow / work queue.  Makeflow
documents define the inputs and outputs of each process and those are 
automatically transfered to the workers when they are needed and the outputs
are transfered back to the workspace.  The only data movement we do "manually"
is the initial movement of input data into the workspace.  We do that with
"LOCAL" commands.  Otherwise, everything that is created is managed by CCTools
processes until those outputs are ultimately written to the Lifemapper shared
file system (using the stockpile.py process).

Local vs remote tasks
---------------------
In Makeflow, task commands that being with the "LOCAL" keyword are run on the
server from Makeflow itself instead of being transfered to a work queue process.
We use local processes when we need database access or files on the front end
that may not be shared with the nodes / workers (file transfers into and out of
the workspace).  We have also had issues with creating shapefiles on nodes in 
the work directories / shared directories, so they are run as local processes 
as well.

How things are cleaned up
-------------------------
Within the Makeflow DAG documents, we have stockpile calls that test the 
outputs of the tasks, move files, and update the database.  That is how we get
the outputs out of the workspace.  Once we do that, we can clean up the 
workspace to save disk space.  We currently do that by deleting the workspace
directory for a work flow after it is completed (if it was successful).  
Ideally, that is something we could set up with Makeflow itself, but that is 
not their standard use case.  Another option would be to run Makeflow with the
"-c" cleanup flag which would remove all of the files defined in the Makeflow
DAG document.  We are not currently doing that because there are a few cases
where we are not absolutely certain what files will be created (big shapefiles,
all maxent outputs, etc.).  At some point, we should handle those cases so we
can have a cleaner system.
