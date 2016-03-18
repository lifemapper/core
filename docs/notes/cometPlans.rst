##################################
Plans for Virtual Cluster on Comet
##################################

Makeflow/Work Queue process
***************************
   
#. We will start generating MF documents from the pipeline (LmServer).  Each   
   MF document will represent one, or more, “experiment(s)” consisting of the chain of 
   jobs/objects created for every species and keeps track of the dependencies 
   within the chain.
      
   #. A MF process will be instantiated on LmServer for each MF document, up to
      some limit of concurrent MF processes.  As one is completed, another will 
      be created.
      
   #. WQ workers will be instantiated on LmCompute, and will communicate with MF 
      processes on LmServer.
      
      * **OLD:** LmCompute requested individual jobs (with the jobMediator process 
        on LmCompute). Then LmServer retrieved one or more jobs of the requested
        type.  LmServer determined what was ready for computation according to 
        a status variable on each job (updated when dependencies were met).
        LmCompute returned results to LmServer, wrote the results to storage, 
        and updated object status in the database.
        
      * **NEW:** A WQ worker on LmCompute connects to a MF process (controlling 
        a MF document) and pulls a ready job out of MF process.  MF determines 
        what is ready for computation from its dependencies.  WQ worker on
        LmCompute returns the results and MF handles dependencies.  Once all the 
        jobs in a MF document/process are complete the MF process goes away.

From CJ
*******

LmServer
--------
#. We will have a new process, running on LmServer, that is in charge of Lifemapper 
   job management.  This process will retrieve job chains from the database that 
   represent Lifemapper experiments (SDM, RAD, BOOM).  One or more of these job 
   chains will be used to generate a Makeflow document that will be fed as input to
   a Makeflow (sub)process.  This Lifemapper process will manage a pool of these
   Makeflow subprocesses and when one finishes, it will be replaced by a new one.
   Each Makeflow subprocess will take a Makeflow document as input and start up an
   instance of Makeflow to manage the dependencies defined by the tasks described 
   in the Makeflow document.

LmCompute
---------
#. We will run Work Queue workers on nodes that will communicate with the pool of
   Makeflow processes.  These workers will retrieve available tasks and then run
   them.  These tasks will be defined as bash commands (we do this now as well).
   The defined outputs (in the Makeflow document) will be sent back to the Makeflow
   process that this worker connected to, where it will be stored or passed on to
   another process.  We will likely want to prevent transfering all of the 
   intermediate outputs to reduce overhead.  Options for this will be discussed in 
   the next section.

#. We can reduce communication overhead by preventing unnecessary data transfers
   between the workers and the Makeflow processes.  We can do this by:
     #. Using Work Queue Foremen processes
     #. Only return success / failure statuses for intermediate jobs and relying on 
        shared storage among nodes (and front end)
   I lean towards a combination of the two approaches.  Primarily, I think we can 
   rely on shared storage on Comet (and all of our LM instances most likely) to 
   either write the outputs directly to their final location, or at least to a
   staging area.  Then if we only send back success / failure messages, we prevent 
   the unnecessary transfer of potentially large files.  We can still use foremen 
   processes for groups of processes that utilize the same inputs.  I see this 
   being particularly useful for RAD experiments.  We will move the PAM to the 
   foreman process and then all of the processes that required that PAM would 
   connect to the foreman instead of connecting to the Makeflow process.  This 
   communication is not particularly painful when the Makeflow process is running
   on the front end of a cluster, but it could save some data transfer if all of 
   the associated workers for a foreman process were on the same node.  This is an
   area of research and I am not sure how much this will save us for a virtual 
   cluster, but it may be useful in the future.

#. Worker management tool.  We will likely want to add a tool that we can use to start 
   and stop workers.  This process will likely just wrap the built-in commands that 
   come with the CC Tools package (likely sge_submit_workers).  The tool should look
   for running workers as well as those submitted to the scheduler and determine how 
   many more workers should be added to the pool.  This tool should also be able to 
   tell existing workers to shut down if we want to scale back the number of worker
   processes.  The job management tool should probably be connected to this process,
   but we may also want to have a maintenance task that refreshes workers every so 
   often


New things needed
=================
  * LmServer
    * A new Lifemapper process will run that manages LM jobs
    * A worker management tool process
    * A directory to store Makeflow files and outputs.  This will be a volatile 
      directory where Makeflow documents will be written as well as associated 
      Makeflow meta files (Makeflow logs of task metadata).  We can use this 
      directory to recover when the Lifemapper job management process is stopped, 
      intentionally or not.  Makeflow can pick up where it left off for a 
      particular document.  This directory does NOT need to be shared with the 
      nodes.
      
Sample workflow for Makeflow
============================
 #. For each of the N job chains in the MF document
   #. Process occurrence data
      * Send back status
      * Data sent to shared storage
   #. Local (on LmServer) process writes data to final location and updates database
   #. SDM model is created using occurrence set and a model job request file (XML, stored in Makeflow workspace)
      * Send back status
      * Data sent to shared storage
   #. Local (on LmServer) process writes model ruleset to proper location and updates database
   #. For each partial projection request associated with this experiment (XML files in the Makeflow workspace)
     #. Build full projection request by pluggin in model ruleset, send full request to shared data
        * Alternative: Take out this step and instead send rulesets directly to projection process
     #. Create SDM projection from full projection request
       * Send back status
       * Data sent to shared storage
     #. Local (on LmServer) worker updates database with projection results and write data
     #. (Alternate) Perform any necessary post processing
       * Reprojection
       * Intersect with PAM (for Global PAM or spatial queries)
       * Scale projection
       * Etc.
     #. (Alternate) Local (on LmServer) worker updates database again and writes modified projection to final location
     
