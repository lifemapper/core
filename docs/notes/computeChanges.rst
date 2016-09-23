With the integration of CC Tools libraries, the following will change:
 * Job mediator will go away
     The job mediator will not be needed as we will utilize Makeflow with Work Queue to run computations
 * LmCompute/environment directory will go away.
     It is possible that we will need something similar in the future for Amazon, etc. but it will not be used as it currently exists
 * Plugins directories will change
     To match the shift in definition of modules, these will become single and multi for single species computations and multispecies
 * Job runner factor goes away
     The architecture switch allows us to remove the job runner factory because Makeflow can define the job commands explicitly (instead of compute trying to figure it out)
 * Job runners take more options
     These will include where the work directory should be, how to finalize outputs, etc
 * Job scripts will be created
     These will call the specific job runner needed and take command line options for job configuration
 * Compute will work with LM objects instead of LM jobs
     Not the objects directly, but their serialized form.  We will be able to write to the object's final location by utilizing shared directories
 * Job server will go away
     Instead of pulling and pushing jobs with the job server, we will do so by utilizing Makeflow as the control mechanism (should work across machines that are not on a shared network too)
 * Rise of MattDaemon
     This new daemon process will manage the catalog server, makeflow instances, and worke queue submitter daemon.  This is the replacement for the job mediator and will run on the front end
 * Job retrievers go away
     These are no longer needed with Makeflow and no job server
 * Job submitters go away
     The work queue factory submits workers to the scheduler that will pull work from Makeflow.  We don't need to interact with the scheduler for every job anymore
 * Job runners are greatly simplified
     The job runners are basically just wrappers now and don't need to manage a lot of communication
 * Job client goes away
     Used to communicate with the job server.  No longer needed
 * Job poster goes away
     Used to communicate with the job server.  obsolete now
             
   
   