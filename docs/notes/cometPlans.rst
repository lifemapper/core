##################################
Plans for Virtual Cluster on Comet
##################################

Makeflow process
****************

#. We will more effectively utilize Makeflow (MF) and Work Queue (WQ) from the 
   CCL group at Notre Dame.  
   
   #. We will start generating MF documents from the pipeline (LmServer).  Each   
      MF document will represent one “experiment” consisting of the chain of 
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

