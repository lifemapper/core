#################
Tuning Lifemapper
#################

Java memory usage
------------------------
  Lifemapper tunes the Java heap so that it pre-allocates memory for Java usage.  This parameter should be tuned so 
  that all concurrent processes can allocate the memory they need without oversubscribing the memory on the system.  
  To tune this, look at the total amount of memory that you want to make available for processing.  This could be 
  nearly all of the system memory for the compute nodes or it may be some portion of memory on the front end 
  (leaving resources for the system, database, web services, solr, etc).  So tune this as: 

  `max_memory_per_java_process = memory_for_processing / number_of_concurrent_compute_processes`

  Be aware that this tuning is based on the assumption that the largest process, in terms of memory usage, is a 
  Java process, like Maxent.  If that is not true in the future, tuning of this parameter may need to be adjusted.  
  Makeflow resource monitoring will likely be a good tool to use at that point.

  To tune - Edit the site configuration file (probably on the nodes, but wherever the computations will happen) 
  and add / edit the "[LmCompute - plugins - maxent]" section and edit appropriately.  For this example, we want to
  reserve 1540 MB per Maxent process.  We are also limiting the amount of memory for layer conversion to 512 MB.
  
  .. code-block::
  
    [LmCompute - plugins - maxent]
    JAVA_MAX_MEM_OPTION: 1540m
    JAVA_INIT_MEM_OPTION: 1540m
    JAVA_EXE: /usr/java/latest/bin/java
    
    ; For converting ASCIIs to MXEs
    CONVERT_JAVA_MAX_MEM_OPTION: 512m
    CONVERT_JAVA_INIT_MEM_OPTION: 512m
  
----

Number of concurrent Makeflow instances
---------------------------------------
  Lifemapper controls computations by running Makeflow on the front end and connecting WorkQueue workers via a 
  catalog server.  In order to make better use of resources and limit idle time for the workers, Lifemapper runs
  multiple instances of Makeflow on the front end of the cluster.  Each of these instances presents available
  work and the workers connect to one of the available Makeflow instances.  When one instance of Makeflow no 
  longer has available work, by either finishing everything, or reaching a point in the workflow where tasks are
  waiting on another task to complete, the workers will disconnect and connect to a different Makeflow instance.
  The number of Makeflow instances needs to be balanced so that there is always enough work for the workers.
  Currently, Makeflow instances are configured to use two cores each.  So the number of instances should be
  approximately:
  
  `number_of_concurrent_makeflows = number_of_cores_on_frontend / 2`
  
  There exists a threshold where adding additional Makeflow instances is no longer helpful and can, in fact,
  delay results for individual workflows.  Additionally, the number of concurrent Makeflows should not 
  oversubscribe the memory resources available on the machine.  So a more complete equation for the optimum
  number of concurrent makeflows is:
  
  .. math::
    
    number_of_concurrent_makeflows = minimum(
                       total_system_memory_for_workflows / memory_per_workflow, 
                       number_of_cores_on_frontend / number_of_cores_per_makeflow,
                       (number_of_workers * computational_queue_constant) / number_of_tasks_per_workflow 
                      )
  
  Where
   * **total_system_memory_for_workflows** is the amount of memory on the front end of the cluster that should
        be reserved for Makeflow instances, leaving room for Solr, database, webserver, local computations,  etc.
   * **memory_per_workflow** is the amount of memory required for each instance of Makeflow.  This will be
        effected by the number of tasks in each workflow
   * **number_of_cores_on_frontend** is the number of cores on the front end to dedicate to workflows.  This may
        not be every core on the system if other operations will need processing
   * **number_of_cores_per_makeflow** is the number of cores dedicated for each active Makeflow instance. This 
        currently defaults to 2
   * **number_of_workers** is the number of active workers across the nodes of the system.  This should take 
        into account potential resource oversubscription and be adjusted accordingly.
   * **computational_queue_constant** is a, somewhat undetermined, constant for the number of queued tasks per
        worker.  It is probably best to determine it related to the number of models and projections rather than
        any general task in a workflow, as they will have a greater impact.  Any other significant computation
        should also be considered.  This constant is the the ratio of the number of total available tasks across
        the system to the number of available workers.  Too short of a line runs the risk of the pipeline 
        running dry, so to speak, and too long of a line uses unnecessary resources on the front end.  The longer
        the computations take, the shorter the line and smaller the constant can be.  
   * **number_of_tasks_per_workflow** is the number of tasks in each workflow and is determined by the number of
        taxa in each workflow
        
----

max connections
size of makeflows

number of workers
