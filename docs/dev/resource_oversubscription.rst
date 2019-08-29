# On 193
  * We are using a large (compared to the system) environmental layer dataset
  * We had to put in a worker option to kill a worker if it uses too many disk resources
  * This seems to kill the running process, but leaves subprocesses
  * Cannot expect cctools to know every subprocess
  * Subprocesses use memory resources and consume all memory over time

# Ideas
 * Chain processes in workflow so that cctools knows running process
  * If process fails, will probably blow up workflow
  * Could work if we can predict and prevent (almost all) failures
 * Single shot workers
  * I tried this in the past with problems, worth trying again
 * Call subprocess as synchronized process rather than subprocess
  * The idea would be that if the parent died, so would the child
 * Signal chaining
  * Similar to synchronized processes.  Any signal that the parent received would be passed to the child
   * Hopefully this would kill both
