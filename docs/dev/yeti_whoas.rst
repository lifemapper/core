Yeti Troubleshooting
####################

Makeflows seem "stuck"
----------------------

Sometimes, some makeflow processes seem to get stuck and won't progress or finish.  It often seems 
to be a single process or a small group of processes that are running without completing.  I 
usually see this when I run `work_queue_status` and a running makeflow has no waiting processes 
but 1 or so running and this doesn't change over time.  I *believe* that this means the process 
is dead or a zombie and probably will never complete.

This situation requires:

  1. We figure out what is going on with the stalled processes

  2. We figure out a way to prevent the process from stalling

  3. We figure out a way to recover if a process is stalled

Attempt 1
^^^^^^^^^
I am going to first try to figure out what to do if this does happen.  I think the easiest thing 
to do will be to add an option to the workers to cycle themselves at some interval.  For now, I 
will set them to have a "wall-time" of 4 hours.  My hope is that a stalled process will be stopped 
when the worker dies and started somewhere else.  If the problem is intermittent or some 
combination of things, hopefully the process will complete when tried again.  If the problem is 
data related, we should be able to determine what is causing the problem.

***This seems to just jam things up more as the workers kill off tasks but pull more and never do more work or die***

Update
^^^^^^
At least one of the makeflows gets stuck on the same process and it looks like an issue with Maxent and that dataset.  Will investigate

**The points are all marine.  Maxent fails but not hard enough on yeti.  On my machine it fails out completely**
