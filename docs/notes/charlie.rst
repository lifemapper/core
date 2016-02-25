###################
Runing Charlie jobs
###################

The current method that I am using to run Charlie's jobs is tangent to the 
Lifemapper core.  I have written scripts to generate the necessary files in 
order to run the jobs on Stampede but they are not integrated into the main
code base as of yet.  They can and will be in the future and a narrative of how
that will work will be found in another portion of the documentation.  Below is
the process currently in use, as of February 23, 2016.

#. Charlie uploaded data and submitted all of the SDM experiments for a single 
   run
     
#. I, CJ, changed the status of all of Charlie's models and projections to 2.  
   I did this so that they would not be picked up by the main Lifemapper
   pipeline or job mediator.

#. Create a CSV file with model job id, projection job id rows using the
   following SQL command.
   
   This command generated the second group of jobs to run for Charlie.  This
   command was repeated for each run in the set.::
     
     \copy (SELECT m.lmjobid, p.lmjobid FROM (SELECT modelid, lmjobid FROM lm_mdljob WHERE mdluserid = 'cgwillis' AND mdlstatus = 2 LIMIT 1023 OFFSET 1023) m, lm_prjjob p WHERE m.modelid = p.modelid) TO 'run2.csv' WITH CSV;
     
#. The CSV file is fed into a script that I wrote to generate makeflow file and
   all of the required inputs for the jobs.  
   
   This script is included in a repository that I created for Charlie related 
   stuff, but I will move it to the Lifemapper core.  The arguments to the 
   script are the CSV file containing (model job id, projection job id) pairs, 
   a location to write out the job requests and makeflow file, and the name of 
   the makeflow file to be generated. The command is::
     
     -bash-4.1$ /opt/python/bin/python /home/lmwriter/charlie/scripts/prepMFsdmJob.py /home/lmwriter/charlie/runCsvs/run2.csv /tank/lmserver/charlie/run2/ run2.mf
     
#. Move all of the run inputs to Stampede.  I keep the run directory as it is
   when I transfer it to Stampede and then create copies of the files in the
   configured directory.  I do this so that I can rerun the jobs if there is 
   a problem as LmCompute cleans up after itself and removes the input files
   when they are no longer needed.  Makeflow does some clean up as well so it
   is just easier to have a fresh copy if needed. From Stampede::
     
     login1.stampede(1)$ scp -R root@yeti.lifemapper.org:/tank/lmserver/charlie/run2/* /scratch/03849/tg831484/run2/
     login1.stampede(2)$ cp /scratch/03849/tg831484/run2/jobRequests/* /scratch/03849/tg831484/jobRequests/
     
#. Move the makeflow file to my home directory.  I am sure that this is not 
   absolutely necessary, but that is where I have the scripts needed to run it.::
     
     login1.stampede(3)$ cp /scratch/03849/tg831484/run2/makeflow/run2.mf ~/
     
#. Modify the main script that I use to run makeflow jobs on Stampede.  These
   are arguments that could be passed to a script, but this is how I set it 
   up originally for testing and it works.  Going forward, this is something
   that we will want to address for a more production quality set up.  Open 
   the mfJob.sh script with your favorite editor and replace these values as 
   necessary. 
   
   - MF_FILE points to the makeflow file that we brought in
   - MF_PROJECT is the name of the Makeflow project.  This name will also be 
     sent to the work queue master catalog and the workers will connect 
     using this name
   - NUM_WORKERS is the number of workers to start for this job.  This should 
     be less than or equal to the number of available cores - 1.::
     
     MF_FILE="/home1/03849/tg831484/run2.mf"
     MF_PROJECT="cgwRun2_1"
     NUM_WORKERS=4095
     
#. Submit the job with the following arguments, followed by the name of the script
   to be run:

   -p : This is the queue to send the job to.  We primarily use the normal
        queue for jobs and the development queue for testing.
   -n : This is the number of cores to use for the job.  4096 has proven to
        cause issues with disk concurrency.  For these runs I am only 
        using 1024 (64 machines).  This should be a multiple of 16 (the 
        number of cores per node) as you are allocated an entire machine.
   -t : This is the amount of time that the resources are requested for
        (HH:MM:SS)

   ::
   login1.stampede(5)$ sbatch -p normal -n 4096 -t 3:00:00 mfJob.sh
    
#. Check the status of the job.  TACC uses SLURM for job management.  You can
   use the command showq with your user id as an argument to see the status 
   of your jobs.::
     
     login1.stampede(6)$ showq -u tg831484
     
#. When the calculations are completed, I move them to a directory that Charlie
   has access to (the main directories are all owned by the group associated 
   with Dan Voss's allocation).  This also makes for a clean command to pull
   the job results.::
    
    login1.stampede(7)$ mv /scratch/03849/tg831484/jobs/completed/*.zip /scratch/03849/tg831484/charlie/run2/
    
#. Charlie pulls the outputs using rsync from the Harvard storage machine::

     rsync {tacc user id}@stampede.tacc.utexas.edu:/scratch/03849/tg831484/charlie/run2/* .
     
     
