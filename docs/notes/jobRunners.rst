With the integration of CC Tools packages, LmCompute job runners will be changing.

In the previous iteration, a job factory was used to determine which job runner subclass to use based on the job configuration file.

Now that we are using workflows with Makeflow, we know, and can tell LmCompute, which job runner to use in the Makeflow file.

Thust, the job runner factory is now obsolete.

Additionally, since we are removing the job server and changing the post method to either directly write back to the final location or have a helper process write to the final location, the job runners themselves become more configurable tools.

To accomplish this, job runners now take command line arguments to specify things like where to set up a workspace, whether or not to log (and at what level), if metrics should be stored, and more.

These are the command line arguments added to the base job runner class, each subclass may add more arguments as needed.

-n', '--job_name', dest='jobName', type=str,
                               help="Use this as the name of the job (for logging and work directory creation).  If omitted, one will be generated")
      self.parser.add_argument('-o', '--out_dir', dest='outDir', type=str, 
                               help="Write the final outputs to this directory")
      self.parser.add_argument('-w', '--work_dir', dest='workDir', type=str, 
                               help="The workspace directory where the work directory should be created.  If omitted, will use current directory")
      self.parser.add_argument('--metrics', type=str, dest='metricsFn', 
                               help="If provided, write metrics to this file")
      self.parser.add_argument('-l', '--log_file', dest='logFn', type=str, 
                               help="Where to log outputs (don't if omitted)")
      self.parser.add_argument('-ll', '--log_level', dest='logLevel', type=str, 
                               help="What level to log at", 
                               choices=['info', 'debug', 'warn', 'error'])
      self.parser.add_argument('--cleanup', type=bool, dest='cleanUp', 
                               help="Clean up outputs or not", 
                               choices=[True, False])
      self.parser.add_argument('jobXml', dest='jobXml', type=str, 
                               help="Job configuration information XML file")