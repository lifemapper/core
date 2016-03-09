#############
Running Tests
#############

Use a testing suite to run tests.  To start, the base testing suite class can
be used to run simple system test. Available options can be found by calling
the suite script with -h or --help::

   cjgrady@sporks:~/git/core/LmCommon/tools/testing$ python testSuite.py --help
   usage: Lifemapper Testing Suite [-h] [-v] [-o OUTPUT] [-e ERROR] [-d OUTDIR]
                                   [--pedantic] [--quickStop] [--summarize]
                                   [--testLevel TESTLEVEL]
                                   [-t [TESTDIR [TESTDIR ...]]]

   Performs a suite of common Lifemapper tests

   optional arguments:
     -h, --help            show this help message and exit
     -v, --version         show program's version number and exit
     -o OUTPUT, --output OUTPUT
                           File to store log output from test suite. Ignored if
                           output directory is specified for individual logs. If
                           omitted, use standard out
     -e ERROR, --error ERROR
                           File to store error output from test suite. Ignored if
                           output directory is specified for individual logs. If
                           omitted, use standard error
     -d OUTDIR, --outDir OUTDIR
                           Write individual log files to this directory. If
                           omitted, uses output path / std out
     --pedantic            Run tests in pedantic mode
     --quickStop           Stop on first failure
     --summarize           Generate a summary report
     --testLevel TESTLEVEL
                           Run tests less than or equal to this level (default:
                           10)
     -t [TESTDIR [TESTDIR ...]], --testDir [TESTDIR [TESTDIR ...]]
                           Add tests from this directory


#######################
How to interpret output
#######################
If you use the summary flag, test results will be listed under three categories, successful, warnings, and failures.

Example output::
   
   cjgrady@sporks:~/git/core/LmCommon/tools/testing$ python testSuite.py --summarize --pedantic -t ~/git/core/LmCommon/tests/config/
   08 Feb 2016 17:11 MainThread.log.info line 113 INFO     Successful Tests
   08 Feb 2016 17:11 MainThread.log.info line 113 INFO     ---------------------
   08 Feb 2016 17:11 MainThread.log.info line 113 INFO     Test Free Memory in 3.00472903252 seconds
   08 Feb 2016 17:11 MainThread.log.info line 113 INFO     Test /var disk usage in 3.00479507446 seconds
   08 Feb 2016 17:11 MainThread.log.info line 113 INFO     
   08 Feb 2016 17:11 MainThread.log.info line 113 INFO     Warnings
   08 Feb 2016 17:11 MainThread.log.info line 113 INFO     ---------------------
   08 Feb 2016 17:11 MainThread.log.info line 113 INFO     
   08 Feb 2016 17:11 MainThread.log.info line 113 INFO     Failures
   08 Feb 2016 17:11 MainThread.log.info line 113 INFO     ---------------------

Additionally, the test suite itself will produce an exit status code upon completion.  This should be checked to determine if any failures have occurred.  If there were any failures, the exit status will be 2, otherwise it will be 0.

#####
Tests
#####
Individual Lifemapper tests have a few common methods across all subclasses that
are used by the test suite to run any test arbitrarily


#############
Test builders
#############
Test builders are used to process input files into tests.  There is a test 
builder for each test type.


#############
Running Tests
#############

LmCommon Tests
**************
.. code-block:: 

   $ /opt/python/bin/python /opt/lifemapper/LmCommon/tools/testing/testSuite.py -t /opt/lifemapper/LmCommon/tests/config/ --summarize


LmWebServer Tests
*****************
.. code-block::

   $ /opt/python/bin/python /opt/lifemapper/LmWebServer/tools/testing/lmTests/webTestSuite.py -t /opt/lifemapper/LmWebServer/tests/config/clients/ /opt/lifemapper/LmWebServer/tests/config/webTests/ --summarize Dermot Dermot

LmCompute Tests (from the nodes)
********************************
.. code-block::

   $ /opt/python/bin/python /opt/lifemapper/LmCompute/tools/testing/computeTestSuite.py -t /opt/lifemapper/LmCompute/tests/config/sampleJobs/ --summarize
   
