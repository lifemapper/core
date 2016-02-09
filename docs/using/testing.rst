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


