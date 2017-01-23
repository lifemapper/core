****************************************************
Initial Scope and Steps for Lifemapper Testing Suite
****************************************************

Set up a testing environment.  This environment should be a set of Virtualbox 
virtual machines configured as a Rocks virtual cluster.  These machines will 
need to be created many times as we produce new rolls and different 
configuration updates, so the procedure for creating them should be mastered.  
The base operating system can vary, but there is documentation for creating 
these when the host environment is also Rocks.  Some of these instances should 
be built using released rolls for testing while others can use development code 
branches.

Research testing standards and best practices for different testing strategies.  
There are many Python testing tools available, but the most widely accepted seem 
to be PyUnit (default Python unit testing framework), PyTest, and DocTest.  
There are several tools that build on these core testing frameworks, such as 
Nose, Nose2, coverage, and Mock.  There are other tools that could be useful 
too, such as Hypothesis for “fuzz” testing.  
https://wiki.python.org/moin/PythonTestingToolsTaxonomy seems to have quite a 
few tools listed that could potentially be useful, but researching the 
aforementioned would be most beneficial.  We need to determine which would be 
best for our use case, keeping in mind our testing goals, existing code, and 
minimizing new dependencies where possible.  Write a report (2 pager) about the 
options and advantages / disadvantages of each.  Include links to documentation 
concerning these features / limitations.  We will meet as a group to discuss 
the options and Aimee and CJ will review the report so that we might come to a 
consensus for a direction moving forward.  We will structure our testing 
framework, to be named LmTest, to provide methods for running the tests in the 
structure that we decide on.

Begin testing by writing unit tests for existing code, focusing on the object 
level.  Start by writing tests for code in the ‘LmServer.legion’ module, as it 
is new and still being developed.  These should be unit tests focusing on the 
functions and objects provided in these modules and should test for their 
successful operation with intended inputs as well as unexpected.  Aim for 
complete test coverage so that we can tell quickly if a new bug has been 
created.    The next unit tests should focus on analysis code and ensuring that 
the outputs produced are what is expected.  While it may not be possible to test 
that an output is a specific value if the process is non-deterministic, some 
basic sanity checks can be performed to determine if the output at least looks 
to be correct.

Following unit tests, write basic workflow tests.  These tests should start from 
the backend object level and should test different workflows as they are seen 
from LmServer side objects.  These tests should work with the Scribe, legion 
objects, SDM objects, etc. to ensure that the backend processes involved in 
accepting, retrieving, updating, deleting, calculating, etc. of data and 
objects works correctly from the server side perspective.  Eventually, move up 
a level to work with web services (but behind CherryPy) to run the same 
workflows (with new tests) to ensure that the services code also works as we 
expect.  
