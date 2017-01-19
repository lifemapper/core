
.. highlight:: rest

General coding standards and best practices for Lifemapper
==========================================================
.. contents::  


************
Organization
************
#. LM module has code that is shared by various modules:

   #. LmBackend is used by Server and Compute
   #. LmCommon is shared everywhere, backend and clients
   #. LmCompute contains code for running jobs with SGE and Makeflow, and 
      calculations
   #. LmDbServer contains database initialization scripts and boom/pipeline code
   #. LmTest contains testing code
   #. LmServer contains the bulk of base objects, tools, utilities for the Server
   #. LmWebServer contains code for web services and the LMDashboard application

LM-specific best practices
**************************
#. Update deprecated code to the current version standard (i.e. string 
   formatting) 
#. Note when Python code is known to be deprecated in the next version 
   (Python 3+) (i.e. calling super class functions)
#. Note new dependencies 3rd party libraries, avoid when possible.  When 
   necessary, notify the developer working on installation packages to ensure 
   the dependency is included in packaging code.

   
Coding conventions
******************
#. Use meaningful variable names
#. Document code that is not self-explanatory (i.e. object.updateStatus)
#. 
#. Use style, naming, and coding conventions at 
   http://www.python.org/dev/peps/pep-0008/ .  We have several exceptions that
   we may change in the future.  Look at existing project code and follow 
   standards in place.  If big inconsistencies are discovered, bring 
   it to the team's attention and we can decide on the course of action. 
   
   * In particular, read the following sections:
   
      #. Imports
      #. Comments
      #. 
      
   * Some LM exceptions:
   
      #. Indent 3 spaces (not 4)
      #. Use mixed case as defined in the PEP 8 document (not lowercase with 
         underscores) for function and method names
      #. Comments must be understandable but are not required to be full sentences.
      
      
#. Read the following, but follow Pep 8 when there is a conflict.  All these 
   links provide good programming tips too.

   #. http://jaynes.colorado.edu/PythonIdioms.html
   #. http://jaynes.colorado.edu/PythonGuidelines.html
   #. http://seesar.lbl.gov/ANAG/staff/sternberg/python-coding-standards.html
   #. http://lists.osafoundation.org/pipermail/dev/2003-March/000479.html
   #. Use simple attribute access for public attributes of classes (rather than 
      getter-setter).  Use properties to hide functional implementations behind 
      attribute access syntax: 
      http://www.python.org/download/releases/2.2.3/descrintro/#property
      
#. Do not hardcode parameters - create a module they can be read from

   #. consider looking into some framework for passing in project parameters
   
#. Use modified Julian date for timestamps in python (using mxDateTime), store 
   as real in database
#. Use the Lifemapper logging extensions for system logging. 

Source Control - Github
***********************

#. https://github.com/lifemapper/
#. Basic git explanations:  https://git-scm.com/book/en/v2/

Databases
*********

#. Always create databases with encoding UTF8

Testing (CJ has a new infrastructure, document it here)
*******************************************************

#. Use the unittest module of PyUnit http://pyunit.sourceforge.net/pyunit.html
   There is a good intro at that link 

Documentation
*************
#. Use docstring conventions at http://www.python.org/dev/peps/pep-0257/
#. (revisit doc generation tools)
   http://engtech.wordpress.com/2007/03/20/automatic_documentation_python_doxygen/ 
   for doxygen documentation generation
