
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
#. Naming conventions, definitions in PEP 8, Naming Conventions:

   #. Variables and method names use "mixed case" names.
   #. Class names use CamelCase 
   #. Internal code tree (module) directories, short, all lowercase names

#. Use consistent naming for similar variables, attributes, functions and 
   methods 
   
#. Use style, documentation, naming, and coding conventions at 
   http://www.python.org/dev/peps/pep-0008/ .  We have several exceptions that
   we may change in the future.  Look at existing project code and follow 
   standards in place.  If big inconsistencies are discovered, bring 
   it to the team's attention and we can decide on the course of action. 
   
   #. In particular, read the following sections:
   
      #. Programming Recommendations
      #. Comments
      #. Whitespace
      #. Imports
      
   #. Some LM exceptions:
   
      #. Indent 3 spaces (not 4)
      #. Use mixed case as defined in the PEP 8 document for function and 
         method names
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
       
#. Do not hardcode parameters - use existing constants in common.lmconstants 
   modules.  If constants are needed in new modules that will never be shared, 
   create a common.lmconstants file in the new module
   
#. Use modified Julian date for timestamps in python (using mxDateTime), store 
   as real in database
#. Use the Lifemapper logging extensions for system logging. 

Documentation
*************
#. Document code that is not self-explanatory (i.e. object.updateStatus)
#. Start and end multi-line documentation at the beginning of a class or method
   with lines containing """ and nothing else
#. Use docstring conventions at http://www.python.org/dev/peps/pep-0257/
#. Reference superclass documentation in subclass docstrings.
#. (revisit doc generation tools)
   http://engtech.wordpress.com/2007/03/20/automatic_documentation_python_doxygen/ 
   for doxygen documentation generation
   
Source Control
***********************

#. Use Github for source control
#. Check in every logical or functional chunk of code you work on, but never
   less than once per day if coding, generally many times.  
#. Check code into the main trunk of code, or a branch.
#. Branch if you are working on unfinished or broken code that will impact 
   other team members.
#. If code is self-contained, or not called, it may be checked in unfinished.
#. Primary lifemapper code repositories are at https://github.com/lifemapper/
#. Basic git explanations:  https://git-scm.com/book/en/v2/

Databases
*********

#. Always create databases with encoding UTF8

Testing (CJ has a new infrastructure, document it here)
*******************************************************

#. Use the unittest module of PyUnit http://pyunit.sourceforge.net/pyunit.html
   There is a good intro at that link 

