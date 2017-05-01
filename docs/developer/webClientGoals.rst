
.. highlight:: rest

Countdown to Botany
===================
.. contents::  

.. _Setup Development Environment : docs/developer/developEnv.rst

UI Goals (* indicates functionality required AFTER Botany)
----------------------------------------------------------
#. Browse single species archive or user data

   #. Query and visualize one or more OccurrenceLayers, SDMProject layers, 
      using filters to find data
      
#. Request and retrieve a single species SDM computation

   #. Upload data and parameters in single or separate requests
   
      #. Species data: Occurrence CSV and Metadata files OR list of 
         OccurrenceLayer IDs
      #. Environmental Data: 
      
         #. modelScenario and projectionScenarios (scenario IDs or codes, 
            public or owned by user) OR 
         #. ScenarioPackage name (package present on server) OR
         #. ScenarioPackage (compressed file with layers and metadata file) (*)
         
      #. SDM parameters
      
         #. Algorithm using default parameters or user-specified parameters (*)
         #. Minimum points for modeling 
         
   #. Query and visualize outputs and retrieve data/metadata (above plus retrieve)
   
#. Define a Gridset, organization for a set of Matrices with a single Grid 
   definition.

   #. Upload Grid layer or definition
      
#. Request and retrieve a multi species PAM (BOOM assembly) for a Gridset.  
   Includes above plus:

   #. Upload data and parameters in single or separate requests
   
      #. Species layer
      #. Intersection parameters
      #. Masking data and/or definition (*) 
      
#. Request and retrieve PAM statistics.  Includes above plus:

   #. Upload parameters for a PAM subset (requires Solr on backend)
   #. Upload parameters for RAD calculations (T/F on some computations)
   #. Retrieve and visualize matrix and individual computations
   
      #. Map site-matrix statistics
      #. Plot species-matrix statistics
      #. Display remaining stats in table or otherwise

#. Request and retrieve a GRIM (env Matrix, like PAM assembly) for a Gridset. (*)

   #. Upload data and parameters in single or separate requests
   
      #. Non-species layer
      #. Intersection parameters

#. Request and retrieve MCPA statistics.  Includes above plus:

   #. Identify, subset, or build PAM
   #. Identify, subset, or build GRIM
   #. Identify, subset, or upload tree for PAM 
   #. Identify or upload layer(s) and metadata for biogeographic hypotheses
   #. Define parameters, 
   #. Retrieve and visualize matrix and individual computations
   
      #. Map site-matrix statistics
      #. Plot species-matrix statistics
      #. Display remaining stats in table or otherwise
      
      

      