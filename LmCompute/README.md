LmCompute
=========

The Lifemapper compute package interacts with the Lifemapper job server.  It 
provides a pluggable framework for different types of Lifemapper jobs.  The 
base compute package includes a client for interacting with the job server and
comes with two computational job plug-ins for openModeller and MaxEnt 
Lifemapper computation jobs.

In any other plug-ins become available, they can be dropped into the "plugins"
directory where they will be automatically registered with the framework.

