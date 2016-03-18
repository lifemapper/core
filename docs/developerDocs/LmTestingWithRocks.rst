##############################
For a development installation
##############################


To set up working LmServer, LmCompute modules with editable LM code::

#. Install latest Rocks installation, including rolls
#. Install latest LmServer and LmCompute rolls
#. Create workspace outside of home directory (/state/partition1/workspace)
#. In workspace 
   #. Checkout Lm source code (https://github.com/lifemapper/core)
   #. LmServer roll code from Github (https://github.com/pragmagrid/lifemapper-server)
   #. LmCompute roll code from Github (https://github.com/pragmagrid/lifemapper-compute)
#. Add lmwriter group to the local user account
   #. Change group (chgrp -R lmwriter <dirname>) for Lifemapper code under 
      workspace
   #. Add group write/create permissions (chmod -R g+ws <dirname>) for Lm 
      code under workspace
#. Under original installation /opt/lifemapper
   #. Delete or rename components 
   #. Add sym links to components in workspace code: Lm*, config
   #. Follow instructions for updating LmServer installations:
      https://github.com/pragmagrid/lifemapper-server/blob/kutest/docs/Updating.rst
   #. Follow instructions for updating LmCompute installations:
      https://github.com/pragmagrid/lifemapper-compute/blob/kutest/docs/Updating.rst

To set up development environment::

#. Install Eclipse
   #. Add Pydev
