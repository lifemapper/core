"""Lifemapper common utilities

These are common utilities for all Lifemapper components
@license: gpl2
@copyright: Copyright (C) 2014, University of Kansas Center for Research

          Lifemapper Project, lifemapper [at] ku [dot] edu, 
          Biodiversity Institute,
          1345 Jayhawk Boulevard, Lawrence, Kansas, 66045, USA
   
          This program is free software; you can redistribute it and/or modify 
          it under the terms of the GNU General Public License as published by 
          the Free Software Foundation; either version 2 of the License, or (at 
          your option) any later version.
  
          This program is distributed in the hope that it will be useful, but 
          WITHOUT ANY WARRANTY; without even the implied warranty of 
          MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU 
          General Public License for more details.
  
          You should have received a copy of the GNU General Public License 
          along with this program; if not, write to the Free Software 
          Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 
          02110-1301, USA.
"""

#To build an egg (must have setuptools installed): python setup.py bdist_egg
#To install this egg: Copy the egg file into the python site-packages 
#                      directory or some other directory that will be found
#                      by python

classifiers = """\
Development Status :: 1 - Alpha
License :: Undetermined
Programming Language :: Python
Operating System :: Unix
Operating System :: Linux
"""

from setuptools import setup

doclines = __doc__.split("\n")

setup(name="common",
      version="1.0",
      maintainer="Lifemapper",
      maintainer_email="lifemapper@ku.edu",
      author="Aimee Stewart",
      author_email="Lifemapper@ku.edu",
      url="http://www.lifemapper.org",
      py_modules=['bbox', 'dataset', 'lmconstants', 'log', 'rasterdata', 'vectordata'],
      license="http://www.lifemapper.org",
      platforms = ["any"],
      description = doclines[0],
      classifiers = [_f for _f in classifiers.split("\n") if _f],
      long_description = "\n".join(doclines[2:]))
