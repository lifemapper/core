"""
@summary: Module containing the Lifemapper Test Factory class
@author: CJ Grady
@license: gpl2
@copyright: Copyright (C) 2018, University of Kansas Center for Research

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
# .............................................................................
class LMTestFactory(object):
   """
   @summary: The Lifemapper Test Factory processes test configuration files and
                selects an appropriate test builder to build a test for each.
                It has default available test builders but allows for new test
                builders to be added.
   """
   # ...........................
   def __init__(self, builders, **kwargs):
      self.builders = {}
      for builder in builders:
         print kwargs
         self.builders[builder.name.lower()] = builder(**kwargs)

   # ...........................
   def getTests(self, tObj):
      """
      @summary: Gets a test object by selecting the appropriate test builder 
                   and giving it the deserialized test object data
      """
      if self.builders.has_key(tObj.testType.lower()):
         return self.builders[tObj.testType.lower()].buildTests(tObj)
