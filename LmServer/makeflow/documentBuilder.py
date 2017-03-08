"""
@summary: Module containing Lifemapper Makeflow document builder
@author: CJ Grady
@status: beta
@version: 1.0
@license: gpl2
@copyright: Copyright (C) 2017, University of Kansas Center for Research

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

from LmServer.base.lmobj import LMObject
from LmServer.makeflow.cmd import MfRule

# .............................................................................
class LMMakeflowDocument(LMObject):
   """
   @summary: Class used to generate a Makeflow document with Lifemapper 
                computational jobs
   """
   # ...........................
   def __init__(self, headers=None):
      """
      @summary: Constructor
      @param headers: If provided, this should be a list of (header, value) 
                         tuples
      """
      self.jobs = []
      self.targets = []
      self.headers = []
      if headers is not None:
         self.addHeaders(headers)
   
   # ...........................
   def _addJobCommand(self, outputs, cmd, dependencies=[], comment=''):
      """
      @summary: Adds a job command to the document
      @param outputs: A list of output files created by this job
      @param cmd: The command to execute
      @param dependencies: A list of dependencies (files that must exist before 
                              this job can run
      """
      job = "# -- {comment}\n{outputs}: {dependencies}\n\t{cmd}\n".format(
         outputs=' '.join(outputs), 
         cmd=cmd, comment=comment, 
         dependencies=' '.join(dependencies))
      self.jobs.append(job)
      # Add the new targets to self.targets
      self.targets.extend(outputs)
   
   # ...........................
   def addCommands(self, ruleList):
      """
      @summary: Adds a list of commands to the Makeflow document
      @param ruleList: A list of MfRule objects
      """
      # Check if this is just a single tuple, if so, make it a list
      if isinstance(ruleList, MfRule):
         ruleList = [ruleList]
         
      # For each tuple in the list
      for rule in ruleList:
         deps = rule.dependencies
         targets = rule.targets
         cmd = rule.command
         comment = rule.comment

         # Check to see if these targets are already defined by creating a new
         #    list of targets that are not in self.targets
         newTargets = [t for t in targets if t not in self.targets]
         
         # If there are targets that have not been defined before
         if len(newTargets) > 0:
            self._addJobCommand(newTargets, cmd, dependencies=deps, 
                                comment=comment)
   
   # ...........................
   def addHeaders(self, headers):
      """
      @summary: Adds headers to the document
      @param headers: A list of (header, value) tuples
      """
      if isinstance(headers, tuple):
         headers = [headers]
      self.headers.extend(headers)
   
   # ...........................
   def write(self, filename):
      """
      @summary: Write the document to the specified location
      @param filename: The file location to write this document
      @raise ValueError: If no jobs exist to be computed (list is right type, 
                            empty is bad value)
      @note: May fail with IOError if there is a problem writing to a location
      """
      if not self.jobs:
         raise ValueError("No jobs to be computed, fail for empty document")

      with open(filename, 'w') as outF:
         for header, value in self.headers:
            outF.write("{header}={value}\n".format(header=header, value=value))
         for job in self.jobs:
            # These have built-in newlines
            outF.write(job) 
      
