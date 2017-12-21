"""
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
from LmBackend.command.base import _LmCommand
from LmBackend.common.lmconstants import CMD_PYBIN, COMMON_SCRIPTS_DIR

# .............................................................................
class ChainCommand(_LmCommand):
   """
   @summary: The command chain class is used in cases where the execution of 
                one command is required before the next can start but these
                commands should be treated as one for organizational purposes
   """
   # ................................
   def __init__(self, cmdList):
      """
      @summary: Constructor, combines the list of commands into a single
                   executable statement
      """
      _LmCommand.__init__(self)
      
      # Set inputs and outputs
      inSet = set.union(*[set(c.inputs) for c in cmdList])
      outSet = set.union(*[set(c.outputs) for c in cmdList])
         
      self.outputs = list(outSet)
      self.inputs = list(inSet.difference(outSet))

      # Assemble commands
      self._cmd = ' ; '.join([c.getCommand() for c in cmdList])
   
   # ................................
   def getCommand(self):
      """
      @summary: Get the aggregated command for each of the instances
      """
      return self._cmd
   
# .............................................................................
class ConcatenateMatricesCommand(_LmCommand):
   """
   @summary: This command will concatenate a list of matrices based on the 
                specified axis
   """
   relDir = COMMON_SCRIPTS_DIR
   scriptName = 'concatenate_matrices.py'

   # ................................
   def __init__(self, matrices, axis, outMtxFilename, 
                      mashedPotatoFilename=None):
      """
      @summary: Construct the command object
      @param matrices: A list of zero or more matrix filenames to concatenate
      @param axis: The axis to concatenate the matrices on
      @param outMtxFilename: The output location of the resulting matrix
      @param mashedPotatoFilename: (optional) If present, get the input matrix 
                                      file names from this file instead 
      """
      _LmCommand.__init__(self)
      
      self.mtxFilenames = matrices
      self.axis = axis
      self.outMtxFilename = outMtxFilename
      self.mpFilename = mashedPotatoFilename
      
      self.inputs.extend(matrices)
      self.outputs.append(outMtxFilename)
      if mashedPotatoFilename is not None:
         self.outputs.append(mashedPotatoFilename)
         
   # ................................
   def getCommand(self):
      """
      @summary: Get the concatenate matrices command
      """
      optionalArgs = ''
      if self.mpFilename is not None:
         optionalArgs = '--mashedPotato={} '.format(self.mpFilename)
         
      posArgs = '{} {} {}'.format(self.outMtxFilename, self.axis, 
                                  ' '.join(self.mtxFilenames))
      
      return '{} {} {} {}'.format(CMD_PYBIN, self.getScript(), 
                  optionalArgs, posArgs)

# .............................................................................
class ModifyAsciiHeadersCommand(_LmCommand):
   """
   @summary: This command will reduce the number of decimal digits in ASCII
                headers
   """
   relDir = COMMON_SCRIPTS_DIR
   scriptName = 'modify_ascii_headers.py'

   # ................................
   def __init__(self, origAsciiFilename, modifiedAsciiFilename, digits=7):
      """
      @summary: Construct the command object
      @param origAsciiFilename: The original ASCII file
      @param modifiedAsciiFilename: The modified ASCII file
      @param digits: The number of decimal digits to keep
      """
      _LmCommand.__init__(self)
      
      self.args = '{} {}'.format(origAsciiFilename, modifiedAsciiFilename)
      self.optArgs = '-d {}'.format(digits)
      
      self.inputs.append(origAsciiFilename)
      self.outputs.append(modifiedAsciiFilename)
         
   # ................................
   def getCommand(self):
      """
      @summary: Get the concatenate matrices command
      """
      cmd = '{} {} {} {}'.format(CMD_PYBIN, self.getScript(), self.optArgs, 
                                 self.args)
      return cmd

# .............................................................................
class SystemCommand(_LmCommand):
   """
   @summary: This command will run a system command
   """
   # ................................
   def __init__(self, script, args, inputs=None, outputs=None):
      """
      @summary: Construct the command object
      @param script: The system script to run
      @param args: A list of arguments to pass to the script
      @param inputs: An optional list of input files needed by this script
      @param outputs: An optional list of output files generated by this script
      """
      _LmCommand.__init__(self)
      
      if inputs is not None:
         if isinstance(inputs, list):
            self.inputs.extend(inputs)
         else:
            self.inputs.append(inputs)
      
      if outputs is not None:
         if isinstance(outputs, list):
            self.outputs.extend(outputs)
         else:
            self.outputs.append(outputs)
         
      self.script = script
      self.args = args
      
   # ................................
   def getCommand(self):
      """
      @summary: Get the concatenate matrices command
      """
      return '{} {}'.format(self.script, self.args)
