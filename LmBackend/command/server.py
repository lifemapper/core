"""
@summary: This module contains command objects for server processes
@author: CJ Grady
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
import os

from LmBackend.command.base import _LmCommand

from LmBackend.common.lmconstants import CMD_PYBIN, SERVER_SCRIPTS_DIR

# .............................................................................
class AddBioGeoAndTreeCommand(_LmCommand):
   """
   @summary: This command will add biogeographic hypotheses and a tree to a 
                grid set
   """
   # ................................
   def __init__(self, gridsetId, hypothesesFilenames, treeFilename=None, 
                      treeName=None, eventField=None):
      """
      @summary: Construct the command object
      @param gridsetId: The database id of the gridset to add to
      @param hypothesesFilenames: A list of file locations of hypothesis 
                                     shapefiles
      @param treeFilename: The file location of the JSON tree to add to the 
                              gridset
      @param treeName: If a tree is provided, this is the name of the tree
      @param eventField: The name of the event field in the hypotheses 
                            shapefiles
      """
      _LmCommand.__init__(self)
      
      self.args = [gridsetId]
      if isinstance(hypothesesFilenames, list):
         self.inputs.extend(hypothesesFilenames)
         self.args.extend(hypothesesFilenames)
      else:
         self.inputs.append(hypothesesFilenames)
         self.args.append(hypothesesFilenames)

      self.optArgs = ''
      if treeFilename is not None:
         self.optArgs += ' -t {}'.format(treeFilename)
         self.inputs.append(treeFilename)
      
      if treeName is not None:
         self.optArgs += ' -tn {}'.format(treeName)
         
      if eventField is not None:
         self.optArgs += ' -e {}'.format(eventField)

   # ................................
   def getCommand(self):
      """
      @summary: Get the raw command to run on the system
      """
      return '{} {} {} {}'.format(CMD_PYBIN, 
            os.path.join(SERVER_SCRIPTS_DIR, 'addBioGeoAndTree.py'),
            self.optArgs, ' '.join(self.args))

# .............................................................................
class CreateConvexHullShapefileCommand(_LmCommand):
   """
   @summary: This command will write a shapefile containing a feature with the 
                convex hull of the occurrence set
   """
   # ................................
   def __init__(self, occId, outFilename, bufferDistance=None):
      """
      @summary: Construct the command object
      @param occId: The database id of the occurrence set to use
      @param outFilename: The file location to write the shapefile
      @param bufferDistance: A buffer, in map units, to include with the convex hull
      """
      _LmCommand.__init__(self)
      self.args = '{} {}'.format(occId, outFilename)
      self.optArgs = ''
      if bufferDistance is not None:
         self.optArgs += ' -b {}'.format(bufferDistance)
      
   # ................................
   def getCommand(self):
      """
      @summary: Get the raw command to run on the system
      """
      return '{} {} {} {}'.format(CMD_PYBIN, 
            os.path.join(SERVER_SCRIPTS_DIR, 'create_convex_hull_shapefile.py'),
            self.optArgs, self.args)

# .............................................................................
class IndexPAVCommand(_LmCommand):
   """
   @summary: This command will post PAV information to a solr index
   """
   # ................................
   def __init__(self, pavFilename, pavId, projId, pamId, pavIdxFilename):
      """
      @summary: Construct the command object
      @param pavFilename: The file location of the PAV matrix
      @param pavId: The database id of the PAV
      @param projId: The database id of the projection used to build the PAV
      @param pamId: The database id of the PAM that the PAV belongs to
      @param pavIdxFilename: The file location to write the POST data
      """
      _LmCommand.__init__(self)
      self.inputs.append(pavFilename)
      self.outputs.append(pavIdxFilename)
      
      self.args = [pavFilename, str(pavId), str(projId), 
                   str(pamId), pavIdxFilename]

   # ................................
   def getCommand(self):
      """
      @summary: Get the raw command to run on the system
      """
      return '{} {} {}'.format(CMD_PYBIN, 
            os.path.join(SERVER_SCRIPTS_DIR, 'indexPAV.py'),
            ' '.join(self.args))

# .............................................................................
class LmTouchCommand(_LmCommand):
   """
   @summary: This command will touch a file, creating necessary directories  
   """
   # ................................
   def __init__(self, filename):
      """
      @summary: Construct the command object
      @param filename: The file location to touch
      """
      _LmCommand.__init__(self)
      self.outputs.append(filename)
      self.filename = filename

   # ................................
   def getCommand(self):
      """
      @summary: Get the raw command to run on the system
      """
      return '{} {} {}'.format(CMD_PYBIN, 
            os.path.join(SERVER_SCRIPTS_DIR, 'lmTouch.py'),
            self.filename)

# .............................................................................
class ShootSnippetsCommand(_LmCommand):
   """
   @summary: This command will shoot snippets into an index
   """
   # ................................
   def __init__(self, occSetId, operation, postFilename, o2ident=None, 
                      url=None, who=None, agent=None, why=None):
      """
      @summary: Construct the command object
      @param occSetId: The occurrence set id to generate snippets for
      @param operation: The operation performed (see SnippetOperations)
      @param postFilename: The file location to store the data posted
      @param o2ident: An identifier for an optional target object
      @param url: A URL associated with this action
      @param who: Who initiated this action
      @param agent: The agent used to initiate this action
      @param why: Why this action was initiated
      """
      _LmCommand.__init__(self)
      self.outputs.append(postFilename)
      
      self.args = [str(occSetId), operation, postFilename]
      self.optArgs = ''
      if o2ident is not None:
         self.optArgs += ' -o2ident {}'.format(o2ident)
      
      if url is not None:
         self.optArgs += ' -url {}'.format(url)
         
      if who is not None:
         self.optArgs += ' -who {}'.format(who)
         
      if agent is not None:
         self.optArgs += ' -agent {}'.format(agent)
         
      if why is not None:
         self.optArgs += ' -why {}'.format(why)

   # ................................
   def getCommand(self):
      """
      @summary: Get the raw command to run on the system
      """
      return '{} {} {} {}'.format(CMD_PYBIN, 
            os.path.join(SERVER_SCRIPTS_DIR, 'shootSnippets.py'),
            self.optArgs, ' '.join(self.args))

# .............................................................................
class SquidIncCommand(_LmCommand):
   """
   @summary: This command will add squids to a tree
   """
   # ................................
   def __init__(self, treeFilename, userId, outTreeFilename):
      """
      @summary: Construct the command object
      @param treeFilename: The file location of the original tree
      @param userId: The user id, used for generating squids
      @param outTreeFilename: The file location of the resulting tree
      """
      _LmCommand.__init__(self)
      self.inputs.append(treeFilename)
      self.outputs.append(outTreeFilename)
      
      self.args = [treeFilename, userId, outTreeFilename]

   # ................................
   def getCommand(self):
      """
      @summary: Get the raw command to run on the system
      """
      return '{} {} {}'.format(CMD_PYBIN, 
            os.path.join(SERVER_SCRIPTS_DIR, 'squid_inc.py'),
            ' '.join(self.args))

# .............................................................................
class StockpileCommand(_LmCommand):
   """
   @summary: This command will stockpile the outputs of a process
   """
   # ................................
   def __init__(self, pType, objectId, successFilename, objOutputFilenames, 
                status=None, statusFilename=None):
      """
      @summary: Construct the command object
      @param pType: The process type of the outputs
      @param objectId: The id of the object
      @param successFilename: The file location of the output file to create if 
                                 the process is successful
      @param objOutputFilenames: A list of object files to test
      @param status: An optional object status to update the database with
      @param statusFilename: The location of a file that contains status 
                                information for the object
      """
      _LmCommand.__init__(self)
      
      self.args = [str(pType), str(objectId), successFilename]
      self.outputs.append(successFilename)
      
      if isinstance(objOutputFilenames, list):
         self.args.extend(objOutputFilenames)
         self.inputs.extend(objOutputFilenames)
      else:
         self.inputs.append(objOutputFilenames)
         self.args.append(objOutputFilenames)
      
      self.optArgs = ''
      if status is not None:
         self.optArgs += ' -s {}'.format(status)
         
      if statusFilename is not None:
         self.optArgs += ' -f {}'.format(statusFilename)
         self.inputs.append(statusFilename)

   # ................................
   def getCommand(self):
      """
      @summary: Get the raw command to run on the system
      """
      return '{} {} {} {}'.format(CMD_PYBIN, 
            os.path.join(SERVER_SCRIPTS_DIR, 'stockpile.py'),
            self.optArgs, ' '.join(self.args))

# .............................................................................
class TriageCommand(_LmCommand):
   """
   @summary: This command will determine which files referenced in the input
                file exist and will output a file containing those references
   """
   # ................................
   def __init__(self, inFilename, outFilename):
      """
      @summary: Construct the command object
      @param inFilename: The file location of a file containing a list of 
                            potential target filenames
      @param outFilename: The file location to write the output file indicating
                             which of the potential targets actually exist
      """
      _LmCommand.__init__(self)
      self.inputs.append(inFilename)
      self.outputs.append(outFilename)
      
      self.args = [inFilename, outFilename]

   # ................................
   def getCommand(self):
      """
      @summary: Get the raw command to run on the system
      """
      return '{} {} {}'.format(CMD_PYBIN, 
            os.path.join(SERVER_SCRIPTS_DIR, 'triage.py'),
            ' '.join(self.args))

