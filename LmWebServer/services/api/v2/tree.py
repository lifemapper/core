"""
@summary: This module provides REST services for trees
@author: CJ Grady
@version: 2.0
@status: alpha
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
import cherrypy
import dendropy
import mx.DateTime

from LmCommon.common.lmconstants import HTTPStatus, DEFAULT_TREE_SCHEMA
from LmServer.legion.tree import Tree
from LmWebServer.common.lmconstants import HTTPMethod
from LmWebServer.services.api.v2.base import LmService
from LmWebServer.services.common.accessControl import checkUserPermission
from LmWebServer.services.cpTools.lmFormat import lmFormatter

# .............................................................................
@cherrypy.expose
@cherrypy.popargs('pathTreeId')
class TreeService(LmService):
   """
   @summary: This class is for the tree service.  The dispatcher is 
                responsible for calling the correct method.
   """
   # ................................
   def DELETE(self, pathTreeId):
      """
      @summary: Attempts to delete a tree
      @param pathTreeId: The id of the tree to delete
      """
      tree = self.scribe.getTree(treeId=pathTreeId)

      if tree is None:
         raise cherrypy.HTTPError(404, "Tree {} not found".format(pathTreeId))
      
      # If allowed to, delete
      if checkUserPermission(self.getUserId(), tree, HTTPMethod.DELETE):
         success = self.scribe.deleteObject(tree)
         if success:
            cherrypy.response.status = 204
            return 
         else:
            # TODO: How can this happen?  Make sure we catch those cases and 
            #          respond appropriately.  We don't want 500 errors
            raise cherrypy.HTTPError(500, 
                        "Failed to delete tree")
      else:
         raise cherrypy.HTTPError(403, 
                 "User does not have permission to delete this tree")

   # ................................
   @lmFormatter
   def GET(self, pathTreeId=None, limit=100, offset=0, name=None, 
           isBinary=None, isUltrametric=None, hasBranchLengths=None, 
           metaString=None, afterTime=None, beforeTime=None, urlUser=None):
      """
      @summary: Performs a GET request.  If a tree id is provided,
                   attempt to return that item.  If not, return a list of 
                   trees that match the provided parameters
      @todo: Should we add the following parameters?
                - is ultrametric
                - is binary
                - minimum number of tips?
                - taxonomic or phylogenetic?
      """
      if pathTreeId is None:
         return self._listTrees(self.getUserId(urlUser=urlUser), 
                     afterTime=afterTime, beforeTime=beforeTime, 
                     isBinary=isBinary, isUltrametric=isUltrametric, 
                     hasBranchLengths=hasBranchLengths, limit=limit, 
                     metaString=metaString, name=name, offset=offset)
      elif pathTreeId.lower() == 'count':
         return self._countTrees(self.getUserId(urlUser=urlUser), 
                     afterTime=afterTime, beforeTime=beforeTime, 
                     isBinary=isBinary, isUltrametric=isUltrametric, 
                     hasBranchLengths=hasBranchLengths, metaString=metaString, 
                     name=name)
      else:
         return self._getTree(pathTreeId)
      
   # ................................
   @lmFormatter
   def POST(self, name=None, treeSchema=DEFAULT_TREE_SCHEMA):
      """
      @summary: Posts a new tree
      @todo: Format
      """
      if name is None:
         raise cherrypy.HTTPError(HTTPStatus.BAD_REQUEST, 'Must provide name for tree')
      tree = dendropy.Tree.get(file=cherrypy.request.body, schema=treeSchema)
      
      newTree = Tree(name, userId=self.getUserId())
      updatedTree = self.scribe.findOrInsertTree(newTree)
      updatedTree.setTree(tree)
      updatedTree.writeTree()
      updatedTree.modTime = mx.DateTime.gmt().mjd
      self.scribe.updateObject(updatedTree)
      
      return updatedTree
   
   # ................................
   def _countTrees(self, userId, afterTime=None, beforeTime=None, 
                   isBinary=None, isUltrametric=None, hasBranchLengths=None, 
                   metaString=None, name=None):
      """
      @summary: Count tree objects matching the specified criteria
      @param userId: The user to count trees for.  Note that this may not 
                        be the same user logged into the system
      @param afterTime: (optional) Return trees modified after this time 
                           (Modified Julian Day)
      @param beforeTime: (optional) Return trees modified before this time 
                            (Modified Julian Day)
      """
      treeCount = self.scribe.countTrees(userId=userId, name=name, isBinary=isBinary, 
                                        isUltrametric=isUltrametric, 
                                        hasBranchLengths=hasBranchLengths, 
                                        metastring=metaString, 
                                        afterTime=afterTime, 
                                        beforeTime=beforeTime)
      # Format return
      # Set headers
      return {"count" : treeCount}

   # ................................
   def _getTree(self, pathTreeId):
      """
      @summary: Attempt to get a tree
      """
      tree = self.scribe.getTree(treeId=pathTreeId)
      if tree is None:
         raise cherrypy.HTTPError(404, 
                        'Tree {} was not found'.format(pathTreeId))
      if checkUserPermission(self.getUserId(), tree, HTTPMethod.GET):
         return tree
      else:
         raise cherrypy.HTTPError(403, 
              'User {} does not have permission to access tree {}'.format(
                     self.getUserId(), pathTreeId))
   
   # ................................
   def _listTrees(self, userId, afterTime=None, beforeTime=None, isBinary=None, 
                     isUltrametric=None, hasBranchLengths=None, limit=100, 
                     metaString=None, name=None, offset=0):
      """
      @summary: List tree objects matching the specified criteria
      @param userId: The user to count trees for.  Note that this may not 
                        be the same user logged into the system
      @param afterTime: (optional) Return trees modified after this time 
                           (Modified Julian Day)
      @param beforeTime: (optional) Return trees modified before this time 
                            (Modified Julian Day)
      @param limit: (optional) Return this number of trees, at most
      @param offset: (optional) Offset the returned trees by this number
      """
      treeAtoms = self.scribe.listTrees(offset, limit, userId=userId, 
                                        name=name, isBinary=isBinary, 
                                        isUltrametric=isUltrametric, 
                                        hasBranchLengths=hasBranchLengths, 
                                        metastring=metaString, 
                                        afterTime=afterTime, 
                                        beforeTime=beforeTime)
      return treeAtoms
