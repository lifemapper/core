#! /usr/bin/env python
# -*- coding: utf-8 -*-
"""This module provides REST services for trees
"""
import cherrypy
import dendropy

from LmCommon.common.lmconstants import HTTPStatus, DEFAULT_TREE_SCHEMA
from LmCommon.common.time import gmt
from LmServer.legion.tree import Tree
from LmWebServer.common.lmconstants import HTTPMethod
from LmWebServer.services.api.v2.base import LmService
from LmWebServer.services.common.access_control import check_user_permission
from LmWebServer.services.cp_tools.lm_format import lm_formatter


# .............................................................................
@cherrypy.expose
@cherrypy.popargs('pathTreeId')
class TreeService(LmService):
    """This class is responsible for tree services.

    Note:
        * The dispatcher is responsible for calling the correct method.
    """
    # ................................
    def DELETE(self, pathTreeId):
        """Attempts to delete a tree

        Args:
            pathTreeId (int) : The id of the tree to delete
        """
        tree = self.scribe.getTree(treeId=pathTreeId)

        if tree is None:
            raise cherrypy.HTTPError(
                HTTPStatus.NOT_FOUND, "Tree {} not found".format(pathTreeId))

        # If allowed to, delete
        if check_user_permission(self.get_user_id(), tree, HTTPMethod.DELETE):
            success = self.scribe.deleteObject(tree)
            if success:
                cherrypy.response.status = HTTPStatus.NO_CONTENT
                return

            raise cherrypy.HTTPError(
                HTTPStatus.INTERNAL_SERVER_ERROR, 'Failed to delete tree')

        raise cherrypy.HTTPError(
            HTTPStatus.FORBIDDEN,
            'User does not have permission to delete this tree')

    # ................................
    @lm_formatter
    def GET(self, pathTreeId=None, limit=100, offset=0, name=None,
            isBinary=None, isUltrametric=None, hasBranchLengths=None,
            metaString=None, afterTime=None, beforeTime=None, urlUser=None,
            **params):
        """Attempts to retrieve a tree or list of trees matching parameters

        Todo:
            Should we add the following parameters?
                - minimum number of tips?
        """
        if pathTreeId is None:
            return self._list_trees(
                self.get_user_id(urlUser=urlUser), afterTime=afterTime,
                beforeTime=beforeTime, isBinary=isBinary,
                isUltrametric=isUltrametric, hasBranchLengths=hasBranchLengths,
                limit=limit, metaString=metaString, name=name, offset=offset)

        if pathTreeId.lower() == 'count':
            return self._count_trees(
                self.get_user_id(urlUser=urlUser), afterTime=afterTime,
                beforeTime=beforeTime, isBinary=isBinary,
                isUltrametric=isUltrametric, hasBranchLengths=hasBranchLengths,
                metaString=metaString, name=name)

        return self._get_tree(pathTreeId)

    # ................................
    @lm_formatter
    def POST(self, name=None, treeSchema=DEFAULT_TREE_SCHEMA, **params):
        """Posts a new tree

        Todo:
            * Format
        """
        if name is None:
            raise cherrypy.HTTPError(
                HTTPStatus.BAD_REQUEST, 'Must provide name for tree')
        tree = dendropy.Tree.get(file=cherrypy.request.body, schema=treeSchema)

        new_tree = Tree(name, userId=self.get_user_id())
        updated_tree = self.scribe.findOrInsertTree(new_tree)
        updated_tree.setTree(tree)
        updated_tree.writeTree()
        updated_tree.mod_time = gmt().mjd
        self.scribe.updateObject(updated_tree)

        return updated_tree

    # ................................
    def _count_trees(self, userId, afterTime=None, beforeTime=None,
                     isBinary=None, isUltrametric=None, hasBranchLengths=None,
                     metaString=None, name=None):
        """Counts the tree objects matching the specified criteria

        Args:
            userId (str) THe user to count trees for.  Note that this may not
                be the same user logged into the system.
            afterTime (MJD float) : Return trees modified after this time.
            beforeTime (MJD float) : Return trees modified before this time.
            isBinary (bool) : Only return trees that are binary.
            isUltrametric (bool) : Only return trees that are ultrametric.
            hasBranchLengths (bool) : Only return trees that have branch
                lengths.
            metaString () : ?
            name (str) : Return trees with this name.
        """
        tree_count = self.scribe.countTrees(
            userId=userId, name=name, isBinary=isBinary,
            isUltrametric=isUltrametric, hasBranchLengths=hasBranchLengths,
            metastring=metaString, afterTime=afterTime, beforeTime=beforeTime)

        return {'count': tree_count}

    # ................................
    def _get_tree(self, pathTreeId):
        """Attempt to get a tree

        Args:
            pathTreeId (int) : The database ID of the tree to retrieve.
        """
        tree = self.scribe.getTree(treeId=pathTreeId)
        if tree is None:
            raise cherrypy.HTTPError(
                HTTPStatus.NOT_FOUND, 'Tree {} was not found'.format(
                    pathTreeId))
        if check_user_permission(self.get_user_id(), tree, HTTPMethod.GET):
            return tree

        raise cherrypy.HTTPError(
            HTTPStatus.FORBIDDEN,
            'User {} does not have permission to access tree {}'.format(
                self.get_user_id(), pathTreeId))

    # ................................
    def _list_trees(self, userId, afterTime=None, beforeTime=None,
                    isBinary=None, isUltrametric=None, hasBranchLengths=None,
                    limit=100, metaString=None, name=None, offset=0):
        """Lists tree objects matching the specified criteria.

        Args:
            userId (str) THe user to count trees for.  Note that this may not
                be the same user logged into the system.
            afterTime (MJD float) : Return trees modified after this time.
            beforeTime (MJD float) : Return trees modified before this time.
            isBinary (bool) : Only return trees that are binary.
            isUltrametric (bool) : Only return trees that are ultrametric.
            hasBranchLengths (bool) : Only return trees that have branch
                lengths.
            limit (int) : The maximum number of trees to return.
            metaString () : ?
            name (str) : Return trees with this name.
            offset (int) : Start returning trees this many from the first.
        """
        tree_atoms = self.scribe.listTrees(
            offset, limit, userId=userId, name=name, isBinary=isBinary,
            isUltrametric=isUltrametric, hasBranchLengths=hasBranchLengths,
            metastring=metaString, afterTime=afterTime, beforeTime=beforeTime)
        return tree_atoms
