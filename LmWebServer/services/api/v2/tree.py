"""This module provides REST services for trees"""
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
@cherrypy.popargs('path_tree_id')
class TreeService(LmService):
    """This class is responsible for tree services.

    Note:
        * The dispatcher is responsible for calling the correct method.
    """

    # ................................
    def DELETE(self, path_tree_id):
        """Attempts to delete a tree

        Args:
            path_tree_id (int) : The id of the tree to delete
        """
        tree = self.scribe.get_tree(tree_id=path_tree_id)

        if tree is None:
            raise cherrypy.HTTPError(
                HTTPStatus.NOT_FOUND, "Tree {} not found".format(path_tree_id))

        # If allowed to, delete
        if check_user_permission(self.get_user_id(), tree, HTTPMethod.DELETE):
            success = self.scribe.delete_object(tree)
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
    def GET(self, path_tree_id=None, limit=100, offset=0, name=None,
            is_binary=None, is_ultrametric=None, has_branch_lengths=None,
            meta_string=None, after_time=None, before_time=None, url_user=None,
            **params):
        """Attempts to retrieve a tree or list of trees matching parameters

        Todo:
            Should we add the following parameters?
                - minimum number of tips?
        """
        if path_tree_id is None:
            return self._list_trees(
                self.get_user_id(url_user=url_user), after_time=after_time,
                before_time=before_time, is_binary=is_binary,
                is_ultrametric=is_ultrametric,
                has_branch_lengths=has_branch_lengths, limit=limit,
                meta_string=meta_string, name=name, offset=offset)

        if path_tree_id.lower() == 'count':
            return self._count_trees(
                self.get_user_id(url_user=url_user), after_time=after_time,
                before_time=before_time, is_binary=is_binary,
                is_ultrametric=is_ultrametric,
                has_branch_lengths=has_branch_lengths, meta_string=meta_string,
                name=name)

        return self._get_tree(path_tree_id)

    # ................................
    @lm_formatter
    def POST(self, name=None, tree_schema=DEFAULT_TREE_SCHEMA, **params):
        """Posts a new tree

        Todo:
            * Format
        """
        if name is None:
            raise cherrypy.HTTPError(
                HTTPStatus.BAD_REQUEST, 'Must provide name for tree')
        tree = dendropy.Tree.get(
            file=cherrypy.request.body, schema=tree_schema)

        new_tree = Tree(name, user_id=self.get_user_id())
        updated_tree = self.scribe.find_or_insert_tree(new_tree)
        updated_tree.setTree(tree)
        updated_tree.writeTree()
        updated_tree.mod_time = gmt().mjd
        self.scribe.update_object(updated_tree)

        return updated_tree

    # ................................
    def _count_trees(self, user_id, after_time=None, before_time=None,
                     is_binary=None, is_ultrametric=None,
                     has_branch_lengths=None, meta_string=None, name=None):
        """Counts the tree objects matching the specified criteria

        Args:
            user_id (str) THe user to count trees for.  Note that this may not
                be the same user logged into the system.
            after_time (MJD float) : Return trees modified after this time.
            before_time (MJD float) : Return trees modified before this time.
            is_binary (bool) : Only return trees that are binary.
            is_ultrametric (bool) : Only return trees that are ultrametric.
            has_branch_lengths (bool) : Only return trees that have branch
                lengths.
            meta_string () : ?
            name (str) : Return trees with this name.
        """
        tree_count = self.scribe.count_trees(
            user_id=user_id, name=name, is_binary=is_binary,
            is_ultrametric=is_ultrametric,
            has_branch_lengths=has_branch_lengths,
            meta_string=meta_string, after_time=after_time,
            before_time=before_time)

        return {'count': tree_count}

    # ................................
    def _get_tree(self, path_tree_id):
        """Attempt to get a tree

        Args:
            path_tree_id (int) : The database ID of the tree to retrieve.
        """
        tree = self.scribe.get_tree(tree_id=path_tree_id)
        if tree is None:
            raise cherrypy.HTTPError(
                HTTPStatus.NOT_FOUND, 'Tree {} was not found'.format(
                    path_tree_id))
        if check_user_permission(self.get_user_id(), tree, HTTPMethod.GET):
            return tree

        raise cherrypy.HTTPError(
            HTTPStatus.FORBIDDEN,
            'User {} does not have permission to access tree {}'.format(
                self.get_user_id(), path_tree_id))

    # ................................
    def _list_trees(self, user_id, after_time=None, before_time=None,
                    is_binary=None, is_ultrametric=None,
                    has_branch_lengths=None, limit=100, meta_string=None,
                    name=None, offset=0):
        """Lists tree objects matching the specified criteria.

        Args:
            user_id (str) THe user to count trees for.  Note that this may not
                be the same user logged into the system.
            after_time (MJD float) : Return trees modified after this time.
            before_time (MJD float) : Return trees modified before this time.
            is_binary (bool) : Only return trees that are binary.
            is_ultrametric (bool) : Only return trees that are ultrametric.
            has_branch_lengths (bool) : Only return trees that have branch
                lengths.
            limit (int) : The maximum number of trees to return.
            meta_string () : ?
            name (str) : Return trees with this name.
            offset (int) : Start returning trees this many from the first.
        """
        tree_atoms = self.scribe.list_trees(
            offset, limit, user_id=user_id, name=name, is_binary=is_binary,
            is_ultrametric=is_ultrametric,
            has_branch_lengths=has_branch_lengths, meta_string=meta_string,
            after_time=after_time, before_time=before_time)
        return tree_atoms
