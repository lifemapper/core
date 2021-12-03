"""This module provides REST services for trees"""
from flask import Response
from http import HTTPStatus
import werkzeug.exceptions as WEXC

from LmCommon.common.lmconstants import DEFAULT_TREE_SCHEMA
from LmCommon.common.time import gmt
from LmServer.legion.tree import Tree
from LmWebServer.common.lmconstants import HTTPMethod
from LmWebServer.services.api.v2.base import LmService
from LmWebServer.services.common.access_control import check_user_permission
from LmWebServer.services.cp_tools.lm_format import lm_formatter
from lmpy.tree import TreeWrapper


# .............................................................................
class TreeService(LmService):
    """This class is responsible for tree services."""

    # ................................
    def delete_tree(self, user_id, tree_id):
        """Attempts to delete a tree

        Args:
            tree_id (int) : The id of the tree to delete
        """
        tree = self.scribe.get_tree(tree_id=tree_id)

        if tree is None:
            raise WEXC.NotFound('Tree {} not found'.format(tree_id))

        # If allowed to, delete
        if check_user_permission(user_id, tree, HTTPMethod.DELETE):
            success = self.scribe.delete_object(tree)
            if success:
                return Response(status=HTTPStatus.NO_CONTENT)

            raise WEXC.InternalServerError('Failed to delete tree')

        raise WEXC.Forbidden('User {} does not have permission to delete tree {}'.format(user_id, tree_id))

    # ................................
    @lm_formatter
    def post_tree(self, user_id, name=None, tree_data=None, tree_schema=DEFAULT_TREE_SCHEMA, **params):
        """Posts a tree
        
        Args:
            name (str): human-readable name for the tree
            tree_data (str): tree data in JSON format
            tree_schema (str): format for the tree data, default is nexus
        """
        if name is None:
            raise WEXC.BadRequest('Must provide name for tree')
        tree = TreeWrapper.get(file=tree_data, schema=tree_schema)

        new_tree = Tree(name, user_id=user_id)
        updated_tree = self.scribe.find_or_insert_tree(new_tree)
        updated_tree.set_tree(tree)
        updated_tree.write_tree()
        updated_tree.mod_time = gmt().mjd
        self.scribe.update_object(updated_tree)

        return updated_tree

    # ................................
    @lm_formatter
    def count_trees(self, user_id, after_time=None, before_time=None,
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
            user_id=user_id, name=name, is_binary=is_binary, is_ultrametric=is_ultrametric,
            has_branch_lengths=has_branch_lengths, meta_string=meta_string, after_time=after_time,
            before_time=before_time)

        return {'count': tree_count}

    # ................................
    @lm_formatter
    def get_tree(self, user_id, tree_id):
        """Attempt to get a tree

        Args:
            tree_id (int) : The database ID of the tree to retrieve.
        """
        tree = self.scribe.get_tree(tree_id=tree_id)
        if tree is None:
            raise WEXC.NotFound('Tree {} was not found'.format(tree_id))
        
        if check_user_permission(user_id, tree, HTTPMethod.GET):
            return tree

        raise WEXC.Forbidden('User {} does not have permission to access tree {}'.format(
                user_id, tree_id))

    # ................................
    @lm_formatter
    def list_trees(
        self, user_id, after_time=None, before_time=None, is_binary=None, is_ultrametric=None,
        has_branch_lengths=None, meta_string=None, name=None, offset=0, limit=100):
        """Lists tree objects matching the specified criteria.

        Args:
            user_id (str): The user to list trees for.  Note that this may not
                be the same user logged into the system.
            after_time (MJD float) : Return trees modified after this time.
            before_time (MJD float) : Return trees modified before this time.
            is_binary (bool) : Only return trees that are binary.
            is_ultrametric (bool) : Only return trees that are ultrametric.
            has_branch_lengths (bool) : Only return trees that have branch lengths.
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
