"""
@summary: This script will convert a Nexus file into JSON
@author: Jeff Cavner (edited by CJ Grady)
@version: 1.0
@status: alpha

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

@todo: Consider using recursion to parse the tree instead of current approach
          
@note: Newick Definition: http://evolution.genetics.washington.edu/phylip/newick_doc.html

@note: From Wikipedia

The grammar nodes

   Tree: The full input Newick Format for a single tree
   Subtree: an internal node (and its descendants) or a leaf node
   Leaf: a node with no descendants
   Internal: a node and its one or more descendants
   BranchSet: a set of one or more Branches
   Branch: a tree edge and its descendant subtree.
   Name: the name of a node
   Length: the length of a tree edge.

The grammar rules

Note, "|" separates alternatives.

   Tree -> Subtree ";" | Branch ";"
   Subtree -> Leaf | Internal
   Leaf -> Name
   Internal -> "(" BranchSet ")" Name
   BranchSet -> Branch | Branch "," BranchSet
   Branch -> Subtree Length
   Name -> empty | string
   Length -> empty | ":" number

"""
import os
import re

from LmCommon.common.lmconstants import PhyloTreeKeys

# .............................................................................
class NewickParseError(Exception):
   """
   @summary: A wrapper around the base exception class for parsing errors
   """
   pass

# .............................................................................
class Parser(object):
   """
   @summary: Parse a Newick tree given a file handle.
   @note: We are not doing anything with confidence (values or comments)
   @note: We are not tracking if the tree is rooted
   @note: Based on the parser in `Bio.Nexus.Trees`.
   @todo: Consider renaming this
   """
   
   # ..............................   
   def __init__(self, newickString):
      """
      @summary: Construct a new parser from a Newick tree string
      @param newickString: A Newick tree as a string
      """
      self.newickString = newickString

      # parentsClade gets built with keys(pathId) for all nodes other than root, 
      #    since it doesn't have a parent.
      # nodeId's value is that node's parent clade and the entire sub tree from 
      #    that parent to it's tips
      self.parentClades = {}
      self.rootClade = {}
      tokens = [
         (r"\(",                                'open parens'),
         (r"\)",                                'close parens'),
         (r"[^\s\(\)\[\]\'\:\;\,]+",            'unquoted node label'),
         (r"\:[0-9]*\.?[0-9]+([eE][+\-]?\d+)?", 'edge length'),
         (r"\,",                                'comma'),
         (r"\[(\\.|[^\]])*\]",                  'comment'),
         (r"\'(\\.|[^\'])*\'",                  'quoted node label'),
         (r"\;",                                'semicolon'),
         (r"\n",                                'newline'),
      ]
      self.tokenizer = re.compile('(%s)' % '|'.join([token[0] for token in tokens]))
      
   # ..............................
   @classmethod
   def fromFileLikeObject(cls, flo):
      """
      @summary: Returns a Parser instance using the contents of a file-like 
                  object
      @param flo: A file-like object to read the tree from
      """
      newickString = flo.read()
      return cls(newickString)
   
   # ..............................
   @classmethod
   def fromFilename(cls, fn):
      """
      @summary: Returns a Parser instance using the contents of a file
      @param flo: A file-like object to read the tree from
      @raise IOError: If the file does not exist
      """
      with open(fn) as inF:
         newickString = inF.read()
      return cls(newickString)
   
   # ..............................   
   def parse(self):
      """
      @summary: Parse the provided Newick tree string
      @note: We assume only one tree specified
      """
      self._parse_tree(self.newickString)

      return self.rootClade, self.parentClades      
      
   # ..............................   
   def getParentClade(self, clade):
      """
      @summary: Returns the parent clade
      @param clade: Child clade to get the parent of
      @note: This is a lookup structure
      """
      return self.parentClades[clade[PhyloTreeKeys.PATH_ID]]
   
   # ..............................   
   def createNewClade(self, cladeId, parent=None):
      """
      @summary: Create a new clade
      @param cladeId: The id for this new clade
      @param parent: (optional) A parent clade
      """
      newClade = {
         PhyloTreeKeys.PATH_ID: cladeId,
         PhyloTreeKeys.CHILDREN: []
      }
      
      # If a parent is provided, update the parent and the path
      if parent is not None:
         parent[PhyloTreeKeys.CHILDREN].append(newClade)
         self.parentClades[newClade[PhyloTreeKeys.PATH_ID]] = parent

      return newClade
      
   # ..............................   
   def _parse_tree(self, text):
      """
      @summary: Parses the text representation into a dictionary
      @param text: The Newick tree text to parse
      """
      cladeId = 0
      self.rootClade = self.createNewClade(cladeId)
      
      cladeId += 1
            
      currentClade = self.rootClade
          
      # Unmatched parentheses:
      #   The tree should have the same number of left and right parens to be
      #      valid.  An exception to that would be unmatched parens in quoted
      #      labels, in which case, the tree is still valid
      unmatchedParens = 0
      
      tokens = re.finditer(self.tokenizer, text.strip())

      for match in tokens:
         token = match.group()
         
         if token.startswith("'"): # quoted label; add characters to clade name
            currentClade[PhyloTreeKeys.NAME] = token.strip("'")
            
         elif token.startswith('['):
            # comment
            pass
         
         elif token == '(': # Start a new clade, which is a child of the current
            unmatchedParens += 1
            # Update the parent
            parentClade = currentClade
            currentClade = self.createNewClade(cladeId, parent=currentClade)
            cladeId += 1
            
         elif token == ',': # start a new child clade
            currentClade = self.createNewClade(cladeId, parent=parentClade)
            cladeId += 1
            
         elif token == ')': # done adding children for this parent clade
            currentClade = parentClade
            if currentClade != self.rootClade:
               parentClade = self.getParentClade(currentClade)
            unmatchedParens -= 1
            
         elif token.startswith(':'): # branch length or confidence
            value = float(token[1:])
            currentClade[PhyloTreeKeys.BRANCH_LENGTH] = value
            
         elif token == '\n':
            pass
         
         elif token == ';':
            break
         
         else: # unquoted node label
            # Spec says to convert underscores to blanks
            currentClade[PhyloTreeKeys.NAME] = token.replace('_', ' ')
      
      # Check there are no unmatched parentheses
      if unmatchedParens != 0:
         raise NewickParseError(
            "Parser error.  Number of open / close parentheses do not match")
      
