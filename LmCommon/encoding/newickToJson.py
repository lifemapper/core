"""
@summary: This script will convert a Nexus file from Jeff into Phylo-XML
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
          
@todo: Should this be generalized?

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

   Tree → Subtree ";" | Branch ";"
   Subtree → Leaf | Internal
   Leaf → Name
   Internal → "(" BranchSet ")" Name
   BranchSet → Branch | Branch "," BranchSet
   Branch → Subtree Length
   Name → empty | string
   Length → empty | ":" number

"""
import os
import re
try:
   from cStringIO import StringIO
except:
   from StringIO import StringIO

# TODO: Move these to constants file since they are shared
CHILDREN_KEY = 'children'
LENGTH_KEY = 'length'
NAME_KEY = 'name'
PATH_KEY = 'path'
PATH_ID_KEY = 'pathId'


# .............................................................................
class Parser(object):
   """
   @summary: Parse a Newick tree given a file handle.

    Based on the parser in `Bio.Nexus.Trees`.
   """
   
   # ..............................   
   def __init__(self, newickString):
      """
      @summary: Construct a new parser from a Newick tree string
      @param newickString: A Newick tree as a string
      """
      self.newickString = newickString

      # parentsDict gets built with keys(pathId) for all nodes other than root, 
      #    since it doesn't have a parent.
      # nodeId's value is that node's parent clade and the entire sub tree from 
      #    that parent to it's tips
      self.parentDicts = {}
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
   def parse(self, values_are_confidence=False, comments_are_confidence=False, rooted=False):
      """
      @todo: Document
      @todo: Fix variable names
      Parse the text stream this object was initialized with."""
      self.values_are_confidence = values_are_confidence
      self.comments_are_confidence = comments_are_confidence
      self.rooted = rooted
      
      
      
      #TODO: It looks like a file can have multiple trees but this isn't a good way to do this
      #TODO: It also looks to be a last one in wins situation
      #buf = ''
      #for line in self.handle:
      #   buf += line.rstrip()
      #   if buf.endswith(';'):
      #      phyloDict, parentDicts = self._parse_tree(buf)
      #      buf = ''
      #if buf:        
      #   # Last tree is missing a terminal ';' character -- that's OK
      #   #yield self._parse_tree(buf)
      #   phyloDict, parentDicts = self._parse_tree(buf)
      #   buf = ''
      
      # TODO: This is better
      for treeStr in self.newickString.split(';'):
         phyloDict, parentDicts = self._parse_tree(treeStr)
         
      
      return phyloDict, parentDicts      
      
   # ..............................   
   def getParentDict(self, clade):
      """
      @summary: Returns the parent dictionary for a clade
      @param clade: Clade dictionary
      @note: This is a lookup structure
      @todo: Better if we use recursion instead?
      """
      return self.parentDicts[clade[PATH_ID_KEY]]
   
   # ..............................   
   def newCladeDict(self, cladeId, parent=None):
      """
      @summary: Create a new clade dictionary
      @param cladeId: The id for this new clade
      @param parent: (optional) A parent clade dictionary
      @todo: Can we use integers or do we really need path id to be a string?
      """
      newClade = {
         PATH_ID_KEY: cladeId,
         PATH_KEY: "{0}".format(cladeId),
         CHILDREN_KEY: []
      }
      
      # If a parent is provided, update the parent and the path
      if parent is not None:
         # Update the path
         newClade[PATH_KEY] = "{cladeId},{parentPath}".format(
                                  cladeId=cladeId, parentPath=parent[PATH_KEY])
         parent[CHILDREN_KEY].append(newClade)
         self.parentDicts[newClade[PATH_ID_KEY]] = parent

      return newClade
      
   # ..............................   
   def _parse_tree(self, text):
      """
      @summary: Parses the text representation into a dictionary
      @param text: The Newick tree text to parse
      """
      cladeId = 0
      rootDict = newCladeDict(cladeId)
      
      cladeId +=1
            
      ########### JSON ###########
      currentCladeDict = rootDict
      ###########################
          
      # Unmatched parentheses:
      #   The tree should have the same number of left and right parens to be
      #      valid.  An exception to that would be unmatched parens in quoted
      #      labels, in which case, the tree is still valid
      unmatchedParens = 0
      
      tokens = re.finditer(self.tokenizer, text.strip())

      # TODO: Consider using a generator and recursion
              
      for match in tokens:
         #print "MATCH"
         token = match.group()
         
         if token.startswith("'"):
            # quoted label; add characters to clade name
            
            ########### JSON ###################
            #TODO: Why not use strip?  or replace?
            currentCladeDict[NAME_KEY] = token[1:-1]
            ####################################
            
         elif token.startswith('['):
            # comment
            if self.comments_are_confidence:
               pass
         
         elif token == '(':
            unmatchedParens += 1
            # start a new clade, which is a child of the current clade
            ########  JSON #####################
            currentCladeDict[CHILDREN_KEY] = []
            tempClade = self.newCladeDict(cladeId, parent=currentCladeDict)
            cladeId += 1
            currentCladeDict = tempClade
            
         elif token == ',':
            # if the current clade is the root, then the external parentheses are missing
            # and a new root should be created
            
            ############  JSON ###############
            if currentCladeDict[PATH_ID_KEY] == "0":
               print "is it getting in here for F(A,B,(C,D)E); Answer: No"
               #TODO: Can this happen?  Handle better if it can
               rootDict = self.newCladeDict(cladeId)
               cladeId +=1
               self.parentDicts[str(currentCladeDict[PATH_ID_KEY])] = rootDict      
            # start a new child clade at the same level as the current clade
            ########### JSON ############
            parentDict = self.getParentDict(currentCladeDict)
            #parentDict["children"] = []
            currentCladeDict = self.newCladeDict(cladeId, parent=parentDict)
            #############################
             
            cladeId +=1
            
         elif token == ')':
            # done adding children for this parent clade
            ######### JSON #################
            parentDict = self.getParentDict(currentCladeDict)
                  
            ##########  JSON ###########
            currentCladeDict = parentDict       
            ############################
            unmatchedParens -= 1
            
         elif token == ';':
            break
         
         elif token.startswith(':'):
            # branch length or confidence
            #print "does it ever get in here, branch length"
            value = float(token[1:])
            currentCladeDict[LENGTH_KEY] = str(value)
            
         elif token == '\n':
            pass
         
         else:
            # unquoted node label            
            ############ JSON ##############
            #nameL = token.split("_")
            #name = "%s_%s" % (nameL[len(nameL)-2],nameL[len(nameL)-1])
            #currentCladeDict["name"] = name
            currentCladeDict[NAME_KEY] = token
            ################################
      
      # Check there are no unmatched parentheses
      # TODO: Add a specific exception
      if unmatchedParens != 0:
         raise Exception, "Parser error.  Number of open / close parentheses do not match"
      
      
      return rootDict, self.parentDicts # Newick.Tree(root=root_clade, rooted=self.rooted)
