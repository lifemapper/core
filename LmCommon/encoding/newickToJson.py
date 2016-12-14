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

#TODO: These shouldn't be module level
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
tokenizer = re.compile('(%s)' % '|'.join([token[0] for token in tokens]))

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
      self.parentDicts = {}
      
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
   
   
   #def __init__(self, handle):
   #   """
   #   @summary: Constructor
   #   @note: This currently takes a file like object (flo). I'm not sure that it should.  We could provide a class method for that as well as a from file method
   #   @todo: move documentation
   #   """
   #   self.handle = handle
   #   # parents Dict get built with keys(pathId) for all nodes other than
   #   # root, since it doesn't have a parent. Node id's value is that node's parent clade 
   #   # and the entire sub tree from that parent to it's tips 
   #   self.parentDicts = {}
   
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
      @summary: returns the parent dictionary for a clade
      @param clade: clade dictionary
      """
      return self.parentDicts[clade["pathId"]]
   
   # ..............................   
   def newCladeDict(self, parent=None, id=None):
      """
      @todo: Document
      @todo: Constants
      """
      
      if parent is not None:
         # find the parent path
         parentPath = parent["path"]
         newClade = {}
         newClade["pathId"] = str(id)
         parent["children"].append(newClade)
         if id is not None:
            path = str(id) + ','+ parentPath
            newClade["path"] = path
         
         
         self.parentDicts[newClade["pathId"]] = parent
      else:
         newClade = {'path':'0', "pathId":"0", "children":[]}
      return newClade
      
   # ..............................   
   def _parse_tree(self, text):
      """
      @summary: Parses the text representation into an Tree object.
      @todo: Document
      @todo: Constants
      """
      tokens = re.finditer(tokenizer, text.strip())      
      newCladeDict = self.newCladeDict
      cladeId  = 0          
      ######## JSON ###########
      rootDict = {"pathId":'0',"path":'0',"children":[]} 
      #########################
      
      cladeId +=1
            
      ########### JSON ###########
      currentCladeDict = rootDict
      ###########################
          
      lp_count = 0
      rp_count = 0
              
      for match in tokens:
         #print "MATCH"
         token = match.group()
         
         if token.startswith("'"):
            # quoted label; add characters to clade name
            
            ########### JSON ###################
            currentCladeDict["name"] = token[1:-1]
            ####################################
            
         elif token.startswith('['):
            # comment
            if self.comments_are_confidence:
               pass
         
         elif token == '(':
            # start a new clade, which is a child of the current clade
            lp_count += 1           
            ########  JSON #####################
            currentCladeDict['children'] = []
            tempClade = newCladeDict(currentCladeDict, id=cladeId)
            cladeId += 1
            currentCladeDict = tempClade
            
         elif token == ',':
            # if the current clade is the root, then the external parentheses are missing
            # and a new root should be created
            
            ############  JSON ###############
            if currentCladeDict["pathId"] == "0":
               print "is it getting in here for F(A,B,(C,D)E); Answer: No"
               rootDict = newCladeDict(id=cladeId)
               cladeId +=1
               self.parentDicts[str(currentCladeDict["pathId"])] = rootDict      
            # start a new child clade at the same level as the current clade
            ########### JSON ############
            parentDict = self.getParentDict(currentCladeDict)
            #parentDict["children"] = []
            currentCladeDict = newCladeDict(parentDict, cladeId)
            #############################
             
            cladeId +=1
            
         elif token == ')':
            # done adding children for this parent clade
            ######### JSON #################
            parentDict = self.getParentDict(currentCladeDict)
                  
            ##########  JSON ###########
            currentCladeDict = parentDict       
            ############################
            rp_count += 1
         
         elif token == ';':
            break
         
         elif token.startswith(':'):
            # branch length or confidence
            #print "does it ever get in here, branch length"
            value = float(token[1:])
            currentCladeDict["length"] = str(value)
            
         elif token == '\n':
            pass
         
         else:
            # unquoted node label            
            ############ JSON ##############
            #nameL = token.split("_")
            #name = "%s_%s" % (nameL[len(nameL)-2],nameL[len(nameL)-1])
            #currentCladeDict["name"] = name
            currentCladeDict["name"] = token
            ################################
      if not lp_count == rp_count:
         print 'Number of open/close parentheses do not match.'
      
      # if ; token broke out of for loop, there should be no remaining tokens
      try:
         next_token = tokens.next()
         print 'Text after semicolon in Newick tree: %s' % (next_token.group())
      except StopIteration:
         pass
      
      return rootDict, self.parentDicts # Newick.Tree(root=root_clade, rooted=self.rooted)
