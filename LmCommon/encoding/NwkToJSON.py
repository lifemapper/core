"""
@summary: This script will convert a Nexus file from Jeff into Phylo-XML
@author: CJ Grady
@version: 1.0
@status: alpha

@license: gpl2
@copyright: Copyright (C) 2014, University of Kansas Center for Research

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

import re
from cStringIO import StringIO




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

class Parser(object):
   """Parse a Newick tree given a file handle.

    Based on the parser in `Bio.Nexus.Trees`.
   """
   
   def __init__(self, handle):
      self.handle = handle
      # parents Dict get built with keys(pathId) for all nodes other than
      # root, since it doesn't have a parent. Node id's value is that node's parent clade 
      # and the entire sub tree from that parent to it's tips 
      self.parentDicts = {}
   
   @classmethod
   def from_string(cls, treetext):
      handle = StringIO(treetext)
      return handle       
      
   def parse(self, values_are_confidence=False, comments_are_confidence=False, rooted=False):
      """Parse the text stream this object was initialized with."""
      self.values_are_confidence = values_are_confidence
      self.comments_are_confidence = comments_are_confidence
      self.rooted = rooted
      buf = ''
      for line in self.handle:
         buf += line.rstrip()
         if buf.endswith(';'):
            phyloDict,parentDicts = self._parse_tree(buf)
            buf = ''
      if buf:        
         # Last tree is missing a terminal ';' character -- that's OK
         #yield self._parse_tree(buf)
         phyloDict,parentDicts = self._parse_tree(buf)
         buf = ''
      return phyloDict,parentDicts      
      
   def getParentDict(self,clade):
      """
      @summary: returns the parent dictionary for a clade
      @param clade: clade dictionary
      """
      return self.parentDicts[clade["pathId"]]
   
   def newCladeDict(self,parent=None,id=None):
      
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
         newClade = {'path':'0',"pathId":"0","children":[]}
      return newClade
      
   def _parse_tree(self, text):
      """Parses the text representation into an Tree object."""
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
            tempClade = newCladeDict(currentCladeDict,id=cladeId)
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
            currentCladeDict = newCladeDict(parentDict,cladeId)         
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
      
      return rootDict,self.parentDicts # Newick.Tree(root=root_clade, rooted=self.rooted)
        

      
      
      
      
      
      
      
      
      