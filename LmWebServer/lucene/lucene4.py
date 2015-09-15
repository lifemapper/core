"""
@summary: Lifemapper Lucene Module for Lucene 4.5.1
@author: CJ Grady
@version: 3.0
@status: beta

@note: Lucene 4.5.1

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
import os, lucene

from java.io import File
from org.apache.lucene.analysis.miscellaneous import LimitTokenCountAnalyzer
from org.apache.lucene.analysis.standard import StandardAnalyzer
from org.apache.lucene.index import DirectoryReader
from org.apache.lucene.queryparser.classic import QueryParser
from org.apache.lucene.document import Document, Field, FieldType
from org.apache.lucene.index import FieldInfo, IndexWriter, IndexWriterConfig
from org.apache.lucene.search import IndexSearcher
from org.apache.lucene.store import SimpleFSDirectory
from org.apache.lucene.util import Version

from LmServer.common.datalocator import EarlJr
from LmServer.common.log import LuceneLogger

START_TERM = "LMSTARTLM"
END_TERM = "LMENDLM"
SPACE_STR = "_"
MAX_RETURNED = 5000

# This is likely not a complete list, but it is much easier to add to than the other way
LUCENE_ESCAPES = [
                  [",", SPACE_STR],
                  [" ", SPACE_STR],
                  ["(", SPACE_STR],
                  [")", SPACE_STR],
                  ["&", SPACE_STR]
                 ]

# .............................................................................
def escapeString(value):
   """
   @summary: Escapes a string for processing by Lucene
   """
   for findStr, replStr in LUCENE_ESCAPES:
      value = value.replace(findStr, replStr)
   return value

# .............................................................................
class LmLucene4(object):
   """
   @summary: The LmLucene4 class runs and waits for connections and
                Lucene related requests
   """
   # ...................................
   def __init__(self, storeDir, log=LuceneLogger):
      initialSizeM = 128
      maxSizeM = 256
      (totalM, usedM, freeM, sharedM, buffers, cachedM) = self._getMemVals()
      # TODO: how should we compute these values from above?
      lucene.initVM(initialheap='%dm' % initialSizeM, 
                    maxheap='%dm' % maxSizeM, 
                    vmargs='-Djava.awt.headless:true')
      self.log = log
      self.storeDir = storeDir
      self.store = SimpleFSDirectory(File(self.storeDir))
      self.analyzer = StandardAnalyzer(Version.LUCENE_CURRENT)
      try:
         self._initializeSearcher()
      except:
         pass
      
   # ...................................
   def _getMemVals(self):
      """
      @summary: Returns memory values in megabytes as integers
      @return: List of: total, used, free, shared, buffers, cached
      """
      memstrs = os.popen("free -m").readlines()[1].split()
      memvals = []
      for i in range(1, len(memstrs)):
         try:
            val = int(memvals[i])
         except:
            val = 0
         memvals.append(val)
         
      return memvals

   # ...................................
   def _initializeSearcher(self):
      # Reset to None
      self.searcher = None
      # Initialize searcher
      self.searcher = IndexSearcher(DirectoryReader.open(self.store))
   
   # ...................................
   def buildIndex(self, speciesList):
      """
      @summary: Builds the Lifemapper Lucene index
      @param speciesList: A list of items to put in the index
      """
      if not os.path.exists(self.storeDir):
         os.mkdir(self.storeDir)

      analyzer = LimitTokenCountAnalyzer(self.analyzer, 1048576)
      config = IndexWriterConfig(Version.LUCENE_CURRENT, analyzer)
      config.setOpenMode(IndexWriterConfig.OpenMode.CREATE)
      writer = IndexWriter(self.store, config)
      
      t1 = FieldType()
      t1.setIndexed(False)
      t1.setStored(True)
      t1.setTokenized(False)
      t1.setIndexOptions(FieldInfo.IndexOptions.DOCS_ONLY)
      
      t2 = FieldType()
      t2.setIndexed(True)
      t2.setStored(False)
      t2.setTokenized(True)
      t2.setIndexOptions(FieldInfo.IndexOptions.DOCS_ONLY)
      
      self.log.debug("Inserting %s items into the index" % len(speciesList))
      
      ej = EarlJr()
      
      for sp in speciesList:
         acceptedName = sp[1].strip()
         nameParts = acceptedName.split(' ')
         
         binomial = nameParts[0]
         
         if len(nameParts) > 1:
            if nameParts[1].lower() == nameParts[1] and \
                  not nameParts[1].startswith('('): # This is a check for an author
               binomial = "%s %s" % (nameParts[0], nameParts[1]) 
         
         occId = str(sp[0])
         numOcc = str(sp[3])
         numMod = str(sp[4])
         
         downloadUrl = "{0}/shapefile".format(ej.constructLMMetadataUrl(
                                                  "occurrences", occId, "sdm"))
         
         # We append and prepend strings that shouldn't match so that we can 
         #    perform "starts with" queries and potentially "ends with" queries
         nameMod = escapeString(acceptedName)
         spSearch = "%s%s%s" % (START_TERM, nameMod, END_TERM)
         
         doc = Document()
         doc.add(Field("species", acceptedName, t1))
         doc.add(Field("occSetId", occId, t1))
         doc.add(Field("speciesSearch", spSearch, t2))
         doc.add(Field("numOcc", numOcc, t1))
         doc.add(Field("numModels", numMod, t1))
         doc.add(Field("binomial", binomial, t1))
         doc.add(Field("downloadUrl", downloadUrl, t1))
         writer.addDocument(doc)
      
      writer.commit()
      writer.close()
      self._initializeSearcher()
   
   # ...................................
   def searchIndex(self, searchString):
      """
      @summary: Searches the Lifemapper Lucene index for items matching the 
                   search string
      @param searchString: The string to search for
      """
      # To ensure that matching item starts with the searchString, we built the
      #    index and search it with a prepended start term.  Append a wildcard
      #    if the search string does not end with a space
      searchStringMod = escapeString(searchString)
      qStr = "%s%s%s" % (START_TERM, searchStringMod, "*")
#       qStr = "%s%s%s" % (START_TERM, searchString, 
#                                         "*" if searchString[-1] != " " else "")
      query = QueryParser(Version.LUCENE_CURRENT, "speciesSearch", 
                                                     self.analyzer).parse(qStr)
      scoreDocs = self.searcher.search(query, MAX_RETURNED).scoreDocs
      
      self.log.debug("Found %s results for query: %s" % (len(scoreDocs), 
                                                         searchString))
      
      return [self.searcher.doc(scoreDoc.doc) for scoreDoc in scoreDocs]
