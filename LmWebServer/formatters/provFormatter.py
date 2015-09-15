"""
@summary: Module containing PROV Formatter and helper functions
@author: CJ Grady
@version: 1.0
@status: beta
@note: Part of the Factory pattern
@see: Formatter
@license: gpl2
@copyright: Copyright (C) 2015, University of Kansas Center for Research

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
from LmWebServer.formatters.formatter import Formatter, FormatterResponse
from LmWebServer.lmProv.lmProvGenerator import ProvDocumentGenerator
from LmWebServer.lmProv.provXml import ProvXml
from LmWebServer.lmProv.provRDF import ProvRDF

# .............................................................................
class ProvFormatter(Formatter):
   """
   @summary: Formatter class for PROV-XML output
   """
   # ..................................
   def format(self):
      """
      @summary: Formats the object
      @return: A response containing the content and metadata of the format 
                  operation
      @rtype: FormatterResponse
      """
      try:
         name = self.obj.serviceType[:-1]
      except:
         name = "items"

      if self.parameters.has_key('rdfformat'):
         rdfFormat = self.parameters['rdfformat']
      else:
         rdfFormat = "xml"
         
      if rdfFormat == "turtle":
         ct = "text/turtle"
         fn = "%s%s.ttl" % (name, self.obj.getId())
      elif rdfFormat == "ntriples":
         ct = "application/n-triples"
         fn = "%s%s.nt" % (name, self.obj.getId())
      elif rdfFormat == "n3":
         ct = "text/n3"
         fn = "%s%s.n3" % (name, self.obj.getId())
      else:
         rdfFormat = "xml"
         ct = "application/rdf+xml"
         fn = "%s%s.rdf" % (name, self.obj.getId())

      doc = ProvDocumentGenerator(self.obj).generate()
      ret = ProvRDF(doc).formatString(rdfFormat=rdfFormat)
      
      headers = {"Content-Disposition" : 'attachment; filename="%s"' % fn}
      
      return FormatterResponse(ret, contentType=ct, filename=fn, 
                                                         otherHeaders=headers)
