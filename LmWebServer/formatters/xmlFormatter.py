"""
@summary: Module containing XML Formatter class and helping functions
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
from types import NoneType, StringType, UnicodeType

from LmCommon.common.lmXml import (Element, register_namespace, 
                     setDefaultNamespace, SubElement, tostring, PI, QName)
from LmCommon.common.lmconstants import (ENCODING, LM_NAMESPACE, LM_NS_PREFIX, 
                                         LM_RESPONSE_SCHEMA_LOCATION)

from LmServer.base.layerset import _LayerSet
from LmServer.base.serviceobject import ServiceObject
from LmServer.base.utilities import escapeString, ObjectAttributeIterator
from LmServer.common.localconstants import ARCHIVE_USER

from LmWebServer.common.lmconstants import XSI_NAMESPACE
from LmWebServer.formatters.formatter import Formatter, FormatterResponse
from LmWebServer.formatters.unmodifiedXmlFormatter import UnmodifiedXmlFormatter

# .............................................................................
class XmlFormatter(Formatter):
   """
   @summary: Formatter class for XML output
   """
   # ..................................
   def __init__(self, obj, parameters={}):
      """
      @summary: Constructor
      @param obj: The object to format
      @param parameters: (optional) A dictionary of parameters that might be
                            useful for the Formatter
      """
      Formatter.__init__(self, obj, parameters)
      self.stylesheet = False

   # ..................................
   def format(self):
      """
      @summary: Formats the object
      @return: A response containing the content and metadata of the format 
                  operation
      @rtype: FormatterResponse
      """
      if isinstance(self.obj, (StringType, UnicodeType)):
         f = UnmodifiedXmlFormatter(self.obj, self.parameters)
         return f.format()
      else:
         try:
            name = self.obj.serviceType[:-1]
         except:
            if isinstance(self.obj, _LayerSet):
               name = "layerset"
               # Wrap layers and add map prefix
               for lyr in self.obj.layers:
                  lyr.mapPrefix = "included_for_compatibility"
            else:
               name = "items"
         
         # Fill metadata properties
         self._getMetadataProperties()
         
         ct = "application/xml"
         if isinstance(self.obj, ServiceObject):
            fn = "%s%s.xml" % (name, self.obj.getId())
            user = self.obj.getUserId()
            name = name
         else:
            fn = "lmFeed.xml"
            user = ARCHIVE_USER
            name = "items"

         cnt = formatXml(ObjectAttributeIterator(name, self.obj), user, 
                         self.title, pages=self.pages, 
                         interfaces=self.interfaces, stylesheet=self.stylesheet)
         
         return FormatterResponse(cnt, contentType=ct, filename=fn)

# .............................................................................
class StyledXmlFormatter(XmlFormatter):
   """
   @summary: Formatter class for XML output without Lifemapper stylesheet
   """
   # ..................................
   def __init__(self, obj, parameters={}):
      """
      @summary: Constructor
      @param obj: The object to format
      @param parameters: (optional) A dictionary of parameters that might be
                            useful for the Formatter
      """
      XmlFormatter.__init__(self, obj, parameters)
      self.stylesheet = True
      
# .............................................................................
def _addObjectToTree(el, obj):
   """
   @summary: Adds an object to the ElementTree element
   """
   for name, value in obj:
      if isinstance(value, ObjectAttributeIterator):
         attribs = dict([(k, value.attributes[k]) for k in value.attributes.keys()])
         subEl = SubElement(el, name, namespace=LM_NAMESPACE, attrib=attribs)
         _addObjectToTree(subEl, value)
      elif not isinstance(value, NoneType):
         if name is not None:
            if value is None or value == "":
               SubElement(el, name, namespace=LM_NAMESPACE)
            else:
               SubElement(el, name, value=escapeString(value, 'xml'), 
                          namespace=LM_NAMESPACE)

# .............................................................................
def formatXml(obj, user, title, pages=[], interfaces=[], stylesheet=False):
   register_namespace(LM_NS_PREFIX, LM_NAMESPACE)
   setDefaultNamespace(LM_NAMESPACE)
   
   el = Element("response", attrib={QName(XSI_NAMESPACE, "schemaLocation") : "%s %s" % (LM_NAMESPACE, LM_RESPONSE_SCHEMA_LOCATION)})

   pis = []
   pis.append(PI("xml", 'version="1.0" encoding="{}"'.format(ENCODING)))
   if stylesheet:
      pis.append(PI("xml-stylesheet", 
                    'type="text/xsl" href="/css/services.xsl?r=20140721"'))
   
   SubElement(el, "title", value=escapeString(title, "xml"))
   SubElement(el, "user", value=user)
   
   if len(interfaces) > 0:
      interfacesEl = SubElement(el, "interfaces")
      for interfaceName, interfaceUrl in interfaces:
         SubElement(interfacesEl, interfaceName, value=interfaceUrl)
   
   if len(pages) > 0:
      pagesEl = SubElement(el, "pages")
      for rel, href, _ in pages:
         SubElement(pagesEl, "page", attrib={"href" : href, 
                                             "rel" : rel})
         
   attribs = dict([(k, obj.attributes[k]) for k in obj.attributes.keys()])
   objEl = SubElement(el, obj.name, attrib=attribs)
   _addObjectToTree(objEl, obj)

   return '%s\n%s' % ('\n'.join([tostring(pi) for pi in pis]), tostring(el))
