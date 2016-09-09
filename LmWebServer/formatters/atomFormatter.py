"""
@summary: Module containing Atom Formatter class and helping functions
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
from types import ListType, NoneType

from LmCommon.common.lmconstants import HTTPStatus, LM_NAMESPACE, LM_NS_PREFIX
from LmCommon.common.lmXml import Element, register_namespace, \
                                  setDefaultNamespace, SubElement, tostring

from LmServer.base.atom import Atom
from LmServer.base.lmobj import LmHTTPError
from LmServer.base.serviceobject import ServiceObject
from LmServer.base.utilities import (escapeString, formatTimeAtom, 
                                     ObjectAttributeIterator)
from LmServer.common.localconstants import SMTP_SENDER

from LmWebServer.common.lmconstants import ATOM_NAMESPACE
from LmWebServer.formatters.formatter import Formatter, FormatterResponse

# .............................................................................
class AtomFormatter(Formatter):
   """
   @summary: Formatter class for Atom output
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
      
      # Fill metadata properties
      self._getMetadataProperties()
      
      if isinstance(self.obj, ListType):
         fn = "lmAtomFeed.xml"
      elif isinstance(self.obj, ServiceObject):
         fn = "%s%satom.xml" % (name, self.obj.getId())
      else:
         raise LmHTTPError(HTTPStatus.UNSUPPORTED_MEDIA_TYPE,
          "Cannot convert %s object to an Atom feed" % str(self.obj.__class__))

      url = self.url
      if url.endswith('/'):
         url = url[:-1]

      cnt = formatAtom(self.obj, url, self.title, formatTimeAtom(self.updateTime), self.pages, self.interfaces, name)
      
      ct = "application/atom+xml"
      
      return FormatterResponse(cnt, contentType=ct, filename=fn)

# .............................................................................
def _addObjectToTree(el, obj):
   """
   @summary: Adds an object to the ElementTree element
   """
   for name, value in obj:
      if isinstance(value, ObjectAttributeIterator):
         subEl = SubElement(el, name, namespace=LM_NAMESPACE)
         _addObjectToTree(subEl, value)
      elif not isinstance(value, NoneType):
         if name is not None:
            if value is None or value == "":
               SubElement(el, name, namespace=LM_NAMESPACE)
            else:
               SubElement(el, name, value=escapeString(value, 'xml'), 
                          namespace=LM_NAMESPACE)

# .............................................................................
def formatAtom(obj, url, title, updateTime, pages, interfaces, name):
   """
   @summary: Formats an object into an Atom feed
   @param obj: The object to format
   @param url: The service url of the object
   @param title: The title of the object
   @param updateTime: The last time this object was updated
   @param pages: A list of pages associated with this object
   @param interfaces: A list of possible interfaces for this object
   @param name: The name of this object 
   """
   setDefaultNamespace(ATOM_NAMESPACE)
   register_namespace('', ATOM_NAMESPACE)
   register_namespace(LM_NS_PREFIX, LM_NAMESPACE)
   el = Element("feed")
   SubElement(el, "id", value=escapeString(url, 'xml'))
   SubElement(el, "title", value=title)
   SubElement(el, "link", attrib={"href" : escapeString(url, 'xml'), 
                                  "rel": "self"})
   SubElement(el, "updated", value=updateTime)
   
   # Author 
   author = SubElement(el, "author")
   SubElement(author, "name", value="Lifemapper")
   SubElement(author, "email", value=SMTP_SENDER)

   for rel, u, _ in pages:
      SubElement(el, "link", attrib={"rel": rel, 
                                     "href" : escapeString(u, 'xml')})

   if isinstance(obj, ListType) and len(obj) > 0 and isinstance(obj[0], Atom):
      for o in obj:
         ent = SubElement(el, "entry")
         SubElement(ent, "id", value=o.url)
         # TODO: These should probably be done differently
         SubElement(ent, "link", attrib={"rel": "self", 
                                "href": url.replace('atom', '%s/atom' % o.id)})
         SubElement(ent, "link", attrib={"rel": "alternate",
                                 "href" : url.replace('atom', '%s/atom' % o.id)})
         SubElement(ent, "title", value=o.title)
         SubElement(ent, "updated", value=formatTimeAtom(o.modTime))
         SubElement(ent, "summary", value=o.title)
   else:
      ent = SubElement(el, "entry")
      SubElement(ent, "id", value=url)
      SubElement(ent, "title", value=title)
      SubElement(ent, "updated", value=updateTime)
      SubElement(ent, "link", attrib={"rel" : "self", "href" : url})
      SubElement(ent, "link", attrib={"rel" : "alternate", 
                                      "href" : url.replace('/atom', '')})
      # TODO: Add interfaces back in with content types
#       if len(interfaces) > 0:
#          for _, interfaceUrl in interfaces:
#             SubElement(ent, "link", attrib={"rel" : "alternate",
#                                             "href" : interfaceUrl})
      
      cntEl = SubElement(ent, "content", attrib={"type" : "text/xml"})
      _addObjectToTree(cntEl, ObjectAttributeIterator(name, obj))
   
   #SubElement(el, "title", value=title)
   #SubElement(el, "update", value=updateTime)
   #SubElement(el, "summary", value=title)
   

   return tostring(el)