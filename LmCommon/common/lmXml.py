"""
@summary: Module containing Lifemapper XML utilities
@note: Mainly wraps elementTree functionality to fit Lifemapper needs
@author: CJ Grady
@version: 2.0
@status: beta

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
@note: For ElementTree distributed with Python 2.7
"""
from types import BuiltinFunctionType, BuiltinMethodType, IntType, FloatType, \
                  FunctionType, LambdaType, ListType, MethodType, NoneType, \
                  StringType, TypeType, DictType, UnicodeType

import xml.etree.ElementTree as ET

from LmCommon.common.lmAttObject import LmAttList, LmAttObj
from LmCommon.common.unicode import toUnicode

# Functions / Classes directly mapped to the Element Tree versions
# ..............................................................................
Comment = ET.Comment
ElementPath = ET.ElementPath
ElementTree = ET.ElementTree
HTML_EMPTY = ET.HTML_EMPTY
PI = ET.PI
ParseError = ET.ParseError
ProcessingInstruction = ET.ProcessingInstruction
QName = ET.QName
TreeBuilder = ET.TreeBuilder
VERSION = ET.VERSION
XML = ET.XML
XMLID = ET.XMLID
XMLParser = ET.XMLParser
XMLTreeBuilder = ET.XMLTreeBuilder
dump = ET.dump
fromstring = ET.fromstring
fromstringlist = ET.fromstringlist
iselement = ET.iselement
iterparse = ET.iterparse
parse = ET.parse
re = ET.re
register_namespace = ET.register_namespace
sys = ET.sys
warnings = ET.warnings


# Functions / classes modified to serve Lifemapper purposes
#global __DEFAULT_NAMESPACE__
__DEFAULT_NAMESPACE__ = None

# .............................................................................
def setDefaultNamespace(defNamespace):
   """
   @summary: Sets the default namespace.  If a namespace is not provided for an
                element, this one will be used
   """
   global __DEFAULT_NAMESPACE__ # Need to specify that we are setting the global variable
   __DEFAULT_NAMESPACE__ = defNamespace

# .............................................................................
class Element(ET.Element):
   """
   @summary: Wrapper around the ElementTree Element class that also adds an 
                optional value on construction and will fill in a default 
                namespace if none is provided
   @see: ElementTree.Element
   """
   # .............................
   def __init__(self, tag, attrib={}, value=None, namespace=-1, **extra):
      """
      @summary: Element constructor
      @param tag: The tag for the element (string or QName)
      @param attrib: A dictionary of element attributes
      @param value: The value for this element (goes in element.text)
      @param namespace: (optional) The namespace of this element
      @param extra: (optional) Extra named parameters that will be added to the 
                       element's attributes
      @note: Providing None for the namespace will result in a QName without a 
                namespace
      @note: Providing -1 for the namespace will result in the default 
                namespace (__DEFAULT_NAMESPACE__) being used
      """
      elQName = _getElemQName(namespace, tag)
      
      ET.Element.__init__(self, elQName, attrib=_processAttribs(attrib), **extra)
   
      if value is not None:
         self.text = toUnicode(value)

# .............................................................................
def CDATA(text=None):
   """
   @summary: Adds the capability to add CDATA elements.  This requires a helper 
                for serialization but will produce valid XML with CDATA
   """
   element = Element('![CDATA[', namespace=None)
   element.text = text
   return element
   
# .............................................................................
def SubElement(parent, tag, attrib={}, value=None, namespace=-1, **extra):
   """
   @summary: Tells the parent to create a child ElementTree Element, adds a 
                value and a namespace to it, and then returns it
   @see: ElementTree.SubElement
   @param parent: The parent Element
   @param tag: A tag for the element (either string or QName)
   @param attrib: (optional) A dictionary of element attributes
   @param value: (optional) The value for this element (goes in element.text)
   @param namespace: (optional) The namespace of this element 
   @param extra: (optional) Extra named parameters that will be added to the 
                    element's attributes
   @note: Providing None for the namespace will result in a QName without a 
             namespace
   @note: Providing -1 for the namespace will result in the default namespace 
             (__DEFAULT_NAMESPACE__) being used
   """
   elQName = _getElemQName(namespace, tag)
   subEl = ET.SubElement(parent, elQName, attrib=_processAttribs(attrib), **extra)
   if value is not None:
      subEl.text = toUnicode(value)
   return subEl
      
# .............................................................................
def tostring(element, encoding=None, method=None):
   """
   @summary: ElementTree.tostring wrapper that converts an Element tree into a 
                pretty printed string
   @see: ElementTree.tostring
   """
   _prettyFormat(element, level=0)
   return ET.tostring(element, encoding=encoding, method=method)

# .............................................................................
def tostringlist(element, encoding=None, method=None):
   """
   @summary: ElementTree.tostringlist wrapper that converts an Element tree 
                into a pretty printed list of strings
   @see: ElementTree.tostring
   """
   _prettyFormat(element, level=0)
   return ET.tostringlist(element, encoding=encoding, method=method)

# =============================================================================
# =                             Helper Functions                              =
# =============================================================================

# .............................................................................
# Monkey patch to add support for CDATA
ET._original_serialize_xml = ET._serialize_xml

def _serialize_xml(write, elem, encoding, qnames, namespaces):
   """
   @summary: Monkey patch to add support for CDATA in serialization
   """
   if elem.tag == '![CDATA[':
      write("<%s%s]]>%s" % (elem.tag, elem.text, elem.tail))
      return
   return ET._original_serialize_xml(write, elem, encoding,qnames, namespaces)
ET._serialize_xml = ET._serialize['xml'] = _serialize_xml

# .............................................................................
def _processAttribs(attribDict):
   """
   @summary: Processes attribute dictionaries to make sure keys and values are 
                encoded correctly
   @todo: Add encoding
   """
   newDict = {}
   for k, v in attribDict.items():
      newDict[toUnicode(k)] = toUnicode(v)
   return newDict

# .............................................................................
def _getElemQName(namespace, tag):
   """
   @summary: Assembles a QName object from a namespace and tag
   @param namespace: The namespace to use for the QName
   @param tag: The tag of the QName
   @note: If namespace is -1, the default namespace (specified by 
             __DEFAULT_NAMESPACE__) will be used
   @note: If namespace is None, the QName will not have a namespace
   @note: If tag is already a QName, it will be returned unmodified (happens 
             for SubElements)
   """
   if isinstance(tag, QName):
      # If the tag is already a QName, no need to recreate it
      return tag
   else:
      if namespace == -1:
         namespace = __DEFAULT_NAMESPACE__
      if namespace is not None:
         elemName = QName(namespace, tag)
      else:
         elemName = QName(tag)
      return elemName

# .............................................................................
def _prettyFormat(elem, level=0):
   """
   @summary: Formats ElementTree element so that it prints pretty (recursive)
   @param elem: ElementTree element to be pretty formatted
   @param level: How many levels deep to indent for
   @todo: May need to add an encoding parameter
   """
   tab = "   "

   i = "\n" + level*tab
   if len(elem):
      if not elem.text or not elem.text.strip():
         elem.text = i + tab
      for e in elem:
         _prettyFormat(e, level+1)
         if not e.tail or not e.tail.strip():
            e.tail = i + tab
      if not e.tail or not e.tail.strip():
         e.tail = i
   else:
      if level and (not elem.tail or not elem.tail.strip()):
         elem.tail = i

# .............................................................................
def _removeNSfunc(s):
   if isinstance(s, QName):
      s = s.text
      
   if s.find('}') > 0:
      return s.split('}')[1]
   else:
      return s

# .............................................................................
def _dontRemoveNSfunc(s):
   if isinstance(s, QName):
      return s.text
   else:
      return s
   
# =============================================================================
# =                   Object Deserialization / Serialization                  =
# =============================================================================
# .............................................................................
def deserialize(element, removeNS=True):
   """
   @summary: Deserializes an ElementTree (Sub)Element into an object
   @param element: The element to deserialize
   @param removeNS: (optional) If true, removes the namespaces from the tags
   @return: A new object
   """
   # If removeNS is set to true, look for namespaces in the tag and remove them
   #    They are enclosed in curly braces {namespace}tag
   
   # Look for QNames
   
   if removeNS:
      #processTag = lambda s: s.text.split("}")[1] if s.text.find("}") >= 0 else s.text
      processTag = _removeNSfunc
   else:
      #processTag = lambda s: s.text
      processTag = _dontRemoveNSfunc
   
   # If the element has no children, just get the text   
   if len(list(element)) == 0 and len(element.attrib.keys()) == 0:
      try:
         val = element.text.strip()
         if len(val) > 0:
            return val
         else:
            return None
      except:
         return None
   else:
      attribs = dict([(processTag(key), element.attrib[key]) for key in \
                                                        element.attrib.keys()])
      obj = LmAttObj(attrib=attribs, name=processTag(element.tag))

      try:
         val = element.text.strip()
         if len(val) > 0:
            obj.value = val
      except:
         pass
      
      # Get a list of all of the element's children's tags
      # If they are all the same type and match the parent, make one list
      tags = [child.tag for child in list(element)]
      reducedTags = list(set(tags))
      
      try:
         firstReducedTag = element.tag.text[:-1]
      except:
         firstReducedTag = element.tag[:-1]
      
      if len(reducedTags) == 1 and reducedTags[0] == firstReducedTag: 
         obj = LmAttList([], attrib=attribs, name=processTag(element.tag))
         for child in list(element):
            obj.append(deserialize(child, removeNS))
      else:
         # Process the children
         for child in list(element):
            if hasattr(obj, processTag(child.tag)):
               tmp = obj.__getattribute__(processTag(child.tag))
               if isinstance(tmp, ListType):
                  tmp.append(deserialize(child, removeNS))
               else:
                  tmp = LmAttList([tmp, deserialize(child, removeNS)], 
                                                name=processTag(child.tag)+'s')
               setattr(obj, processTag(child.tag), tmp)
            else:
               setattr(obj, processTag(child.tag), deserialize(child, removeNS))
      return obj

# .............................................................................
def serialize(obj, parent=None):
   """
   @summary: This function serializes an object into xml
   @note: Recursive
   @param parent: (optional) A parent element to attach this one to
   @return: An ElementTree element representing the object
   @rtype: ElementTree (Sub)Element
   """
   value = None
   attrib = {}
   if hasattr(obj, 'value'):
      value = obj.value
   elif isinstance(obj, (StringType, UnicodeType)):
      value = obj
      
   fltr = lambda x: not x.startswith('_') and not x == "attrib" and \
                                              not x == "value"
   objAttribs = [att for att in dir(obj) if fltr(att)]

   if hasattr(obj, 'attrib'):
      for k, v in [(key, obj.attrib[key]) for key in obj.attrib.keys()]:
         attrib[k] = v
   try:
      atts = obj.getAttributes()
      for key in atts.keys():
         if isinstance(atts[key], (IntType, StringType, FloatType, UnicodeType)):
            attrib[key] = str(atts[key])
         elif isinstance(atts[key], (NoneType)):
            pass
         else:
            objAttribs.append(key)
   except Exception:
      pass

   if isinstance(obj, TypeType):
      elName = obj.__name__
   elif isinstance(obj, LmAttObj):
      elName = obj.__name__
   else:
      elName = obj.__class__.__name__

   if isinstance(obj, Element):
      if parent is None:
         el = obj
      else:
         parent.append(obj)
         el = obj
   else:
      if parent is None:
         el = Element(elName, value=value, attrib=attrib)
      else:
         el = SubElement(parent, elName, value=value, attrib=attrib)
   
      for att in objAttribs:
         subObj = getattr(obj, att)
         if isinstance(subObj, ListType):
            for i in subObj:
               serialize(i, el)
         elif isinstance(subObj, (MethodType, FunctionType, LambdaType, 
                                         BuiltinMethodType, BuiltinFunctionType)):
            pass
         elif isinstance(subObj, (StringType, IntType, FloatType, UnicodeType)):
            SubElement(el, att, value=subObj)
         elif isinstance(subObj, DictType):
            sEl = SubElement(el, att)
            for key in subObj.keys():
               if isinstance(subObj[key], (StringType, IntType, FloatType, UnicodeType)):
                  SubElement(sEl, key, value=subObj[key])
               elif isinstance(subObj[key], NoneType):
                  pass
               else:
                  serialize(subObj[key], sEl)
         else:
            serialize(getattr(obj, att), el)
      if isinstance(obj, ListType):
         for i in obj:
            serialize(i, el)
   return el
