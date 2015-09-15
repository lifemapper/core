"""
@summary: Module containing some base Lifemapper objects
@author: CJ Grady
@status: beta
@version: 2.0

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
# .............................................................................
class LmAttObj(object):
   """
   @summary: Object that includes attributes.  Compare this to the attributes
                attached to XML elements and the object members would be the
                sub-elements of the element.
   @note: <someElement att1="value1" att2="value2">
             <subEl1>1</subEl1>
             <subEl2>banana</subEl2>
          </someElement>
          
          translates to:
          
          obj.subEl1 = 1
          obj.subEl2 = 'banana'
          obj.getAttributes() = {'att1': 'value1', 'att2': 'value2'}
          obj.att1 = 'value1'
          obj.att2 = 'value2'
   """
   # ......................................
   def __init__(self, attrib={}, name="LmObj"):
      """
      @summary: Constructor
      @param attrib: (optional) Dictionary of attributes to attach to the 
                         object
      @param name: (optional) The name of the object (useful for serialization)
      """
      self.__name__ = name
      self._attrib = attrib
   
   # ......................................
   def __getattr__(self, name):
      """
      @summary: Called if the default getattribute method fails.  This will 
                   attempt to return the value from the attribute dictionary
      @param name: The name of the attribute to return
      @return: The value of the attribute
      """
      return self._attrib[name]

   # ......................................
   def getAttributes(self):
      """
      @summary: Gets the dictionary of attributes attached to the object
      @return: The attribute dictionary
      @rtype: Dictionary
      """
      return self._attrib
   
   # ......................................
   def setAttribute(self, name, value):
      """
      @summary: Sets the value of an attribute in the attribute dictionary
      @param name: The name of the attribute to set
      @param value: The new value for the attribute
      """
      self._attrib[name] = value

# .............................................................................
class LmAttList(list, LmAttObj):
   """
   @summary: Extension to lists that adds attributes
   @note: obj = LmAttList([1, 2, 3], {'id': 'attList'})
          print obj[0] >>  1
          obj.append('apple')
          print obj >> [1, 2, 3, 'apple']
          print obj.id >> 'attList'
   """
   def __init__(self, items=[], attrib={}, name="LmList"):
      """
      @summary: Constructor
      @param items: (optional) A list of initial values for the list
      @param attrib: (optional) Dictionary of attributes to attach to the list
      @param name: (optional) The name of the object (useful for serialization) 
      """
      LmAttObj.__init__(self, attrib, name)
      for item in items:
         self.append(item)
