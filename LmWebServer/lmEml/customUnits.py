"""
@summary: Module containing functions to process custom units from strings to 
             XML elements
@author: CJ Grady
@version: 2.0
@status: alpha

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
from LmWebServer.common.lmconstants import STMML_NAMESPACE, STMML_NS_PREFIX
from LmCommon.common.lmXml import fromstring, register_namespace


def processCustomUnits(parent, customUnits):
   """
   @summary: Processes a set of custom units from strings into ElementTree
                elements.  This is necessary to handle namespaces in the 
                strings that may not be in the document yet.
   @param parent: The parent element tree element to add these sub elements to
   @param customUnits: A set of custom unit strings to process
   """
   registeredStmml = False
   
   for cu in customUnits:
      if cu is not None:
         el = fromstring(cu)
         
         # Look for STMML NS
         if el.tag.find(STMML_NAMESPACE) >= 0:
            if not registeredStmml:
               register_namespace(STMML_NS_PREFIX, STMML_NAMESPACE)
               registeredStmml = True
         parent.append(el)
