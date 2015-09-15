"""
@summary: Module containing method for adding contact information to an eml 
             document
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
from LmCommon.common.lmXml import SubElement
from LmCommon.common.localconstants import WEBSERVICES_ROOT

from LmBackend.common.localconstants import SMTP_SENDER

# .............................................................................
def addContactElement(el):
   """
   @summary: Adds a contact elment to the EML document
   """
   contact = SubElement(el, "contact")
   SubElement(contact, "organizationName", 
              value="University of Kansas Biodiversity Institute")
   
   addEl = SubElement(contact, "address")
   SubElement(addEl, "deliveryPoint", value="1345 Jayhawk Boulevard, Room 606")
   SubElement(addEl, "city", value="Lawrence")
   SubElement(addEl, "administrativeArea", value="Kansas")
   SubElement(addEl, "country", value="United States")

   SubElement(contact, "electronicMailAddress", value=SMTP_SENDER)
   SubElement(contact, "onlineUrl", value=WEBSERVICES_ROOT)
