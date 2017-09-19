"""
@summary: Module containing functions used to send emails from Lifemapper
@author: CJ Grady
@version: 1.0
@status: beta
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
import smtplib
from types import ListType

from LmServer.common.localconstants import SMTP_SERVER, SMTP_SENDER 
# .............................................................................
class EmailNotifier(object):
   """
   @summary: Class used to connect to an SMTP server and send emails
   """
   # ....................................
   def __init__(self, server=SMTP_SERVER, 
                      fromAddr=SMTP_SENDER):
      """
      @summary: Constructor
      @param server: (optional) SMTP server to send email from
      @param fromAddr: (optional) The email address to send emails from
      """
      self.fromAddr = fromAddr
      self.server = smtplib.SMTP(server)
   
   # ....................................
   def sendMessage(self, toAddrs, subject, msg):
      """
      @summary: Sends an email using the EmailNotifier's SMTP server to the 
                   specified recipients
      @param toAddrs: List of recipients
      @param subject: The subject of the email
      @param msg: The content of the email
      """
      if not isinstance(toAddrs, ListType):
         toAddrs = [toAddrs]
      
      mailMsg = ("From: {}\r\nTo: {}\r\nSubject: {}\r\n\r\n{}".format(
                              self.fromAddr, ", ".join(toAddrs), subject, msg))
      try:
         self.server.sendmail(self.fromAddr, toAddrs, mailMsg)
      except Exception, e:
         #raise LMError(currargs=e.args)
         # This had to be changed because we don't want to put LMError on LmBackend
         raise e
