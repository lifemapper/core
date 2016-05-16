"""
@summary: This script will send a user an email
"""
import argparse

# .............................................................................
if __name__ == "__main__":
   pass

"""
@summary: This script will build job requests and write them as files
@author: CJ Grady
@status: alpha
@version: 0.1
@license: gpl2
@copyright: Copyright (C) 2016, University of Kansas Center for Research

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
import argparse

from LmServer.notifications.email import EmailNotifier

# .............................................................................
if __name__ == "__main__":
   
   parser = argparse.ArgumentParser(prog="Lifemapper notifier script",
                      version="1.0.0")
   parser.add_argument('-t', type=str, help="Send email to this address", nargs='+')
   parser.add_argument('-s', type=str, help="The subject of the message", nargs=1)
   parser.add_argument('-m', nargs='1', type=str, help="The message to send")
   
   args = parser.parse_args()
   
   toAddresses = args.t
   subject = args.s
   msg = args.m

   notifier = EmailNotifier()
   notifier.sendMessage(toAddresses, subject, msg)
      
      