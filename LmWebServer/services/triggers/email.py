"""
@summary: Module containing email triggers
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

from LmBackend.notifications.email import EmailNotifier

class TriggerEmail(object):
   """
   @summary: Class used to send trigger emails
   """
   # ....................................
   def __init__(self):
      self.cl = EmailNotifier()
      
   # ....................................
   def postSDMExperiment(self, exp, toAddrs):
      subject = "Successfully posted Lifemapper experiment"
      msg = """\
Thank you for using the the Lifemapper web application to create a new experiment.

Processing the projection maps included in your experiment can take several minutes or a few hours, depending on the number of distribution points for the species you chose. If you wish to close the Lifemapper web application while your experiment is processing you can return to your experiment by typing this url into your web browser:

%s

Check it out!

Scientists are using species distribution modeling to answer real world problems. Find out more on our webpage: http://lifemapper.org/?page_id=63""" % exp.metadataUrl
      self.cl.sendMessage(toAddrs, subject, msg)
      
   