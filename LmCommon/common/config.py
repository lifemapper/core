"""
@summary: Location of local configuration options
@author: CJ Grady
@contact: cjgrady [at] ku [dot] edu
@version: 1.0
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
"""
import ConfigParser
import os
from types import StringType, UnicodeType, ListType, TupleType

from LmCommon.common.singleton import singleton

# Looks for a Lifemapper configuration file path environment variable.  If one
#    cannot be found, raise an exception
COMPUTE_CONFIG_FILENAME = os.getenv('LIFEMAPPER_COMPUTE_CONFIG_FILE') 
SERVER_CONFIG_FILENAME = os.getenv('LIFEMAPPER_SERVER_CONFIG_FILE') 
SITE_CONFIG_SECTION = 'SiteConfig'
SITE_CONFIG_ITEM = 'SITE_CONFIG'

#    
# .............................................................................
@singleton
class Config(object):
   """
   @summary: Lifemapper configuration object will read config.lmserver.ini and/or
             config.lmcompute.ini and site.ini and store values
   """
   # .....................................
   def __init__(self, fns=[COMPUTE_CONFIG_FILENAME, SERVER_CONFIG_FILENAME]):
      if fns is None or len(fns) == 0:
         raise ValueError, "Missing LIFEMAPPER_SERVER_CONFIG_FILE or LIFEMAPPER_COMPUTE_CONFIG_FILE environment variable"
      fileList = []
      if isinstance(fns, StringType) or isinstance(fns, UnicodeType):
         fileList.append(fns)
      elif isinstance(fns, ListType) or isinstance(fns, TupleType):
         for tmp in fns:
            if tmp is not None:
               fileList.append(tmp)
      else:
         raise Exception('Construct Config with a list')
      self.configFiles = fileList
      self.reload()
      
   # .....................................
   def get(self, section, item):
      return self.config.get(section, item)
   
   # .....................................
   def getboolean(self, section, item):
      return self.config.getboolean(section, item)

   # .....................................
   def getfloat(self, section, item):
      return self.config.getfloat(section, item)
   
   # .....................................
   def getint(self, section, item):
      return self.config.getint(section, item)

   # .....................................
   def getlist(self, section, item):
      listStr = self.config.get(section, item).strip('[').strip(']')
      return [itm.strip() for itm in listStr.split(',')]

   # .....................................
   def reload(self):
      """
      @summary: This function will reload the configuration file(s) and the 
                   site-specific configuration file.  This will catch any  
                   updates to the configuration without having to stop and 
                   restart the process.
      """
      self.config = ConfigParser.SafeConfigParser()
      found = False
      for fn in self.configFiles:
         if os.path.exists(fn):
            self.config.read(fn)
            found = True
            pth, tmp = os.path.split(fn)
            try:
               fname = self.get(SITE_CONFIG_SECTION, SITE_CONFIG_ITEM)
            except Exception, e:
               print('Missing site config in file = {}'.format(fn))
#                msg = str(e) + '\n'
#                msg += 'Missing site config in file = ' + fn
#                raise Exception(msg)
            else:
               self.site = os.path.join(pth, fname)
               self.config.read(self.site)
      if not found:
         raise Exception('No config files found matching {0}'.format(self.configFiles))