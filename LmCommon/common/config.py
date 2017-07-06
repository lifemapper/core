"""
@summary: Location of local configuration options
@author: CJ Grady
@contact: cjgrady [at] ku [dot] edu
@license: gpl2
@copyright: Copyright (C) 2017, University of Kansas Center for Research

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

from LmCommon.common.singleton import singleton

# Looks for a Lifemapper configuration file path environment variable.  If one
#    cannot be found, raise an exception
COMPUTE_CONFIG_FILENAME = os.getenv('LIFEMAPPER_COMPUTE_CONFIG_FILE') 
SERVER_CONFIG_FILENAME = os.getenv('LIFEMAPPER_SERVER_CONFIG_FILE') 
SITE_CONFIG_FILENAME = os.getenv('LIFEMAPPER_SITE_CONFIG_FILE')

#    
# .............................................................................
@singleton
class Config(object):
   """
   @summary: Lifemapper configuration object will read config.lmserver.ini and/or
             config.lmcompute.ini and site.ini and store values
   """
   # .....................................
   def __init__(self, fns=None, siteFn=SITE_CONFIG_FILENAME, 
                   defaultFns=[COMPUTE_CONFIG_FILENAME, SERVER_CONFIG_FILENAME]):
      """
      @summary: Constructor.  Uses config files in order fns then siteFn then defaultFns
      @note: Last file specified wins
      """
      # Start a list of config files.  Begin with default config files
      fileList = defaultFns
      if isinstance(fileList, basestring):
         fileList = list(fileList)
      
      # Add site config files
      if siteFn is not None:
         fileList.append(siteFn)
         
      # Add specified config files (ex BOOM config)
      if fns is not None:
         if not isinstance(fns, list):
            fns = [fns]
         fileList.extend(fns)
         
      # Remove Nones if they exist
      fileList = [f for f in fileList if f is not None]
      
      if fileList is None or len(fileList) == 0:
         raise ValueError, "Missing LIFEMAPPER_SERVER_CONFIG_FILE or LIFEMAPPER_COMPUTE_CONFIG_FILE environment variable"
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
   def getsections(self, sectionPrefix):
      matching = []
      for section in self.config.sections():
         if section.startswith(sectionPrefix):
            matching.append(section)
      return matching

   # .....................................
   def getoptions(self, section):
      return self.config.option(section)

   # .....................................
   def reload(self):
      """
      @summary: This function will reload the configuration file(s) and the 
                   site-specific configuration file.  This will catch any  
                   updates to the configuration without having to stop and 
                   restart the process.
      """
      self.config = ConfigParser.SafeConfigParser()
      readConfigFiles = self.config.read(self.configFiles)
      if len(readConfigFiles) == 0:
         raise Exception('No config files found matching {0}'.format(self.configFiles))
      