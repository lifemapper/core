# -*- coding: UTF-8 -*-
"""
@summary: This is a module that contains tools for accessing system metadata
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
from collections import OrderedDict
import platform
import socket

MEM_INFO_FN = '/proc/meminfo'
CPU_INFO_FN = '/proc/cpuinfo'

# .............................................................................
def getSystemConfigurationDictionary():
   """
   @summary: Gets system configuration information and returns it in a dictionary
   """
   meminfo = OrderedDict()
   try:
      with open(MEM_INFO_FN) as f2:
         for line in f2:
            meminfo[line.split(':')[0]] = line.split(':')[1].strip()
   except IOError, e:
      raise e
   
   cpus = []
   x = 0
   try:
      with open(CPU_INFO_FN) as f2:
         for line in f2:
            if line.strip():
               if line.rstrip('\n').startswith('model name'):
                  cpus.append("{0} -{1}".format(x, line.rstrip('\n').split(':')[1]))
                  x += 1
   except IOError, e:
      raise e
   
   sysConfig = {
      "machine name" : platform.node(),
      "machine ip" : socket.gethostbyname(socket.gethostname()),
      "architecture" : platform.processor(),
      "os" : platform.system(),
      "total memory" : meminfo['MemTotal'],
      "cpus" : '\n'.join(cpus),
      "python version" : platform.python_version(),
      "linux version" : ' '.join(platform.linux_distribution())
   }
   return sysConfig
