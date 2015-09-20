"""
@summary: This module will create a tar ball with all of the layers in a 
             scenario that can be used for layer seeding.  It will also produce 
             a configuration file to be used with the layer seeder to seed 
             these layers for an LmCompute instance.
@author: CJ Grady
@version: 1.0
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
import argparse
import glob
import os
from StringIO import StringIO
import sys
from tempfile import NamedTemporaryFile
from zipfile import ZipFile, ZIP_DEFLATED

from LmServer.common.lmconstants import ENV_DATA_PATH
from LmServer.common.localconstants import DATA_PATH
from LmServer.common.log import ConsoleLogger
from LmServer.db.scribe import Scribe

LYR_BASE_PATH = os.path.join(DATA_PATH, ENV_DATA_PATH)

# .............................................................................
if __name__ == "__main__":
   
   # Use the argparse.ArgumentParser class to handle the command line arguments
   parser = argparse.ArgumentParser(
             description="Build a package of layers from a group of scenarios")
   parser.add_argument("pkgName", metavar='PackageName', type=str, nargs=1,
                       help="The name of this package")
   parser.add_argument("scnIds", metavar='ScenarioId', type=int, nargs='+',
                       help="The id of a scenario to add to the package")
   parser.add_argument('-f', '--fileTypes', type=str, choices=['a', 't', 'b'], 
                       help="Specify which file types to use: a - ascii grids, t - geotiffs, b - both (default both)")
   
   args = parser.parse_args()
   
   # Pull out the package name and scenario ids
   pkgName = args.pkgName[0] # Returns a list so get the first element
   scnIds = args.scnIds
   
   addTiffs = True
   addAsciis = True

   if args.fileTypes is not None:
      if args.fileTypes == 'a': # Only ASCII grids
         addTiffs = False
      elif args.fileTypes == 't': # Only GeoTiffs
         addAsciis = False
      
   layers = []
   scribe = Scribe(ConsoleLogger())
   scribe.openConnections()

   # Create a zip file
   with ZipFile('%s.zip' % pkgName, 'w', compression=ZIP_DEFLATED, 
                allowZip64=True) as myZip:
      
      # Process each scenario
      for scnId in scnIds:
         scn = scribe.getScenario(scnId)
         
         # Add each layer to the zip file
         for lyr in scn.layers:
            # host, urlpath, fn
            urlParts = lyr.metadataUrl.strip('http://').split('/')
            host = urlParts[0].replace('.', '_')
            upath = os.path.join(*urlParts[1:])
            
            if addTiffs:
               tiffLyrFn = lyr.getDLocation()
               tiffSeededFn = os.path.relpath(tiffLyrFn, LYR_BASE_PATH)
               layers.append(["%s/GTiff" % lyr.metadataUrl, tiffSeededFn])
               myZip.write(tiffLyrFn, tiffSeededFn)
            
            if addAsciis:
               tmp = os.path.relpath(lyr.getDLocation(), LYR_BASE_PATH)
               asciiBase = os.path.splitext(tmp)[0]
               ascSeededFn = '%s.asc' % asciiBase
               ascLyrFn = NamedTemporaryFile(suffix='.asc', delete=False).name
               layers.append(["%s/AAIGrid" % lyr.metadataUrl, ascSeededFn])
               lyr.copyData(tiffLyrFn, targetDataLocation=ascLyrFn, format='AAIGrid')
               myZip.write(ascLyrFn, ascSeededFn)
               # Remove temporary files
               for fn in glob.iglob("%s*" % os.path.splitext(ascLyrFn)[0]):
                  os.remove(fn)
               
      # Write layer csv file
      lyrCSVStr = []
      for row in layers:
         lyrCSVStr.append("%s, %s\n" % (row[0], row[1]))
      layerCsvFlo = StringIO(''.join(lyrCSVStr))
      layerCsvFlo.seek(0)
      myZip.writestr("%slayers.csv" % pkgName, layerCsvFlo.getvalue())

   scribe.closeConnections()
