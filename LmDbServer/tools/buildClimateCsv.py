"""
@summary: This module will create a csv file with a list of 2-record lines.  
          Each line will contain a relative pathname of an environmental layer
          in TIFF format, and the sha256sum of that layer.
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
import hashlib
import os

def _walkit(topDir):
   output = []
   basename = os.path.basename(topDir)
   discard = topDir.rstrip(basename)
   for dirname, dirlist, filelist in os.walk(topDir, topdown=True):
      for fname in filelist:
         if fname.endswith('.tif'):
            fullfname = os.path.join(dirname, fname)
            shasum = hashlib.sha256(fullfname).hexdigest()
            relfname = fullfname.lstrip(discard)
            output.append([shasum, relfname])
#             print shasum, ',  ', relfname
   return output

# .............................................................................
if __name__ == "__main__":
   
#    # Use the argparse.ArgumentParser class to handle the command line arguments
#    parser = argparse.ArgumentParser(
#              description="Build a package of layers from a group of scenarios")
#    parser.add_argument("topDir", metavar='TopDirectory', type=str, nargs='1',
#                        help="The path to the directory containing desired layers")
#    parser.add_argument("pkgName", metavar='PackageName', type=str, nargs=1,
#                        help="The name of this package")
#    
#    args = parser.parse_args()
#    
#    # Pull out the package name and scenario ids
#    topDir = args.topDir
#    basename = os.path.basename(topDir)
# 
#    if args.pkgName is not None:
#       pkgName = args.pkgName
#    else:
#       pkgName = basename

   topDir = '/tank/data/input/climate/5min'
   pkgName = '5min-past-present-future'
      
   if not os.path.exists(topDir):
      raise Exception('{} directory does not exist'.format(topDir))
   
   output = _walkit(topDir)
   outfname = os.path.join(topDir, pkgName+'.csv')
   if os.path.exists(outfname):
      os.remove(outfname)
   f = open(outfname, 'w')
   for fileinfo in output:
      f.write('{},  {}\n'.format(fileinfo[0], fileinfo[1]))
   f.close()
