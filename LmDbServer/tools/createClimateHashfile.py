"""
@summary: This module will create a CSV file with the sha256sum and relative
          path of all layers in a directory. 
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
import os

from LmCommon.common.lmconstants import LMFormat
from LmCommon.common.verify import computeHash

# .............................................................................
if __name__ == "__main__":
   
   # Use the argparse.ArgumentParser class to handle the command line arguments
   parser = argparse.ArgumentParser(
            description=('Build a CSV file with sha256sum hashes and relative'+ 
                         'filename for a directory of climate layers'))
   parser.add_argument('pkgName', metavar='packageName', type=str,
                       help="The name of this package")
   parser.add_argument('datapath', metavar='absolutePath', type=str, 
                       help='The absolute path of the top directory containing layers')
   
   args = parser.parse_args()
   
   # Pull out the package name and scenario ids
   pkgName = args.pkgName
   datapath = args.datapath
   if datapath.endswith(os.path.sep):
      datapath = datapath.rstrip(os.path.sep)
   basepath, topDir = os.path.split(datapath)
   
   outfname = os.path.join(basepath, pkgName + LMFormat.CSV.ext)
   if os.path.exists(outfname):
      os.remove(outfname)
      
   try:
      outf = open(outfname, 'w')
      
      for dirpath, dirnames, filenames in os.walk(datapath):
         for fname in filenames:
            fullname = os.path.join(dirpath, fname)
            if fullname.endswith(LMFormat.GTIFF.ext):
               relname = fullname.strip(basepath)
               print('Computing hash for {} (in {})'.format(relname, basepath))
               hashval = computeHash(dlocation=fullname)
               outf.write('{},  {}\n'.format(hashval, relname))
   except Exception, e:
      print('Failed to write {}, ({})'.format(outfname, e))
   finally:
      outf.close()
      
"""
$PYTHON LmDbServer/tools/createClimateHashfile.py Worldclim-GTOPO-ISRIC-SoilGrids-ConsensusLandCover /share/lm/data/layers/ryan/Worldclim-GTOPO-ISRIC-SoilGrids-ConsensusLandCover

$PYTHON LmDbServer/tools/createClimateHashfile.py 10min-past-present-future /share/lm/data/layers/10min-past-present-future


"""