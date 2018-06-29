"""
@summary: Script to convert a raster from one format to another
@author: CJ Grady
@license: gpl2
@copyright: Copyright (C) 2018, University of Kansas Center for Research

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

from LmBackend.common.layerTools import convertTiffToAscii

# .............................................................................
if __name__ == '__main__':
   parser = argparse.ArgumentParser(
      description='This script converts a tiff to an ascii')
   
   parser.add_argument('tiff_fn', type=str)
   parser.add_argument('ascii_fn', type=str)
   args = parser.parse_args()

   convertTiffToAscii(args.tiff_fn, args.ascii_fn)

