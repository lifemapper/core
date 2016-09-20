"""
@summary: This script is used to purge layers that have not been used for some
             amount of time.
@author: CJ Grady
@version: 1.0.0
@status: beta

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
from mx.DateTime import gmt

from LmCompute.common.layerManager import LayerManager
from LmCompute.common.localconstants import SHARED_DATA_PATH

# .............................................................................
if __name__ == "__main__":
   
   parser = argparse.ArgumentParser(prog="Lifemapper LmCompute Layer Purger",
                           description="Purges old layers from the database",
                           version="1.0.0")
   parser.add_argument('-t', '--daysOld', type=int, default=30,
      help="Purge (non-seeded) layers that haven't been touched in more than this many days")
   
   args = parser.parse_args()
   
   lm = LayerManager(SHARED_DATA_PATH)
   lm.purgeLayers(gmt().mjd - args.daysOld)
   lm.close()
   