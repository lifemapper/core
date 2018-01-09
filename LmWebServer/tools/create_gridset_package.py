"""
@summary: Create a package to be returned to a client based on a gridset
@author: CJ Grady
@status: alpha
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
@todo: Move this as necessary
@todo: Probably want to split of EML generating code to separate module(s)
"""
import argparse
import json
import os
import zipfile

from LmCommon.common.lmXml import tostring
from LmCommon.common.matrix import Matrix

from LmServer.common.log import ConsoleLogger
from LmServer.db.borgscribe import BorgScribe

from LmWebServer.formatters.emlFormatter import makeEml
from LmCommon.common.lmconstants import LMFormat, MatrixType
from LmWebServer.formatters.geoJsonFormatter import geoJsonify

# ..........................................................................
def assemble_package_for_gridset(gridset, outfile):
   """
   @summary: Creates an output zip file from the gridset
   """
   gsEml = tostring(makeEml(gridset))
   with zipfile.ZipFile(outfile, 
                        mode='w', 
                        compression=zipfile.ZIP_DEFLATED,
                        allowZip64=True) as outZip:
      outZip.writestr('gridset_{}.eml'.format(gridset.getId()), gsEml)
      outZip.write(gridset.tree.getDLocation(), os.path.basename(gridset.gree.getDLocation()))
      sg = gridset.getShapegrid()
      for mtx in gridset.getMatrices():
         # Need to get geojson where we can
         if mtx.matrixType in [MatrixType.PAM, MatrixType.ROLLING_PAM, 
                            MatrixType.ANC_PAM, MatrixType.SITES_COV_OBSERVED, 
                            MatrixType.SITES_COV_RANDOM, 
                            MatrixType.SITES_OBSERVED, MatrixType.SITES_RANDOM]:
            mtxObj = Matrix.load(mtx.getDLocation())
            mtxFn = '{}{}'.format(
               os.path.splitext(
                  os.path.basename(mtx.getDLocation()))[0], 
                                  LMFormat.GEO_JSON.ext)
            outZip.writestr(mtxFn, json.dumps(geoJsonify(sg.getDLocation(), 
                                                         matrix=mtxObj, 
                                                         mtxJoinAttrib=0)))
         else:
            outZip.write(mtx.getDLocation(), 
                      os.path.basename(mtx.getDLocation()))

# ..........................................................................
if __name__ == '__main__':
   parser = argparse.ArgumentParser(
                description='This script creates a package of gridset outputs')
   
   parser.add_argument('gsId', type=int, help='The gridset id number')
   parser.add_argument('out_file', type=str, 
                       help='The file location to write the output package')
   
   args = parser.parse_args()
   
   scribe = BorgScribe(ConsoleLogger())
   scribe.openConnections()
   
   gs = scribe.getGridset(gridsetId=args.gsId, fillMatrices=True)
   scribe.closeConnections()
   
   assemble_package_for_gridset(gs, args.out_file)
   