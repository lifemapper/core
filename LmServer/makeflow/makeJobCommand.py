"""
@summary: This module contains functions used to generate compute commands
@author: CJ Grady
@version: 1.0
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
import os

from LmCommon.common.lmconstants import ProcessType
from LmServer.common.localconstants import POINT_COUNT_MAX

SINGLE_SPECIES_SCRIPTS_PATH = os.path.join(APP_PATH, 'LmCompute/tools/single')
MULTI_SPECIES_SCRIPTS_PATH = os.path.join(APP_PATH, 'LmCompute/tools/multi')

WORK_DIR = ''

# .............................................................................
#class CommandBuilder(object):
#   """
#   @summary: The CommandBuilder class is used to create commands to be used 
#                with LmCompute
#   """
#   pass

def makeBisonOccurrenceSetCommand(occ):
   """
   @summary: Assemble command to fill in BISON points
   """
   # NOTE: This may need to change to something else in the future, but for now,
   #          we'll save a step and have the outputs written to their final 
   #          location
   outDir = os.path.dirname(occ.createLocalDLocation())
   occStatusFn = "{0}-{1}.status".format(ProcessType.BISON_TAXA_OCCURRENCE, occ.getId())
   
   options = {
      "-n" : "{0}-{1}".format(ProcessType.BISON_TAXA_OCCURRENCE, occ.getId()),
      "-o" : outDir,
      "-w" : WORK_DIR,
      "-l" : "bisonPoints-{0}.log".format(occ.getId()),
      "-s" : occStatusFn,
   }

   # Join arguments
   args = ' '.join(["{opt} {val}".format(opt=o, val=v) for o, v in options.iteritems()])
   
   pointsUrl = occ.getRawDLocation()
   maxPoints = POINT_COUNT_MAX
   outName = os.path.basename(occ.createLocalDLocation()).replace('.shp', '')

   occCmd = "{python} {jobRunner} {pointsUrl} {maxPoints} {outName} {options}".format(
      python="$PYTHON",
      jobRunner=os.path.join(SINGLE_SPECIES_SCRIPTS_PATH, "bison_points.py"),
      pointsUrl=pointsUrl,
      maxPoints=maxPoints,
      outName=outName,
      options=args)
   
   return occCmd, occStatusFn

# .............................................................................
def makeGbifOccurrenceSetCommand(occ):
   """
   @summary: Generate command to fill a GBIF occurrence set
   """
   # NOTE: This may need to change to something else in the future, but for now,
   #          we'll save a step and have the outputs written to their final 
   #          location
   outDir = os.path.dirname(occ.createLocalDLocation())
   occStatusFn = "{0}-{1}.status".format(ProcessType.GBIF_TAXA_OCCURRENCE, occ.getId())
   
   options = {
      "-n" : "{0}-{1}".format(ProcessType.GBIF_TAXA_OCCURRENCE, occ.getId()),
      "-o" : outDir,
      "-w" : WORK_DIR,
      "-l" : "gbifPoints-{0}.log".format(occ.getId()),
      "-s" : occStatusFn,
   }

   # Join arguments
   args = ' '.join(["{opt} {val}".format(opt=o, val=v) for o, v in options.iteritems()])
   
   rawCsvFn = occ.getRawDLocation()
   count = occ.queryCount
   maxPoints = POINT_COUNT_MAX
   outName = os.path.basename(occ.createLocalDLocation()).replace('.shp', '')

   occCmd = "{python} {jobRunner} {pointsCsv} {rawCount} {maxPoints} {outName} {options}".format(
      python="$PYTHON",
      jobRunner=os.path.join(SINGLE_SPECIES_SCRIPTS_PATH, "gbif_points.py"),
      pointsCsv=rawCsvFn,
      rawCount=count,
      maxPoints=maxPoints,
      outName=outName,
      options=args)
   
   return occCmd, occStatusFn

# .............................................................................
def makeIdigbioOccurrenceSetCommand(occ):
   """
   @summary: Generate command to fill an iDigBio occurrence set
   """
   # NOTE: This may need to change to something else in the future, but for now,
   #          we'll save a step and have the outputs written to their final 
   #          location
   outDir = os.path.dirname(occ.createLocalDLocation())
   occStatusFn = "{0}-{1}.status".format(ProcessType.IDIGBIO_TAXA_OCCURRENCE, occ.getId())
   
   options = {
      "-n" : "{0}-{1}".format(ProcessType.IDIGBIO_TAXA_OCCURRENCE, occ.getId()),
      "-o" : outDir,
      "-w" : WORK_DIR,
      "-l" : "idigbioPoints-{0}.log".format(occ.getId()),
      "-s" : occStatusFn,
   }

   # Join arguments
   args = ' '.join(["{opt} {val}".format(opt=o, val=v) for o, v in options.iteritems()])
   
   taxonKey = occ.getRawDLocation()
   maxPoints = POINT_COUNT_MAX
   outName = os.path.basename(occ.createLocalDLocation()).replace('.shp', '')

   occCmd = "{python} {jobRunner} {taxonKey} {maxPoints} {outName} {options}".format(
      python="$PYTHON",
      jobRunner=os.path.join(SINGLE_SPECIES_SCRIPTS_PATH, "idigbio_points.py"),
      taxonKey=taxonKey,
      maxPoints=maxPoints,
      outName=outName,
      options=args)
   
   return occCmd, occStatusFn

# .............................................................................
def makeMaxentSdmModelCommand(mdl):
   return cmd, statusFn

# .............................................................................
def makeMaxentSdmProjectionCommand(prj):
   return prjCmd, prjStatusFn

# .............................................................................
def makeOmSdmModelCommand(mdl):
   return mdlCmd, mdlStatusFn

# .............................................................................
def makeOmSdmProjectionCommand(prj):
   return prjCmd, prjStatusFn

