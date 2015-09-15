"""
@summary: This the Lifemapper Range and Diversity Macroecology plugin
@author: CJ Grady
@contact: cjgrady [at] ku [dot] edu
@version: 3.1.0
@status: beta
@note: This is a transition from the old Lifemapper job framework

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
from LmCommon.common.lmconstants import ProcessType

__version__ = "3.1.0"

jobTypes = [
            (ProcessType.RAD_BUILDGRID, 'rad.buildShapegrid.buildShapegridRunner', 'BuildShapegridRunner'), # RAD Build shapegrid job runner
            (ProcessType.RAD_INTERSECT, 'rad.intersect.intersectRunner', 'IntersectRunner'), # RAD intersect job runner
            (ProcessType.RAD_COMPRESS, 'rad.compress.compressRunner', 'CompressRunner'), # RAD compress job runner
            (ProcessType.RAD_SWAP, 'rad.randomize.randomizeRunners', 'RandomizeSwapRunner'), # RAD randomize swap job runner
            (ProcessType.RAD_SPLOTCH, 'rad.randomize.randomizeRunners', 'RandomizeSplotchRunner'), # RAD randomize splotch job runner
            (ProcessType.RAD_CALCULATE, 'rad.calculate.calculateRunner', 'CalculateRunner'), # RAD calculate job runner
            (ProcessType.RAD_GRADY, 'rad.randomize.randomizeRunners', 'RandomizeGradyRunner') # RAD randomize CJ's method job runner
           ]
