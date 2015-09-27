"""
@summary: This the Lifemapper Species Distribution Modeling plugin
@author: CJ Grady
@contact: cjgrady [at] ku [dot] edu
@version: 3.0.0
@status: beta

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

__version__ = "3.0.0"

jobTypes = [
            (ProcessType.ATT_MODEL, 'sdm.maxent.meRunners', 'MEModelRunner'), # MaxEnt model job runner
            (ProcessType.ATT_PROJECT, 'sdm.maxent.meRunners', 'MEProjectionRunner'), # MaxEnt projection job runner
            (ProcessType.OM_MODEL, 'sdm.openModeller.omRunners', 'OMModelRunner'), # openModeller model job runner
            (ProcessType.OM_PROJECT, 'sdm.openModeller.omRunners', 'OMProjectionRunner'), # openModeller projection job runner
            (ProcessType.GBIF_TAXA_OCCURRENCE, 'sdm.gbif.gbifRunners', 'GBIFRetrieverRunner'), # GBIF processor job runner
            (ProcessType.BISON_TAXA_OCCURRENCE, 'sdm.bison.bisonRunners', 'BisonRetrieverRunner'), # BISON processor job runner
            (ProcessType.IDIGBIO_TAXA_OCCURRENCE, 'sdm.idigbio.idigbioRunners', 'IDIGBIORetrieverRunner') # iDigBio retriever runner
            (ProcessType.USER_TAXA_OCCURRENCE, 'sdm.csvocc.csvoccRunners', 'CSVRetrieverRunner') # CSV retriever runner
           ]
