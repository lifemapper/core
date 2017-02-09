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
@todo: Make sure that this is clear and usable outside of a makeflow context
"""
import os

from LmCommon.common.lmconstants import ProcessType
from LmServer.common.localconstants import POINT_COUNT_MAX, APP_PATH

PYTHON = "$PYTHON"

SINGLE_SPECIES_SCRIPTS_PATH = os.path.join(APP_PATH, 'LmCompute/tools/single')
MULTI_SPECIES_SCRIPTS_PATH = os.path.join(APP_PATH, 'LmCompute/tools/multi')
COMMON_SCRIPTS_PATH = os.path.join(APP_PATH, 'LmCompute/tools/common')

# .............................................................................
#class CommandBuilder(object):
#   """
#   @summary: The CommandBuilder class is used to create commands to be used 
#                with LmCompute
#   """
#   pass

def makeCommand(obj):
   """
   @summary: Assemble command for different processes
   # .......... SDM ..........
   ATT_MODEL = 110
   ATT_PROJECT = 120
   OM_MODEL = 210
   OM_PROJECT = 220
   # .......... RAD ..........
   RAD_BUILDGRID = 305
   RAD_INTERSECT = 310
   RAD_COMPRESS = 320
   RAD_SWAP = 331
   RAD_SPLOTCH = 332
   RAD_GRADY = 333
   RAD_CALCULATE = 340
   # .......... Occurrences ..........
   GBIF_TAXA_OCCURRENCE = 405
   BISON_TAXA_OCCURRENCE = 410
   IDIGBIO_TAXA_OCCURRENCE = 415
   # .......... User-defined ..........
   USER_TAXA_OCCURRENCE = 420
   # .......... Notify ..........
   SMTP = 510
   """
   ptype = obj.processType
   if ProcessType.isOccurrence(ptype):
      if ptype == ProcessType.GBIF_TAXA_OCCURRENCE:
         makeGbifOccurrenceSetCommand(obj)
      elif ptype == ProcessType.BISON_TAXA_OCCURRENCE:
         makeBisonOccurrenceSetCommand(obj)
      elif ptype == ProcessType.IDIGBIO_TAXA_OCCURRENCE:
         makeIdigbioOccurrenceSetCommand(obj)
      elif ptype == ProcessType.USER_TAXA_OCCURRENCE:
         make
   
   elif ProcessType.isSDM(ptype):
      if ptype == ProcessType.ATT_PROJECT:
         mfdoc.addMaxentProjection(o)
      elif ptype == ProcessType.OM_PROJECT:
         mfdoc.addOmProjection(o)
   
   elif ptype == ProcessType.RAD_INTERSECT:
      mfdoc.addIntersect(o)
         
   elif ProcessType.isRAD(ptype):
      
   

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
      "-l" : "bisonPoints-{0}.log".format(occ.getId()),
      "-s" : occStatusFn,
   }

   # Join arguments
   args = ' '.join(["{opt} {val}".format(opt=o, val=v) for o, v in options.iteritems()])
   
   pointsUrl = occ.getRawDLocation()
   maxPoints = POINT_COUNT_MAX
   outName = os.path.basename(occ.createLocalDLocation()).replace('.shp', '')

   occCmd = "{python} {jobRunner} {pointsUrl} {maxPoints} {outName} {options}".format(
      python=PYTHON,
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
      python=PYTHON,
      jobRunner=os.path.join(SINGLE_SPECIES_SCRIPTS_PATH, "gbif_points.py"),
      pointsCsv=rawCsvFn,
      rawCount=count,
      maxPoints=maxPoints,
      outName=outName,
      options=args)
   
   return occCmd, occStatusFn

# .............................................................................
def makeUserOccurrenceSetCommand(occ):
   """
   @summary: Generate command to fill a GBIF occurrence set
   """
   # NOTE: This may need to change to something else in the future, but for now,
   #          we'll save a step and have the outputs written to their final 
   #          location
   outDir = os.path.dirname(occ.createLocalDLocation())
   occStatusFn = "{0}-{1}.status".format(occ.processType, occ.getId())
   
   options = {
      "-n" : "{0}-{1}".format(occ.processType, occ.getId()),
      "-o" : outDir,
      "-l" : "userPoints-{0}.log".format(occ.getId()),
      "-s" : occStatusFn,
   }

   # Join arguments
   args = ' '.join(["{opt} {val}".format(opt=o, val=v) for o, v in options.iteritems()])
   
   rawCsvFn = occ.getRawDLocation()
   count = occ.queryCount
   maxPoints = POINT_COUNT_MAX
   outName = os.path.basename(occ.createLocalDLocation()).replace('.shp', '')

   occCmd = "{python} {jobRunner} {pointsCsv} {rawCount} {maxPoints} {outName} {options}"
   .format(python=PYTHON,
           jobRunner=os.path.join(SINGLE_SPECIES_SCRIPTS_PATH, "user_points.py"),
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
      "-l" : "idigbioPoints-{0}.log".format(occ.getId()),
      "-s" : occStatusFn,
   }

   # Join arguments
   args = ' '.join(["{opt} {val}".format(opt=o, val=v) for o, v in options.iteritems()])
   
   taxonKey = occ.getRawDLocation()
   maxPoints = POINT_COUNT_MAX
   outName = os.path.basename(occ.createLocalDLocation()).replace('.shp', '')

   occCmd = "{python} {jobRunner} {taxonKey} {maxPoints} {outName} {options}".format(
      python=PYTHON,
      jobRunner=os.path.join(SINGLE_SPECIES_SCRIPTS_PATH, "idigbio_points.py"),
      taxonKey=taxonKey,
      maxPoints=maxPoints,
      outName=outName,
      options=args)
   
   return occCmd, occStatusFn

# .............................................................................
def makeMaxentSdmModelCommand(mdl, jrFn):
   """
   @summary: Generate a command to create a Maxent model for the model object
   @param mdl: The model object to generate the command for
   @param jrFn: The filename of the XML request to use for model generation
   @todo: Add metrics
   @todo: Constant for script path
   """
   outDir = os.path.dirname(mdl.createLocalDLocation())
   mdlStatusFn = "{0}-{1}.status".format(ProcessType.ATT_MODEL, mdl.getId())
   
   options = {
      "-n" : "{0}-{1}".format(ProcessType.ATT_MODEL, mdl.getId()),
      "-o" : outDir,
      "-l" : "meMdl-{0}.log".format(mdl.getId()),
      "-s" : mdlStatusFn,
   }
   
   # Join arguments
   args = ' '.join(["{opt} {val}".format(opt=o, val=v) for o, v in options.iteritems()])

   mdlCmd = "{python} {meRunner} {mdlXml} {options}".format(
      python=PYTHON,
      meRunner=os.path.join(SINGLE_SPECIES_SCRIPTS_PATH, "me_model.py"),
      mdlXml=jrFn,
      options=args)
   
   return mdlCmd, mdlStatusFn
   
# .............................................................................
def makeMaxentSdmProjectionCommand(prj, jrFn):
   """
   @summary: Generate a command to create a Maxent projection for the projection object
   @param prj: The projection object to generate the command for
   @param jrFn: The filename of the XML request to use for projection generation
   @todo: Add metrics
   @todo: Constant for script path
   """
   outDir = os.path.dirname(prj.createLocalDLocation())
   prjStatusFn = "{0}-{1}.status".format(ProcessType.ATT_PROJECT, prj.getId())
   
   options = {
      "-n" : "{0}-{1}".format(ProcessType.ATT_PROJECT, prj.getId()),
      "-o" : outDir,
      "-l" : "mePrj-{0}.log".format(prj.getId()),
      "-s" : prjStatusFn,
   }
   
   # Join arguments
   args = ' '.join(["{opt} {val}".format(opt=o, val=v) for o, v in options.iteritems()])

   prjCmd = "{python} {meRunner} {prjXml} {options}".format(
      python=PYTHON,
      meRunner=os.path.join(SINGLE_SPECIES_SCRIPTS_PATH, "me_projection.py"),
      prjXml=jrFn,
      options=args)
   
   return prjCmd, prjStatusFn

# .............................................................................
def makeOmSdmModelCommand(mdl, jrFn):
   """
   @summary: Generate a command to create a openModeller model for the model object
   @param mdl: The model object to generate the command for
   @param jrFn: The filename of the XML request to use for model generation
   @todo: Add metrics
   @todo: Constant for script path
   """
   outDir = os.path.dirname(mdl.createLocalDLocation())
   mdlStatusFn = "{0}-{1}.status".format(ProcessType.OM_MODEL, mdl.getId())
   
   options = {
      "-n" : "{0}-{1}".format(ProcessType.OM_MODEL, mdl.getId()),
      "-o" : outDir,
      "-l" : "omMdl-{0}.log".format(mdl.getId()),
      "-s" : mdlStatusFn,
   }
   
   # Join arguments
   args = ' '.join(["{opt} {val}".format(opt=o, val=v) for o, v in options.iteritems()])

   mdlCmd = "{python} {omRunner} {mdlXml} {options}".format(
      python=PYTHON,
      omRunner=os.path.join(SINGLE_SPECIES_SCRIPTS_PATH, "om_model.py"),
      mdlXml=jrFn,
      options=args)
   
   return mdlCmd, mdlStatusFn

# .............................................................................
def makeOmSdmProjectionCommand(prj, jrFn):
   """
   @summary: Generate a command to create a openModeller projection for the projection object
   @param prj: The projection object to generate the command for
   @param jrFn: The filename of the XML request to use for projection generation
   @todo: Add metrics
   @todo: Constant for script path
   """
   outDir = os.path.dirname(prj.createLocalDLocation())
   prjStatusFn = "{0}-{1}.status".format(ProcessType.OM_PROJECT, prj.getId())
   
   options = {
      "-n" : "{0}-{1}".format(ProcessType.OM_PROJECT, prj.getId()),
      "-o" : outDir,
      "-l" : "mePrj-{0}.log".format(prj.getId()),
      "-s" : prjStatusFn,
   }
   
   # Join arguments
   args = ' '.join(["{opt} {val}".format(opt=o, val=v) for o, v in options.iteritems()])

   prjCmd = "{python} {omRunner} {prjXml} {options}".format(
      python=PYTHON,
      omRunner=os.path.join(SINGLE_SPECIES_SCRIPTS_PATH, "om_projection.py"),
      prjXml=jrFn,
      options=args)
   
   return prjCmd, prjStatusFn

# .............................................................................
# .                               Multi Species                               .
# .............................................................................

# .............................................................................
def makeBuildShapegridCommand(sgFn, minX, minY, maxX, maxY, cellSize, epsg,
                              cellSides, cutoutWktFn=None):
   """
   @summary: Creates a command that will generate a shapegrid
   @param sgFn: The file location to write the shapegrid
   @param minX: The minimum X value of this shapegrid
   @param minY: The minimum Y value of this shapegrid
   @param maxX: The maximum X value of this shapegrid
   @param maxY: The maximum Y value of this shapegrid
   @param cellSize: The size of each cell in the appropriate units for the EPSG
   @param epsg: The EPSG code for this shapegrid
   @param cellSides: The number of sides for each cell (4 or 6)
   @param cutoutWktFn: A location of a file containing well-known text for an
                          area to cut out of the shapegrid 
   """
   options = ''
   if cutoutWktFn is not None:
      options = "--cutoutWktFn={0}".format(cutoutWktFn)
      
   cmd = "{python} {script} {sgFn} {minX} {minY} {maxX} {maxY} {cellSize} {epsg} {cellSides} {options}".format(
      python=PYTHON,
      script=os.path.join(MULTI_SPECIES_SCRIPTS_PATH, 'build_shapegrid.py'),
      sgFn=sgFn,
      minX=minX,
      minY=minY,
      maxX=maxX,
      maxY=maxY,
      cellSize=cellSize,
      epsg=epsg,
      cellSides=cellSides,
      options=options
   )
   return cmd

# .............................................................................
def makeCalculatePamStatsCommand(pamFn, sitesFn, speciesFn, diversityFn, 
                                 treeFn=None, schluter=False, speciesCovFn=None, 
                                 siteCovFn=None):
   """
   @summary: Creates a command that will generate statistics for a PAM
   @param pamFn: A file location for an assembled PAM Matrix
   @param sitesFn: A file location to write the resulting site statistics Matrix
   @param speciesFn: A file location to write the resulting species statistics
   @param diversityFn: A file location to write the diversity statistics Matrix
   @param treeFn: An optional tree file to be used for statistics
   @param schluter: Boolean indicating if Schluter statistics should be 
                       calculated
   @param speciesCovFn: If provided, write species covariance statistics to 
                           this file
   @param siteCovFn: If provided, write site covariance statistics to this file
   """
   options = ""
   if treeFn is not None:
      options += "-t {0} ".format(treeFn)
   
   if schluter:
      options += "--schluter "
      
   if speciesCovFn is not None:
      options += "--speciesCovFn={0} ".format(speciesCovFn)
   
   if siteCovFn is not None:
      options += "--siteCovFn={0} ".format(siteCovFn)
   
   cmd = "{python} {script} {pamFn} {sitesFn} {speciesFn} {diversityFn} {options}".format(
      python=PYTHON,
      script=os.path.join(MULTI_SPECIES_SCRIPTS_PATH, 'calculate_pam_stats.py'),
      pamFn=pamFn, 
      sitesFn=sitesFn, 
      speciesFn=speciesFn, 
      diversityFn=diversityFn, 
      options=options
   )
   return cmd

# .............................................................................
def makeConcatenateMatricesCommand(mtxFnList, outFn, axis=0):
   """
   @summary: Creates a command to concatenate a list of Matrix objects
   @param mtxFnList: A list of Matrix file names to concatenate
   @param outFn: The file location to store the resulting Matrix
   @param axis: The axis to concatenate on
   """
   cmd = "{python} {script} {outFn} {axis} {mtxFns}".format(
      python=PYTHON,
      script=os.path.join(COMMON_SCRIPTS_PATH, 'concatenate_matrices.py'),
      outFn=outFn,
      axis=axis,
      mtxFns=' '.join(mtxFnList)
   )
   return cmd

# .............................................................................
def makeEncodeHypothessCommand(shapegridFn, outFn, lyrs, eventField=None):
   """
   @summary: Creates a command to encode similar biogeographic hypotheses
   @param shapegridFn: The file location of the shapegrid shapefile
   @param outFn: The file location to write the resulting Matrix
   @param lyrs: A list of layer file names to encode
   @param eventField: If provided, uses this field in the layers to determine 
                         events
   @note: Unless all hyptheses are encoded using the same method, you will 
             likely want to run this multiple times and concatenate the results
   """
   options = ''
   if eventField is not None:
      options += "-e {0}".format(eventField)
      
   cmd = "{python} {script} {sgFn} {outFn} {layers} {options}".format(
      python=PYTHON,
      script=os.path.join(MULTI_SPECIES_SCRIPTS_PATH, 'encode_hypotheses.py'),
      sgFn=shapegridFn,
      outFn=outFn,
      layers=' '.join(lyrs),
      options=options
   )
   return cmd

# .............................................................................
def makeEncodePhylogenyCommand(treeFn, pamFn, outFn, mtxIndices=None):
   """
   @summary: Creates a command to encode a phylogenetic tree into a matrix
   @param treeFn: A file location of a LmTree
   @param pamFn: A file location of a PAM Matrix
   @param outFn: A file location to write the resulting matrix
   @param mtxIndices: An optional file location of a JSON document where the 
                         keys are tip labels and the values are matrix indices
                         for this PAM
   """
   options = ''
   if mtxIndices is not None:
      options += "-m {0}".format(mtxIndices)
   cmd = "{python} {script} {treeFn} {pamFn} {outFn} {options}".format(
      python=PYTHON,
      script=os.path.join(MULTI_SPECIES_SCRIPTS_PATH, 'encode_phylogeny.py'),
      treeFn=treeFn,
      pamFn=pamFn,
      outFn=outFn,
      options=options
   )
   return cmd

# .............................................................................
def makeGradyRandomizeCommand(pamFn, outRandomFn):
   """
   @summary: Creates a command that will generated a random PAM using the Grady
                method
   @param pamFn: The file location of the PAM Matrix
   @param outRandomFn: The file location to write the resulting Matrix
   """
   cmd = "{python} {script} {pamFn} {outFn}".format(
      python=PYTHON,
      script=os.path.join(MULTI_SPECIES_SCRIPTS_PATH, 'grady_randomize.py'),
      pamFn=pamFn,
      outFn=outRandomFn
   )
   return cmd

# .............................................................................
def makeGrimRasterIntersectCommand(shapegridFn, rasterFn, grimColFn, resolution,
                                   minPercent=None, ident=None):
   """
   @summary: Creates a command to generated a GRIM column
   @param shapegridFn: The file location of the shapegrid shapefile
   @param rasterFn: The file location of the raster environmental layer
   @param grimColFn: The file location to write the resulting Matrix column
   @param resolution: The resolution of the raster file
   @param minPercent: If provided, use the largest class method, otherwise,
                         use weighted mean [0,100]
   @param ident: An identifier for this Matrix column for metadata
   """
   options = ''
   if minPercent is not None:
      options += "--minPercent={0} ".format(minPercent)
   if ident is not None:
      options += "--ident={0}".format(ident)
      
   cmd = "{python} {script} {sgFn} {rFn} {outFn} {res} {options}".format(
      python=PYTHON,
      script=os.path.join(MULTI_SPECIES_SCRIPTS_PATH, 'grim_raster.py'),
      sgFn=shapegridFn,
      rFn=rasterFn,
      outFn=grimColFn,
      res=resolution,
      options=options
   )
   return cmd

# .............................................................................
def makeMCPACorrectPValuesCommand(obsValFn, pValFn, fValFns):
   """
   @summary: Creates a command to calculate and correct P-values
   @param obsValFn: A file location of an observed values Matrix
   @param pValFn: A file location to write the corrected P-values
   @param fValFns: A list of f value Matrix objects
   """
   cmd = "{python} {script} {obsFn} {pValsFn} {fVals}".format(
      python=PYTHON,
      script=os.path.join(MULTI_SPECIES_SCRIPTS_PATH, 'mcpa_correct_pvalues.py'),
      obsFn=obsValFn,
      pValsFn=pValFn,
      fVals=' '.join(fValFns)
   )
   return cmd

# .............................................................................
def makeMCPAObservedCommand(pamFn, phyloMtxFn, eMtxFn, adjRsqFn, partCorMtxFn,
                            fGlobalFn, fPartialFn, bgMtxFn=None):
   """
   @summary: Creates a command to run an observed MCPA run
   @param pamFn: The file location of the PAM Matrix
   @param phyloMtxFn: The file location of the encoded Phylogenetic Matrix
   @param eMtxFn: The file location of the GRIM Matrix
   @param adjRsqFn: The file location to store the adjusted R-squared values
   @param partCorMtxFn: The file location to store the partial correlation 
                           Matrix
   @param fGlobalFn: The file location to store the Matrix of F Global values
   @param fPartialFn: The file location to store the matrix of F partial values
   @param bgMtxFn: The file location of the encoded BioGeographic hypotheses
   """
   options = ""
   if bgMtxFn is not None:
      options += "-b {0}".format(bgMtxFn)
      
   cmd = "{python} {script} {pamFn} {pFn} {eFn} {r2} {partMtx} {fGlobal} {fPart} {options}".format(
      python=PYTHON,
      script=os.path.join(MULTI_SPECIES_SCRIPTS_PATH, 'mcpa_observed.py'),
      pamFn=pamFn,
      pFn=phyloMtxFn,
      eFn=eMtxFn,
      r2=adjRsqFn,
      partMtx=partCorMtxFn,
      fGlobal=fGlobalFn,
      fPart=fPartialFn,
      options=options
   )
   return cmd

# .............................................................................
def makeMCPARandomCommand(pamFn, phyloMtxFn, eMtxFn, fGlobalFn, fPartialFn,
                          bgMtxFn=None, numRandomizations=1):
   """
   @summary: Creates a command to run a number of MCPA random runs
   @param pamFn: The file location of the PAM Matrix
   @param phyloMtxFn: The file location of the encoded Phylogenetic Matrix
   @param eMtxFn: The file location of the GRIM Matrix
   @param fGlobalFn: The file location to store the Matrix of F Global values
   @param fPartialFn: The file location to store the matrix of F partial values
   @param bgMtxFn: The file location of the encoded BioGeographic hypotheses
   @param numRandomizations: The number of randomizations to perform in this run
   """
   options = "-n {0} ".format(numRandomizations)
   if bgMtxFn is not None:
      options += "-b {0}".format(bgMtxFn)
      
   cmd = "{python} {script} {pamFn} {pFn} {eFn} {fGlobal} {fPart} {options}".format(
      python=PYTHON,
      script=os.path.join(MULTI_SPECIES_SCRIPTS_PATH, 'mcpa_random.py'),
      pamFn=pamFn,
      pFn=phyloMtxFn,
      eFn=eMtxFn,
      fGlobal=fGlobalFn,
      fPart=fPartialFn,
      options=options
   )
   return cmd

# .............................................................................
def makePAVIntersectRasterCommand(shapegridFn, rasterFn, pavFn, resolution, 
                                  minPresence, maxPresence, percentPresence,
                                  squid=None):
   """
   @summary: Creates a command to intersect a raster layer and a shapegrid to 
                produce a PAV
   @param shapegridFn: The file location of the shapegrid shapefile
   @param rasterFn: The file location of the raster file
   @param pavFn: The file location to write the resulting PAV Matrix
   @param resolution: The resolution of the raster file
   @param minPresence: The minimum value of the attribute to be considered
                          present
   @param maxPresence: The maximum value of the attribute to be considered
                          present
   @param percentPresence: The portion of a cell that must be present for the 
                              cell to be present
   @param squid: A Lifemapper SQUID to be added as metadata
   """
   options = ''
   if squid is not None:
      options = "--squid={0}".format(squid)
      
   cmd = "{python} {script} {sgFn} {rFn} {pavFn} {res} {minP} {maxP} {percent} {options}".format(
      python=PYTHON,
      script=os.path.join(SINGLE_SPECIES_SCRIPTS_PATH, 'intersect_raster.py'),
      sgFn=shapegridFn,
      rFn=rasterFn,
      pavFn=pavFn,
      res=resolution,
      minP=minPresence,
      maxP=maxPresence,
      percent=percentPresence,
      options=options
   )
   return cmd

# .............................................................................
def makePAVIntersectVectorCommand(shapegridFn, vectorFn, pavFn, presenceAttrib,
                                  minPresence, maxPresence, percentPresence,
                                  squid=None):
   """
   @summary: Creates a command to intersect a vector layer and a shapegrid to 
                produce a PAV
   @param shapegridFn: The file location of the shapegrid shapefile
   @param vectorFn: The file location of the vector shapefile
   @param pavFn: The file location to write the resulting PAV Matrix
   @param presenceAttrib: The attribute of the vector to be used to determine
                             presence
   @param minPresence: The minimum value of the attribute to be considered
                          present
   @param maxPresence: The maximum value of the attribute to be considered
                          present
   @param percentPresence: The portion of a cell that must be present for the 
                              cell to be present
   @param squid: A Lifemapper SQUID to be added as metadata
   """
   options = ''
   if squid is not None:
      options = "--squid={0}".format(squid)
      
   cmd = "{python} {script} {sgFn} {vFn} {pavFn} {pa} {minP} {maxP} {percent} {options}".format(
      python=PYTHON,
      script=os.path.join(SINGLE_SPECIES_SCRIPTS_PATH, 'intersect_vector.py'),
      sgFn=shapegridFn,
      vFn=vectorFn,
      pavFn=pavFn,
      pa=presenceAttrib,
      minP=minPresence,
      maxP=maxPresence,
      percent=percentPresence,
      options=options
   )
   return cmd

# .............................................................................
def makeSplotchRandomizeCommand(pamFn, shapegridFn, numSides, outRandomFn):
   """
   @summary: Creates a command that will generated a random PAM using the 
                splotch method
   @param pamFn: The file location of the PAM Matrix
   @param shapegridFn: The file location of the associated shapegrid
   @param numSides: The number of sides each cell has
   @param outRandomFn: The file location to write the resulting Matrix
   """
   cmd = "{python} {script} {pamFn} {sgFn} {numSides} {outFn}".format(
      python=PYTHON,
      script=os.path.join(MULTI_SPECIES_SCRIPTS_PATH, 'splotch_randomize.py'),
      pamFn=pamFn,
      sgFn=shapegridFn,
      numSides=numSides,
      outFn=outRandomFn
   )
   return cmd

# .............................................................................
def makeSwapRandomizeCommand(pamFn, numSwaps, outRandomFn):
   """
   @summary: Creates a command that will generated a random PAM using the swap
                method
   @param pamFn: The file location of the PAM Matrix
   @param numSwaps: The number of swaps to perform
   @param outRandomFn: The file location to write the resulting Matrix
   """
   cmd = "{python} {script} {pamFn} {numSwaps} {outFn}".format(
      python=PYTHON,
      script=os.path.join(MULTI_SPECIES_SCRIPTS_PATH, 'swap_randomize.py'),
      pamFn=pamFn,
      numSwaps=numSwaps,
      outFn=outRandomFn
   )
   return cmd
