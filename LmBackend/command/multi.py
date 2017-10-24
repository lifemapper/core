"""
@license: gpl2
@copyright: Copyright (C) 2017, University of Kansas Center for Research

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

from LmBackend.command.base import _LmCommand
from LmBackend.common.lmconstants import CMD_PYBIN, MULTI_SPECIES_SCRIPTS_DIR

# .............................................................................
class BuildShapegridCommand(_LmCommand):
   """
   @summary: This command will 
   """
   # ................................
   def __init__(self, shapegridFilename, minX, minY, maxX, maxY, cellSize, 
                epsg, numSides, cutoutWKTFilename=None):
      """
      @summary: Construct the command object
      """
      _LmCommand.__init__(self)
      self.outputs.append(shapegridFilename)
      
      self.sgFn = shapegridFilename
      self.minX = minX
      self.minY = minY
      self.maxX = maxX
      self.maxY = maxY
      self.cellSize = cellSize
      self.epsg = epsg
      self.cellSides = numSides
      self.cutout = cutoutWKTFilename
      if cutoutWKTFilename is not None:
         self.inputs.append(cutoutWKTFilename)
         self.optArgs = ' --cutoutWktFn={}'.format(cutoutWKTFilename)
      else:
         self.optArgs = ''

   # ................................
   def getCommand(self):
      """
      @summary: Get the raw command to run on the system
      """
      return '{pyBin} {script}{optArgs} {posArgs}'.format(pyBin=CMD_PYBIN, 
         script=os.path.join(MULTI_SPECIES_SCRIPTS_DIR, 'build_shapegrid.py'),
         optArgs=self.optArgs,
         posArgs = ' '.join([self.sgFn, self.minX, self.minY, self.maxX, 
                             self.maxY, self.cellSize, self.epsg, 
                             self.cellSides]))

# .............................................................................
class CalculateStatsCommand(_LmCommand):
   """
   @summary: This command will calculate statistics for a PAM
   """
   # ................................
   def __init__(self, pamFilename, sitesFilename, speciesFilename, 
                  diversityFilename, treeFilename=None, schluter=False,
                  speciesCovarianceFilename=None, sitesCovarianceFilename=None):
      """
      @summary: Construct the command object
      """
      _LmCommand.__init__(self)
      self.inputs.append(pamFilename)
      self.outputs.extend([sitesFilename, speciesFilename, diversityFilename])
      
      self.args = [pamFilename, sitesFilename, speciesFilename, 
                   diversityFilename]

      self.optArgs = []
      if treeFilename is not None:
         self.inputs.append(treeFilename)
         self.optArgs.append('-t {}'.format(treeFilename))
         
      if schluter is not None:
         self.optArgs.append('--schluter')
         
      if speciesCovarianceFilename is not None:
         self.outputs.append(speciesCovarianceFilename)
         self.optArgs.append(
            '--speciesCovFn={}'.format(speciesCovarianceFilename))
      
      if sitesCovarianceFilename is not None:
         self.outputs.append(sitesCovarianceFilename)
         self.optArgs.append('--sitCovFn={}'.format(sitesCovarianceFilename))

   # ................................
   def getCommand(self):
      """
      @summary: Get the raw command to run on the system
      """
      return '{} {} {} {}'.format(CMD_PYBIN, 
            os.path.join(MULTI_SPECIES_SCRIPTS_DIR, 'calculate_pam_stats.py'),
            ' '.join(self.optArgs), ' '.join(self.args))

# .............................................................................
class CreateAncestralPamCommand(_LmCommand):
   """
   @summary: This command will create an ancestral PAM from a PAM and tree
   """
   # ................................
   def __init__(self, pamFilename, treeFilename, outputFilename):
      """
      @summary: Construct the command object
      @param pamFilename: The file path to the PAM to use
      @param treeFilename: The file path to the tree to use
      @param outputFilename: The file path to store the output matrix
      """
      _LmCommand.__init__(self)
      self.inputs.extend([pamFilename, treeFilename])
      self.outputs.append(outputFilename)
      self.args = [pamFilename, treeFilename, outputFilename]

   # ................................
   def getCommand(self):
      """
      @summary: Get the raw command to run on the system
      """
      return '{} {} {}'.format(CMD_PYBIN, 
            os.path.join(MULTI_SPECIES_SCRIPTS_DIR, 'create_ancestral_pam.py'),
            self.args)

# .............................................................................
class EncodeHypothesesCommand(_LmCommand):
   """
   @summary: This command will encode biogeographic hypotheses with a shapegrid
   """
   # ................................
   def __init__(self, shapegridFilename, layerFilenames, outputFilename, 
                      eventField=None):
      """
      @summary: Construct the command object
      @param shapegridFilename: The file location of the shapegrid to use for 
                                   encoding
      @param layerFilenames: File location(s) of layers to encode
      @param outputFilename: The file location to store the encoded matrix
      @param eventField: Use this field in the layers to determine events
      """
      _LmCommand.__init__(self)
      
      self.sgFn = shapegridFilename
      if isinstance(layerFilenames, list):
         self.lyrFns = layerFilenames
      else:
         self.lyrFns = [layerFilenames]
      
      self.outFn = outputFilename
      self.eventField = eventField
      
      self.inputs.append(self.sgFn)
      self.inputs.extend(self.lyrFns)
      
      self.outputs.append(self.outFn)

   # ................................
   def getCommand(self):
      """
      @summary: Get the raw command to run on the system
      """
      return '{} {}{} {} {} {}'.format(CMD_PYBIN, 
            os.path.join(MULTI_SPECIES_SCRIPTS_DIR, 'encode_hypotheses.py'),
            ' -e {}'.format(self.eventField) if self.eventField is not None else '',
            self.sgFn, self.outFn, ' '.join(self.lyrFns))

# .............................................................................
class EncodePhylogenyCommand(_LmCommand):
   """
   @summary: This command will encode a tree and PAM into a matrix
   """
   # ................................
   def __init__(self, treeFilename, pamFilename, outMtxFilename, 
                      mashedPotato=None):
      """
      @summary: Construct the command object
      @param treeFilename: The file location of the tree to use for encoding
      @param pamFilename: The file location of the PAM to use for encoding
      @param outMtxFilename: The file location to write the encoded tree
      @param mashedPotato: The file location of the mashed potato 
      """
      _LmCommand.__init__(self)
      
      self.treeFn = treeFilename
      self.pamFn = pamFilename
      self.outMtxFn = outMtxFilename
      self.mashedPotato = mashedPotato
      
      self.inputs.extend([self.treeFn, self.pamFn])
      self.outputs.append(self.outMtxFn)
      
      if self.mashedPotato is not None:
         self.inputs.append(self.mashedPotato)

   # ................................
   def getCommand(self):
      """
      @summary: Get the raw command to run on the system
      """
      return '{} {}{} {} {} {}'.format(CMD_PYBIN, 
            os.path.join(MULTI_SPECIES_SCRIPTS_DIR, 'encode_phylogeny.py'),
            ' -m {}'.format(self.mashedPotato) if self.mashedPotato is not None else '',
            self.treeFn, self.pamFn, self.outMtxFn)

# .............................................................................
class McpaAssembleCommand(_LmCommand):
   """
   @summary: This command will assemble the outputs of MCPA into a single matrix
   """
   # ................................
   def __init__(self, envPartCorFilename, envAdjRsqFilename, envFGlobFilename, 
                envFPartFilename, envBHFGlobFilename, envBHFPartFilename,
                bgPartCorFilename, bgAdjRsqFilename, bgFGlobFilename,
                bgFPartFilename, bgBHFGlobFilename, bgBHFPartFilename, 
                outMtxFilename):
      """
      @summary: Construct the command object
      """
      _LmCommand.__init__(self)
      self.envPartCorFn = envPartCorFilename
      self.envAdjRsqFn = envAdjRsqFilename
      self.envFGlobFn = envFGlobFilename
      self.envFPartFn = envFPartFilename
      self.envBHFGlobFn = envBHFGlobFilename
      self.envBHFPartFn = envBHFPartFilename
      
      self.bgPartCorFn = bgPartCorFilename
      self.bgAdjRsqFn = bgAdjRsqFilename
      self.bgFGlobFn = bgFGlobFilename
      self.bgFPartFn = bgFPartFilename
      self.bgBHFGlobFn = bgBHFGlobFilename
      self.bgBHFPartFn = bgBHFPartFilename
      
      self.outMtxFn = outMtxFilename
      
      self.inputs.extend(
         [self.envPartCorFn, self.envAdjRsqFn, self.envFGlobFn, 
          self.envFPartFn, self.envBHFGlobFn, self.envBHFPartFn,
          self.bgPartCorFn, self.bgAdjRsqFn, self.bgFGlobFn, self.bgFPartFn, 
          self.bgBHFGlobFn, self.bgBHFPartFn])
      
      self.outputs.append(self.outMtxFn)

   # ................................
   def getCommand(self):
      """
      @summary: Get the raw command to run on the system
      """
      return '{} {} {}'.format(CMD_PYBIN, 
            os.path.join(MULTI_SPECIES_SCRIPTS_DIR, 'mcpa_assemble.py'),
            ' '.join([self.envPartCorFn, self.envAdjRsqFn, self.envFGlobFn, 
                      self.envFPartFn, self.envBHFGlobFn, self.envBHFPartFn,
                      self.bgPartCorFn, self.bgAdjRsqFn, self.bgFGlobFn, 
                      self.bgFPartFn, self.bgBHFGlobFn, self.bgBHFPartFn, 
                      self.outMtxFn]))

# .............................................................................
class McpaCorrectPValuesCommand(_LmCommand):
   """
   @summary: This command will correct the P-values generated by MCPA
   """
   # ................................
   def __init__(self, observedFilename, outPvaluesFilename, bhValuesFilename, 
                      fValueFilenames):
      """
      @summary: Construct the command object
      @param observedFilename: The file location of the observed values to test
      @param outPvaluesFilename: The file location to store the P-values
      @param bhValuesFilename: The file location to store the 
                                  Benjamini-Hochberg ouptut matrix used for 
                                  determining significant results
      @param fValueFilenames: The file location or list of file locations of
                                 F-values or a stack of F-values to correct
      """
      _LmCommand.__init__(self)
      
      if isinstance(fValueFilenames, list):
         self.fValFns = fValueFilenames
      else:
         self.fValFns = [fValueFilenames]
      
      self.obsFn = observedFilename
      self.pValFn = outPvaluesFilename
      self.bhFn = bhValuesFilename
      
      self.inputs.append(self.obsFn)
      self.inputs.extend(self.fValFns)
      
      self.outputs.extend([self.pValFn, self.bhFn])

   # ................................
   def getCommand(self):
      """
      @summary: Get the raw command to run on the system
      """
      return '{} {} {} {} {} {}'.format(CMD_PYBIN, 
            os.path.join(MULTI_SPECIES_SCRIPTS_DIR, 'mcpa_correct_pvalues.py'),
            self.obsFn, self.pValFn, self.bhFn, ' '.join(self.fValFns))

# .............................................................................
class McpaObservedCommand(_LmCommand):
   """
   @summary: This command will 
   """
   # ................................
   def __init__(self, pamFilename, treeMtxFilename, grimFilename, 
                      adjRsqFilename, partCorMtxFilename, 
                      fGlobalMtxFilename, fPartMtxFilename, 
                      hypothesesFilename=None):
      """
      @summary: Construct the command object
      @param pamFilename: The file location of the PAM to use
      @param treeMtxFilename: The file location of the encoded tree matrix
      @param grimFilename: The file location of the environmental data matrix
      @param fGlobalRandMtxFilename: The file location to write the random 
                                        F-Global values matrix
      @param fPartRandMtxFilename: The file location to write the random 
                                      F-partial correlation values matrix
      @param hypothesesFilename: The file location of the encoded biogeographic 
                                    hypotheses matrix
      @param numRandomizations: The number of randomization runs to perform
      """
      _LmCommand.__init__(self)
      self.inputs.extend([pamFilename, treeMtxFilename, grimFilename])
      self.outputs.extend([adjRsqFilename, partCorMtxFilename, 
                           fGlobalMtxFilename, fPartMtxFilename])
      
      self.pamFn = pamFilename
      self.treeMtxFn = treeMtxFilename
      self.grimFn = grimFilename
      self.adjRsqFn = adjRsqFilename
      self.partCorMtxFn = partCorMtxFilename
      self.fGlobFn = fGlobalMtxFilename
      self.fPartFn = fPartMtxFilename

      self.optArgs = ''
      if hypothesesFilename is not None:
         self.inputs.append(hypothesesFilename)
         self.optArgs += ' -b {}'.format(hypothesesFilename)

   # ................................
   def getCommand(self):
      """
      @summary: Get the raw command to run on the system
      """
      return '{} {}{} {} {} {} {} {} {} {}'.format(CMD_PYBIN, 
            os.path.join(MULTI_SPECIES_SCRIPTS_DIR, 'mcpa_observed.py'),
            self.optArgs, self.pamFn, self.treeMtxFn, self.grimFn, 
            self.adjRsqFn, self.partCorMtxFn, self.fGlobFn, self.fPartFn)

# .............................................................................
class McpaRandomCommand(_LmCommand):
   """
   @summary: This command will compute MCPA on randomized data
   """
   # ................................
   def __init__(self, pamFilename, treeMtxFilename, grimFilename, 
                      fGlobalRandMtxFilename, fPartRandMtxFilename, 
                      hypothesesFilename=None, numRadomizations=None):
      """
      @summary: Construct the command object
      @param pamFilename: The file location of the PAM to use
      @param treeMtxFilename: The file location of the encoded tree matrix
      @param grimFilename: The file location of the environmental data matrix
      @param fGlobalRandMtxFilename: The file location to write the random 
                                        F-Global values matrix
      @param fPartRandMtxFilename: The file location to write the random 
                                      F-partial correlation values matrix
      @param hypothesesFilename: The file location of the encoded biogeographic 
                                    hypotheses matrix
      @param numRandomizations: The number of randomization runs to perform
      """
      _LmCommand.__init__(self)
      self.inputs.extend([pamFilename, treeMtxFilename, grimFilename])
      self.outputs.extend([fGlobalRandMtxFilename, fPartRandMtxFilename])
      
      self.pamFn = pamFilename
      self.treeMtxFn = treeMtxFilename
      self.grimFn = grimFilename
      self.fGlobFn = fGlobalRandMtxFilename
      self.fPartFn = fPartRandMtxFilename

      self.optArgs = ''
      if hypothesesFilename is not None:
         self.inputs.append(hypothesesFilename)
         self.optArgs += ' -b {}'.format(hypothesesFilename)
      if numRadomizations is not None:
         self.optArgs += ' -n {}'.format(numRadomizations)

   # ................................
   def getCommand(self):
      """
      @summary: Get the raw command to run on the system
      """
      return '{} {}{} {} {} {} {} {}'.format(CMD_PYBIN, 
            os.path.join(MULTI_SPECIES_SCRIPTS_DIR, 'mcpa_random.py'),
            self.optArgs, self.pamFn, self.treeMtxFn, self.grimFn, 
            self.fGlobFn, self.fPartFn)

# .............................................................................
class OccurrenceBucketeerCommand(_LmCommand):
   """
   @summary: This command will split a CSV into buckets based on the group field
   """
   # ................................
   def __init__(self, outBasename, groupPosition, inFilename, position=None, 
                width=None, headerRow=False):
      """
      @summary: Construct the command object
      @param outBasename: The base name to use for output files
      @param groupPosition: The field to use for grouping / bucketing
      @param inFilename: A file location or list of file locations to use as 
                            input
      @param position: The position in the group field to use for bucketing
      @param width: The number of characters to use for bucketing
      @param headerRow: Does the input file have a header row?
      """
      _LmCommand.__init__(self)
      if isinstance(inFilename, list):
         self.inFiles = inFilename
      else:
         self.inFiles = [inFilename]
      
      # Outputs are unknown unless you know the data
      #self.outputs.append()
      
      self.outBase = outBasename
      self.groupPos = groupPosition
      self.pos = position
      self.width = width
      self.headerRow = headerRow

   # ................................
   def getCommand(self):
      """
      @summary: Get the raw command to run on the system
      """
      optArgs = ''
      if self.pos is not None:
         optArgs += ' -pos {}'.format(self.pos)
      if self.width is not None:
         optArgs += ' -num {}'.format(self.width)
      if self.headerRow:
         optArgs += ' -header'
      
      return '{} {}{} {} {} {}'.format(CMD_PYBIN, 
            os.path.join(MULTI_SPECIES_SCRIPTS_DIR, 'occurrence_bucketeer.py'),
            optArgs, self.outBase, self.groupPos, ' '.join(self.inFiles))

# .............................................................................
class OccurrenceSorterCommand(_LmCommand):
   """
   @summary: This command will sort a CSV file on a group field
   """
   # ................................
   def __init__(self, inFilename, outFilename, groupPosition):
      """
      @summary: Construct the command object
      @param inFilename: The CSV input file to sort
      @param outFilename: The file location of the output CSV
      @param groupPosition: The field position to use for sorting
      """
      _LmCommand.__init__(self)
      self.inputs.append(inFilename)
      self.outputs.append(outFilename)
      
      self.inFn = inFilename
      self.outFn = outFilename
      self.groupPos = groupPosition

   # ................................
   def getCommand(self):
      """
      @summary: Get the raw command to run on the system
      """
      return '{} {} {} {} {}'.format(CMD_PYBIN, 
            os.path.join(MULTI_SPECIES_SCRIPTS_DIR, 'occurrence_sorter.py'),
            self.outFn, self.groupPos, self.inFn)

# .............................................................................
class OccurrenceSplitterCommand(_LmCommand):
   """
   @summary: This command will split a sorted CSV file on a group field
   """
   # ................................
   def __init__(self, groupPosition, inFilename, outDir, prefix=None):
      """
      @summary: Construct the command object
      @param groupPosition: The field to group on
      @param inFilename: The input CSV file
      @param outDir: A directory location to write the output files
      @param prefix: A filename prefix to use for the output files
      """
      _LmCommand.__init__(self)
      self.inputs.append(inFilename)
      # output files are not deterministic from inputs, need to look at file
      #self.outputs.append()
      
      self.groupPos = groupPosition
      self.inFn = inFilename
      self.outDir = outDir
      self.prefix = prefix

   # ................................
   def getCommand(self):
      """
      @summary: Get the raw command to run on the system
      """
      return '{} {}{} {} {} {}'.format(CMD_PYBIN, 
            os.path.join(MULTI_SPECIES_SCRIPTS_DIR, 'occurrence_splitter.py'),
            ' -p {}'.format(self.prefix) if self.prefix is not None else '',
            self.groupPos, self.inFn, self.outDir)

# .............................................................................
class RandomizeGradyCommand(_LmCommand):
   """
   @summary: This command will randomize a PAM using CJ's method
   """
   # ................................
   def __init__(self, pamFilename, randPamFilename):
      """
      @summary: Construct the command object
      @param pamFilename: The file location of the PAM to randomize
      @param randPamFilename: The file location to write the randomized PAM
      """
      _LmCommand.__init__(self)
      self.inputs.append(pamFilename)
      self.outputs.append(randPamFilename)
      
      self.pamFn = pamFilename
      self.outFn = randPamFilename

   # ................................
   def getCommand(self):
      """
      @summary: Get the raw command to run on the system
      """
      return '{} {} {} {}'.format(CMD_PYBIN, 
            os.path.join(MULTI_SPECIES_SCRIPTS_DIR, 'grady_randomize.py'),
            self.pamFn, self.outFn)

# .............................................................................
class RandomizeSplotchCommand(_LmCommand):
   """
   @summary: This command will randomize a PAM using the splotch method
   """
   # ................................
   def __init__(self, pamFilename, numSides, outFilename):
      """
      @summary: Construct the command object
      @param pamFilename: The file location of the PAM
      @param numSides: The number of sides for each cell in the shapegrid
      @param outFilename: The file location to write the randomized PAM
      """
      _LmCommand.__init__(self)
      self.inputs.append(pamFilename)
      self.outputs.append(outFilename)
      
      self.pamFn = pamFilename
      self.numSides = numSides
      self.outFn = outFilename

   # ................................
   def getCommand(self):
      """
      @summary: Get the raw command to run on the system
      """
      return '{} {} {} {} {}'.format(CMD_PYBIN, 
            os.path.join(MULTI_SPECIES_SCRIPTS_DIR, 'splotch_randomize.py'),
            self.pamFn, self.numSides, self.outFn)

# .............................................................................
class RandomizeSwapCommand(_LmCommand):
   """
   @summary: This command will randomize a PAM using the swap method
   """
   # ................................
   def __init__(self, pamFilename, numSwaps, outFilename):
      """
      @summary: Construct the command object
      @param pamFilename: The file location of the PAM
      @param numSwaps: The number of successful swaps to perform
      @param outFilename: The file location to write the randomized PAM
      """
      _LmCommand.__init__(self)
      self.inputs.append(pamFilename)
      self.outputs.append(outFilename)
      
      self.pamFn = pamFilename
      self.numSwaps = numSwaps
      self.outFn = outFilename

   # ................................
   def getCommand(self):
      """
      @summary: Get the raw command to run on the system
      """
      return '{} {} {} {} {}'.format(CMD_PYBIN, 
            os.path.join(MULTI_SPECIES_SCRIPTS_DIR, 'swap_randomize.py'),
            self.pamFn, self.numSwaps, self.outFn)

