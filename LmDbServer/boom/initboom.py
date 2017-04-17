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
import argparse
import ConfigParser
import mx.DateTime
import os
import sys

from LmCommon.common.lmconstants import (DEFAULT_POST_USER, OutputFormat, 
                                         JobStatus, MatrixType)
from LmDbServer.common.lmconstants import (TAXONOMIC_SOURCE, SpeciesDatasource)
from LmDbServer.common.localconstants import (GBIF_OCCURRENCE_FILENAME, 
                        BISON_TSN_FILENAME, IDIG_FILENAME, USER_OCCURRENCE_DATA)
from LmServer.base.lmobj import LMError
from LmServer.common.datalocator import EarlJr
from LmServer.common.lmconstants import (Algorithms, ENV_DATA_PATH, 
         GPAM_KEYWORD, ARCHIVE_KEYWORD, LMFileType)
from LmServer.common.localconstants import PUBLIC_USER, APP_PATH
from LmServer.common.lmuser import LMUser
from LmServer.common.log import ScriptLogger
from LmServer.base.serviceobject2 import ServiceObject
from LmServer.base.utilities import isCorrectUser
from LmServer.db.borgscribe import BorgScribe
from LmServer.sdm.algorithm import Algorithm
from LmServer.legion.envlayer import EnvLayer
from LmServer.legion.gridset import Gridset
from LmServer.legion.lmmatrix import LMMatrix  
from LmServer.legion.mtxcolumn import MatrixColumn          
from LmServer.legion.scenario import Scenario
from LmServer.legion.shapegrid import ShapeGrid

CURRDATE = (mx.DateTime.gmt().year, mx.DateTime.gmt().month, mx.DateTime.gmt().day)
CURR_MJD = mx.DateTime.gmt().mjd
# ...............................................
def addUsers(scribe, userId, userEmail):
   """
   @summary Adds PUBLIC_USER, anon user and USER from metadata to the database
   """
   userList = [{'id': PUBLIC_USER,
                'email': '{}@nowhere.org'.format(PUBLIC_USER)},
               {'id': DEFAULT_POST_USER,
                'email': '{}@nowhere.org'.format(DEFAULT_POST_USER)}]
   if userId != PUBLIC_USER:
      userList.append({'id': userId,'email': userEmail})

   for usrmeta in userList:
      try:
         user = LMUser(usrmeta['id'], usrmeta['email'], usrmeta['email'], modTime=CURR_MJD)
      except:
         pass
      else:
         scribe.log.info('  Insert user {} ...'.format(usrmeta['id']))
         tmp = scribe.findOrInsertUser(user)

# ...............................................
def addAlgorithms(scribe):
   """
   @summary Adds algorithms to the database from the algorithm dictionary
   """
   ids = []
   for alginfo in Algorithms.implemented():
      meta = {'name': alginfo.name, 
              'isDiscreteOutput': alginfo.isDiscreteOutput,
              'outputFormat': alginfo.outputFormat,
              'acceptsCategoricalMaps': alginfo.acceptsCategoricalMaps}
      alg = Algorithm(alginfo.code, metadata=meta)
      scribe.log.info('  Insert algorithm {} ...'.format(alginfo.code))
      algid = scribe.findOrInsertAlgorithm(alg)
      ids.append(algid)
   return ids
#    for algcode, algdict in ALGORITHM_DATA.iteritems():
#       algmeta = {}
#       for k, v in algdict.iteritems():
#          if k != 'parameters':
#             algmeta[k] = v
#       alg = Algorithm(algcode, metadata=algmeta)
#       scribe.log.info('  Insert algorithm {} ...'.format(algcode))
#       algid = scribe.findOrInsertAlgorithm(alg)
#       ids.append(algid)
#    return ids

# ...............................................
def _addIntersectGrid(scribe, gridname, cellsides, cellsize, mapunits, epsg, bbox, usr):
   shp = ShapeGrid(gridname, usr, epsg, cellsides, cellsize, mapunits, bbox,
                   status=JobStatus.INITIALIZE, statusModTime=CURR_MJD)
   newshp = scribe.findOrInsertShapeGrid(shp)
   try:
      newshp.buildShape()
   except Exception, e:
      scribe.log.warning('Unable to build Shapegrid ({})'.format(str(e)))
   else:
      newshp.updateStatus(JobStatus.COMPLETE)
      success =  scribe.updateShapeGrid(newshp)
      if success is False:
         scribe.log.warning('Failed to update Shapegrid record')
   return newshp
   
# ...............................................
def addArchive(scribe, predScens, gridname, configFname, archiveName, 
               cellsides, cellsize, mapunits, epsg, gridbbox, usr):
   """
   @summary: Create a Shapegrid, PAM, and Gridset for this archive's Global PAM
   """
   shp = _addIntersectGrid(scribe, gridname, cellsides, cellsize, mapunits, epsg, 
                           gridbbox, usr)
   # "BOOM" Archive
   meta = {ServiceObject.META_DESCRIPTION: ARCHIVE_KEYWORD,
           ServiceObject.META_KEYWORDS: [ARCHIVE_KEYWORD]}
   grdset = Gridset(name=archiveName, metadata=meta, shapeGrid=shp, 
                    configFilename=configFname, epsgcode=epsg, 
                    userId=usr, modTime=CURR_MJD)
   updatedGrdset = scribe.findOrInsertGridset(grdset)
   # "Global" PAMs (one per scenario)
   globalPAMs = []
   for scen in predScens.values():
      scen.gcmCode
      scen.altpredCode
      scen.dateCode

      meta = {ServiceObject.META_DESCRIPTION: GPAM_KEYWORD,
              ServiceObject.META_KEYWORDS: [GPAM_KEYWORD]}
      tmpGpam = LMMatrix(None, matrixType=MatrixType.PAM, 
                         gcmCode=scen.gcmCode, altpredCode=scen.altpredCode, 
                         dateCode=scen.dateCode, metadata=meta, userId=usr, 
                         gridset=updatedGrdset, 
                         status=JobStatus.GENERAL, statusModTime=CURR_MJD)
      gpam = scribe.findOrInsertMatrix(tmpGpam)
      globalPAMs.append(gpam)
   
   return shp, updatedGrdset, globalPAMs
   
# ...............................................
def _getbioName(obsOrPred, res, 
                gcm=None, tm=None, altpred=None, 
                lyrtype=None, 
                suffix=None, isTitle=False):
   sep = '-'
   if isTitle: 
      sep = ', '
   name = obsOrPred
   if lyrtype is not None:
      name = sep.join((lyrtype, name))
   for descriptor in (gcm, altpred, tm, res, suffix):
      if descriptor is not None:
         name = sep.join((name, descriptor))
   return name
 
# ...............................................
def _getBaselineLayers(usr, pkgMeta, baseMeta, elyrMeta, lyrtypeMeta):
   """
   @summary Assembles layer metadata for a single layerset
   """
   layers = []
   staticLayers = {}
   for envcode in pkgMeta['layertypes']:
      ltmeta = lyrtypeMeta[envcode]
      envKeywords = [k for k in baseMeta['keywords']]
      relfname, isStatic = _findFileFor(ltmeta, pkgMeta['baseline'], 
                                        gcm=None, tm=None, altPred=None)
      lyrname = _getbioName(pkgMeta['baseline'], pkgMeta['res'], lyrtype=envcode, 
                            suffix=pkgMeta['suffix'])
      lyrmeta = {'title': ' '.join((pkgMeta['baseline'], ltmeta['title'])),
                 'description': ' '.join((pkgMeta['baseline'], ltmeta['description']))}
      envmeta = {'title': ltmeta['title'],
                 'description': ltmeta['description'],
                 'keywords': envKeywords.extend(ltmeta['keywords'])}
      dloc = os.path.join(ENV_DATA_PATH, relfname)
      if not os.path.exists(dloc):
         print('Missing local data %s' % dloc)
      envlyr = EnvLayer(lyrname, usr, elyrMeta['epsg'], 
                        dlocation=dloc, 
                        lyrMetadata=lyrmeta,
                        dataFormat=elyrMeta['gdalformat'], 
                        gdalType=elyrMeta['gdaltype'],
                        valUnits=ltmeta['valunits'],
                        mapunits=elyrMeta['mapunits'], 
                        resolution=elyrMeta['resolution'], 
                        bbox=pkgMeta['bbox'], 
                        modTime=CURR_MJD, 
                        envCode=envcode, 
                        dateCode=pkgMeta['baseline'],
                        envMetadata=envmeta,
                        envModTime=CURR_MJD)
      layers.append(envlyr)
      if isStatic:
         staticLayers[envcode] = envlyr
   return layers, staticLayers

# ...............................................
def _findFileFor(ltmeta, obsOrPred, gcm=None, tm=None, altPred=None):
   isStatic = False
   ltfiles = ltmeta['files']
   if len(ltfiles) == 1:
      isStatic = True
      relFname = ltfiles.keys()[0]
      if obsOrPred in ltfiles[relFname]:
         return relFname, isStatic
   else:
      for relFname, kList in ltmeta['files'].iteritems():
         if obsOrPred in kList:
            if gcm == None and tm == None and altPred == None:
               return relFname, isStatic
            elif (gcm in kList and tm in kList and
                  (altPred is None or altPred in kList)):
               return relFname, isStatic
   print('Failed to find layertype {} for {}, gcm {}, altpred {}, time {}'
         .format(ltmeta['title'], obsOrPred, gcm, altPred, tm))
   return None, None
      
# ...............................................
def _getPredictedLayers(usr, pkgMeta, elyrMeta, lyrtypeMeta, staticLayers,
                        observedPredictedMeta, predRpt, tm, gcm=None, altpred=None):
   """
   @summary Assembles layer metadata for a single layerset
   """
   mdlvals = observedPredictedMeta[predRpt]['models'][gcm]
   tmvals = observedPredictedMeta[predRpt]['times'][tm]
   layers = []
   rstType = None
   layertypes = pkgMeta['layertypes']
   for envcode in layertypes:
      keywords = [k for k in observedPredictedMeta[predRpt]['keywords']]
      ltmeta = lyrtypeMeta[envcode]
      relfname, isStatic = _findFileFor(ltmeta, predRpt, 
                                        gcm=gcm, tm=tm, altPred=altpred)
      if not isStatic:
         lyrname = _getbioName(predRpt, pkgMeta['res'], gcm=gcm, tm=tm, 
                               altpred=altpred, lyrtype=envcode, 
                               suffix=pkgMeta['suffix'], isTitle=False)
         lyrtitle = _getbioName(predRpt, pkgMeta['res'], gcm=gcm, tm=tmvals['name'], 
                                altpred=altpred, lyrtype=envcode, 
                                suffix=pkgMeta['suffix'], isTitle=True)
         scentitle = _getbioName(predRpt, pkgMeta['res'], gcm=mdlvals['name'], 
                                 tm=tmvals['name'], altpred=altpred, 
                                 suffix=pkgMeta['suffix'], isTitle=True)
         lyrdesc = '{} for {}'.format(ltmeta['description'], scentitle)
         
         lyrmeta = {'title': lyrtitle, 'description': lyrdesc}
         envmeta = {'title': ltmeta['title'],
                    'description': ltmeta['description'],
                    'keywords': keywords.extend(ltmeta['keywords'])}
         dloc = os.path.join(ENV_DATA_PATH, relfname)
         if not os.path.exists(dloc):
            print('Missing local data %s' % dloc)
            dloc = None
         envlyr = EnvLayer(lyrname, usr, elyrMeta['epsg'], 
                           dlocation=dloc, 
                           lyrMetadata=lyrmeta,
                           dataFormat=elyrMeta['gdalformat'], 
                           gdalType=rstType,
                           valUnits=ltmeta['valunits'],
                           mapunits=elyrMeta['mapunits'], 
                           resolution=elyrMeta['resolution'], 
                           bbox=pkgMeta['bbox'], 
                           modTime=CURR_MJD,
                           envCode=envcode, 
                           gcmCode=gcm, altpredCode=altpred, dateCode=tm,
                           envMetadata=envmeta, 
                           envModTime=CURR_MJD)
      else:
         # Use the observed data
         envlyr = staticLayers[envcode]
      layers.append(envlyr)
   return layers


# ...............................................
def createBaselineScenario(usr, pkgMeta, elyrMeta, lyrtypeMeta, 
                           observedPredictedMeta, climKeywords):
   """
   @summary Assemble Worldclim/bioclim scenario
   """
   obsKey = pkgMeta['baseline']
   baseMeta = observedPredictedMeta[obsKey]
#    tm = baseMeta['times'].keys()[0]
   basekeywords = [k for k in climKeywords]
   basekeywords.extend(baseMeta['keywords'])
   
   scencode = _getbioName(obsKey, pkgMeta['res'], suffix=pkgMeta['suffix'])
   lyrs, staticLayers = _getBaselineLayers(usr, pkgMeta, baseMeta, elyrMeta, 
                                           lyrtypeMeta)
   scenmeta = {'title': baseMeta['title'], 'author': baseMeta['author'], 
               'description': baseMeta['description'], 'keywords': basekeywords}
   scen = Scenario(scencode, usr, elyrMeta['epsg'], 
                   metadata=scenmeta, 
                   units=elyrMeta['mapunits'], 
                   res=elyrMeta['resolution'], 
                   dateCode=pkgMeta['baseline'],
                   bbox=pkgMeta['bbox'], 
                   modTime=CURR_MJD,  
                   layers=lyrs)
   return scen, staticLayers

# ...............................................
def createPredictedScenarios(usr, pkgMeta, elyrMeta, lyrtypeMeta, staticLayers,
                             observedPredictedMeta, climKeywords):
   """
   @summary Assemble predicted future scenarios defined by IPCC report
   """
   predScenarios = {}
   try:
      predScens = pkgMeta['predicted']
   except:
      return predScenarios
   
   for predRpt in predScens.keys():
      for modelDef in predScens[predRpt]:
         gcm = modelDef[0]
         tm = modelDef[1]
         try:
            altpred = modelDef[2]
         except:
            altpred = None
            altvals = {}
         else:
            altvals = observedPredictedMeta[predRpt]['alternatePredictions'][altpred]
         mdlvals = observedPredictedMeta[predRpt]['models'][gcm]
         tmvals = observedPredictedMeta[predRpt]['times'][tm]
         # Reset keywords
         scenkeywords = [k for k in climKeywords]
         scenkeywords.extend(observedPredictedMeta[predRpt]['keywords'])
         for vals in (mdlvals, tmvals, altvals):
            try:
               scenkeywords.extend(vals['keywords'])
            except:
               pass
         # LM Scenario code, title, description
         scencode = _getbioName(predRpt, pkgMeta['res'], gcm=gcm, 
                                tm=tm, altpred=altpred, 
                                suffix=pkgMeta['suffix'], isTitle=False)
         scentitle = _getbioName(predRpt, pkgMeta['res'], gcm=mdlvals['name'], 
                                 tm=tmvals['name'], altpred=altpred, 
                                 suffix=pkgMeta['suffix'], isTitle=True)
         obstitle = observedPredictedMeta[pkgMeta['baseline']]['title']
         scendesc =  ' '.join((obstitle, 
                  'and predicted climate calculated from {}'.format(scentitle)))
         scenmeta = {'title': scentitle, 'author': mdlvals['author'], 
                     'description': scendesc, 'keywords': scenkeywords}
         lyrs = _getPredictedLayers(usr, pkgMeta, elyrMeta, lyrtypeMeta, 
                              staticLayers, observedPredictedMeta, predRpt, tm, 
                              gcm=gcm, altpred=altpred)
         
         scen = Scenario(scencode, usr, elyrMeta['epsg'], 
                         metadata=scenmeta, 
                         units=elyrMeta['mapunits'], 
                         res=elyrMeta['resolution'], 
                         gcmCode=gcm, altpredCode=altpred, dateCode=tm,
                         bbox=pkgMeta['bbox'], 
                         modTime=CURR_MJD, 
                         layers=lyrs)
         predScenarios[scencode] = scen
   return predScenarios

# ...............................................
def addScenarioAndLayerMetadata(scribe, scenarios):
   """
   @summary Add scenario and layer metadata to database  
   """
   for scode, scen in scenarios.iteritems():
      scribe.log.info('Insert scenario {}'.format(scode))
      newscen = scribe.findOrInsertScenario(scen)

# ...............................................

# ...............................................
def _findClimatePackageMetadata(envPackageName):
   # TODO: Remove `v2` Debug
   debugMetaname = os.path.join(ENV_DATA_PATH, '{}.v2{}'.format(envPackageName, 
                                                            OutputFormat.PYTHON))
   if os.path.exists(debugMetaname):
      metafname = debugMetaname
   else:
      metafname = os.path.join(ENV_DATA_PATH, '{}{}'.format(envPackageName, 
                                                            OutputFormat.PYTHON))      
   if not os.path.exists(metafname):
      raise LMError(currargs='Climate metadata {} does not exist'
                    .format(metafname))
   # TODO: change to importlib on python 2.7 --> 3.3+  
   try:
      import imp
      META = imp.load_source('currentmetadata', metafname)
   except Exception, e:
      raise LMError(currargs='Climate metadata {} cannot be imported; ({})'
                    .format(metafname, e))
   return META, metafname

# ...............................................
def pullClimatePackageMetadata(envPackageName):
   META, metafname = _findClimatePackageMetadata(envPackageName)
   # Combination of scenario and layer attributes making up these data 
   pkgMeta = META.CLIMATE_PACKAGES[envPackageName]
   
   try:
      epsg = META.EPSG
   except:
      raise LMError('Failed to specify EPSG for {}'.format(envPackageName))
   try:
      mapunits = META.MAPUNITS
   except:
      raise LMError('Failed to specify MAPUNITS for {}'.format(envPackageName))
   try:
      resInMapunits = META.RESOLUTIONS[pkgMeta['res']]
   except:
      raise LMError('Failed to specify res (or RESOLUTIONS values) for {}'
                    .format(envPackageName))
   try:
      gdaltype = META.ENVLYR_GDALTYPE
   except:
      raise LMError('Failed to specify ENVLYR_GDALTYPE for {}'.format(envPackageName))
   try:
      gdalformat = META.ENVLYR_GDALFORMAT
   except:
      raise LMError('Failed to specify META.ENVLYR_GDALFORMAT for {}'.format(envPackageName))
   # Spatial and format attributes of data files
   elyrMeta = {'epsg': epsg, 
                 'mapunits': mapunits, 
                 'resolution': resInMapunits, 
                 'gdaltype': gdaltype, 
                 'gdalformat': gdalformat}
   return META, metafname, pkgMeta, elyrMeta
      
# ...............................................
def writeConfigFile(archiveName, envPackageName, userid, userEmail, 
                     speciesSource, speciesData, speciesDataDelimiter,
                     elyrMeta, minpoints, algorithms, 
                     gridname, grid_cellsize, grid_cellsides, intersectParams,
                     mdlScen=None, prjScens=None, mdlMask=None, prjMask=None,
                     assemblePams=True):
   """
   """
   earl = EarlJr()
   pth = earl.createDataPath(userid, LMFileType.BOOM_CONFIG)
   newConfigFilename = os.path.join(pth, 
                              '{}{}'.format(archiveName, OutputFormat.CONFIG))
   f = open(newConfigFilename, 'w')
   f.write('[LmServer - pipeline]\n')
   f.write('ARCHIVE_USER: {}\n'.format(userid))
   f.write('ARCHIVE_NAME: {}\n'.format(archiveName))
   if userEmail is not None:
      f.write('TROUBLESHOOTERS: {}\n'.format(userEmail))
   f.write('\n')   

   f.write('; ...................\n')
   f.write('; SDM Params\n')
   f.write('; ...................\n')
   # Expiration date triggering re-query and computation
   f.write('SPECIES_EXP_YEAR: {}\n'.format(CURRDATE[0]))
   f.write('SPECIES_EXP_MONTH: {}\n'.format(CURRDATE[1]))
   f.write('SPECIES_EXP_DAY: {}\n'.format(CURRDATE[2]))
   f.write('\n')
   # SDM Algorithm and minimun number of required species points   
   f.write('POINT_COUNT_MIN: {}\n'.format(minpoints))
   algs = ','.join(algorithms)
   f.write('ALGORITHMS: {}\n'.format(algs))
   f.write('\n')
   
   f.write('; ...................\n')
   f.write('; Species data vals\n')
   f.write('; ...................\n')
   f.write('DATASOURCE: {}\n'.format(speciesSource))
   # Species source type (for processing) and file
   if speciesSource == SpeciesDatasource.GBIF:
      varname = 'GBIF_OCCURRENCE_FILENAME'
      if speciesData is None:
         speciesData = GBIF_OCCURRENCE_FILENAME
   elif speciesSource == SpeciesDatasource.BISON:
      varname = 'BISON_TSN_FILENAME'
      if speciesData is None:
         speciesData = BISON_TSN_FILENAME
   elif speciesSource == SpeciesDatasource.IDIGBIO:
      varname = 'IDIG_FILENAME'
      if speciesData is None:
         speciesData = IDIG_FILENAME
   else:
      varname = 'USER_OCCURRENCE_DATA'
      if speciesData is None:
         speciesData = USER_OCCURRENCE_DATA
      f.write('USER_OCCURRENCE_DATA_DELIMITER: {}\n'
              .format(speciesDataDelimiter))
   f.write('{}: {}\n'.format(varname, speciesData))
   f.write('\n')

   f.write('; ...................\n')
   f.write('; Env Package Vals\n')
   f.write('; ...................\n')
   # Input environmental data, pulled from SCENARIO_PACKAGE metadata
   f.write('SCENARIO_PACKAGE: {}\n'.format(envPackageName))
   f.write('SCENARIO_PACKAGE_EPSG: {}\n'.format(elyrMeta['epsg']))
   f.write('SCENARIO_PACKAGE_MAPUNITS: {}\n'.format(elyrMeta['mapunits']))
   # Scenario codes, created from environmental metadata  
   f.write('SCENARIO_PACKAGE_MODEL_SCENARIO: {}\n'.format(mdlScen))
   pcodes = ','.join(prjScens)
   f.write('SCENARIO_PACKAGE_PROJECTION_SCENARIOS: {}\n'.format(pcodes))
   
   if mdlMask is not None:
      f.write('MODEL_MASK_NAME: {}\n'.format(mdlMask))
   if prjMask is not None:
      f.write('PROJECTION_MASK_NAME: {}\n'.format(prjMask))
   f.write('\n')
   
   f.write('; ...................\n')
   f.write('; Global PAM vals\n')
   f.write('; ...................\n')
   # Intersection grid
   f.write('GRID_NAME: {}\n'.format(gridname))
   f.write('GRID_CELLSIZE: {}\n'.format(grid_cellsize))
   f.write('GRID_NUM_SIDES: {}\n'.format(grid_cellsides))
   f.write('\n')
   for k, v in intersectParams.iteritems():
      f.write('INTERSECT_{}:  {}\n'.format(k.upper(), v))
   f.write('ASSEMBLE_PAMS: {}\n'.format(str(assemblePams)))
   f.write('\n')
      
   f.close()
   return newConfigFilename

def getConfiguredArguments(configFname):
   section = '[BOOM Config]'
   if configFname is not None and os.path.exists(configFname):
      config = ConfigParser.SafeConfigParser()
      config.read(configFname)
   usr = config.get(section, 'ARCHIVE_USER')
   usrEmail = config.get(section, 'ARCHIVE_USER_EMAIL')
   archiveName = config.get(section, 'ARCHIVE_NAME')
   envPackageName = config.get(section, 'SCENARIO_PACKAGE')
   speciesSource = config.get(section, 'DATASOURCE').upper()
   speciesData = config.get(section, 'USER_OCCURRENCE_DATA')
   speciesDataDelimiter = config.get(section, 'USER_OCCURRENCE_DATA_DELIMITER')
   minpoints = config.get(section, 'POINT_COUNT_MIN')
   assemblePams = config.getboolean(section, 'ASSEMBLE_PAMS')
   algstring = config.getlist(section, 'ARCHIVE_USER')
   algorithms = [alg.strip().upper() for alg in algstring.split(',')]
   cellsize = args.grid_cellsize
   cellsides = config.get(section, 'GRID_NUM_SIDES')
   gridname = '{}-Grid-{}'.format(archiveName, cellsize)
   gridbbox = eval(config.get(section, 'GRID_BBOX'))
   intersectParams = {
         MatrixColumn.INTERSECT_PARAM_FILTER_STRING: None,
         MatrixColumn.INTERSECT_PARAM_VAL_NAME: config.get(section, 'INTERSECT_VALNAME'),
         MatrixColumn.INTERSECT_PARAM_MIN_PRESENCE: config.get(section, 'INTERSECT_MINPRESENCE'),
         MatrixColumn.INTERSECT_PARAM_MAX_PRESENCE: config.get(section, 'INTERSECT_MAXPRESENCE'),
         MatrixColumn.INTERSECT_PARAM_MIN_PERCENT: config.get(section, 'INTERSECT_MINPERCENT')}
   return (usr, usrEmail, archiveName, envPackageName, speciesSource, speciesData, 
           speciesDataDelimiter, minpoints, assemblePams, algorithms, cellsize,
           cellsides, gridname, gridbbox, intersectParams)
   
# ...............................................
if __name__ == '__main__':
   if not isCorrectUser():
      print("Run this script as `lmwriter`")
      sys.exit(2)

   sampleConfigFile = os.path.join(APP_PATH, 'LmDbServer/tools/boom.sample.ini')
   # Use the argparse.ArgumentParser class to handle the command line arguments
   parser = argparse.ArgumentParser(
            description=('Populate a Lifemapper database with metadata ' +
                         'specific to an \'archive\', populated with '
                         'user-specified values in the config file argument and '
                         'configured environmental package metadata.'))
   parser.add_argument('-', '--config_file', default=sampleConfigFile,
            help=('Configuration file for the archive, gridset, and grid ' +
                  'to be created from these data.'))
   args = parser.parse_args()
   configFname = args.config_file

   (usr, usrEmail, archiveName, envPackageName, speciesSource, speciesData, 
    speciesDataDelimiter, minpoints, assemblePams, algorithms, cellsize,
    cellsides, gridname, gridbbox, intersectParams) = \
        getConfiguredArguments(configFname)
   # Imports META
   META, metafname, pkgMeta, elyrMeta = pullClimatePackageMetadata(envPackageName)
      
# .............................
   basefilename = os.path.basename(__file__)
   basename, ext = os.path.splitext(basefilename)
   logger = ScriptLogger(basename+'_borg')
   scribeWithBorg = BorgScribe(logger)
   success = scribeWithBorg.openConnections()

   if not success: 
      logger.critical('Failed to open database')
      exit(0)
   
   try:      
# .............................
      logger.info('  Insert user metadata ...')
      addUsers(scribeWithBorg, usr, usrEmail)
# .............................
      logger.info('  Insert algorithm metadata ...')
      aIds = addAlgorithms(scribeWithBorg)
# .............................
      logger.info('  Insert climate {} metadata ...'.format(envPackageName))
      # Current
      basescen, staticLayers = createBaselineScenario(usr, pkgMeta, elyrMeta, 
                                                      META.LAYERTYPE_META,
                                                      META.OBSERVED_PREDICTED_META,
                                                      META.CLIMATE_KEYWORDS)
      logger.info('     Created base scenario {}'.format(basescen.code))
      # Predicted Past and Future
      predScens = createPredictedScenarios(usr, pkgMeta, elyrMeta, 
                                           META.LAYERTYPE_META, staticLayers,
                                           META.OBSERVED_PREDICTED_META,
                                           META.CLIMATE_KEYWORDS)
      logger.info('     Created predicted scenarios {}'.format(predScens.keys()))
      predScens[basescen.code] = basescen
      addScenarioAndLayerMetadata(scribeWithBorg, predScens)
# .............................
      # Shapefile, Gridset, Matrix for GPAM/BOOMArchive
      logger.info('  Insert, build shapegrid {} ...'.format(gridname))
      shpGrid, archiveGridset, globalPAMs = addArchive(scribeWithBorg, 
                         predScens, gridname, metafname, archiveName, 
                         cellsides, cellsize, 
                         elyrMeta['mapunits'], elyrMeta['epsg'], 
                         gridbbox, usr)
      
# .............................
      # Insert all taxonomic sources for now
      logger.info('  Insert taxonomy metadata ...')
      for name, taxInfo in TAXONOMIC_SOURCE.iteritems():
         taxSourceId = scribeWithBorg.findOrInsertTaxonSource(taxInfo['name'],
                                                           taxInfo['url'])
# .............................
      # Write config file for this archive
      mdlScencode = basescen.code
      prjScencodes = predScens.keys()
      newConfigFilename = writeConfigFile(archiveName, envPackageName, usr, 
                           usrEmail, speciesSource, 
                           speciesData, speciesDataDelimiter, elyrMeta, 
                           minpoints, algorithms, gridname, cellsize, cellsides, 
                           intersectParams, mdlScen=mdlScencode, 
                           prjScens=prjScencodes, assemblePams=assemblePams)
   except Exception, e:
      logger.error(str(e))
      raise
   finally:
      scribeWithBorg.closeConnections()
       
"""
$PYTHON LmDbServer/boom/initboom.py  -n 'Heuchera archive'  \
                                     -u ryan                \
                                     -m rfolk@flmnh.ufl.edu \
                                     -ep 10min-past-present-future  \
                                     -ss user          \
                                     -sf heuchera_all  \
                                     -sd ','           \
                                     -p 25             \
                                     -ap True          \
                                     -a bioclim        \
                                     -gz 1             \
                                     -gp square        \
                                     -gb [-180, -60, 180, 90]

$PYTHON LmDbServer/boom/initboom.py  --archive_name 'Heuchera archive' \
                                     --user ryan2                  \
                                     --email ryanfolk@ufl.edu  \
                                     --environmental_package 10min-past-present-future  \
                                     --species_source user        \
                                     --species_file heuchera_all  \
                                     --species_delimiter ','      \
                                     --min_points 25              \
                                     --algorithms bioclim         \
                                     --assemblePams True          \
                                     --grid_cellsize 2            \
                                     --grid_shape square          \
                                     -gb '[-180, 10, 180, 90]'

$PYTHON LmDbServer/boom/initboom.py  -n 'Heuchera archive' \
                                     -u ryan                  \
                                     -m rfolk@flmnh.ufl.edu  \
                                     -ep Worldclim-GTOPO-ISRIC-SoilGrids-ConsensusLandCover  \
                                     -ss user      \
                                     -sf heuchera_all  \
                                     -sd ','       \
                                     -p 25        \
                                     -a bioclim   \
                                     -ap True          \
                                     -gz 2        \
                                     -gp square   \
                                     -gb '[-180, 10, 180, 90]'

$PYTHON LmDbServer/boom/initboom.py  --archive_name 'Biotaphy iDigBio archive' \
                                     --user idigbio                  \
                                     --email aimee.stewart@ku.edu  \
                                     --environmental_package 10min-past-present-future  \
                                     --species_source IDIGBIO        \
                                     --min_points 25              \
                                     --algorithms bioclim         \
                                     --assemblePams False          \
                                     --grid_cellsize 2            \
                                     --grid_shape square          \
                                     -gb '[-180, -90, 180, 90]'
                                     
$PYTHON LmDbServer/boom/initboom.py  --archive_name 'GBIF archive' \
                                     --user kubi         \
                                     --email lifemapper@ku.edu  \
                                     --environmental_package 10min-past-present-future  \
                                     --species_source gbif        \
                                     --min_points 30              \
                                     --algorithms bioclim         \
                                     --assemblePams True          \
                                     --grid_cellsize 1            \
                                     --grid_shape square          \
                                     --grid_bbox '[-180, -90, 180, 90]'


"""