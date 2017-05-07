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
import inspect
import mx.DateTime
import os
import sys

from LmCommon.common.config import Config
from LmCommon.common.lmconstants import (DEFAULT_POST_USER, OutputFormat, 
                                    JobStatus, MatrixType, SERVER_BOOM_HEADING)
from LmDbServer.common.lmconstants import (TAXONOMIC_SOURCE, SpeciesDatasource)
from LmDbServer.common.localconstants import (ALGORITHMS, ASSEMBLE_PAMS, 
      GBIF_TAXONOMY_FILENAME, GBIF_PROVIDER_FILENAME, GBIF_OCCURRENCE_FILENAME, 
      BISON_TSN_FILENAME, IDIG_FILENAME, 
      USER_OCCURRENCE_DATA, USER_OCCURRENCE_DATA_DELIMITER,
      INTERSECT_FILTERSTRING, INTERSECT_VALNAME, INTERSECT_MINPERCENT, 
      INTERSECT_MINPRESENCE, INTERSECT_MAXPRESENCE, SCENARIO_PACKAGE,
      GRID_CELLSIZE, GRID_NUM_SIDES)
from LmServer.base.lmobj import LMError
from LmServer.common.datalocator import EarlJr
from LmServer.common.lmconstants import (Algorithms, LMFileType, ENV_DATA_PATH, 
         SPECIES_DATA_PATH, GPAM_KEYWORD, ARCHIVE_KEYWORD, PUBLIC_ARCHIVE_NAME,
         DEFAULT_EMAIL_POSTFIX)
from LmServer.common.localconstants import (PUBLIC_USER, APP_PATH, DATASOURCE, 
                                            POINT_COUNT_MIN)
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
                'email': '{}{}'.format(PUBLIC_USER, DEFAULT_EMAIL_POSTFIX)},
               {'id': DEFAULT_POST_USER,
                'email': '{}{}'.format(DEFAULT_POST_USER, DEFAULT_EMAIL_POSTFIX)}]
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
   matrixType = MatrixType.PAM
   if usr == PUBLIC_USER:
      matrixType = MatrixType.ROLLING_PAM
   for scen in predScens.values():
      meta = {ServiceObject.META_DESCRIPTION: GPAM_KEYWORD,
              ServiceObject.META_KEYWORDS: [GPAM_KEYWORD]}
      tmpGpam = LMMatrix(None, matrixType=matrixType, 
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
def _checkScenarios(scribe, legalUsers, modelScenCode, prjScenCodeList):
   epsgcode = mapunits = None
   if modelScenCode not in prjScenCodeList:
      prjScenCodeList.append(modelScenCode)
   for code in prjScenCodeList:
      scen = scribe.getScenario(code)
      if scen is None:
         raise LMError('Missing Scenario for code or id {}'.format(code))
      else:
         if scen.getUserId() not in legalUsers:
            raise LMError('legalUsers {} missing {}'.format(legalUsers,
                                                            scen.getUserId()))
      if epsgcode is None:
         epsgcode = scen.epsgcode
         mapunits = scen.units
         bbox = scen.bbox
   return epsgcode, mapunits, bbox
      
# ...............................................
def _checkOccurrenceSets(scribe, legalUsers, occIdFname, limit=10):
   missingCount = 0
   wrongUserCount = 0
   nonIntCount = 0
   if not os.path.exists(occIdFname):
      raise LMError('Missing OCCURRENCE_ID_FILENAME {}'.format(occIdFname))
   else:
      f = open(occIdFname, 'r')
      for i in range(limit):
         try:
            tmp = f.readline()
         except Exception, e:
            scribe.log.info('Failed to readline {} on line {}, stopping'
                            .format(str(e), i))
            break
         try:
            id = int(tmp.strip())
         except Exception, e:
            scribe.log.info('Unable to get Id from data {} on line {}'
                            .format(tmp, i))
            nonIntCount += 1
         else:
            occ = scribe.getOcc(id)
            if occ is None:
               missingCount += 1
            elif occ.getUserId() not in legalUsers:
               scribe.log.info('Unauthorized user {} for ID {}'
                               .format(occ.getUserId(), id))
               wrongUserCount += 1
   scribe.log.info('Errors out of the first {} occurrenceIds:'. format(limit))
   scribe.log.info('  Missing: {} '.format(missingCount))
   scribe.log.info('  Unauthorized data: {} '.format(wrongUserCount))
   
# ...............................................
def writeConfigFile(usr, usrEmail, archiveName, 
           envPackageName, dataSource, occIdFname,
           gbifFname, idigFname, bisonFname, userOccFname, userOccSep, 
           minpoints, algorithms, epsgcode, mapunits, 
           gridname, gridbbox, cellsides, cellsize, intersectParams, 
           mdlScenCode=None, prjScenCodes=None, 
           mdlMaskName=None, prjMaskName=None, 
           assemblePams=True):
   """
   """
   earl = EarlJr()
   pth = earl.createDataPath(usr, LMFileType.BOOM_CONFIG)
   newConfigFilename = os.path.join(pth, 
                              '{}{}'.format(archiveName, OutputFormat.CONFIG))
   f = open(newConfigFilename, 'w')
   f.write('[{}]\n'.format(SERVER_BOOM_HEADING))
   f.write('ARCHIVE_USER: {}\n'.format(usr))
   f.write('ARCHIVE_NAME: {}\n'.format(archiveName))
   if usrEmail is not None:
      f.write('TROUBLESHOOTERS: {}\n'.format(usrEmail))
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
   f.write('DATASOURCE: {}\n'.format(dataSource))
   if occIdFname is not None:
      f.write('OCCURRENCE_ID_FILENAME: {}\n'.format(occIdFname))
   # Species source type (for processing) and file
   if dataSource == SpeciesDatasource.GBIF:
      varname = 'GBIF_OCCURRENCE_FILENAME'
      dataFname = gbifFname
      # TODO: allow overwrite of these vars in initboom --> archive config file
      f.write('GBIF_TAXONOMY_FILENAME: {}\n'.format(GBIF_TAXONOMY_FILENAME))
      f.write('GBIF_PROVIDER_FILENAME: {}\n'.format(GBIF_PROVIDER_FILENAME))
   elif dataSource == SpeciesDatasource.BISON:
      varname = 'BISON_TSN_FILENAME'
      dataFname = bisonFname
   elif dataSource == SpeciesDatasource.IDIGBIO:
      varname = 'IDIG_FILENAME'
      dataFname = idigFname
   else:
      varname = 'USER_OCCURRENCE_DATA'
      dataFname = userOccFname
      f.write('USER_OCCURRENCE_DATA_DELIMITER: {}\n'
              .format(userOccSep))
   f.write('{}: {}\n'.format(varname, dataFname))
   f.write('\n')

   f.write('; ...................\n')
   f.write('; Env Package Vals\n')
   f.write('; ...................\n')
   # Input environmental data, pulled from SCENARIO_PACKAGE metadata
   f.write('SCENARIO_PACKAGE: {}\n'.format(envPackageName))
   f.write('SCENARIO_PACKAGE_EPSG: {}\n'.format(epsgcode))
   f.write('SCENARIO_PACKAGE_MAPUNITS: {}\n'.format(mapunits))
   # Scenario codes, created from environmental metadata  
   f.write('SCENARIO_PACKAGE_MODEL_SCENARIO: {}\n'.format(mdlScenCode))
   pcodes = ','.join(prjScenCodes)
   f.write('SCENARIO_PACKAGE_PROJECTION_SCENARIOS: {}\n'.format(pcodes))
   
   if mdlMaskName is not None:
      f.write('MODEL_MASK_NAME: {}\n'.format(mdlMaskName))
   if prjMaskName is not None:
      f.write('PROJECTION_MASK_NAME: {}\n'.format(prjMaskName))
   f.write('\n')
   
   f.write('; ...................\n')
   f.write('; Global PAM vals\n')
   f.write('; ...................\n')
   # Intersection grid
   f.write('GRID_NAME: {}\n'.format(gridname))
   f.write('GRID_BBOX: {}\n'.format(gridbbox))
   f.write('GRID_CELLSIZE: {}\n'.format(cellsize))
   f.write('GRID_NUM_SIDES: {}\n'.format(cellsides))
   f.write('\n')
   for k, v in intersectParams.iteritems():
      f.write('INTERSECT_{}:  {}\n'.format(k.upper(), v))
   f.write('ASSEMBLE_PAMS: {}\n'.format(str(assemblePams)))
   f.write('\n')
      
   f.close()
   return newConfigFilename

# ...............................................
def _findConfigOrDefault(config, varname, defaultValue, isList=False):
   var = None
   try:
      var = config.get(SERVER_BOOM_HEADING, varname)
   except:
      pass
   if var is None:
      var = defaultValue
   else:
      if isList and var:
         try:
            tmplist = [v.strip() for v in var.split(',')]
            var = []
         except:
            raise LMError('Failed to split variables on \',\'')
         for v in tmplist:
            try:
               var.append(int(v))
            except:
               var.append(v)
   return var

# ...............................................
def readConfigArgs(configFname):
   if configFname is None or not os.path.exists(configFname):
      print('Missing config file {}, using defaults'.format(configFname))
      configFname = None
   config = Config(siteFn=configFname)

   # Fill in missing or null variables for archive.config.ini
   usr = _findConfigOrDefault(config, 'ARCHIVE_USER', PUBLIC_USER)
   usrEmail = _findConfigOrDefault(config, 'ARCHIVE_USER_EMAIL', 
                              '{}{}'.format(PUBLIC_USER, DEFAULT_EMAIL_POSTFIX))
   archiveName = _findConfigOrDefault(config, 'ARCHIVE_NAME', PUBLIC_ARCHIVE_NAME)
   envPackageName = _findConfigOrDefault(config, 'SCENARIO_PACKAGE', SCENARIO_PACKAGE)
   if envPackageName is not None:
      modelScenCode = _findConfigOrDefault(config, 'SCENARIO_PACKAGE_MODEL_SCENARIO', 
                                           None, isList=False)
      prjScenCodeList = _findConfigOrDefault(config, 
                     'SCENARIO_PACKAGE_PROJECTION_SCENARIOS', None, isList=True)
   dataSource = _findConfigOrDefault(config, 'DATASOURCE', DATASOURCE)
   dataSource = dataSource.upper()
   occIdFname = _findConfigOrDefault(config, 'OCCURRENCE_ID_FILENAME', None)
   gbifFname = _findConfigOrDefault(config, 'GBIF_OCCURRENCE_FILENAME', 
                                    GBIF_OCCURRENCE_FILENAME)
   idigFname = _findConfigOrDefault(config, 'IDIG_FILENAME', IDIG_FILENAME)
   bisonFname = _findConfigOrDefault(config, 'BISON_TSN_FILENAME', 
                                    BISON_TSN_FILENAME) 
   userOccFname = _findConfigOrDefault(config, 'USER_OCCURRENCE_DATA', 
                                    USER_OCCURRENCE_DATA)
   userOccSep = _findConfigOrDefault(config, 'USER_OCCURRENCE_DATA_DELIMITER', 
                                    USER_OCCURRENCE_DATA_DELIMITER)
   minpoints = _findConfigOrDefault(config, 'POINT_COUNT_MIN', POINT_COUNT_MIN)
   algstring = _findConfigOrDefault(config, 'ALGORITHMS', ALGORITHMS)
   try:
      algorithms = [alg.strip().upper() for alg in algstring.split(',')]
   except:
      algorithms = algstring
   assemblePams = _findConfigOrDefault(config, 'ASSEMBLE_PAMS', ASSEMBLE_PAMS)
   gridbbox = _findConfigOrDefault(config, 'GRID_BBOX', None)
   cellsides = _findConfigOrDefault(config, 'GRID_NUM_SIDES', GRID_NUM_SIDES)
   cellsize = _findConfigOrDefault(config, 'GRID_CELLSIZE', GRID_CELLSIZE)
   gridname = '{}-Grid-{}'.format(archiveName, cellsize)
   # TODO: allow filter
   gridFilter = _findConfigOrDefault(config, 'INTERSECT_FILTERSTRING', 
                                     INTERSECT_FILTERSTRING)
   gridIntVal = _findConfigOrDefault(config, 'INTERSECT_VALNAME', 
                                     INTERSECT_VALNAME)
   gridMinPct = _findConfigOrDefault(config, 'INTERSECT_MINPERCENT', 
                                     INTERSECT_MINPERCENT)
   gridMinPres = _findConfigOrDefault(config, 'INTERSECT_MINPRESENCE', 
                                      INTERSECT_MINPRESENCE)
   gridMaxPres = _findConfigOrDefault(config, 'INTERSECT_MAXPRESENCE', 
                                      INTERSECT_MAXPRESENCE)
   intersectParams = {MatrixColumn.INTERSECT_PARAM_FILTER_STRING: gridFilter,
                      MatrixColumn.INTERSECT_PARAM_VAL_NAME: gridIntVal,
                      MatrixColumn.INTERSECT_PARAM_MIN_PRESENCE: gridMinPres,
                      MatrixColumn.INTERSECT_PARAM_MAX_PRESENCE: gridMaxPres,
                      MatrixColumn.INTERSECT_PARAM_MIN_PERCENT: gridMinPct}
   return (usr, usrEmail, archiveName, envPackageName, modelScenCode, prjScenCodeList, 
           dataSource, occIdFname, gbifFname, idigFname, bisonFname, 
           userOccFname, userOccSep, 
           minpoints, algorithms, assemblePams, gridbbox, cellsides, cellsize, 
           gridname, intersectParams)
   
# ...............................................
if __name__ == '__main__':
   # Created on roll install: lifemapper-server:lmdata-species
   defaultConfigFile = None
   # Use the argparse.ArgumentParser class to handle the command line arguments
   parser = argparse.ArgumentParser(
            description=('Populate a Lifemapper database with metadata ' +
                         'specific to an \'archive\', populated with '
                         'user-specified values in the config file argument and '
                         'configured environmental package metadata.'))
   parser.add_argument('-', '--config_file', default=defaultConfigFile,
            help=('Configuration file for the archive, gridset, and grid ' +
                  'to be created from these data.'))
   args = parser.parse_args()
   configFname = args.config_file

   (usr, usrEmail, archiveName, envPackageName, modelScenCode, prjScenCodeList, 
    dataSource, occIdFname, gbifFname, idigFname, bisonFname, userOccFname, userOccSep, 
    minpoints, algorithms, assemblePams, gridbbox, cellsides, cellsize, gridname, 
    intersectParams) = readConfigArgs(configFname)

   if not isCorrectUser():
      print("""
            When not running this script as `lmwriter`, make sure to fix
            permissions on the newly created shapegrid {}
            """.format(gridname))
   
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
      legalUsers = [PUBLIC_USER, usr]
# .............................
      logger.info('  Insert algorithm metadata ...')
      aIds = addAlgorithms(scribeWithBorg)
# .............................
      if modelScenCode and prjScenCodeList:
         metafname = None
         epsgcode, mapunits, bbox = _checkScenarios(scribeWithBorg, legalUsers,
                                              modelScenCode, prjScenCodeList)
         if gridbbox is None:
            gridbbox = bbox
      else:
         # Imports META
         META, metafname, pkgMeta, elyrMeta = pullClimatePackageMetadata(envPackageName)
         if gridbbox is None:
            gridbbox = pkgMeta['bbox']
         epsgcode = elyrMeta['epsg']
         mapunits = elyrMeta['mapunits']
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
      # Test a subset of OccurrenceIds provided as BOOM species input
      if occIdFname:
         _checkOccurrenceSets(scribeWithBorg, legalUsers, occIdFname)

# .............................
      # Shapefile, Gridset, Matrix for GPAM/BOOMArchive
      logger.info('  Insert, build shapegrid {} ...'.format(gridname))
      shpGrid, archiveGridset, globalPAMs = addArchive(scribeWithBorg, 
                         predScens, gridname, metafname, archiveName, 
                         cellsides, cellsize, mapunits, epsgcode,
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
      """(usr, usrEmail, archiveName, envPackageName, dataSource,  
           gbifFname, idigFname, bisonFname, userOccFname, userOccSep, 
           minpoints, algorithms, assemblePams, cellsides, cellsize, gridname, 
           intersectParams)
      """
      newConfigFilename = writeConfigFile(usr, usrEmail, archiveName, 
           envPackageName, dataSource, occIdFname,
           gbifFname, idigFname, bisonFname, userOccFname, userOccSep, 
           minpoints, algorithms, epsgcode, mapunits, 
           gridname, gridbbox, cellsides, cellsize, intersectParams, 
           mdlScenCode=mdlScencode, prjScenCodes=prjScencodes, 
           assemblePams=assemblePams)
   except Exception, e:
      logger.error(str(e))
      raise
   finally:
      scribeWithBorg.closeConnections()
       
"""
$PYTHON LmDbServer/boom/initboom.py  --config_file /opt/lifemapper/LmDbServer/tools/boom.sample.ini

"""