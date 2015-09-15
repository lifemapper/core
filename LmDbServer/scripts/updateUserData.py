from LmCommon.common.lmconstants import OCCURRENCES_SERVICE
from LmServer.common.datalocator import EarlJr
from LmServer.common.lmconstants import MODEL_PATH, USER_LAYER_PATH, \
                                    OutputFormat, MAP_PATH, SERVICES_PREFIX
from LmServer.common.localconstants import DATA_PATH
from LmServer.common.log import ThreadLogger
from LmServer.db.scribe import Scribe

import glob
import os
import shutil
import subprocess
import sys
from types import TupleType

CORE_SHAPEFILE_EXTENSIONS = [".shp", ".shx", ".dbf", ".prj", ".qix"]

# ...............................................
def moveOneUserMaps(olddir, newdir):
   if not os.path.exists(newdir):
      os.makedirs(newdir)
   if os.path.exists(olddir):
      mapfiles = os.listdir(olddir)
      for mf in mapfiles:
         if mf.endswith(OutputFormat.MAP):
            shutil.copy2(os.path.join(olddir, mf), 
                         os.path.join(newdir, mf))
            print ('Copied %s from %s to %s' % (mf, olddir, newdir))
   
# ...............................................
def moveAllUserMaps():
   basedir = os.path.join(DATA_PATH, MODEL_PATH)
   entries = os.listdir(basedir)
   
   for entry in entries:
      usrdir = os.path.join(basedir, entry)
      if os.path.isdir(usrdir):
         mapdir = os.path.join(usrdir, MAP_PATH)
         
         moveOneUserMaps(usrdir, mapdir)
         usrentries = os.listdir(usrdir)
         for uentry in usrentries:
            if os.path.isdir(os.path.join(usrdir, uentry)):
               try:
                  int(uentry)
               except:
                  pass
               else:
                  lyrdir = os.path.join(usrdir, uentry, USER_LAYER_PATH)
                  moveOneUserMaps(lyrdir, mapdir)
                  
# ...............................................
def copyDelete(logger, oldfname, newpth):
   success = False
   if os.path.exists(oldfname):
      if not os.path.exists(newpth):
         os.makedirs(newpth)
      try:
         shutil.copy2(oldfname, newpth)
         success = True
         logger.debug('    Copied %s to %s' % (oldfname, newpth))
      except Exception, e:
         logger.debug('    Failed to copy %s (%s)' % (oldfname, str(e)))
      else:
         try:
            os.remove(oldfname)
            logger.debug('      and removed')
         except Exception, e:
            logger.debug('      failed to remove (%s)' % (str(e)))
   return success

# ...............................................
def moveFiles(logger, fullfilenameList, newpth, bakpth=None):
   success = True
   for nameOrList in fullfilenameList:
      correctFileName = None
      if nameOrList is not None:
         fname = nameOrList
         if isinstance(nameOrList, TupleType):
            fname = nameOrList[0]
            correctFileName = nameOrList[1]
         if correctFileName is not None:
            tmp, oldBasename = os.path.split(of)
            tmp, ext = os.path.splitext(oldBasename)
            success = copyDelete(logger, of, correctFileName)
         success = copyDelete(logger, fname, newpth)
         # Move raster metadata files
         if fname.endswith('.tif'):
            mfname = fname + '.aux.xml'
            msuccess = copyDelete(logger, mfname, newpth)
            
#         # Move all shapefiles
#         if fname.endswith('.shp'):
#            otherfiles = glob.glob(fname.replace('.shp','.*'))
#            # If missing files, check backup location
#            if len(otherfiles) <= 1 and bakpth is not None:
#               tmp, basename = os.path.split(fname)
#               bakfname = os.path.join(bakpth, basename)
#               otherfiles = glob.glob(bakfname.replace('.shp','.*'))
#            for of in otherfiles:
#               if correctBaseName is not None:
#                  tmp, oldBasename = os.path.split(of)
#                  tmp, ext = os.path.splitext(oldBasename)
#                  newfname = os.path.join(newpth, correctBaseName + ext)
#                  success = copyDelete(logger, of, newfname)
#               else:
#                  success = copyDelete(logger, of, newpth)
#         else:  
#            success = copyDelete(logger, fname, newpth)
#            # Move raster metadata files
#            if fname.endswith('.tif'):
#               mfname = fname + '.aux.xml'
#               msuccess = copyDelete(logger, mfname, newpth)
   return success
   
# ...............................................
def moveFailedSDMData(scribe, earljr, usr):
   occs = scribe.getOccurrenceSetsForUser(usr)
   oldpth = os.path.join(DATA_PATH, MODEL_PATH, usr)
   for occ in occs:
      filesToMove = []
      modelsToUpdate = []
      projsToUpdate = []
      newpth = earljr.createDataPath(usr, occsetId=occ.getId())
      oBadDloc = os.path.join(oldpth, 'lyr_%s.shp' % str(occ.getId()))
      correctBaseName = 'pt%s' % str(occ.getId())
      oGoodDloc = os.path.join(newpth, correctBaseName+'.shp')
      oDbDloc = occ.getDLocation()
      scribe.log.debug('Occurrence: %d' % (occ.getId()))
      if not os.path.exists(oGoodDloc) or os.path.exists(oBadDloc):
         filesToMove.append((oBadDloc, correctBaseName))
         if oDbDloc != oGoodDloc:
            occ.clearDLocation()
            occ.setDLocation()
            scribe.log.debug('  Occurrence new dlocation %s' % occ.getDLocation())
   
         exps = scribe.getExperimentsForOccurrenceSet(occ.getId()) 
         for exp in exps:         
            for prj in exp.projections:
               pDbDloc = prj.getDLocation()
               dbpth, basename = os.path.split(pDbDloc)
               pBadDloc = os.path.join(oldpth, basename)
               pGoodDloc = os.path.join(newpth, basename)
               if os.path.exists(pGoodDloc):
                  break
               elif os.path.exists(pBadDloc):
                  filesToMove.append((pBadDloc, pGoodDloc))
               if pDbDloc != pGoodDloc:
                  prj.clearDLocation()
                  prj.setDLocation()
                  projsToUpdate.append(prj)
                  scribe.log.debug('  Proj new dlocation %s' % prj.getDLocation())
               
         scribe.log.debug('  Files: %s' % str(filesToMove))
         success = moveFiles(scribe.log, filesToMove, newpth)
         if success:
            if oDbDloc != oGoodDloc:
               scribe.updateOccset(occ)
            for mdl in modelsToUpdate:
               scribe.updateModel(mdl)
            for prj in projsToUpdate:
               scribe.updateProjection(prj)
            
# ...............................................
def moveOneSDMDataUpdateObjects(scribe, occ, oldpth, newpth, bakpth=None):
   occFname = occ.getDLocation()
   occId = occ.getId()
   badOccNamewoext = os.path.join(oldpth, 'lyr_%s' % str(occId))
   tmp, occbasename = os.path.split(occFname)
   occnamewoext, tmp = os.path.splitext(occbasename)
   newOldOccShapenames = []
   for ext in CORE_SHAPEFILE_EXTENSIONS:
      newOldOccShapenames.append((os.path.join(newpth, occnamewoext + ext),
                                  os.path.join(oldpth, occnamewoext + ext),
                                  os.path.join(oldpth, badOccNamewoext + ext)))
      if bakpth is not None:
         bakOccFname = os.path.join(bakpth, occbasename)
   
   # Check files 
   for (newOccFname, oldOccFname, badOccFname) in newOldOccShapenames:
      if not os.path.exists(newOccFname):
         if os.path.exists(oldOccFname):
            scribe.log.debug('Moving occ %d  to %s' % (occId, newOccFname))
            success = moveFiles(scribe.log, [oldOccFname], newpth)
         elif os.path.exists(badOccFname):
            scribe.log.debug('Correcting occ %d file %s to %s' 
                             % (occId, badOccFname, newOccFname))
            success = moveFiles(scribe.log, [badOccFname, newOccFname], newpth)
         elif bakpth is not None and os.path.exists(bakOccFname):
            success = moveFiles(scribe.log, [bakOccFname], newpth)
            scribe.log.error('Occ data only in backup dir %s' % bakOccFname)
         else:
            scribe.log.error('Failed to find occ file %s' % newOccFname)
         
    # Check object 
   if not occFname.startswith(newpth):
      occ._dlocation = newOccFname
      scribe.updateOccset(occ)
   
   exps = scribe.getExperimentsForOccurrenceSet(occ.getId()) 
   for exp in exps:
      mdlFname = exp.model.getDLocation()
      tmp, mdlbasename = os.path.split(mdlFname)
      newMdlFname = os.path.join(newpth, mdlbasename)
      oldMdlFname = os.path.join(oldpth, mdlbasename)
      for fname in (mdlFname, exp.model.getModelRequestFilename(), 
                    exp.model.getModelStatisticsFilename()):
         tmp, basename = os.path.split(fname)
         newFname = os.path.join(newpth, basename)
         oldFname = os.path.join(oldpth, basename)
         if bakpth is not None:
            bakFname = os.path.join(bakpth, basename)
         # Check files 
         if not os.path.exists(newFname):
            if os.path.exists(oldFname):
               success = moveFiles(scribe.log, [oldFname], newpth, bakpth=bakpth)
            elif bakpth is not None and os.path.exists(bakFname):
               success = moveFiles(scribe.log, [bakFname], newpth)
               scribe.log.error('Mdl file only in backup dir %s' % bakFname)
            else:
               scribe.log.error('Failed to find mdl file %s' % newFname)

      # Check object 
      if not mdlFname.startswith(newpth):
         exp.model.update(ruleset=newMdlFname)
         scribe.updateModel(exp.model)
   
      for prj in exp.projections:
         prjFname = prj.getDLocation()
         tmp, prjbasename = os.path.split(prjFname)
         newPrjFname = os.path.join(newpth, prjbasename)
         oldPrjFname = os.path.join(oldpth, prjbasename)
         # Check files 
         for fname in (prjFname, prj.getProjRequestFilename(),
                       prj.getProjPackageFilename()):
            tmp, basename = os.path.split(fname)
            newFname = os.path.join(newpth, basename)
            oldFname = os.path.join(oldpth, basename)
            if bakpth is not None:
               bakFname = os.path.join(bakpth, basename)
            # Check files 
            if not os.path.exists(newFname):
               if os.path.exists(oldFname):
                  success = moveFiles(scribe.log, [oldFname], newpth)
               elif bakpth is not None and os.path.exists(bakFname):
                  success = moveFiles(scribe.log, [bakFname], newpth)
                  scribe.log.error('Prj file only in backup dir %s' % bakFname)
               else:
                  scribe.log.error('Failed to find prj file %s' % newFname)
         # Check object 
         if not prjFname.startswith(newpth):
            prj._dlocation = newPrjFname
            scribe.updateProjection(prj)
   scribe.log.debug('Updated occ %d and experiments' % occ.getId())
         
               
# ...............................................
def fixUserData(scribe, earljr, usr):
   oldusrpth = '/share/data/archive/%s/' % (usr)
   bakusrpth = '/PVRAID0-1/UserData/%s' % (usr)
   occs = scribe.getOccurrenceSetsForUser(usr)
   for occ in occs:
      occ._metadataUrl = '%s/%s/%s/%s' % ('http://lifemapper.org', 
                           SERVICES_PREFIX, OCCURRENCES_SERVICE, occ.getId())
      newpth = earljr.createDataPath(usr, occsetId=occ.getId())      
      oldpth = os.path.join(oldusrpth, str(occ.epsgcode), 'Layers')
      bakpth = os.path.join(bakusrpth, str(occ.epsgcode), 'Layers')
      moveOneSDMDataUpdateObjects(scribe, occ, oldpth, newpth, bakpth=bakpth)
         
# ...............................................
def substitute(log, mapfname, findstring, replacestring):
   args = ['sed', '-i', 's:%s:%s:g' %  (findstring, replacestring), mapfname]
   try:
      retcode = subprocess.call(args)
      if retcode == 0:
         log.debug('replaced path to %s ' % (replacestring))
      else:
         log.error('sed process returned ' + str(retcode))
   except Exception, e:
       log.error('sed process exception: ' + str(e))
       
# ...............................................
def editOneSDMLayerDefn(scribe, earljr, mapfname, usr, occ, oldpth):
   subPairs = []
   newOccDLocation = earljr.createSDMLayerFilename(usr, occ.getId(), ext='.shp')
   tmp, occBasename = os.path.split(newOccDLocation)
   oldOccDLocation = os.path.join(oldpth, occBasename)
   subPairs.append((oldOccDLocation, newOccDLocation))

   exps = scribe.getExperimentsForOccurrenceSet(occ.getId()) 
   for exp in exps:         
      for prj in exp.projections:
         newPrjDLocation = earljr.createSDMLayerFilename(usr, occ.getId(), 
                                          prjId=prj.getId(), ext='.tif')
         tmp, prjBasename = os.path.split(newPrjDLocation)
         oldPrjDLocation = os.path.join(oldpth, prjBasename)
         subPairs.append((oldPrjDLocation, newPrjDLocation))
         
   for oldstring, newstring in subPairs:
      substitute(scribe.log, mapfname, oldstring, newstring)

# ...............................................
if __name__ == '__main__':  
   usr = 'changeThinking'
   logger = ThreadLogger('updateUserData')
   earljr = EarlJr()
   scribe = Scribe(logger, overrideDB='hera.nhm.ku.edu')
   success = scribe.openConnections()
   if success: 
#       moveFailedSDMData(scribe, earljr, 'changeThinking')
      users = scribe.getAllUserIds()
      for usr in ['elseweb']:  #users:
         if usr not in ('bison', 'lm2', 'changeThinking', 'Dermot', 'pragma'):
            scribe.log.debug('User: %s' % usr)
            fixUserData(scribe, earljr, usr)
#          moveCtSDMData(logger)
      scribe.closeConnections()


