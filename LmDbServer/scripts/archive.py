"""
@summary: Deletes old data

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
import argparse
import csv
import glob
import mx.DateTime
import os
import subprocess
import tarfile

from LmCommon.common.lmconstants import DEFAULT_POST_USER, OutputFormat
from LmCommon.common.localconstants import ARCHIVE_USER
from LmServer.common.datalocator import EarlJr
from LmServer.common.lmconstants import MAL_STORE, LM_SCHEMA
from LmServer.common.localconstants import APP_PATH
from LmDbServer.populate.GBIF.sortGbifExport import datapath
 
DEBUG = False
USER_REPLACE_STR = '#USERS#'
# list of (table, columns, selectStatement)
USER_DEPENDENCIES = [
   ('{}.lmuser'.format(LM_SCHEMA), 
    'SELECT * FROM {}.lmuser WHERE userid IN ({})'.format(LM_SCHEMA, USER_REPLACE_STR)),
                     
   ('{}.lmjob'.format(LM_SCHEMA), 
    'SELECT j.* FROM {}.lmjob j, '.format(LM_SCHEMA)+
    ' {0}.lm_occJob oj, {0}.lm_mdlJob mj, {0}.lm_prjJob pj '.format(LM_SCHEMA)+
    ' WHERE (j.lmjobid = oj.lmjobid  '+
    '        AND oj.occuserId IN ({}))'.format(USER_REPLACE_STR)+
    '    OR (j.lmjobid = mj.lmjobid  '+
    '        AND mj.mdluserId IN ({}))'.format(USER_REPLACE_STR)+
    '    OR (j.lmjobid = pj.lmjobid  '+
    '        AND pj.mdluserId IN ({}))'.format(USER_REPLACE_STR)),
                     
   ('{}.experiment'.format(LM_SCHEMA), 
    'SELECT * FROM {}.experiment WHERE userid IN ({})'.format(LM_SCHEMA, USER_REPLACE_STR)),
   ('{}.occurrenceset'.format(LM_SCHEMA), 
    'SELECT * FROM {}.occurrenceset WHERE userid IN ({})'.format(LM_SCHEMA, USER_REPLACE_STR)),
   ('{}.scenario'.format(LM_SCHEMA), 
    'SELECT * FROM {}.scenario WHERE userid IN ({})'.format(LM_SCHEMA, USER_REPLACE_STR)),
   ('{}.layertype'.format(LM_SCHEMA), 
    'SELECT * FROM {}.layertype WHERE userid IN ({})'.format(LM_SCHEMA, USER_REPLACE_STR)),
   ('{}.layer'.format(LM_SCHEMA), 
    'SELECT * FROM {}.layer WHERE userid IN ({})'.format(LM_SCHEMA, USER_REPLACE_STR)),
                     
   ('{}.scenariolayers'.format(LM_SCHEMA), 
    'SELECT sl.* FROM {0}.scenariolayers sl, {0}.scenario s'.format(LM_SCHEMA)+ 
    ' WHERE sl.scenarioid = s.scenarioid AND s.userid IN ({})'.format(USER_REPLACE_STR)),
   
   ('{}.keyword'.format(LM_SCHEMA), 
    'SELECT k.* FROM {0}.keyword k, {0}.layertypekeyword ltk, {0}.layertype lt, {0}.scenariokeywords sk, {0}.scenario s '.format(LM_SCHEMA)+
    ' WHERE (k.keywordid = ltk.keywordid AND ltk.layertypeid = lt.layertypeid AND lt.userid IN ({}))'.format(USER_REPLACE_STR)+
    ' OR (k.keywordid = sk.keywordid AND sk.scenarioid = s.scenarioid AND s.userid IN ({}))'.format(USER_REPLACE_STR)),
   ('{}.layertypekeyword'.format(LM_SCHEMA), 
    'SELECT * FROM {0}.layertypekeyword ltk, {0}.layertype lt'.format(LM_SCHEMA)+ 
    ' WHERE ltk.layertypeid = lt.layertypeid '+
    ' AND lt.userid IN ({})'.format(USER_REPLACE_STR)),
   ('{}.scenariokeywords'.format(LM_SCHEMA), 
    'SELECT * FROM {0}.scenariokeywords sk, {0}.scenario s'.format(LM_SCHEMA)+
    ' WHERE sk.scenarioid = s.scenarioid '+
    ' AND s.userid IN ({})'.format(USER_REPLACE_STR)),

   ('{}.model'.format(LM_SCHEMA), 
    'SELECT * FROM {}.model WHERE userid IN ({})'.format(LM_SCHEMA, USER_REPLACE_STR)),
   ('{}.projection'.format(LM_SCHEMA), 
    'SELECT * FROM {0}.projection p, {0}.model m'.format(LM_SCHEMA)+
    ' WHERE p.modelid = m.modelid and m.userid IN ({})'.format(USER_REPLACE_STR)) ]

# MAXSIZE = '1T'
# MULTI_VOLUME_SCRIPT='new-volume.sh'
# ...............................................
class SetPass:
   def __call__(self):
      hostname = subprocess.check_output('hostname').strip()
      if hostname == 'hera':
         os.environ['PGPASSWORD'] = 'jetsons'
      else:
         # more /opt/lifemapper/rocks/etc/users | grep admin | awk '{print $2}'
         fname = os.path.join(APP_PATH, 'rocks/etc/users')
         cmd = 'cat {} | grep admin | awk \'!print $2?\''.format(fname)
         cmd = cmd.replace('!', '{')
         cmd = cmd.replace('?', '}')
         pswd = subprocess.check_output(cmd).strip()
         os.environ['PGPASSWORD'] = pswd

# ...............................................
def getTimestamp(dateonly=False):
   '''
   @note: options must be valid options to the linux 'date' command
   '''
   args = ['date']
   if dateonly:
      args.append('+%F')
   timestamp = subprocess.check_output(args).strip()
   return timestamp
   
# ...............................................
def getFilename(outpath, basefname, dumptype, table=None, user=None):
   '''
   @param outpath: base path for filenames
   @param basefname: common base filename for all dumptypes 
   @param dumptype: options are db_schema, db_data, file_data, readme, log
   @param table: 2-tuple, first is the tablename, 2nd is an integer indicating 
          the order in which to process this file
   @param user: username string
   '''
   outfname = None
   choices = ('db_schema', 'db_data', 'file_data', 'readme', 'table', 'table_restore', 'log')
   if dumptype not in choices:
      raise Exception('Unknown dumptype {}; choices are {}'.format(dumptype, choices))
   
   if dumptype == 'table' and table is not None:
      basefname = '{}-{:02d}-{}'.format(basefname, table[1], table[0])
   elif dumptype == 'file_data' and user is not None:
      basefname = '{}-{}'.format(basefname, user)
      
   if dumptype == 'db_schema':
      outfname = os.path.join(outpath, '{}.schema.dump'.format(basefname))
   elif dumptype == 'db_data':
      outfname = os.path.join(outpath, '{}.data.dump'.format(basefname))
   elif dumptype == 'file_data':
      outfname = os.path.join(outpath, '{}{}'.format(basefname, OutputFormat.TAR_GZ))
   elif dumptype == 'readme':
      outfname = os.path.join(outpath, '{}.README'.format(basefname))
   elif dumptype == 'table':
      outfname = os.path.join(outpath, '{}{}'.format(basefname, OutputFormat.TXT))
   elif dumptype == 'table_restore':
      outfname = os.path.join(outpath, '{}{}'.format(basefname, OutputFormat.CSV))
   elif dumptype == 'log':
      outfname = os.path.join(outpath, '{}{}'.format(basefname, OutputFormat.LOG))

   return outfname
      
# ...............................................
def getColumns(dbcmd, fulltablename):
   getColumnsStmt = ' {} --command=\"\d {}\" | grep \'|\' | awk \'!print $1?\''.format(dbcmd, fulltablename)
   getColumnsStmt = getColumnsStmt.replace('!', '{')
   getColumnsStmt = getColumnsStmt.replace('?', '}')
   print 
   print getColumnsStmt
   print 
   columnStr = subprocess.check_output(getColumnsStmt, preexec_fn=SetPass, shell=True)
   cols = columnStr.split('\n')
   cols.remove('Column')
   cols.remove('') 
   return cols

# ...............................................
def copyOutDatabaseUsers(outpath, basefname, lmusers, dbcmd, restrictToTables=None):
   ingestfname = getFilename(outpath, basefname, 'table_restore')
   readmeLines = ['', '',
            'The following files contain the user-specific metadata contained ',
            'in the Lifemapper SDM.  Files are numbered according to the order ',
            'in which they should be restored.  Ingest statements are listed, ', 
            'in order, in {} '.format(ingestfname) ]
   escapedUsers = ["\'{}\'".format(usr) for usr in lmusers]
   userSetStr = ', '.join(escapedUsers)
   csvfile = open(ingestfname, 'wb')
   csvwriter = csv.writer(csvfile, delimiter=CSV_DELIMITER)
   for i in range(len(USER_DEPENDENCIES)):
      (tablename, selstr) = USER_DEPENDENCIES[i]
      if restrictToTables is None or tablename in restrictToTables:
         cols = getColumns(dbcmd, tablename)
         outfname = getFilename(outpath, basefname, 'table', table=(tablename, i))
         queryStmt = selstr.replace(USER_REPLACE_STR, userSetStr)
         copyToStmt = '\"COPY ({}) TO STDOUT \"'.format(queryStmt)      
         dumptableStmt = '{} --command={}'.format(dbcmd, copyToStmt)
         
         print 'Dumping table: ', tablename
         with open(outfname, 'w') as outf:
            dProc = subprocess.Popen(dumptableStmt, stdout=outf, preexec_fn=SetPass, shell=True)
         outf.close()
         
         csvwriter.writerow([os.path.basename(outfname), tablename, ', '.join(cols)])
         
         readmeLines.extend(['', 
            '{}:'.format(outfname),
            '    contains the metadata contained in the Lifemapper SDM',
            '    database, table {}, including the columns, in order. '.format(tablename)])
   csvfile.close()
   return readmeLines
      
# ...............................................
def ingestRecordData(inpath, inbasename, tablename, columnStr):
   infname = os.path.join(inpath, inbasename)
   copyFromStmt = '\"COPY {} ({}) FROM STDIN \"'.format(tablename, columnStr)
   ingesttableStmt = '{} --command={}'.format(dbcmd, copyFromStmt)
   print('Copying records from {} into {} ...'.format(ingestfname, tablename))
   with open(infname, 'r') as inf:
      if not DEBUG:
         dProc = subprocess.Popen(ingesttableStmt, stdin=inf, preexec_fn=SetPass, shell=True)
   inf.close()
      
# ...............................................
def untarFileData(inpath, scriptname):
   tarnames = glob.glob(os.path.join(inpath, scriptname+'*'+OutputFormat.TAR_GZ))
   for tarball in tarnames:
      cmd = 'tar -xvzf {}'.format(tarball)
      dProc = subprocess.Popen(cmd, shell=True)
      
      
# ...............................................
def dumpDbSchema(outpath, basefname, dbuser, dumpformat, dbschema, dbname):
   outfname = getFilename(outpath, basefname, 'db_schema')
   dumpschemStmt = 'pg_dump --username={} --format={} --schema-only --schema={} --verbose --file={} {}'.format(
                           dbuser, dumpformat, dbschema, outfname, dbname)
   if not DEBUG:
      dProc = subprocess.Popen(dumpschemStmt, preexec_fn=SetPass, shell=True)
   restoreSchemaArgs = ['pg_restore', '--verbose',
                        '--username={}'.format(dbuser), 
                        '--dbname={}'.format(dbname), 
                        '--format={}'.format(dumpformat), 
                        '--create',
                        outfname]
   restoreCmd = ' '.join(restoreSchemaArgs)
   readmeLines = ['', '',
            '{}:'.format(outfname),
            '   contains the database structure for the {} schema of the Lifemapper SDM database'.format(outfname, dbschema),
            '   If an up-to-date \'{}\' database exists, and this data is to be appended, the {} file is not needed.'.format(dbname, outfname),
            '   To drop any existing \'{}\' database, and create the schema into a new database, execute: '.format(dbname),
            '      {}'.format(restoreCmd)]
   return readmeLines

# ...............................................
def dumpAllDbData(outpath, basefname, dbuser, dumpformat, dbschema, dbname):
   outfname = getFilename(outpath, basefname, 'db_data')
   dumpdataStmt = 'pg_dump --username={} --data-only --format={} --verbose  --file={}  {}'.format(
                                       dbuser, dumpformat, outfname, MAL_STORE)
   if not DEBUG:
      dProc = subprocess.Popen(dumpdataStmt, shell=True)
   restoreCmd = 'pg_restore --verbose --username={} --dbname={} --format={} []'.format(
                                          dbuser, dbname, dumpformat, outfname)
   readmeLines = ['', '',
            '{}:'.format(outfname),
            '    contains the metadata contained in the Lifemapper SDM database'
            '   To append data to the \'{}\' database, execute: '.format(dbname),
            '   {}'.format(restoreCmd)]
   return readmeLines

# ...............................................
def ignoreUser(usr, ignoreUsers, ignoreUserPrefixes):
   ignoreMe = False
   ignoreUserPrefixes.append('(')
   ignoreUsers.append('')
   if usr in ignoreUsers:
      ignoreMe = True
   else:
      for pre in ignoreUserPrefixes:
         if usr.startswith(pre):
            ignoreMe = True
   return ignoreMe
         
# ...............................................
def getActiveUsers(dbcmd, dbschema, defaultUser, ignoreUsers, ignoreUserPrefixes, days=180):
      pastdate = mx.DateTime.gmt().mjd - days
      queryStmt = ('select distinct(m.userid) from {0}.model m, {0}.scenario s '.format(dbschema)+
                   'WHERE statusmodtime > {} '.format(pastdate)+
                   'AND m.scenarioid = s.scenarioid '+
                   'AND s.userid != \'{}\' '.format(defaultUser))
      cmd = '\"COPY ({}) TO STDOUT \"'.format(queryStmt)
      fullStmt = ' {} --command={}'.format(dbcmd, cmd)
      backupuserStr = subprocess.check_output(fullStmt, preexec_fn=SetPass, shell=True)
      backupusers = backupuserStr.split('\n')
      activeUsers = []
      for usr in backupusers:
         if not ignoreUser(usr, ignoreUsers, ignoreUserPrefixes):
            activeUsers.append(usr)
      print 'Active Users: ', str(activeUsers)
      return activeUsers

# ...............................................
def getUsersToBackup(backupChoice, dbcmd, dbschema, defaultUser, 
                     ignoreUsers=[], ignoreUserPrefixes=[]):
   hostname = subprocess.check_output('hostname').strip()
   if hostname == 'hera':
      datapath = '/share/data/archive'
   else:
      earl = EarlJr()
      datapath = earl.createArchiveDataPath()
   if backupChoice not in ('users', 'all'):
      backupusers = [backupChoice]
   elif backupChoice == 'users':
      ignoreUsers.append(defaultUser)
      backupusers = getActiveUsers(dbcmd, dbschema, defaultUser, ignoreUsers=ignoreUsers, 
                                   ignoreUserPrefixes=ignoreUserPrefixes)
   elif backupChoice == 'all':
      backupusers = []
      for entry in os.listdir(datapath):
         if (not entry.startswith('.') and 
             not entry in ignoreUsers and
             os.path.isdir(os.path.join(datapath, entry))):
            backupusers.append(entry)
   return datapath, backupusers

# ...............................................
def dumpFileData(outpath, basefname, datapath, backupusers):
   readmeLines = []
   pwd = os.curdir
   # Backup, compress requested DATA_PATH/MODEL_PATH/<user> directories
   os.chdir(datapath)
   for entry in os.listdir(datapath):
      if entry in backupusers:
         if len(readmeLines) == 0:
            readmeLines.extend(['', '', 
            '   The following files contain the file data for Lifemapper data for users: ',
            str(backupusers),
            '   In the target Lifemapper installation, file data should be moved into the ',
            '   DATA_PATH/MODEL_PATH directory; these data came from the',
            '   {} directory in the origin Lifemapper installation on {}'.format(datapath, getTimestamp(dateonly=True)),
            '',
            '   If the existing data archive contains a matching username, data could',
            '   be overwritten.  If this is the case, uncompress to a different',
            '   location, and merge with \"rsync -av source destination\".',
            '   To uncompress data, execute the following commands: '])

         outfname = getFilename(outpath, basefname, 'file_data', user=entry)
         if not DEBUG:
            with tarfile.open(outfname, mode='w:gz') as archive:
               archive.add(os.path.join(datapath, entry), recursive=True)
         
         restoreCmd = 'tar -xvzf {}'.format(outfname)
         readmeLines.append('      {}'.format(restoreCmd)) 
   os.chdir(pwd)     
            
   return readmeLines

# ...............................................
def writeReadme(outpath, basefname, readmeLineLists):
   readmeFname = getFilename(outpath, basefname, 'readme')
   rmf = open(readmeFname, 'w')
   rmf.write('*********************************************************************')
   rmf.write('This README was generated to accompany data dumped into files: ')
   for helpLines in readmeLineLists:
      for line in helpLines:
         rmf.write(line+'\n')
   rmf.close()
   return readmeFname

# ..............................................................................
# MAIN
# ..............................................................................
# Note: Add any existing users to ignore list
IGNORE_USERS = [DEFAULT_POST_USER, 'aimee', 'astewart', 'changeThinking', 'cgwillis', 
               'pragma', 'unitTest', 'zeppo', 'aimee.stewart@ku.edu', 'cjgrady@ku.edu', 
               'DermotYeti', 'DermotYeti2', 'DermotYeti3', 'DermotYeti4', 'camposds1', 
               'kubi'] 
IGNORE_USER_PREFIXES = ['CT', 'elseweb', 'Dermot', 'Workshop']
ONLY_USERS = [ 'changeThinking'] 
ONLY_USERS_PREFIXES = ['CT', 'elseweb']
CSV_DELIMITER='\t'
ARCHIVE_DEPENDENCIES = ['{}.lmuser'.format(LM_SCHEMA), 
                        '{}.scenario'.format(LM_SCHEMA), 
                        '{}.layertype'.format(LM_SCHEMA), 
                        '{}.layer'.format(LM_SCHEMA), 
                        '{}.scenariolayers'.format(LM_SCHEMA), 
                        '{}.keyword'.format(LM_SCHEMA), 
                        '{}.layertypekeyword'.format(LM_SCHEMA), 
                        '{}.scenariokeywords'.format(LM_SCHEMA)]


if __name__ == '__main__':
   parser = argparse.ArgumentParser(
                description="Script to backup or restore one or more users' data")
   
   parser.add_argument('command', type=str, choices=['backup','restore'], 
                        help='Backup from existing database or restore to existing database')
   
   group1 = parser.add_mutually_exclusive_group()
   group1.add_argument('-o', "--outpath", type=str, 
                        help='Directory for output files')
   group1.add_argument('-i', "--infile", type=str, 
                        help='Input file containing lines of: '+
                             'input record filename, tablename, column names; '+
                             'input record files should be in the same directory')
   
   group2 = parser.add_mutually_exclusive_group()
   group2.add_argument('-A', '--archive', help='Backup Archive data',
                       action='store_true')
   group2.add_argument('-U', '--users', help='Backup all (non-archive) user data (Default)',
                       action='store_true')
   group2.add_argument('-a', '--all', help='Backup Archive and non-archive user data',
                       action='store_true')
   group2.add_argument('-u', '--singleUser', type=str, 
                       help='Backup a single user')

   args = parser.parse_args()
   cmd = args.command
   
   if cmd == 'backup':
      if args.users:
         backupChoice = 'users'
      elif args.all:
         backupChoice = 'all'
      elif args.archive:
         backupChoice = ARCHIVE_USER
      elif args.singleUser:
         backupChoice = args.singleUser
      filepath = args.outpath
      if not os.path.exists(filepath):
         print '{} does not exist'.format(filepath)
         exit(-1)
         
   elif cmd == 'restore':
      ingestfname = args.infile
      if not os.path.exists(ingestfname):
         print '{} does not exist'.format(ingestfname)
         exit(-1)
      filepath = os.path.split(ingestfname)[0]
      
   # Naming variables
   scriptname = os.path.splitext(os.path.basename(__file__))[0]
   datestr = getTimestamp(dateonly=True).replace('-', '_')
#    logfname = getFilename(filepath, basefname, 'log')
    
   # PostgreSQL dump/restore options
   dbuser = 'admin'
   dumpformat = 'custom'
   dbcmd = 'psql --username={} --dbname={} '.format(dbuser, MAL_STORE)
   
   
   if cmd == 'backup':
      basefname = '{}.{}.{}'.format(scriptname, backupChoice, datestr)
      # Text for README
      dbschemaHelp = [''] 
      tableHelp = [''] 
      tarballHelp = ['']
      
      # Identify users for file backup
      cutoffDate = mx.DateTime.gmt().mjd - (180)
      datapath, backupusers = getUsersToBackup(backupChoice, dbcmd, LM_SCHEMA, ARCHIVE_USER, 
                                               ignoreUsers=IGNORE_USERS, 
                                               ignoreUserPrefixes=IGNORE_USER_PREFIXES)
      # Dump Schema and Database
      dbschemaHelp = dumpDbSchema(filepath, basefname, dbuser, dumpformat, 
                                  LM_SCHEMA, MAL_STORE)
      # Dump tables of default input data 
      if backupChoice == ARCHIVE_USER:
         tableHelp = copyOutDatabaseUsers(filepath, basefname, backupusers, dbcmd, 
                                          restrictToTables=ARCHIVE_DEPENDENCIES)
      # Dump tables of user data 
      else:
         tableHelp = copyOutDatabaseUsers(filepath, basefname, backupusers, dbcmd)
      
#       # Backup, compress requested DATA_PATH/MODEL_PATH/<user> directories 
#       if backupChoice != ARCHIVE_USER:
#          tarballHelp = dumpFileData(filepath, basefname, datapath, backupusers)
      
      # Explain outputs
      readmeFname = writeReadme(filepath, basefname, 
                                [dbschemaHelp, tableHelp, tarballHelp])
      
      print 'Instructions for using output from this script are in {}'.format(readmeFname)
      
   elif cmd == 'restore':
#       untarFileData(filepath, scriptname)
      
      with open(ingestfname) as csvfile:
         reader = csv.reader(csvfile, delimiter=CSV_DELIMITER)
         for row in reader:
            baseinfname, tablename, columnStr = row
            ingestRecordData(filepath, baseinfname, tablename, columnStr)
      csvfile.close()
      
      '''
      lm3.lmuser
      lm3.lmjob
      lm3.experiment
      lm3.occurrenceset
      lm3.scenario
      lm3.layertype
      lm3.layer
      lm3.scenariolayers
      lm3.keyword
      lm3.layertypekeyword
      lm3.scenariokeywords
      lm3.model
      lm3.projection
      '''   
   
