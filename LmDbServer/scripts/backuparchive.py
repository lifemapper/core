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
import os
import subprocess
import sys
import tarfile

from LmCommon.common.lmconstants import DEFAULT_POST_USER
from LmCommon.common.localconstants import ARCHIVE_USER
from LmServer.common.datalocator import EarlJr
from LmServer.common.lmconstants import MAL_STORE, LM_SCHEMA
from LmDbServer.populate.GBIF.sortGbifExport import datapath

# MAXSIZE = '1T'
# MULTI_VOLUME_SCRIPT='new-volume.sh'

# ...............................................
def getTimestamp(*options):
   '''
   @note: options must be valid options to the linux 'date' command
   '''
   args = ['date']
   args.extend(options)
   timestamp = subprocess.check_output(['date']).strip()
   return timestamp
   
# ...............................................
def getFilename(outpath, basefname, dumptype):
   '''
   @param outpath: base path for filenames
   @param basefname: common base filename for all dumptypes 
   @param dumptype: options are db_schema, db_data, file_data, readme, log
   '''
   outfname = None
   choices = ('db_schema', 'db_data', 'file_data', 'readme', 'log')
   if dumptype not in choices:
      raise Exception('Unknown dumptype {}; choices are {}'.format(dumptype, choices))
   
   if dumptype == 'db_schema':
      outfname = os.path.join(outpath, '{}.schema.dump'.format(basefname))
   elif dumptype == 'db_data':
      outfname = os.path.join(outpath, '{}.data.dump'.format(basefname))
   elif dumptype == 'file_data':
      outfname = os.path.join(outpath, '{}.tar.gz'.format(basefname))
   elif dumptype == 'readme':
      outfname = os.path.join(outpath, '{}.README'.format(basefname))
   elif dumptype == 'log':
      outfname = os.path.join(outpath, '{}.log'.format(basefname))

   return outfname
      
# ...............................................
def dumpDatabase(outpath, basefname, dbuser, dumpformat, dbschema, dbname):
   outfname = getFilename(outpath, basefname, 'db_schema')
   # Backup entire database, Lifemapper schema only
   dumpargs = ['pg_dump', 
               '--username={}'.format(dbuser), 
               '--file={}'.format(outfname),
               '--format={}'.format(dumpformat),
               '--schema={}'.format(dbschema),
               '--oids',
               '--verbose',
               dbname]
   dProc = subprocess.Popen(dumpargs, shell=True)
   restoreArgs = ['pg_restore', '--verbose',
                        '--username={}'.format(dbuser), 
                        '--dbname={}'.format(dbname), 
                        '--format={}'.format(dumpformat), 
                        outfname]
   restoreCmd = ' '.join(restoreArgs)
   readmeLines = ['', '',
            '{}:'.format(outfname),
            '   contains the data records for the {} schema of the Lifemapper SDM database'.format(outfname, dbschema),
            '   To add these records into the \'{}\' database, execute: '.format(dbname),
            '      {}'.format(restoreCmd)]
   return outfname, readmeLines

# ...............................................
def copyDatabaseUser(outpath, basefname, lmusers, dbschema, dbname):
   '''
   COPY table_name [ ( column [, ...] ) ]
    FROM { 'filename' | STDIN }
    [ [ WITH ] ( option [, ...] ) ]

COPY { table_name [ ( column [, ...] ) ] | ( query ) }
    TO { 'filename' | STDOUT }
    [ [ WITH ] ( option [, ...] ) ]

where option can be one of:

    FORMAT format_name
    OIDS [ boolean ]
    DELIMITER 'delimiter_character'
    NULL 'null_string'
    HEADER [ boolean ]
    QUOTE 'quote_character'
    ESCAPE 'escape_character'
    FORCE_QUOTE { ( column [, ...] ) | * }
    FORCE_NOT_NULL ( column [, ...] )
   '''
   lmuserStr = ' \''.join(lmusers)
   tables = {'lmuser': 'SELECT * from {}.lmuser where userid in {}'.format(dbschema, lmuserStr),
             'lmjob': 'SELECT j.* from {}.lmjob j, '.format(dbschema)+
                      '{}.lm_occJob oj, {}.lm_mdlJob mj, {}.lm_prjJob pj '.format(dbschema, dbschema, dbschema)+
                      'WHERE (j.lmjobid = oj.lmjobid  '+
                      '       AND oj.occuserId in {})'.format(lmuserStr)+
                      '   OR (j.lmjobid = mj.lmjobid  '+
                      '       AND mj.mdluserId in {})'.format(lmuserStr)+
                      '   OR (j.lmjobid = pj.lmjobid  '+
                      '       AND pj.mdluserId in {})'.format(lmuserStr),
             }

# ...............................................
def dumpSchema(outpath, basefname, dbuser, dumpformat, dbschema, dbname):
   outfname = getFilename(outpath, basefname, 'db_schema')
   dumpschemargs = ['pg_dump', 
                    '--username={}'.format(dbuser), 
                    '--format={}'.format(dumpformat),
                    '--schema-only',
                    '--schema={}'.format(dbschema),
                    '--verbose',
                    '--file={}'.format(outfname),
                    dbname]
   dProc = subprocess.Popen(dumpschemargs, shell=True)
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
   return outfname, restoreCmd

# ...............................................
def dumpData(outpath, basefname, dbuser, dumpformat, dbschema, dbname):
   outfname = getFilename(outpath, basefname, 'db_data')
   dumpdataargs = ['pg_dump', 
                    '--username={}'.format(dbuser), 
                    '--data-only',
                    '--format={}'.format(dumpformat),
                    '--verbose',
                    '--file={}'.format(outfname),
                    MAL_STORE]
   dProc = subprocess.Popen(dumpdataargs, shell=True)
   restoreDataArgs = ['pg_restore', '--verbose',
                        '--username={}'.format(dbuser), 
                        '--dbname={}'.format(dbname), 
                        '--format={}'.format(dumpformat), 
                        outfname]
   restoreCmd = ' '.join(restoreDataArgs)
   readmeLines = ['', '',
            '{}:'.format(outfname),
            '    contains the metadata contained in the Lifemapper SDM database'
            '   To append data to the \'{}\' database, execute: '.format(dbname),
            '   {}'.format(restoreCmd)]
   return outfname, readmeLines

# ...............................................
def getUsersToBackup(defaultUser, anonUser):
   earl = EarlJr()
   datapath = earl.createArchiveDataPath()
   if users == 'archive':
      backupusers = [defaultUser]
   else:
      backupusers = []
      for entry in os.listdir(datapath):
         if (not entry.startswith('.') and 
             not entry == anonUser and
             os.path.isdir(os.path.join(datapath, entry))):
            backupusers.append(entry)
      if users == 'users':
         backupusers.remove(defaultUser)
   return datapath, backupusers

# ...............................................
def dumpFileData(outpath, basefname, datapath, backupusers):
   outfname = getFilename(outpath, basefname, 'file_data')
   # Backup, compress requested DATA_PATH/MODEL_PATH/<user> directories 
   with tarfile.open(outfname, mode='w:gz') as archive:
      for entry in os.listdir(datapath):
         if entry in backupusers:
            archive.add(os.path.join(datapath, entry), recursive=True)
            
   restoreCmd = 'tar -xvzf {}'.format(outfname)
   readmeLines = ['', '', '{}:'.format(outfname),
      '   contains the file data pointed to by this database',
      '   In the target Lifemapper installation, file data should be moved into the ',
      '   DATA_PATH/MODEL_PATH directory; these data came from the',
      '   {} directory in the origin Lifemapper installation'.format(datapath),
      '',
      '   If the existing data archive contains the same usernames, data for those',
      '   usernames will be overwritten.  If this is the case, uncompress to a ',
      '   different location, and merge. Usernames contained in this data archive file are:',
       str(backupusers),
      '   To uncompress data, execute: ',
      '      {}'.format(restoreCmd)]
   return outfname, readmeLines

# ...............................................
def writeReadme(outpath, basefname, readmeLineLists):
   readmeFname = getFilename(outpath, basefname, 'readme')
   rmf = open(readmeFname, 'r')
   rmf.write('*********************************************************************')
   rmf.write('This README was generated to accompany data dumped into files: ')
   for helpLines in readmeLineLists:
      rmf.writelines(helpLines)
   rmf.close()
   return readmeFname

# ...............................................
def usage():
   output = """
   Usage:
      backuparchive [archive | users | all ] <outpath>
   """
   print output
   exit(-1)
   
# ..............................................................................
# MAIN
# ..............................................................................
if __name__ == '__main__':   
   if len(sys.argv) == 3:
      users = sys.argv[1].lower()
      outpath = sys.argv[2]
      if not os.path.exists(outpath):
         print '{} does not exist'.format(outpath)
         usage()
      if not users in ('archive', 'users', 'all'):
         usage()
   else:
      usage()
      
   # Naming variables
   scriptname = os.path.splitext(os.path.basename(__file__))[0]
   datestr = getTimestamp('+%F').replace('-', '_')
   tmp = ARCHIVE_USER if users == 'archive' else users
   basefname = '{}.{}.{}'.format(scriptname, tmp, datestr)
   
   # PostgreSQL dump/restore options
   dbuser = 'admin'
   dumpformat = 'custom'
   
   # Identify users for file backup
   datapath, backupusers = getUsersToBackup(ARCHIVE_USER, DEFAULT_POST_USER)
              
   # Dump Schema and Database
   dbschemaFname, dbschemaHelp = dumpSchema(outpath, basefname, dbuser, 
                                                dumpformat, LM_SCHEMA, MAL_STORE)
   dbdataFname, dbdataHelp = dumpData(outpath, basefname, dbuser, 
                                          dumpformat, LM_SCHEMA, MAL_STORE)
   
#    # Dump entire database 
#    dumpformat = 'plain'
#    dbFname, restoreCmd = dumpDatabase(outpath, basefname, dbuser, 
#                                       dumpformat, LM_SCHEMA, MAL_STORE)
   
   # Backup, compress requested DATA_PATH/MODEL_PATH/<user> directories 
   tarballFname, dataHelp = dumpFileData(outpath, basefname, datapath, backupusers)

   # Explain outputs
   
   readmeFname = writeReadme(outpath, basefname, 
                             [dbschemaHelp, dbdataHelp, dataHelp])
   
   
   print 'Instructions for using output from this script are in {}'.format(readmeFname)