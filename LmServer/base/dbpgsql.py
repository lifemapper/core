"""
    Library to interact with the Lifemapper PostgreSQL/PostGIS databases 
    (RAD, MAL)
    @author: Aimee Stewart
    @requires: psycopg2 (tested with v2.0.5.1), 
               available from http://www.initd.org/

@license: gpl2
@copyright: Copyright (C) 2014, University of Kansas Center for Research

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
import psycopg2

from LmServer.base.atom import Atom
from LmServer.base.db import _DbConn
from LmServer.base.lmobj import LMError
from LmServer.common.lmconstants import LM_SCHEMA

# ............................................................................
class DbPostgresql(_DbConn):
   """
   This class is specific for interacting with a simple PostgreSQL/PostGIS 
   database.
   @todo: Reference LmServer.base.DBConn documentation (how?)
   """    

   def __init__(self, logger, db=None, user=None, password=None, host=None, 
                port=None, schema=LM_SCHEMA):
      """
      @summary Constructor for the DbPostgresql class
      @param db: database name
      @param user: database user name
      @param password: password for this user and database
      @param host: full dns name of the database hosting server 
                   (i.e. 'lm2hydra.nhm.ku.edu')
      """
      self.mapConnStr =("user={} password={} dbname={} host={} port={}"
                        .format(user, password, db, host, port))
      self.log = logger
      self.user = user
      self.password = password
      self.host = host
      self.port = port
      self.db = db
      self.schema = schema
      self.lastCommands = []
      self.pconn = None
      self.cursor = None
      
# ............................................................................
# ............................................................................
   def open(self):
      """
      @summary Opens a connection to the database
      @raise LMError: if opening fails
      """
      if self.pconn is None:
         self.pconn = psycopg2.connect(user=self.user, 
                                       password=self.password, 
                                       host=self.host, port=self.port, 
                                       database=self.db)
      if self.pconn is None:
         raise LMError(currargs='Unable to open connection to {}'.format(self.db))

# ............................................................................
   def close(self):
      """
      @summary: Close database connection
      """
      if self.pconn is not None and not(self.pconn.closed):
         self.pconn.close()
      self.pconn = None
      
   # ............................................................................
   def executeQueryOneFunction(self, qry):
      """
      @summary Execute the provided query consisting of a stored function and 
               its parameters returning a single row and indexes for the column names.
      @param qry: string containing a stored function name and parameters
      @exception LMError: on error returned from the database.
      """
      cmd = 'select * from {};'.format(qry)
      try:
         rows, idxs = self._sendCommand(cmd)
         if rows:
            for val in rows[0]:
               if val is not None:
                  return rows[0], idxs
         return None, None
      except Exception, e:
         self._handleException(e)
   
   # ............................................................................
   def executeQueryFunction(self, cols, fromClause, whereEtcClause=None):
      """
      @summary Execute the provided query consisting of a stored function and 
               its parameters returning a single row and indexes for the column names.
      @param cols: string containing comma delimited list of columns
      @param fromClause: string containing comma delimited list of tables or views
      @param whereEtcClause: string containing all modifiers 
                             following 'where' in a query
      @exception LMError: on error returned from the database.
      """
      cmd = 'select {} from {}'.format(cols, fromClause)
      if whereEtcClause is not None and whereEtcClause != '':
         cmd += ' where {};'.format(whereEtcClause)
      else:
         cmd += ';'
      try:
         rows, idxs = self._sendCommand(cmd)
         return rows, idxs
      except Exception, e: 
         self._handleException(e)

   # ............................................................................
   def executeSelectOneFunction(self, fnName, *fnArgs):
      """
      @summary Execute a stored function in the database, returning a single row
               and indexes for the column names.  Returns None if no rows are 
               found; throws an exception if greater than 1 row is found.
      @param fnName: stored function name
      @param *fnArgs: 0..n arguments to the stored function
      @return a tuple of values representing the matching row, and a 
              dictionary of indexes for the column names.  Returns None for 
              the values and None for the indexes if no matching row is found.   
      @raise LMError: on error returned from the database, 
              or more than 1 row found
      @todo: make sure stored procedure throws exception if len(rows) != 1,
              then remove the check here                          
      """
      try:
         # exception is thrown by the stored function if len(rows) > 1;
         # row of nulls is returned if nothing matches the query
         rows, idxs = self._executeFunction(fnName, fnArgs)
         if rows:
            for val in rows[0]:
               if val is not None:
                  return rows[0], idxs
         return None, None
      except Exception, e:
         self._handleException(e)
   
   # ............................................................................
   def executeSelectManyFunction(self, fnName, *fnArgs):
      """
      @summary Execute a stored function in the database, returning rows and 
               indexes for the column names.
      @param fnName: stored function name
      @param *fnArgs: 0..n arguments to the stored function
      @raise LMError: on error returned from the database.
      """
      try:
         rows, idxs = self._executeFunction(fnName, fnArgs)
         return rows, idxs
      except Exception, e:
         self._handleException(e)
      
   # ............................................................................
   def executeSelectAndModifyManyFunction(self, fnName, *fnArgs):
      """
      @summary Execute a stored function in the database which modifies the rows
               it has selected.  Return rows and indexes for the column names.  
               This function is used when models or projections are queued, 
               and their status is changed in the database before returning.
      @param fnName: stored function name
      @param *fnArgs: 0..n arguments to the stored function
      @raise LMError: on error returned from the database.
      """
      try:
         rows, idxs = self._executeFunction(fnName, fnArgs)
         self.pconn.commit() 
         return rows, idxs
      except Exception, e:
         self._handleException(e)

   # ............................................................................
   def _handleException(self, e):
      """
      @summary Log serialization error or raise an exception.
      @param e: Exception returned from code
      @raise LMError: on any error except a Serialization error.
      """
      if isinstance(e, LMError):
         raise e
#       elif e.pgcode == 40001:
#          self.log.error('Serialization exception on command %s' % 
#                         str(self.lastCommands))
      else:
         raise LMError(currargs='Exception on command {}'.format(self.lastCommands), 
                       prevargs=e.args, doTrace=True)

   # ............................................................................
   def executeModifyFunction(self, fnName, *fnArgs):
      """
      @summary Execute a stored function in the database which modifies records 
               in the database.  Return True on success, False on failure.
      @param fnName: stored function name
      @param *fnArgs: 0..n arguments to the stored function
      @return: True/False for success
      @raise LMError: on error returned from the database.
      """
      try:
         rows, idxs = self._executeFunction(fnName, fnArgs)
         self.pconn.commit()
         if len(rows) == 1:
            if rows[0][0] == 0:
               return True
            else:
               return False
      except Exception, e:
         self._handleException(e)
         return False
#       except LMError, e: 
#          raise
#       except Exception, e:
#          raise LMError(currargs='Exception on command %s' % 
#                        str(self.lastCommands), prevargs=e.args, doTrace=True)         
#       if len(rows) == 1:
#          if rows[0][0] == 0:
#             return True
#          else:
#             return False

   # ............................................................................
   def executeModifyReturnValue(self, fnName, *fnArgs):
      """
      @summary Execute a stored function in the database which modifies records 
               in the database.  On success, return the number of records changed.
      @param fnName: stored function name
      @param *fnArgs: 0..n arguments to the stored function
      @exception LMError: on error returned from the database.
      """
      try:
         rows, idxs = self._executeFunction(fnName, fnArgs)
         self.pconn.commit()
      except Exception, e:
         self._handleException(e)
         
      if len(rows) == 1:
         return rows[0][0]
      else:
         return None
   
   # ............................................................................
   def executeInsertFunction(self, fnName, *fnArgs):
      """
      @summary Execute a stored function in the database to insert a single
               row, returning the primary key on success, False on failure.
      @param fnName: stored function name
      @param *fnArgs: 0..n arguments to the stored function
      @return: Integer, either success code or primary key of the new record.
      @exception LMError: on error returned from the database.

      @summary Execute a stored function in the database which inserts a record 
               in the database.  If the primary key of the record is an integer, 
               return it on successful find or insert, -1 on failure.  If the 
               primary key is not an integer (such as a join record), return 0 
               on success, -1 on failure.
      @param fnName: stored function name
      @param *fnArgs: 0..n arguments to the stored function
      @return 0 on success, -1 on failure.
      @exception LMError: on error returned from the database.
      """
      try:
         rows, idxs = self._executeFunction(fnName, fnArgs)
         self.pconn.commit()
      except Exception, e:
         self._handleException(e)

      if rows:
         if len(rows) == 1:
            retval = rows[0][0]
            if retval is None or retval == -1:
               raise LMError(currargs='Error inserting record: {}'
                             .format(self.lastCommands))
            else:
               return retval

         else:
            raise LMError(currargs=['DbPostgresql.executeInsertFunction returned multiple rows',
                           str(self.lastCommands)])
      else:
         raise LMError(currargs=['DbPostgresql.executeInsertFunction returned nothing',
                        str(self.lastCommands)])

   # ............................................................................
   def executeInsertAndSelectOneFunction(self, fnName, *fnArgs):
      """
      @summary Execute a stored function in the database, returning a single row
               and indexes for the column names.  Returns None if no rows are 
               found; throws an exception if greater than 1 row is found.
               This is used to insert a record and return multiple values in
               a row (i.e. multiple record ids for an object using joined tables)
      @param fnName: stored function name
      @param *fnArgs: 0..n arguments to the stored function
      @return a tuple of values representing the matching row, and a 
              dictionary of indexes for the column names.  Returns None for 
              the values and None for the indexes if no matching row is found.   
      @raise LMError: on error returned from the database, 
              or more than 1 row found
      @todo: make sure stored procedure throws exception if len(rows) != 1,
              then remove the check here                          
      """
      try:
         # exception is thrown by the stored function if len(rows) > 1;
         # row of nulls is returned if nothing matches the query
         rows, idxs = self._executeFunction(fnName, fnArgs)
         self.pconn.commit()
         if rows:
            for val in rows[0]:
               if val is not None:
                  return rows[0], idxs
         return None, None
      except Exception, e:
         self._handleException(e)

            
# ............................................................................
   def _executeFunction(self, fnName, fnArgs):
      """
      @summary Call a given stored function, returning list of rows and 
               dictionary of indexes of column names into those rows.
               This method does not commit changes.
      @param fnName: A stored function name
      @param fnArgs: A sequence of arguments to the given function
      @return: Returns list of rows and dictionary of indexes
      """
      cmd = 'select * from {}.{}({});'.format(self.schema, fnName, self._formatArgs(fnArgs))
      self.lastCommands = [cmd]
      try:
         rows, idxs = self._sendCommand(cmd)
      except LMError, e: 
         raise
      except Exception, e:
         raise LMError(currargs='Exception on command {}'
                       .format(self.lastCommands), prevargs=e.args, doTrace=True)

      return rows, idxs

# ............................................................................ 
   def _sendCommand(self, *cmds):
      """
      @summary Execute one or more commands in the database, returning rows  
               and indexes for the column names.  Rows will be fetched only
               after the last command has been executed.
      @param *cmds: 0..n commands to be executed.
      @return: a list of tuples and a dictionary of indexes for the column 
               names.  Each tuple contains values for a single row. 
      @exception LMError: on error returned from the database.
      """
      idxs = None
      self.lastCommands = [cmds]
      
      if self.pconn is None:
         self.log.warning('Database connection is None! Trying to re-open ...')
         self.open()
         
      if self.pconn is not None:
         cursor = self.pconn.cursor()
         try:
            for cmd in cmds:
               cursor.execute(cmd)
               
            idxs = self._getColPositionsByName(cursor)            
            rows = cursor.fetchall()

         except LMError, e: 
            raise
         except Exception, e:
            raise LMError(currargs='Exception on command {}'
                          .format(self.lastCommands), prevargs=e.args, doTrace=True)

         cursor.close()
         return rows, idxs
      else:
         raise LMError(currargs='Database connection is still None!')

# ............................................................................ 
   def _createAtom(self, row, idxs):
      atom = None
      if row is not None:
         atom = Atom(row[idxs['id']], row[idxs['name']], row[idxs['url']], 
                     row[idxs['modtime']], epsg=row[idxs['epsgcode']])
      return atom
      
# ............................................................................ 
   def _getAtoms(self, rows, idxs):
      atoms = []
      for r in rows: 
         atoms.append(self._createAtom(r, idxs))         
      return atoms
   
   def _getCount(self, row):
      if row:
         return row[0]
      else:
         raise LMError(currargs='Failed to return count', doTrace=True)

# ...............................................
   def _getColumnValue(self, r, idxs, fldnameList):
      val = None
      for fldname in fldnameList:
         try: 
            val = r[idxs[fldname]]
         except:
            pass
         else:
            return val
