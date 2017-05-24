# coding=utf-8
"""
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
from types import (IntType, LongType, FloatType, NoneType, BooleanType)

from LmBackend.common.lmobj import LMError
from LmCommon.common.unicode import fromUnicode, toUnicode
from LmServer.base.lmobj import LMAbstractObject

# ............................................................................
class _DbConn(LMAbstractObject):
   """
   @summary: Base class for accessing LM databases using stored procedures. 
   """
# ............................................................................
   def open(self): 
      """
      @summary Opens a connection to the database
      """
      self.abstract()      

# ............................................................................
   def close(self):
      """
      @summary: Close database connection.
      """
      self.abstract()

# ............................................................................
   def _isOpen(self):
      return (self.pconn is not None)

   isOpen = property(_isOpen)
   
# ............................................................................
   def executeSelectManyFunction(self, *args): 
      """
      @summary: Return a collection of records from a stored procedure call.
      """      
      self.abstract()

# ............................................................................
   def executeSelectOneFunction(self, *args): 
      """
      @summary: Return a single record from a stored procedure call.
      """      
      self.abstract()

# ............................................................................
   def executeModifyFunction(self, *args):
      """
      @summary: Modify one or more records with a stored procedure call. Return 
                true/false for success.
      """      
      self.abstract()
      
# ............................................................................
   def executeInsertFunction(self, *args):
      """
      @summary: Insert a record and return the primary key.  If the record 
                exists, return the primary key for the existing record.
      """      
      self.abstract()
      
# ............................................................................
   def _getColPositionsByName(self, cursor):
      """
      @summary This function takes a DB API 2.0 cursor object that has been 
               executed and returns a dictionary of the field names and column 
               numbers.  Field names are the key, column numbers are the value.
               This lets you do a simple cursor_row[field_dict[fieldname]] to 
               get the value of the column.
      @return dictionary of the field names and column numbers
      """
      results = {}
      column = 0
      if cursor.description:
         for d in cursor.description:
#            colname = d[0].lstrip('@')
            results[d[0]] = column
            column = column + 1
      return results
   
# ............................................................................
   def _getColName(self, colArgName):
      """
      @summary Gets the column name from a string by stripping all characters
               to the left of the '@' in the colArgName parameter.  Only 
               MySQL output parameters will have this, but others will be 
               unaffected.
      @param colArgName: The string argument to pull the column name from
      @return String column name
      """
      return colArgName.lstrip('@')

# ............................................................................
   def _formatArgs(self, fnArgs):
      """
      @summary Formats a list of arguments for passing within a function call
      @param fnArgs: Arguments to be formatted
      @return Formatted arguments
      """
      formattedArgs = []
      for val in fnArgs:
         formattedArgs.append(self._formatVal(val))
      return ','.join(formattedArgs) 
   
# ............................................................................
   def _formatVal(self, val):
      """
      @summary Reformat the values into a string acceptable for a SQL command.  
               This includes single (not double) quotes for strings, escape 
               internal quotes, no quotes for numbers.
               Boolean handling: SQL standard; format as TRUE or FALSE
      @param val: Value to be formatted
      @todo Finish documenting
      @todo verify unicode is working correctly
      @note: double single quotes are used for internal quotes in strings in 
             for PostgreSQL.  Not sure if this works for MySQL or not, but 
             should not encounter it in the current (or future) Lifemapper 
             configuration.
      """
      if isinstance(val, basestring):
         val = fromUnicode(toUnicode(val))
         val = val.replace('\'', "''")
         val = val.replace('\\', '\\\\')
         dbVal = "E'%s'" % (val)
      elif isinstance(val, BooleanType):
         dbVal = '%s' % (str(val).upper())
      elif isinstance(val, IntType):
         dbVal = '%d' % (val)
      elif isinstance(val, LongType):
         dbVal = '%d' % (val)
      elif isinstance(val, FloatType):
         dbVal = '%s' % (str(val))
      elif isinstance(val, NoneType):
         dbVal = 'NULL'
      else:
         raise LMError(currargs='DbConn._formatVal: unsupported type {} for {}'
                       .format(type(val), val), lineno=self.getLineno())
      return dbVal

