"""Library to interact with the Lifemapper PostgreSQL/PostGIS databases 
"""
import psycopg2

from LmBackend.common.lmobj import LMError

from LmServer.base.atom import Atom
from LmServer.base.lmobj import LMAbstractObject
from LmServer.common.lmconstants import LM_SCHEMA        

# ............................................................................
class DbPostgresql(LMAbstractObject):
    """
    This class is specific for interacting with a simple PostgreSQL/PostGIS 
    database.
    @todo: Reference LmServer.base.DBConn documentation (how?)
    """     
    RETRY_COUNT = 4
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
    def _isOpen(self):
        if self.pconn is None:
            return False
        elif self.pconn.closed:
            return False
        else:
            return True

    isOpen = property(_isOpen)

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
#                colname = d[0].lstrip('@')
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
        if isinstance(val, str):
            val = val.replace('\'', "''")
            val = val.replace('\\', '\\\\')
            dbVal = "E'%s'" % (val)
        elif isinstance(val, bool):
            dbVal = '%s' % (str(val).upper())
        elif isinstance(val, int):
            dbVal = '%d' % (val)
        elif isinstance(val, float):
            dbVal = '%s' % (str(val))
        elif val is None:
            dbVal = 'NULL'
        else:
            raise LMError(
                'DbConn._formatVal: unsupported type {} for {}'.format(
                    type(val), val), line_num=self.get_line_num())
        return dbVal

# ............................................................................
    def open(self):
        """
        @summary Opens a connection to the database
        @raise LMError: if opening fails
        """
        # if pconn is open, do nothing
        if self.pconn is not None and self.pconn.closed:
            self.pconn = None
            
        if self.pconn is None:
            self.pconn = psycopg2.connect(user=self.user, 
                                          password=self.password, 
                                          host=self.host, port=self.port, 
                                          database=self.db)
        if self.pconn is None:
            raise LMError('Unable to open connection to {}'.format(self.db))

# ............................................................................
    def close(self):
        """
        @summary: Close database connection
        """
        if self.pconn is not None:
            self.pconn.close()
        self.pconn = None
        
# ............................................................................
    def reopen(self):
        """
        @summary: Close database connection
        """
        self.close()
        self.open()
        
    # ............................................................................
    def executeQueryOneFunction(self, qry):
        """
        @summary Execute the provided query consisting of a stored function and 
                    its parameters returning a single row and indexes for the column names.
        @param qry: string containing a stored function name and parameters
        @exception LMError: on error returned from the database.
        """
        rows = None
        cmd = 'select * from {};'.format(qry)
        try:
            rows, idxs = self._sendCommand(cmd)
        except Exception as e: 
            # Sometimes needs a reset, try up to 5 times 
            tries = 0
            success = False
            self.log.warning('Db command failed! Try more times ...')
            self.reopen()            
            while not success and tries < self.RETRY_COUNT:
                tries += 1
                try:
                    rows, idxs = self._sendCommand(cmd)
                    success = True
                except Exception as e:
                    self.log.warning('   #{} Trying to re-open, isOpen {}, ...'
                                     .format(tries, self.isOpen))
                    self.reopen()            
        if rows:
            for val in rows[0]:
                if val is not None:
                    return rows[0], idxs
        return None, None
    
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
        except Exception as e: 
            # Sometimes needs a reset, try up to 5 times 
            tries = 0
            success = False
            self.log.warning('Db command failed! Try more times ...')
            self.reopen()            
            while not success and tries < self.RETRY_COUNT:
                tries += 1
                try:
                    rows, idxs = self._sendCommand(cmd)
                    success = True
                except Exception as e:
                    self.log.warning('   #{} Trying to re-open, isOpen {}, ...'
                                     .format(tries, self.isOpen))
                    self.reopen()
            if not success:
                raise LMError('Failed to execute command {}, pconn={}, err={}'
                              .format(cmd, self.pconn, e))
        return rows, idxs

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
        # exception is thrown by the stored function if len(rows) > 1;
        # row of nulls is returned if nothing matches the query
        rows, idxs = self._executeFunction(fnName, fnArgs)
        if rows:
            for val in rows[0]:
                if val is not None:
                    return rows[0], idxs
        return None, None
    
    # ............................................................................
    def executeSelectManyFunction(self, fnName, *fnArgs):
        """
        @summary Execute a stored function in the database, returning rows and 
                    indexes for the column names.
        @param fnName: stored function name
        @param *fnArgs: 0..n arguments to the stored function
        @raise LMError: on error returned from the database.
        """
        rows, idxs = self._executeFunction(fnName, fnArgs)
        return rows, idxs
        
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
        rows, idxs = self._executeFunction(fnName, fnArgs)
        self.pconn.commit() 
        return rows, idxs

#     # ............................................................................
#     def _handleException(self, e):
#         """
#         @summary Log serialization error or raise an exception.
#         @param e: Exception returned from code
#         @raise LMError: on any error except a Serialization error.
#         """
#         if isinstance(e, LMError):
#             raise e
#         else:
#             raise LMError('Exception on command {}'.format(self.lastCommands), 
#                               e, do_trace=True)

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
        success = False
        rows, idxs = self._executeFunction(fnName, fnArgs)
        self.pconn.commit()
        if len(rows) == 1:
            if rows[0][0] == 0:
                success = True
        return success

    # ............................................................................
    def executeModifyReturnValue(self, fnName, *fnArgs):
        """
        @summary Execute a stored function in the database which modifies records 
                    in the database.  On success, return the number of records changed.
        @param fnName: stored function name
        @param *fnArgs: 0..n arguments to the stored function
        @exception LMError: on error returned from the database.
        """
        rows, idxs = self._executeFunction(fnName, fnArgs)
        self.pconn.commit()
            
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
        rows, idxs = self._executeFunction(fnName, fnArgs)
        self.pconn.commit()

        if rows:
            if len(rows) == 1:
                retval = rows[0][0]
                if retval is None or retval == -1:
                    raise LMError('Error inserting record: {}'.format(
                        self.lastCommands))
                else:
                    return retval

            else:
                raise LMError('DbPostgresql.executeInsertFunction returned multiple rows',
                                    str(self.lastCommands))
        else:
            raise LMError('DbPostgresql.executeInsertFunction returned nothing',
                                str(self.lastCommands))

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
        # exception is thrown by the stored function if len(rows) > 1;
        # row of nulls is returned if nothing matches the query
        rows, idxs = self._executeFunction(fnName, fnArgs)
        self.pconn.commit()
        if rows:
            for val in rows[0]:
                if val is not None:
                    return rows[0], idxs
        return None, None
                
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
        except Exception as e:
            # Sometimes needs a reset, try up to 5 times 
            tries = 0
            success = False
            self.log.warning('Db command failed! Try more times ...')
            self.reopen()            
            while not success and tries < self.RETRY_COUNT:
                tries += 1
                try:
                    rows, idxs = self._sendCommand(cmd)
                    success = True
                except Exception as e:
                    self.log.warning('   #{} Trying to re-open, isOpen {}, ...'
                                     .format(tries, self.isOpen))
                    self.reopen()
            if not success:
                raise LMError('Failed to execute command {}, pconn={}, err={}'
                              .format(cmd, self.pconn, e))
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
                    
        if self.isOpen:
            cursor = self.pconn.cursor()
            try:
                for cmd in cmds:
                    cursor.execute(cmd)
                    
                idxs = self._getColPositionsByName(cursor)                
                rows = cursor.fetchall()

            except LMError as e: 
                raise
            except Exception as e:
                raise LMError('Exception on command {}'.format(
                    self.lastCommands), e, do_trace=True)

            cursor.close()
            return rows, idxs
        else:
            raise LMError('Database connection is still None!')

# ............................................................................ 
    def _createAtom(self, row, idxs):
        atom = None
        if row is not None:
            atom = Atom(row[idxs['id']], row[idxs['name']], None,
                            row[idxs['modtime']], epsg=row[idxs['epsgcode']])
        return atom
        
# ............................................................................ 
    def _getAtoms(self, rows, idxs, serviceType, parentMetadataUrl=None):
        from LmServer.common.datalocator import EarlJr
        earl = EarlJr()

        atoms = []
        url = None
        
        for r in rows: 
            atom = self._createAtom(r, idxs)
            if serviceType is not None:
                url = earl.constructLMMetadataUrl(serviceType, atom.get_id(),
                                                  parentMetadataUrl=parentMetadataUrl)
            atom.url = url
            atoms.append(atom)            
        return atoms
    
# ...............................................
    def _getCount(self, row):
        if row:
            return row[0]
        else:
            raise LMError('Failed to return count', do_trace=True)

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
