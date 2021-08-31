"""Library to interact with the Lifemapper PostgreSQL/PostGIS databases

Todo:
    Consider using namedtuple objects for rows.  To me, it looks cleaner if
        nothing else
"""
import psycopg2

from LmBackend.common.lmobj import LMError
from LmServer.base.atom import Atom
from LmServer.base.lmobj import LMAbstractObject
from LmServer.common.data_locator import EarlJr
from LmServer.common.lmconstants import LM_SCHEMA


# ............................................................................
class DbPostgresql(LMAbstractObject):
    """Class for interacting with PostgreSQL."""
    RETRY_COUNT = 4

    # ................................
    def __init__(self, logger, db=None, user=None, password=None, host=None,
                 port=None, schema=LM_SCHEMA):
        """Constructor for the DbPostgresql class

        Args:
            db: database name
            user: database user name
            password: password for this user and database
            host: full dns name of the database hosting server
        """
        self.map_conn_str = (
            'user={} password={} dbname={} host={} port={}'.format(
                user, password, db, host, port))
        self.log = logger
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.db = db
        self.schema = schema
        self.last_commands = []
        self.pconn = None
        self.cursor = None

    # ................................
    def _is_open(self):
        if self.pconn is None or self.pconn.closed:
            return False
        return True

    is_open = property(_is_open)

    # ................................
    @staticmethod
    def _get_col_positions_by_name(cursor):
        """Get a dictionary of field names and column numbers

        This function takes a DB API 2.0 cursor object that has been executed
        and returns a dictionary of the field names and column numbers.  Field
        names are the key, column numbers are the value. This lets you do a
        simple cursor_row[field_dict[fieldname]] to get the value of the
        column.

        Returns:
            dict - Of {field name: column number}
        """
        results = {}
        column = 0
        if cursor.description:
            for desc in cursor.description:
                results[desc[0]] = column
                column = column + 1
        return results

    # ................................
    @staticmethod
    def _get_col_name(col_arg_name):
        """Get the column name from a string

        Args:
            col_arg_name: The string argument to pull the column name from

        Returns:
            str - Column name
        """
        return col_arg_name.lstrip('@')

    # ................................
    def _format_args(self, fn_args):
        """Formats a list of arguments for passing within a function call

        Args:
            fn_args: Arguments to be formatted
        """
        return ','.join([self._format_val(val) for val in fn_args])

    # ................................
    def _format_val(self, val):
        """Reformat the values into a string acceptable for a SQL command.

        Note:
            * Single (not double) quotes for strings
            * Escape internal quotes
            * No quotes for numbers
            * Boolean handling - SQL standard - TRUE or FALSE

        Args:
            val: Value to be formatted
        """
        if isinstance(val, str):
            val = val.replace('\'', "''").replace('\\', '\\\\')
            db_val = "E'{}'".format(val)
        elif isinstance(val, bool):
            db_val = str(val).upper()
        elif isinstance(val, (int, float)):
            db_val = str(val)
        elif val is None:
            db_val = 'NULL'
        else:
            raise LMError(
                'DbConn._formatVal: unsupported type {} for {}'.format(
                    type(val), val), line_num=self.get_line_num())
        return db_val

    # ................................
    def open(self):
        """Opens a connection to the database

        Raises:
            LMError: if opening fails
        """
        # if pconn is open, do nothing
        if self.pconn is not None and self.pconn.closed:
            self.pconn = None

        if self.pconn is None:
            self.pconn = psycopg2.connect(
                user=self.user, password=self.password, host=self.host,
                port=self.port, database=self.db)

        if self.pconn is None:
            raise LMError('Unable to open connection to {}'.format(self.db))

    # ................................
    def close(self):
        """Close database connection"""
        if self.pconn is not None:
            self.pconn.close()
        self.pconn = None

    # ................................
    def reopen(self):
        """Close database connection and reopen"""
        self.close()
        self.open()

    # ................................
    def execute_query_one_function(self, query):
        """Execute the provided query and return a single row

        Args:
            query string containing a stored function name and parameters

        Returns:
            tuple of values representing the matching row, and a dictionary of
                indexes for the column names

        Raises:
            LMError: on error returned from the database.
        """
        rows = None
        cmd = 'select * from {};'.format(query)
        try:
            rows, idxs = self._send_command(cmd)
        except LMError:
            # Sometimes needs a reset, try up to 5 times
            tries = 0
            success = False
            self.log.warning('Db command failed! Try more times ...')
            self.reopen()
            while not success and tries < self.RETRY_COUNT:
                tries += 1
                try:
                    rows, idxs = self._send_command(cmd)
                    success = True
                except LMError:
                    self.log.warning(
                        '   #{} Trying to re-open, isOpen {}, ...'.format(
                            tries, self.is_open))
                    self.reopen()
        if rows:
            for val in rows[0]:
                if val is not None:
                    return rows[0], idxs
        return None, None

    # ................................
    def execute_query_function(self, cols, from_clause, where_etc_clause=None):
        """Execute query and return rows and index list

        Args:
            cols: string containing comma delimited list of columns
            from_clause: string containing comma delimited list of tables or
                views
            where_etc_clause: string containing all modifiers following 'where'
                in a query

        Returns:
            List of rows (row = tuple of values for a record) and dictionary of
                field names and column indexes.

        Raises:
            LMError: on error returned from the database.
        """
        cmd = 'select {} from {}'.format(cols, from_clause)
        if where_etc_clause is not None and where_etc_clause != '':
            cmd += ' where {};'.format(where_etc_clause)
        else:
            cmd += ';'
        try:
            rows, idxs = self._send_command(cmd)
        except LMError as err:
            # Sometimes needs a reset, try up to 5 times
            tries = 0
            success = False
            self.log.warning('Db command failed! Try more times ...')
            self.reopen()
            while not success and tries < self.RETRY_COUNT:
                tries += 1
                try:
                    rows, idxs = self._send_command(cmd)
                    success = True
                except LMError:
                    self.log.warning(
                        '   #{} Trying to re-open, isOpen {}, ...'.format(
                            tries, self.is_open))
                    self.reopen()
            if not success:
                raise LMError(
                    'Failed to execute command {}, pconn={}, err={}'.format(
                        cmd, self.pconn, err))
        return rows, idxs

    # ................................
    def execute_select_one_function(self, fn_name, *fn_args):
        """Execute a stored function and return a single row and indexes.

        Args:
            fn_name: The name of the store function
            fn_args: 0..n arguments for the stored function

        Returns:
            tuple of values representing the matching row, and a dictionary of
                indexes for the column names

        Raises:
            LMError: on error returned from the database, or more than 1 row
                found

        Todo:
            Make sure stored procedure throws exception if len(rows) != 1,
                then remove the check here
        """
        # exception is thrown by the stored function if len(rows) > 1;
        # row of nulls is returned if nothing matches the query
        rows, idxs = self._execute_function(fn_name, fn_args)
        if rows:
            for val in rows[0]:
                if val is not None:
                    return rows[0], idxs
        return None, None

    # ................................
    def execute_select_many_function(self, fn_name, *fn_args):
        """Execute a stored function and return multiple rows and indexes.

        Args:
            fn_name: The name of the store function
            fn_args: 0..n arguments for the stored function

        Returns:
            List of rows (row = tuple of values for a record) and dictionary of
                field names and column indexes.

        Raises:
            LMError: on error returned from the database
        """
        rows, idxs = self._execute_function(fn_name, fn_args)
        return rows, idxs

    # ................................
    def execute_select_and_modify_many_function(self, fn_name, *fn_args):
        """Execute a function in the database and return the response

        Args:
            fn_name: stored function name
            fn_args: 0..n arguments to the stored function

        Returns:
            List of rows (row = tuple of values for a record) and dictionary of
                field names and column indexes.

        Raises:
            LMError: on error returned from the database.
        """
        rows, idxs = self._execute_function(fn_name, fn_args)
        self.pconn.commit()
        return rows, idxs

    # ................................
    def execute_modify_function(self, fn_name, *fn_args):
        """Execute a modify function in the database

        Args:
            fn_name: stored function name
            fn_args: 0..n arguments to the stored function

        Returns:
            bool - Indication of success

        Raises:
            LMError: on error returned from the database.
        """
        success = False
        rows, _ = self._execute_function(fn_name, fn_args)
        self.pconn.commit()
        if len(rows) == 1:
            if rows[0][0] == 0:
                success = True
        return success

    # ................................
    def execute_modify_return_value(self, fn_name, *fn_args):
        """Execute a modify function in the database and return modified count.

        Args:
            fn_name: stored function name
            fn_args: 0..n arguments to the stored function

        Returns:
            An integer indicating the number of records modified.

        Raises:
            LMError: on error returned from the database.
        """
        rows, _ = self._execute_function(fn_name, fn_args)
        self.pconn.commit()

        if len(rows) == 1:
            return rows[0][0]
        return None

    # ................................
    def execute_insert_function(self, fn_name, *fn_args):
        """Execute a stored function to insert a row into the database.
        Args:
            fn_name: stored function name
            fn_args: 0..n arguments to the stored function

        Returns:
            int - Success code or primary key of new record

        Raises:
            LMError: on error returned from the database.
        """
        rows, _ = self._execute_function(fn_name, fn_args)
        self.pconn.commit()

        if rows:
            if len(rows) == 1:
                retval = rows[0][0]
                if retval is None or retval == -1:
                    raise LMError(
                        'Error inserting record: {}'.format(
                            self.last_commands))
                return retval

            raise LMError(
                'DbPostgresql.executeInsertFunction returned multiple rows',
                str(self.last_commands))
        raise LMError(
            'DbPostgresql.executeInsertFunction returned nothing',
            str(self.last_commands))

    # ................................
    def execute_insert_and_select_one_function(self, fn_name, *fn_args):
        """Execute a db function and return a single row and indexes.

        Args:
            fn_name: stored function name
            fn_args: 0..n arguments to the stored function

        Returns:
            tuple of values representing the matching row, and a dictionary of
                indexes for the column names

        Raises:
            LMError: on error returned from the database.
        """
        # exception is thrown by the stored function if len(rows) > 1;
        # row of nulls is returned if nothing matches the query
        rows, idxs = self._execute_function(fn_name, fn_args)
        self.pconn.commit()
        if rows:
            for val in rows[0]:
                if val is not None:
                    return rows[0], idxs
        return None, None

    # ................................
    def _execute_function(self, fn_name, fn_args):
        """Call a stored database function

        Args:
            fn_name: stored function name
            fn_args: 0..n arguments to the stored function

        Returns:
            List of rows (row = tuple of values for a record) and dictionary of
                field names and column indexes.

        Raises:
            LMError: on error returned from the database.
        """
        cmd = 'select * from {}.{}({});'.format(
            self.schema, fn_name, self._format_args(fn_args))
        self.last_commands = [cmd]
        try:
            rows, idxs = self._send_command(cmd)
        except:
            # Sometimes needs a reset, try up to 5 times
            tries = 0
            success = False
            self.log.warning('Db command failed! Try more times ...')
            self.reopen()
            while not success and tries < self.RETRY_COUNT:
                tries += 1
                try:
                    rows, idxs = self._send_command(cmd)
                    success = True
                except:
                    self.log.warning(
                        '   #{} Trying to re-open, is_open {}, ...'.format(
                            tries, self.is_open))
                    self.reopen()
            if not success:
                raise LMError(
                    'Failed to execute command {} after {} tries, pconn={}'.format(
                        cmd, self.pconn, tries))
        return rows, idxs

    # ................................
    def _send_command(self, *cmds):
        """Send commands to the database and get response

        Args:
            *cmds: 0..n commands to be executed.

        Returns:
            List of rows (row = tuple of values for a record) and dictionary of
                field names and column indexes

        Raises:
            LMError: on error returned from the database.
        """
        idxs = None
        self.last_commands = [cmds]

        if self.is_open:
            cursor = self.pconn.cursor()
            try:
                for cmd in cmds:
                    cursor.execute(cmd)

                idxs = self._get_col_positions_by_name(cursor)
                rows = cursor.fetchall()

            except LMError:
                raise
            except Exception as err:
                raise LMError(
                    'Exception on command {}'.format(
                        self.last_commands), err, do_trace=True)

            cursor.close()
            return rows, idxs
        raise LMError('Database connection is still None!')

    # ................................
    @staticmethod
    def _create_atom(row, idxs):
        atom = None
        if row is not None:
            atom = Atom(
                row[idxs['id']], row[idxs['name']], None, row[idxs['modtime']],
                epsg=row[idxs['epsgcode']])
        return atom

    # ................................
    def _get_atoms(self, rows, idxs, service_type, parent_metadata_url=None):
        earl = EarlJr()

        atoms = []
        url = None

        for row in rows:
            atom = self._create_atom(row, idxs)
            if service_type is not None:
                url = earl.construct_lm_metadata_url(
                    service_type, atom.get_id(),
                    parent_metadata_url=parent_metadata_url)
            atom.url = url
            atoms.append(atom)
        return atoms

    # ................................
    @staticmethod
    def _get_count(row):
        if row:
            return row[0]

        raise LMError('Failed to return count', do_trace=True)

    # ................................
    @staticmethod
    def _get_column_value(row, idxs, field_names):
        """Get the value of the first field found
        """
        for name in field_names:
            try:
                return row[idxs[name]]
            except:
                pass
        return None
