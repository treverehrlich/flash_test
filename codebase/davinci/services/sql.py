import numpy as np
import pandas as pd
import sqlalchemy as sa
import numbers

from datetime import datetime

from davinci.services.auth import open_sql_connection, get_secret
from davinci.utils.logging import log, logger
from davinci.services.auth import get_secret
from davinci.utils.global_config import ENV


@log()
def get_table(table, db='FINAL_SQL_DATABASE', **kwargs):
    """
    Query for an entire table. This can be useful for initial EDA
    or small tables that won't impact code performance.
    
    :param table: The table to get.
    :type table: str

    :param db: The database to get from.
    :type db: str

    :param kwargs: kwargs passed to pd.read_sql
    :type kwargs: dict
    """
    stmt = f"SELECT * FROM {table};"
    with open_sql_connection(db=db) as conn: 
        df = pd.read_sql(stmt, con=conn, **kwargs)
    return df

@log()
def get_sql(sql_stmt, db='FINAL_SQL_DATABASE', **kwargs):
    """
    Generic query handler. This is the one you will most
    likely use to bring in data to a script.

    :param table: The table to get.
    :type table: str

    :param db: The database to get from.
    :type db: str
        
    :param kwargs: kwargs passed to pd.read_sql
    :type kwargs: dict
    """
    with open_sql_connection(db=db) as conn: 
        df = pd.read_sql(sql_stmt, con=conn, **kwargs)
    return df

@log()
def write_df_to_table(df, table, db='FINAL_SQL_DATABASE'):
    """
    Write an entire dataframe to a table. The column
    names must be in one-to-one correspondence between
    df and table.

    :param df: dataframe that will be used.
    :type df: pd.DataFrame

    :param table: The table to write to.
    :type table: str

    :param db: The database to write into.
    :type db: str
    """

    def try_int(x):
        if hasattr(x, 'dtype') and isinstance(x.dtype, numbers.Integral):
            return int(x)
        else:
            return x

    def cast_null(x):
        try:
            if np.isnan(x):
                return None
            return x
        except TypeError:
            return x

    with open_sql_connection(db=db) as conn:
        cursor = conn.cursor()
        # define the insert query
        column_list = df.columns
        placeholder = ", ".join(["?"] * len(column_list))
        stmt = "INSERT INTO {table} ({columns}) VALUES ({values});".format(
            table=table,
            columns=",".join(column_list),
            values=placeholder)
        # loop through each row in the matrix
        for _, row in df.iterrows():
            values = row.to_list()
            values = [cast_null(try_int(v)) for v in values]
            cursor.execute(stmt, values)
            cursor.commit()
        cursor.close()

@log()
def update_sql_rows(data: dict, where_clause: str, table: str, db='FINAL_SQL_DATABASE'):

    """
    Write an update to row(s) in a table. 
    
    :param data: a dictionary where the keys are the sql SET column names in the table, and the values are the desired update values
    :param type: dict
    
    :param where_clause: defines which rows in the table should be updated
    :type where_clause: str

    :param table: The table to write to.
    :type table: str

    :param db: The database to write into.
    :type db: str

    :return int: number of matched rows (not guaranteed updated)
    """

    # Parametrized query portion
    set_clause = ",".join([f"{key}=?" for key in data]) 
    
    # SQL Statement
    stmt = f"UPDATE {table} SET {set_clause} WHERE {where_clause}"

    with open_sql_connection(db=db) as conn:
        cursor = conn.cursor()
        result = cursor.execute(stmt, list(data.values())).rowcount
        cursor.commit()
        cursor.close()

    return result

@log()
def delete_sql_rows(where_clause: str, table: str, db='FINAL_SQL_DATABASE'):

    """
    Delete row(s) from a table with mandatory WHERE clause for safety. 
       
    :param where_clause: defines which rows in the table should be deleted
    :type where_clause: str

    :param table: The table to delete from
    :type table: str

    :param db: The database to delete from
    :type db: str

    :return int: number of matched rows (not guaranteed deleted)
    """    


    # SQL Statement
    stmt = f"DELETE FROM {table} WHERE {where_clause}"

    with open_sql_connection(db=db) as conn:
        cursor = conn.cursor()
        result = cursor.execute(stmt).rowcount
        cursor.commit()
        cursor.close()

    return result

@log()
def add_audit_info(df):
    """
    Adds environment, created_date, and
    created_by standard columns to dataframe.
    Use before writing to SQL DB.

    :param df: The df to add data to.
    :type df: pd.DataFrame

    :return: Dataframe with new data.
    :rtype: pd.DataFrame
    """
    df["environment"] = ENV
    df["created_date"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    df["created_by"] = "Python"
    return df

@log()
def get_ordered_cols(table, db='FINAL_SQL_DATABASE'):
    """
    Get all of the columns from SQL table in order.
    This is useful for prepping a dataframe to be
    written to the DB, as well as ensuring you
    have all the names correct from the schema.

    :param table: The table to get.
    :type table: str

    :param db: The database to get from.
    :type db: str
        
    :return: list of ordered column names
    :rtype: List[Str]
    """
    empty_df = get_sql(f"""
        SELECT * FROM {table} WHERE 0 = 1
    """, db=db)
    return empty_df.columns.tolist()

@log()
def fast_insert_from_dataframe(df, name, db='SQL_DATABASE', schema=None, truncate=False):
    """
    Writes SQL table by truncating then fast_executemany appends.
    NOTE that the SQL table should, ideally, already have been defined.
    Otherwise, SQLAlchemy will give you poorly chosen default data types.

    :param df: Dataframe to write
    :type df: pd.DataFrame
    
    :param name: The table to either create or insert into
    :type name: str
    
    :param db: The database to get from.
    :type db: str
    
    :param schema: the schema name
    :type schema: str

    :param truncate: Whether to truncate the table before write.
    :type truncate: Boolean

    :return: None
    """
    db = get_secret(db, doppler=True)
    connection_uri = sa.engine.URL.create(
        "mssql+pyodbc",
        username=get_secret('SQL_USER'),
        password=get_secret('SQL_PASSWORD'),
        host=get_secret('SQL_SERVER'),
        database=db,
        query={"driver": "ODBC Driver 17 for SQL Server"},
    )

    dbEngine = sa.create_engine(connection_uri, connect_args={'connect_timeout': 5}, echo=False,
        fast_executemany=True)

    @sa.event.listens_for(dbEngine, 'before_cursor_execute')
    def receive_before_cursor_execute(conn, cursor, statement, params, context, executemany):
        if executemany:
            cursor.fast_executemany = True
            cursor.commit()
    try:
        with dbEngine.connect() as conn:
            if truncate:
                conn.execute(sa.text("TRUNCATE TABLE {}.{}.{}".format(db, schema, name)).execution_options(autocommit=True))
        with dbEngine.connect() as conn:
            df.to_sql(con=dbEngine, schema=schema,
                name=name, if_exists='append', index=False, chunksize=1000)
    except Exception as e:
        logger.info(f'Failed on SQLAlchemy FastExecute. See the DaVinci pip package and following error string: {str(e)}')
        raise e

class SQLUpsert:
    def __init__(
        self, df: pd.DataFrame, column_specs: str, table_name: str, merge_cols: list,
        update_cols: list=None, delete_after_merge: bool=False, db: str='SQL_DATABASE',
        schema: str='dbo', env: str=ENV):
        """
        A helper class to facilitate SQL init/table creation and an UPSERT management style between the two.

        :param df: Dataframe which to write.
        :param column_specs: SQL-like statement containing column definitions.
        :param table_name: Name of the table.
        :param merge_cols: list of columns used as the merge indices.
        :param update_cols: Optional list of columns that should be updated. When omitted, this will be inferred
            based on the column_specs and merge_cols.
        :param delete_after_merge: If False (default), then the UPSERT will only
            update new records and MODIFY old ones. If True, then the UPSERT will also REMOVE records not
            found in the most recent dataframe. Essentially two different ways of handling updates.
        :param db: the database secret variable.
        :param schema: the prefix of the table for schema tracking.
        :param env: the ENV variable for prod/dev.

        Example usage:

        .. code-block:: python

            column_specs = \"""
                [Site] VARCHAR(200),
                SKU VARCHAR(200),
                Model VARCHAR(200),
                ProductBucket VARCHAR(50),
                Customer VARCHAR(200),
                CustomerBucket VARCHAR(200),
                ForecastUpdateDate DATETIME,
                Date DATETIME,
                Price FLOAT,
                ForecastUnits INT,
                ForecastSales FLOAT,
                ProductFile VARCHAR(50),
            \"""

            upsert_manager = SQLUpsert(
                df,
                column_specs,
                "ChervonForecastForecastSales",
                merge_cols=['SKU', 'Model', 'Customer', 'Site', 'ForecastUpdateDate'],
                update_cols=['Price', 'ForecastUnits'],
            )

            upsert_manager.create_init()
            upsert_manager.ensure_table_exists()
            upsert_manager.merge_update()

        """

        column_specs = self._preprocess_column_specs(column_specs)

        if not update_cols:
            update_cols = self._infer_update_cols(column_specs, merge_cols)

        column_specs += """
        Env VARCHAR(10),
        [Created] DATETIME DEFAULT GETDATE(),
        [Modified] DATETIME DEFAULT GETDATE(),
        [CreatedBy] VARCHAR(200) DEFAULT 'python',
        [ModifiedBy] VARCHAR(200) DEFAULT 'python'
        """

        self.df = df
        if not 'Env' in df.columns:
            df['Env'] = env
        self.column_specs = column_specs
        self.table_name = table_name
        self.init_table = 'init_' + table_name
        self.stg_table = table_name #rename stg_ to main_
        self.merge_cols = merge_cols + ['Env']
        self.update_cols = update_cols + ['Modified', 'ModifiedBy']
        self.audit_cols = ['Created', 'Modified', 'CreatedBy', 'ModifiedBy']
        self.delete_after_merge = delete_after_merge
        self.db_key = db
        self.db = get_secret(db, doppler=True)
        self.schema = schema
        self._make_connection()

    def _preprocess_column_specs(self, column_specs):
        """Format the column specs to always have a ,\n line ending and a terminating comma."""
        res = "\n".join(list(map(str.strip, column_specs.strip().split('\n'))))
        res += ',' if res[-1] != ',' else ''
        return res

    def _infer_update_cols(self, specs, index):
        """Helper function for SQLUpsert when no update cols passed"""
        cols = list(map(lambda s: s.strip().split(' ')[0], specs.split(',\n')))
        update_cols = list(filter(lambda c: c and (c not in index), cols))
        return update_cols

    def _make_connection(self):
        """Make the connection to DB via SQLAlchemy"""
        connection_uri = sa.engine.URL.create(
            "mssql+pyodbc",
            username=get_secret('SQL_USER'),
            password=get_secret('SQL_PASSWORD'),
            host=get_secret('SQL_SERVER'),
            database=self.db,
            query={"driver": "ODBC Driver 17 for SQL Server"},
        )
        dbEngine = sa.create_engine(connection_uri, connect_args={'connect_timeout': 5}, echo=False,
            fast_executemany=True)
        @sa.event.listens_for(dbEngine, 'before_cursor_execute')
        def receive_before_cursor_execute(conn, cursor, statement, params, context, executemany):
            if executemany:
                cursor.fast_executemany = True
                cursor.commit()
        self.dbEngine = dbEngine


    def _drop_table(self, name: str):
        """
        Drop the table.

        :param name: table name
        """
        with self.dbEngine.begin() as con:
            schema_name = f'{self.db}.{self.schema}.{name}'
            con.execute(
                f"""
                IF OBJECT_ID('{schema_name}', 'U') IS NOT NULL 
                DROP TABLE {schema_name};
                """
            ) 

    def _create_table(self, name: str, force: bool=False, populate: bool=False):
        """
        Create a table.

        :param name: table name
        :param force: drop any preexisting table with that name.
        :param populate: insert the records from self.df into the new table.
        """
        # Optionally drop table
        self._drop_table(name) if force else False

        # Create table if it doesn't exist already.
        with self.dbEngine.begin() as con:
            schema_name = f'{self.db}.{self.schema}.{name}'
            con.execute(
                f"""
                if not exists (select * from sysobjects where name='{name}' and xtype='U')
                CREATE TABLE {schema_name} (
                    {self.column_specs}
                )
                """
            )

        # Write records
        if populate:
            query_name = f"{self.db}.{self.schema}.{name}"
            col_order = get_ordered_cols(query_name)
            self.val_cols = col_order
            col_order_no_audit = [c for c in get_ordered_cols(query_name) if c not in self.audit_cols]
            fast_insert_from_dataframe(self.df[col_order_no_audit], name, db = self.db_key, schema=self.schema, truncate=False)


    def _make_merge_stmt(self):
        """
        Create the UPSERT logic dynamically based on the merge_cols and update_cols passed in.
        """
        merge_on = ' AND '.join([f'(Source.{c} = Target.{c})' for c in self.merge_cols])
        insert_part = f"({', '.join(list(map(lambda x: f'[{x}]', self.val_cols)))})"
        insert_val_part = f"({', '.join(list(map(lambda x: f'Source.[{x}]', self.val_cols)))})"
        update_part = ', '.join([f'Target.[{c}] = Source.[{c}]' for c in self.update_cols])
        delete_part = "WHEN NOT MATCHED BY Source THEN DELETE" if self.delete_after_merge else ""
        sql_merge = f"""
            MERGE {self.db}.{self.schema}.{self.stg_table} AS Target
            USING {self.db}.{self.schema}.{self.init_table} AS Source
                ON {merge_on}
            /* new records ('right match') */
            WHEN NOT MATCHED BY Target  THEN
                INSERT {insert_part}
                VALUES {insert_val_part}
            /* matching records ('inner match') */
            WHEN MATCHED THEN 
                UPDATE SET
                {update_part}
            /* deprecated records ('left match') */
                {delete_part}
            ;
            """
        return sql_merge
        

    def create_init(self, force=True, populate=True):
        """Create init table."""
        self._create_table(self.init_table, force, populate)

    def ensure_table_exists(self, force=False, populate=False):
        """Make sure the main table exists."""
        self._create_table(self.stg_table, force, populate)

    def merge_update(self):
        """Call the UPSERT procedure."""
        merge_stmt = self._make_merge_stmt()
        with self.dbEngine.begin() as con:
            con.execute(merge_stmt)
