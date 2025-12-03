# %%
################################################################################
# Ensure your environment has the pandas-compatible version of the Snowflake   
# Connector for python:
#
# >pip install "snowflake-connector-python[pandas]"

import pandas as pd
from snowflake.connector.pandas_tools import write_pandas
import snowflake.connector
from pathlib import Path
from dotenv import load_dotenv
import os

load_dotenv()
# get path to snowflake private key directory
SF_KEY_DIR = Path(__file__).resolve().parents[0]
# full path to snowflake private key
SF_KEY_DEV_FILE = SF_KEY_DIR / 'sf_key_dev.p8'

RUN_CHECKS = False

# %%

class SnowflakeConnection:
    '''
    Class for defining a snowflake connection and performing basic functions
    
    Parameters
    ----------
    account : 'str'
        Snowflake account identifier.
    warehouse : 'str'
        Default snowflake warehouse.
    database : 'str'
        Name of the default snowflake database to use.
    schema : 'str' 
        Name of the default snowflake schema to use for the database.
    user_env: 'str', default None
        Environment in which snowflake login name for user is saved.
    user: 'str', default None
        Snowflake login name for user. Preferable to use user_env.
    key_path: 'str', default None
        Path to private key; should be a .p8 file
        
    Methods
    -------
    create_connection
        Create connection to snowflake.
    write_df
        Write pandas dataframe to table.
    drop_table
        Drop table in Snowflake.
    '''
    def __init__(
        self, 
        warehouse: 'str',
        database: 'str',
        schema: 'str',
        account_env: 'str' = None,
        account: 'str' = None,
        key_path: 'str' = None,
        user_env: 'str' = None,
        user: 'str' = None,
    ):  # Constructor
        self.account = account
        self.warehouse = warehouse
        self.database = database
        self.schema = schema

        if account_env is not None:
            self.account=os.getenv(account_env)
        else:
            if account is not None:
                self.account=account
            else:
                print('Please pass account_env')

        if user_env is not None:
            self.user=os.getenv(user_env)
        else:
            if user is not None:
                self.user=user
            else:
                print('Please pass user_env.')

        # key for snowflake authentication
        if key_path is not None:
            self.key_path = key_path
        else:
            print('Please pass key_path.')

        self.connection = self.create_connection()


    def create_connection(self, print_progress=True):
        '''
        Create snowflake connection.
        '''
        # Method implementation
        if print_progress:
            print('Connecting to Snowflake...')
        conn = snowflake.connector.connect(
            user=self.user,
            private_key_file=self.key_path,
            authenticator='SNOWFLAKE_JWT',
            account=self.account,
            warehouse=self.warehouse,
            database=self.database,
            schema=self.schema
        )
        if print_progress:
            print('Connection successful.')

        return conn
    
    
    def cursor(self):
        '''
        Pass snowflake connection cursor function, for convenience.
        '''
        return self.connection.cursor()
    
    
    def write_df(self, df, table_name: 'str', how: 'str'='replace'):
        '''
        Write pandas dataframe to table.
        
        Parameters
        ----------
        df : pd.DataFrame
            DataFrame to save as a table in Snowflake.
        table_name : 'str'
            Desired name of table in snowflake
        how : {'replace', 'append', 'truncate'}, default 'replace'
            How to write dataframe as a table in Snowflake.
            * replace: replace existing table in Snowflake
            * append: append data to table in snowflake
            * truncate: will keep all columns from the original table, but otherwise
              replace the table.
        '''
        
        print(f'---- {how.title()} Table {table_name} ----')
        print(f'Row count of df: {len(df):,}')
        
        table_exists_bool = self._check_table_exists(table_name)
        if table_exists_bool:
            print(f'Note: {table_name} exists')
            orig_row_count = self._get_table_row_count(table_name)
            print(f'Original row count in snowflake: {orig_row_count:,}')          
        else:
            print(f'{table_name} does not exist')
        
        if how == 'replace':
            auto_create_table=True
            overwrite=True
        elif how == 'append':
            auto_create_table=False
            overwrite=False
        elif how == 'truncate':
            auto_create_table=False
            overwrite=True
        else:
            print('Please pass appropriate "how" for write_df.')
            return
        
        print(f'Writing pandas dataframe to snowflake...')
        success, nchunks, nrows, output_copy_into = write_pandas(
            conn=self.connection, 
            df=df,
            table_name=table_name,
            quote_identifiers=False,
            auto_create_table=auto_create_table,
            overwrite=overwrite
        )
        
        print(f'Success: {success}')
        print(f'Rows added: {nrows:,}')
        new_row_count = self._get_table_row_count(table_name)
        print(f'Updated row count in snowflake: {new_row_count:,}')
        
        
    def drop_table(self, table_name:'str'):
        '''Drop table in Snowflake.'''
        if self._check_table_exists(table_name):
            query = f'DROP TABLE {self.database}.{self.schema}.{table_name};'
            with self.connection.cursor() as cur:
                cur.execute(query)
            print(f'Dropped table {table_name} in Snowflake.')
        else:
            print(f'Table {table_name} does not exist in Snowflake.')

            
    def read_table(self, table_name:'str'):
        '''Read snowflake table as a pandas dataframe.'''
        if self._check_table_exists(table_name):
            query = f'SELECT * FROM {self.database}.{self.schema}.{table_name};'
            with self.connection.cursor() as cur:
                cur.execute(query)
                read_df = cur.fetch_pandas_all()
                return read_df
        else:
            print(f'Table {table_name} does not exist in Snowflake')
            return pd.DataFrame()
        
    
    def _check_table_exists(self, table_name:'str', print_query=False):
        '''[Case sensitive] check of whether a table exists'''
        query = (
            "SELECT * FROM INFORMATION_SCHEMA.TABLES"
            " WHERE TABLE_CATALOG = CURRENT_DATABASE ()"
            " AND TABLE_SCHEMA = CURRENT_SCHEMA ()"
            f" AND TABLE_NAME = '{table_name.upper()}';"
        )
        
        if print_query:
            print(query)
        
        cur = self.connection.cursor()
        cur.execute(query)
        temp_df = cur.fetch_pandas_all()
        cur.close()
    
        # if length of returned dataframe is 0, returns false
        return bool(len(temp_df))
    
    def _check_date_last_altered(self, table_name:'str'):
        query = (
            "SELECT TO_VARCHAR(LAST_ALTERED, 'YYYY-MM-DD') AS date_last_altered"
            " FROM INFORMATION_SCHEMA.TABLES"
            " WHERE 1=1"
            " AND TABLE_CATALOG = CURRENT_DATABASE ()"
            " AND TABLE_SCHEMA = CURRENT_SCHEMA ()"
            f" AND TABLE_NAME = '{table_name.upper()}'"
        )
        with self.connection.cursor() as cur:
            cur.execute(query)
            temp_df = cur.fetch_pandas_all()
        
        return temp_df['DATE_LAST_ALTERED'].item()
    
    def _get_table_row_count(self, table_name, print_query=False):
        '''
        Get number of rows for a particular table, if it exists; otherwise returns 0.
        Note: could use information schema query and query ROW_COUNT
        '''
        if self._check_table_exists(table_name):
            with self.connection.cursor() as cur:
                query = f'SELECT COUNT(*) AS CNT FROM {self.database}.{self.schema}.{table_name}'
                cur.execute(query)
                count_df = cur.fetch_pandas_all()
                row_count = count_df["CNT"].loc[0]
        else:
            row_count = 0
        return row_count


# %% Check conncection

if __name__ == '__main__':
    sf_conn = SnowflakeConnection(
        user_env='SF_USER_DEV_ENV',
        key_path=SF_KEY_DEV_FILE,
        account_env='SF_ACCOUNT_DEV_ENV',
        warehouse=os.getenv('SF_WH_DEV_ENV'),
        database=os.getenv('SF_DATABASE_DEV_ENV'),
        schema=os.getenv('SF_SCHEMA_DEV_ENV')
    )

# %%

if __name__ == '__main__':
    data_dict = {'A': [1, 2], 'B': [5, 6]}
    test_df_a = pd.DataFrame(data=data_dict)
    data_dict = {'A': [3], 'B': [7]}
    test_df_b = pd.DataFrame(data=data_dict)
    data_dict = {'A': [4], 'B': [8], 'C': [9]}
    test_df_c = pd.DataFrame(data=data_dict)
    data_dict = {'A': [10, 11], 'B': [12, 13], 'C': [14, 15]}
    test_df_d = pd.DataFrame(data=data_dict)
    
    print('Test Data Frame A: ')
    print(test_df_a)
    
    print('Test Data Frame B: ')
    print(test_df_b)
    
    print('Test Data Frame C: ')
    print(test_df_c)

    print('Test Data Frame D: ')
    print(test_df_d)
    
    # test the query for checking a table exists; drop if it does
    test_table = 'temp_sf_table'
    sf_conn.drop_table(test_table)
    assert ~sf_conn._check_table_exists(test_table)
    
    print('\n--------------------------------------------------------------------')
    print('Testing different methods for writing dataframes to snowflake tables')
    
    print('\n-------------------')
    print('Replace A; Append B')
    sf_conn.write_df(test_df_a, test_table, how='replace')
    sf_conn.write_df(test_df_b, test_table, how='append')
    print(sf_conn.read_table(test_table))
    
    print('\n------------------')
    print('Replace C; Append B')
    sf_conn.write_df(test_df_c, test_table, how='replace')
    sf_conn.write_df(test_df_b, test_table, how='append')
    print(sf_conn.read_table(test_table))
    
    print('\n------------------')
    print('Replace C; Truncate A')
    sf_conn.write_df(test_df_c, test_table, how='replace')
    sf_conn.write_df(test_df_a, test_table, how='truncate')
    sf_conn.read_table(test_table)
    print(sf_conn.read_table(test_table))
    
    print('\n------------------')
    print('Replace D; Truncate B')
    sf_conn.write_df(test_df_d, test_table, how='replace')
    sf_conn.write_df(test_df_b, test_table, how='truncate')
    sf_conn.read_table(test_table)
    print(sf_conn.read_table(test_table))
          
    sf_conn.drop_table(test_table)

# %%
