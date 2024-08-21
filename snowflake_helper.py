# %%
################################################################################
# Ensure your environment has the pandas-compatible version of the Snowflake   
# Connector for python:
#
# >pip install "snowflake-connector-python[pandas]"
#
# Link: https://docs.snowflake.com/en/developer-guide/python-connector/python-connector-pandas
# 
# Additionally, configure snowflake as a datasource in domino. To create a new 
# data source in domino:
#     * Select Data > "+ Create a Data Source" or "Connect to to external data"
#     * From Select Data Store dropdown menu, select Snowflake
#     * Fill in the appropriate information:
#         - Authenticate with your credentials
#         - Add other collaborators
# In your project, click Data > + Add Data source. You should be able to add 
# your created data source (it might take a minute to show up). Add the data 
# source to project. 

import os
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from dotenv import load_dotenv

load_dotenv()

# %%


def rename_cols_for_snowflake(df):
    """Snowflake columns need to adopt certain conventions, such as
    no spaces and no parentheses.
    """
    df = df.copy()
    df.columns = (
        df.columns.str.replace(r'\(([^)]*)\)', r'\1', regex=True)
        .str.replace(' ', '_', regex=False)
        .str.replace(r'^Group$', 'Group_Name', regex=True)
        .str.replace('%', 'percent')
    )
    return df


def csv_to_snowflake(
    csv:'str', 
    table_name:'str', 
    snowflake_connection: 'SnowflakeConnection'
):
    csv_df = pd.read_csv(csv)
    snowflake_connection.write_df(
        df=csv_df,
        table_name=table_name, 
        how='replace'
    ) 


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
    pwd_env: 'str', default None
        Environment in which snowflake password for user is saved.
    pwd: 'str', default None
        Snowflake password for user. Preferable to use pwd_env.
        
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
        account: 'str', 
        warehouse: 'str',
        database: 'str',
        schema: 'str',
        user_env: 'str' = None,
        user: 'str' = None,
        pwd_env: 'str' = None,
        pwd: 'str' = None
    ):  # Constructor
        self.account = account
        self.warehouse = warehouse
        self.database = database
        self.schema = schema
        if user_env is not None:
            self.user=os.getenv(user_env)
        else:
            if user is not None:
                self.user=user
            else:
                print('Please pass user_env.')
                
        if pwd_env is not None:
            self.pwd_env = pwd_env
        else:
            if pwd is not None:
                self.pwd = pwd
            else:
                print('Please pass pwd_env.')
                
        self.connection = self.create_connection()


    def create_connection(self):
        '''
        Create snowflake connection.
        '''
        if self.pwd_env is not None:
            password = os.getenv(self.pwd_env)
        else:
            if self.pwd is not None:
                password = self.pwd
        
        # Method implementation
        print('Connecting...')
        conn = snowflake.connector.connect(
            user=self.user,
            password=password,
            account=self.account,
            warehouse=self.warehouse,
            database=self.database,
            schema=self.schema
        )
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
    
    def _get_table_row_count(self, table_name, print_query=False):
        '''Get number of rows for a particular table, if it exists; otherwise returns 0'''
        if self._check_table_exists(table_name):
            with self.connection.cursor() as cur:
                query = f'SELECT COUNT(*) AS CNT FROM {self.database}.{self.schema}.{table_name}'
                cur.execute(query)
                count_df = cur.fetch_pandas_all()
                row_count = count_df["CNT"].loc[0]
        else:
            row_count = 0
        return row_count


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
    
    print('Setting up test connection for RatStat')
    sf_conn = SnowflakeConnection(
        user_env='SNOWFLAKE_USER',
        pwd_env='SNOWFLAKE_PWD',
        account='NYC-OTI_ODA_DEV',
        warehouse='LOADER_WH',
        database='ODADB',
        schema='DASHBOARD'
    )
    
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