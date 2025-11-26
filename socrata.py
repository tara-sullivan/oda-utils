# %%
import pandas as pd
import os
from sodapy import Socrata
from dotenv import load_dotenv
import time
import math

load_dotenv()

def socrata_api_query(
    dataset_id: 'str',
    token: 'str' = '',
    timeout: 'int' = 30,
    **kwargs
):
    """
    Create a dataframe using the Socrata Data API.
    
    Parameters
    ----------
    dataset_id: str
        Socrata dataset identifier. Datasets have a unique identifier - eight 
        alphanumeric characters split into two four-character phrases by a dash.
    token: None
        Socrata Open Data API application token. Code will test if you have a 
        token saved in the environment variable 'SOCRATA_APP'
    timeout: int = 30
        API timeout, in seconds.
    **kwargs
        Can be used to pass additional socrata app parameters.

    Socrara app parameters that can be passed: 
        select : the set of columns to be returned, defaults to *
        where : filters the rows to be returned, defaults to limit
        order : specifies the order of results
        group : column to group results on
        limit : max number of results to return, defaults to 1000
        offset : offset, used for paging. Defaults to 0
        q : performs a full text search for a value
        query : full SoQL query string, all as one parameter
        exclude_system_fields : defaults to true. If set to false, the
            response will include system fields (:id, :created_at, and
            :updated_at)
    
    Returns
    -------
    DataFrame
        A DataFrame created using the Socrata OpenData API

    Examples
    --------
    Get the geometry of flatbush community distrinct (314):
    
    socrata_api_query(
        dataset_id='jp9i-3b7y', 
        timeout=10, 
        where='boro_cd = 314', 
        select='boro_cd, the_geom',
    )

    Get the number of 311 complaints by borough on July 4, 2024:
    
    socrata_api_query(
        dataset_id='erm2-nwe9',
        timeout=360,
        select='borough, count(*) as sr_count',
        where="(date_trunc_ymd(created_date) = '2024-07-04')",
        group="borough",
    )
    
    """
    # Check if token exists. 
    if token == '':
        if os.getenv('SOCRATA_APP') is None:
            print('No token passed')
        app_token = os.getenv('SOCRATA_APP')
    else: 
        app_token=token

    # pass parameters to socrata get method.     
    params = {
        "select": kwargs.pop("select", None),
        "where": kwargs.pop("where", None),
        "order": kwargs.pop("order", None),
        "group": kwargs.pop("group", None),
        "limit": kwargs.pop("limit", None),
        "offset": kwargs.pop("offset", None),
        "q": kwargs.pop("q", None),
        "query": kwargs.pop("query", None),
        "exclude_system_fields": kwargs.pop("exclude_system_fields", None),
    }

    start_time = time.time()
    print('Running query...')
    
    client = Socrata("data.cityofnewyork.us", app_token)
    client.timeout = timeout
    
    results = client.get(dataset_id, **params)
    opendata_df = pd.DataFrame.from_records(results)

    end_time = time.time()
    len_time = end_time - start_time

    min_time = math.floor(len_time / 60)
    print(
        f'Duration: {min_time} min'
        f' {len_time - min_time * 60:.4} sec'
    )
    
    return opendata_df

if __name__ == '__main__':
    print('Geometry of flatbush community distrinct (314)')
    start_time = time.time()
    od_df = socrata_api_query(
        dataset_id='6ak9-vek3', 
        timeout=10, 
        where='boro_cd = 314', 
        select='boro_cd, the_geom',
        )
    end_time = time.time()
    print(od_df)
    len_sec = end_time - start_time
    min_time = math.floor(len_sec / 60)
    print(
        f'Duration: {min_time} min'
        f' {len_sec - min_time * 60:.4} sec'
    )

# %%
if __name__ == '__main__':
    
    print('\nNumber of SR on July 4th, 2024, by borough')
    start_time = time.time()
    od_df = socrata_api_query(
        dataset_id='erm2-nwe9',
        timeout=480,
        select='borough, count(*) as sr_count',
        where="(date_trunc_ymd(created_date) = '2024-07-04')",
        group="borough",
    )
    end_time = time.time()
    print(od_df)
    len_sec = end_time - start_time
    min_time = math.floor(len_sec / 60)
    print(
        f'Duration: {min_time} min'
        f' {len_sec - min_time * 60:.4} sec'
    )

    
# %%
