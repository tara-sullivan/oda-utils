# ODA Utilities

Helpful programs for ODA projects.
I save environment variables in a `.env` file in my root directory, then load
variables using [python-dotenv](https://pypi.org/project/python-dotenv/).

## Socrata.py

Program for querying [Open Data](https://opendata.cityofnewyork.us/) using the 
Socrata API. 
The `socrata_api_query` function will test if you have a [Socrata Open Data
API token](https://dev.socrata.com/docs/app-tokens) saved in the environment 
variable `SOCRATA_APP`.
See [API docs](https://dev.socrata.com/docs/queries/) for details on how to
build queries.

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