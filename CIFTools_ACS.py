from dataclasses import dataclass
from typing import Union, List
from aiohttp import ClientSession
import requests
import asyncio
import re
from CIF_Config import ACSConfig
import pandas as pd
import functools


def batchify_variables(config):
    if config.acs_type == '':
        source = 'acs/acs5'
    else:
        source = f'acs/acs5/{config.acs_type}'
    table = config.variables
    batch_size = 49
    if len(table) > batch_size:
        num_full = len(table)//batch_size
        table_split = [table[k*batch_size:(k+1)*batch_size] for k in range(num_full)]
        table_split.append(table[num_full*batch_size:])
        return table_split
    else:
        return [table]


async def donwload_for_batch(config, table: str, key: str, session: ClientSession) -> List[str]:
    if config.acs_type == '':
        source = 'acs/acs5'
    else:
        source = f'acs/acs5/{config.acs_type}'
#     table = ','.join(batchify_variables(config)[0])
    if isinstance(config.state_fips, str) or isinstance(config.state_fips, int):
        if config.query_level == 'state':
            acs_url = f'https://api.census.gov/data/{config.year}/{source}?get={table}&for=state:{config.state_fips}&key={key}'
        elif config.query_level == 'county':
            acs_url = f'https://api.census.gov/data/{config.year}/{source}?get=NAME,{table}&for=county:*&in=state:{config.state_fips}&key={key}'
        elif config.query_level == 'county subdivision':
            acs_url = f'https://api.census.gov/data/{config.year}/{source}?get=NAME,{table}&for=county%20subdivision:*&in=state:{config.state_fips}&in=county:*&key={key}'
        elif config.query_level == 'tract':
            acs_url = f'https://api.census.gov/data/{config.year}/{source}?get=NAME,{table}&for=tract:*&in=state:{config.state_fips}&in=county:*&key={key}'
        elif config.query_level == 'block':
            acs_url = f'https://api.census.gov/data/{config.year}/{source}?get=NAME,{table}&for=block%20group:*&in=state:{config.state_fips}&in=county:*&in=tract:*&key={key}'
        elif config.query_level == 'zip':
            acs_url = f'https://api.census.gov/data/{config.year}/{source}?get=NAME,{table}&for=zip%20code%20tabulation%20area:*&in=state:{config.state_fips}&key={key}'
        else:
            print('The region level is not found in the system')
    elif isinstance(config.state_fips, list):
        config.state_fips = [str(x) for x in config.state_fips]
        states = ','.join(config.state_fips)
        if config.query_level == 'state':
            acs_url = f'https://api.census.gov/data/{config.year}/{source}?get={table}&for=state:{states}&key={key}'
        elif config.query_level == 'county':
            acs_url = f'https://api.census.gov/data/{config.year}/{source}?get=NAME,{table}&for=county:*&in=state:{states}&key={key}'
        elif config.query_level == 'county subdivision':
            acs_url = f'https://api.census.gov/data/{config.year}/{source}?get=NAME,{table}&for=county%20subdivision:*&in=state:{states}&in=county:*&key={key}'
        elif config.query_level == 'tract':
            acs_url = f'https://api.census.gov/data/{config.year}/{source}?get=NAME,{table}&for=tract:*&in=state:{states}&in=county:*&key={key}'
        elif config.query_level == 'block':
            acs_url = f'https://api.census.gov/data/{config.year}/{source}?get=NAME,{table}&for=block%20group:*&in=state:{states}&in=county:*&in=tract:*&key={key}'
        elif config.query_level == 'zip':
            acs_url = f'https://api.census.gov/data/{config.year}/{source}?get=NAME,{table}&for=zip%20code%20tabulation%20area:*&in=state:{states}&key={key}'
        else:
            print('The region level is not found in the system')

        
        
        
        
    resp = await session.request(method="GET", url=acs_url)
    resp.raise_for_status()    
    json_raw =  await resp.json()
    return json_raw



async def download_all(config, key):
    tables = batchify_variables(config)
    async with ClientSession() as session:
        tasks = [donwload_for_batch(config, f"{','.join(table)}", key, session) for table in tables]
        return await asyncio.gather(*tasks)
    
    
    
def acs_data(key, config = None, **kwargs):
    if config:
        pass
    else:
        config = ACSConfig(**kwargs)
    result = asyncio.run(download_all(config, key))
    if len(result) == 1:
        df = pd.DataFrame(result[0][1:], columns = result[0][0])
        data_columns = df.columns[df.columns.str.contains(config.acs_group)].tolist()
        index_columns = df.columns[~df.columns.str.contains(config.acs_group)].tolist()
        df = df[index_columns + data_columns]
    else:
        df = pd.DataFrame(result[0][1:], columns = result[0])
        index_columns = df.columns[~df.columns.str.contains(config.acs_group)].tolist()
        del df
        dataframes = [pd.DataFrame(res[0][1:], columns = res[0][0]).drop(merge_columns, axis = 1) for res in result]
        df = pd.concat([index_columns] + dataframes)
        del dataframes
    output = df.convert_dtypes(convert_string = False, convert_boolean = False)
    del df
    return output


def custom_acs_data(key, config = None, **kwargs):    
    def decorator_transform(func, key = key, config = config):
        if config:
            pass
        else:
            config = ACSConfig(**kwargs)
        @functools.wraps(func)
        def wrapper(**kwargs):
            df = acs_data(key, config)
            df = func(df)
            return df
        return wrapper
    return decorator_transform

    
