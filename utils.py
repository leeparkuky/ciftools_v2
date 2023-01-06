from dataclasses import dataclass
from typing import Union, List
from aiohttp import ClientSession
import pandas as pd
import requests
import asyncio
import re


def gen_variable_names(year: Union[str, int], acs_type: str, group_id:str = None) -> List[str]:
    """
    This function retunrs a list of ACS groups IDs available for each acs types: acs, profile, and subject.
    """
    if acs_type in ['','profile','subject']:
        pass
    else:
        raise ValueError
    if acs_type != '':
        url = f'https://api.census.gov/data/{year}/acs/acs5/{acs_type}/variables'
    else:
        url = f'https://api.census.gov/data/{year}/acs/acs5/variables'
    
    resp = requests.get(url)
    json_raw =  resp.json()
    if group_id:
        variables = [x[0] for x in json_raw if re.match(f"{group_id}_\d+E",x[0])]
    else:
        variables = [x[0] for x in json_raw if re.match(".+_\d+E",x[0])]
    variables.sort()
    
    return variables, json_raw


async def gen_group_names(year: Union[str, int], acs_type: str, session: ClientSession) -> List[str]:
    """
    This function retunrs a list of ACS groups IDs available for each acs types: acs, profile, and subject.
    """
    if acs_type in ['','profile','subject']:
        pass
    else:
        raise ValueError
    if acs_type != '':
        url = f'https://api.census.gov/data/{year}/acs/acs5/{acs_type}/groups'
    else:
        url = f'https://api.census.gov/data/{year}/acs/acs5/groups'
    
    resp = await session.request(method="GET", url=url)
    resp.raise_for_status()    
    json_raw =  await resp.json()
    groups = [x['name'] for x in json_raw['groups']]
    return groups

async def groups(year: Union[str, int]):
    async with ClientSession() as session:
        tasks = [gen_group_names(year, acs_class, session) for acs_class in ['','profile','subject']]
        return await asyncio.gather(*tasks)
    
def gen_group_names_acs(config):
    year = config.year
    result = asyncio.run(groups(year))
    output = []
    for r in result:
        output += r
    del result
    return output

def check_acs_type(config):
    year = config.year
    result = asyncio.run(groups(year))
    acs_class = ['','profile','subject']
    output = None
    for acs, r in zip(acs_class, result):
        if config.acs_group in r:
            output = acs
            break
    if output is None:
        raise AttributeError("Check the ACS group id")
    else:
        return acs


