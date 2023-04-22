#async packages
from aiohttp import ClientSession
import aiofiles
import asyncio
# basic python packages
from dataclasses import dataclass
from typing import Union, List
import requests
import re
from io import StringIO
from itertools import product
# dataframes
import pandas as pd
import geopandas as gpd
# file systems
import chromedriver_autoinstaller
from glob import glob
import os
import sys





def census_shape(years: Union[List[str], List[int], str, int], states: Union[List[str], str]):
    if sys.platform in ['win32','cygwin']:
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
        # tract first
        tracts = asyncio.get_event_loop().run_until_complete(shape_download_all_tracts(years, states))
    else:                                                     
        tracts = asyncio.run(shape_download_all_tracts(years, states))
    tracts = tracts.groupby(['FIPS','Tract'], as_index = False).last().reset_index(drop = True)
    # then county
    county = shape_county(states)
    shape_dict = {'county_shape': county, 'tract_shape':tracts}
    return shape_dict


def shape_county(state_fips: Union[str, List[str]] = None, year = 2021):
    url = f'https://www2.census.gov/geo/tiger/TIGER{year}/COUNTY/tl_{year}_us_county.zip'
    gdf = gpd.read_file(url)
    gdf.GEOID = gdf.GEOID.astype(str)
    gdf = gdf[['GEOID','NAMELSAD','geometry']]
    gdf = gdf.rename(columns = {'GEOID':'FIPS', 'NAMELSAD': 'County', 'geometry':'Shape'}).reset_index(drop = True)
    if state_fips:
        if isinstance(state_fips, str):
            assert state_fips.isnumeric()
            assert len(state_fips) == 2
            gdf = gdf.loc[gdf.FIPS.str[:2].eq(state_fips),:].reset_index(drop = True)
        else:
            for s in state_fips:
                assert s.isnumeric()
                assert len(s) == 2
            gdf = gdf.loc[gdf.FIPS.str[:2].isin(state_fips),:].reset_index(drop = True)
    return gdf



async def shape_tract_download(year: Union[str, int], state: str, session):
    assert len(state) == 2
    url = f"https://www2.census.gov/geo/tiger/TIGER{year}/TRACT/tl_{year}_{state}_tract.zip"
    resp = await session.request(method="GET", url=url)
    async with aiofiles.open(f'tl_{year}_{state}_tract.zip', 'wb') as f:
#     async with aiofiles.tempfile.NamedTemporaryFile(suffix = '.zip') as f:
        async for chunk in resp.content.iter_chunked(2**30):
            await f.write(chunk)
#     gdf = gpd.read_file(f.name, dtype = {'GEOID':str})
    gdf = gpd.read_file(f'tl_{year}_{state}_tract.zip', dtype = {'GEOID':str})
    gdf = gdf[['GEOID','NAMELSAD','geometry']]\
    .rename(columns = {'GEOID':'FIPS','NAMELSAD':'Tract','geometry':'Shape'})\
    .sort_values('FIPS').reset_index(drop = True)
    os.remove(f'tl_{year}_{state}_tract.zip')
    return gdf


    
async def shape_download_all_tracts(years: Union[List[str], List[int], str, int], states: Union[List[str], str]):
    async with ClientSession() as session:
        if isinstance(years, int) or isinstance(years, str):
            if isinstance(states, str):
                assert len(states) == 2
                assert states.isnumeric()
                tasks = [shape_tract_download(years, states, session)]
            else:
                tasks = [shape_tract_download(years, s, session) for s in states]
        else:
            if isinstance(states, str):
                assert len(states) == 2
                assert states.isnumeric()
                tasks = [shape_tract_download(y, states, session) for y in years]
            else:
                tasks = [shape_tract_download(y, s, session) for s,y in product(states, years)]
        tables = await asyncio.gather(*tasks)
        if len(tables) > 1:
            df = pd.concat(tables).drop_duplicates().reset_index(drop = True)
        else:
            df = tables[0].reset_index(drop = True)
        return df










def download_zip_files():
    if os.getenv("COLAB_RELEASE_TAG"):
        from google.colab import files
        zip_file = glob('*.zip')
        if len(zip_file):
            for file in zip_file:
                files.download(file)
        return None
    else:
        raise OSError("This function can be run only under google colab environment")


def import_custom_ca_file():
    if os.getenv("COLAB_RELEASE_TAG"):
        from google.colab import files
        uploaded = files.upload()
        fpath = os.path.join(os.getcwd(), list(uploaded.keys())[0])
        return fpath
    else:
        print('This function is only for the google colab environment')


def check_ca_file(ca_file_path):
    if os.path.split(ca_file_path)[0] == '':
        if len(ca_file_path.split('.')) == 1:
            ca_file_path = ca_file_path + '.csv'
        if len(glob(os.path.join(os.getcwd(), '*', ca_file_path))) == 0:
            raise ValueError("Please check if your file exists in catchment_area folder or \n upload your own catchment_area file using 'import_custom_ca_file' function from utils package")
        else:
            glob_result = glob(os.path.join(os.getcwd(), '*', ca_file_path))
        return glob_result[0]

    else:
        glob_result = glob(ca_file_path)
        if len(glob_result):
            return glob_result[0]
            
        
        
def write_bash_script(bash_file_name: str, 
                      catchment_area_name: str, 
                      ca_file_path: str, 
                      query_level : Union[str, List[str]], 
                      acs_year : int,
                      download_file_type : Union[str, List[str]], 
                      census_api_key: str, 
                      cif_data_pull = True,
                     generate_zip_file = True,
                     install_packages = False,
                      puma_ca_file_path: str = None,
                     **kwargs):
    
    
    ca_dir = catchment_area_name.replace(" ", "_") + "_catchment_data"

    
    ca_file_path = check_ca_file(ca_file_path)
    
    if 'socrata_user_name' in kwargs.keys():
        socrata = True
    else:
        socrata = False
    
#     for key, value in kwargs.items():
#         print("%s == %s" % (key, value))
    
    
    if isinstance(query_level, str):
        assert query_level in ['county subdivision','tract','block', 'county', 'state','zip','puma']
        query_level = f'"{query_level}"'
    else:
        for level in query_level:
            assert level in ['county subdivision','tract','block', 'county', 'state','zip', 'puma']
        query_level = ' '.join(f'"{x}"' for x in query_level)
        
        
    if isinstance(download_file_type, str):
        assert download_file_type in ['pickle','excel','csv']
        download_file_type = f'"{download_file_type}"'
    else:
        for file_type in download_file_type:
            assert file_type in ['pickle','excel','csv']
        download_file_type = ' '.join(f'"{x}"' for x in download_file_type)
    
    if bash_file_name[-3:] != '.sh':
        bash_file_name += '.sh'
        
    bash_file_path = os.path.join(os.getcwd(), bash_file_name)
    with open(bash_file_name, 'w') as f:
        f.write(f'catchment_area_name="{catchment_area_name}"');
        f.write('\n')
        f.write(f'ca_file_path="{ca_file_path}"');
        f.write('\n')
        f.write(f'year={acs_year}');
        f.write('\n')
        f.write(f'census_api_key="{census_api_key}"');
        f.write('\n\n\n')
        if install_packages:
            f.write('pip install -r requirements.txt');
            f.write('\n\n\n');
        f.write('clear');
        f.write('\n\n\n')
        f.write(f"python CIFTools.py --ca_file_path $ca_file_path --query_level {query_level} --year $year --census_api_key $census_api_key");
        if socrata:
            f.write(f' --socrata_user_name "{kwargs["socrata_user_name"]}" --socrata_password "{kwargs["socrata_password"]}"');
        f.write('\n\n')
        output = os.path.join(os.getcwd(), 'cif_raw_data.pickle')
        if cif_data_pull:
            f.write(f'python CIF_pull_data.py --ca_name "$catchment_area_name" --ca_file_path $ca_file_path --pickle_data_path "cif_raw_data.pickle" --download_file_type {download_file_type}')
            if 'puma' in query_level:
                f.write(' --add_puma_level true')
                if isinstance(puma_ca_file_path, str):
                    f.write(f' --puma_id_file "{puma_ca_file_path}"')
            else:
                f.write(' --add_puma_level false')

                    
            f.write('\n\n')
            output = ca_dir
            if generate_zip_file:
                f.write(f'zip -r {ca_dir}.zip {ca_dir}'); f.write('\n\n')
                f.write('echo "\n\n\n\n\n\n\n\n\n\n\n"'); f.write('\n\n')
                f.write(f'echo "The ziped file is located at {os.path.join(os.getcwd(), ca_dir)}.zip"')
                output = ca_dir + '.zip'
        f.close()
    return output





def gen_variable_names(year: Union[str, int], 
                       acs_type: Union[str, List[str]], 
                       group_id:Union[str, List[str]] = None) -> List[str]:
    """
    This function retunrs a list of ACS groups IDs available for each acs types: acs, profile, and subject.
    """
    if isinstance(acs_type, str):
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

    elif isinstance(acs_type, list):
        urls = []
        for at in acs_type:
            if at in ['','profile','subject']:
                if at != '':
                    url = f'https://api.census.gov/data/{year}/acs/acs5/{at}/variables'
                    urls.append(url)
                else:
                    url = f'https://api.census.gov/data/{year}/acs/acs5/variables'
                    urls.append(url)
            else:
                raise ValueError
        urls = pd.Series(urls).unique().tolist()
        json_raw = []
        for url in urls:
            resp = requests.get(url)
            json_raw += resp.json()

    if group_id:
        if isinstance(group_id, str):
            variables = [x[0] for x in json_raw if re.match(f"{group_id}_\d+E",x[0])]
        else:
            variables = []
            for gid in group_id:
                variables += [x[0] for x in json_raw if re.match(f"{gid}_\d+E",x[0])]
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
    if isinstance(config.acs_group, str):
        for acs, r in zip(acs_class, result):
            if config.acs_group in r:
                output = acs
                break
        if output is None:
            raise AttributeError("Check the ACS group id")
        else:
            return output
    elif isinstance(config.acs_group, list):
        for acs_group in config.acs_group:
            for acs, r in zip(acs_class, result):
                if acs_group in r:
                    if output == None:
                        output = [acs]
                    else:
                        output.append(acs)
        if len(output) != len(config.acs_group):
            raise AttributeError("Check the ACS group id")
        else:
            if pd.Series(output).unique().shape[0] == 1:
                return output[0]
            else:
                raise AttributeError("All the groups must be in the same acs_type")
        

state = '''State,FIPS2,StateAbbrev
    Alabama,01,AL
    Alaska,02,AK
    Arizona,04,AZ
    Arkansas,05,AR
    California,06,CA
    Colorado,08,CO
    Connecticut,09,CT
    Delaware,10,DE
    District of Columbia,11,DC
    Florida,12,FL
    Georgia,13,GA
    Hawaii,15,HI
    Idaho,16,ID
    Illinois,17,IL
    Indiana,18,IN
    Iowa,19,IA
    Kansas,20,KS
    Kentucky,21,KY
    Louisiana,22,LA
    Maine,23,ME
    Maryland,24,MD
    Massachusetts,25,MA
    Michigan,26,MI
    Minnesota,27,MN
    Mississippi,28,MS
    Missouri,29,MO
    Montana,30,MT
    Nebraska,31,NE
    Nevada,32,NV
    New Hampshire,33,NH
    New Jersey,34,NJ
    New Mexico,35,NM
    New York,36,NY
    North Carolina,37,NC
    North Dakota,38,ND
    Ohio,39,OH
    Oklahoma,40,OK
    Oregon,41,OR
    Pennsylvania,42,PA
    Rhode Island,44,RI
    South Carolina,45,SC
    South Dakota,46,SD
    Tennessee,47,TN
    Texas,48,TX
    Utah,49,UT
    Vermont,50,VT
    Virginia,51,VA
    Washington,53,WA
    West Virginia,54,WV
    Wisconsin,55,WI
    Wyoming,56,WY
    '''
    
dfCsv = StringIO(state)

stateDf = pd.read_csv(dfCsv, sep=',', dtype={'State':str, 'FIPS2':str, 'StateAbbrev':str})
stateDf['State'] = stateDf.State.str.strip()


def c2p_all(county_FIPS:list = None):
    old_tract_to_puma = 'https://www2.census.gov/geo/docs/maps-data/data/rel/2010_Census_Tract_to_2010_PUMA.txt'
    new_tract_to_puma = 'https://www2.census.gov/geo/docs/maps-data/data/rel2020/2020_Census_Tract_to_2020_PUMA.txt'
    t2p2010 = pd.read_table(old_tract_to_puma, sep = ',', dtype = {'STATEFP': str,
                                                        'COUNTYFP': str,
                                                        'TRACTCE':str,
                                                        'PUMA5CE':str})
    t2p2010 = t2p2010.rename(columns = {k:v for k,v in zip(t2p2010.columns.tolist(), 
                                                 ['STATEFIPS','COUNTYFIPS','TRACTFIPS','PUMA_ID'])})
    t2p2010 = t2p2010.merge(stateDf, how = 'left', left_on = 'STATEFIPS', right_on = 'FIPS2') \
                    .dropna().drop(['FIPS2'], axis = 1)
    t2p2010['FIPS'] = t2p2010['STATEFIPS'] + t2p2010['COUNTYFIPS']
    t2p2010['TRACTFIPS'] = t2p2010['STATEFIPS'] + t2p2010['COUNTYFIPS'] + t2p2010['TRACTFIPS']
    t2p2010 = t2p2010.drop(['STATEFIPS','COUNTYFIPS'], axis = 1)
    t2p2020 = pd.read_table(new_tract_to_puma, sep = ',', dtype = {'STATEFP': str,
                                                        'COUNTYFP': str,
                                                        'TRACTCE':str,
                                                        'PUMA5CE':str})
    t2p2020 = t2p2020.rename(columns = {k:v for k,v in zip(t2p2020.columns.tolist(), 
                                                 ['STATEFIPS','COUNTYFIPS','TRACTFIPS','PUMA_ID'])})
    t2p2020 = t2p2020.merge(stateDf, how = 'left', left_on = 'STATEFIPS', right_on = 'FIPS2') \
                    .dropna().drop(['FIPS2'], axis = 1)
    t2p2020['FIPS'] = t2p2020['STATEFIPS'] + t2p2020['COUNTYFIPS']
    t2p2020['TRACTFIPS'] = t2p2020['STATEFIPS'] + t2p2020['COUNTYFIPS'] + t2p2020['TRACTFIPS']
    t2p2020 = t2p2020.drop(['STATEFIPS','COUNTYFIPS'], axis = 1)
    t2p_all = pd.DataFrame({'TRACTFIPS':list(set(t2p2020.TRACTFIPS.tolist()+ t2p2010.TRACTFIPS.tolist()))})
    t2p_all_2020 = t2p_all.merge(t2p2020, how = 'left').dropna()
    t2p_all_2010 = t2p_all.merge(t2p2010, how = 'left').dropna()
    t2p_all_2010['Year'] = '2010'; t2p_all_2020['Year'] = '2020'    
    t2p_all = pd.concat([t2p_all_2010.loc[~t2p_all_2010.TRACTFIPS.isin(t2p_all_2020.TRACTFIPS.tolist()),:], t2p_all_2020])
    t2p_all = t2p_all.sort_values(['State','PUMA_ID','FIPS','TRACTFIPS']).reset_index(drop = True)
    c2p_all = t2p_all.drop(['TRACTFIPS', 'Year'], axis = 1)
    c2p_all = c2p_all.drop_duplicates().sort_values(['State','PUMA_ID','FIPS']).reset_index(drop = True)
    if county_FIPS:
        puma = c2p_all.loc[c2p_all.FIPS.isin(county_FIPS),['PUMA_ID','State']].drop_duplicates()
    else:
        puma = c2p_all.loc[:,['PUMA_ID','State']].drop_duplicates()
    return puma.reset_index(drop = True)    

    