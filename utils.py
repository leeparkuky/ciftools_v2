from dataclasses import dataclass
from typing import Union, List
from aiohttp import ClientSession
import pandas as pd
import requests
import asyncio
import re
from io import StringIO
import chromedriver_autoinstaller
import os
from glob import glob

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
                     install_packages = False):
    
    
    ca_dir = catchment_area_name.replace(" ", "_") + "_catchment_data"

    
    ca_file_path = check_ca_file(ca_file_path)
    
    
    
    if isinstance(query_level, str):
        assert query_level in ['county subdivision','tract','block', 'county', 'state','zip']
        query_level = f'"{query_level}"'
    else:
        for level in query_level:
            assert level in ['county subdivision','tract','block', 'county', 'state','zip']
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
        f.write('\n\n')
        output = os.path.join(os.getcwd(), 'cif_raw_data.pickle')
        if cif_data_pull:
            f.write(f'python CIF_pull_data.py --ca_name $catchment_area_name --ca_file_path $ca_file_path --pickle_data_path "cif_raw_data.pickle" --download_file_type {download_file_type}')
            f.write('\n\n')
            output = ca_dir
            if generate_zip_file:
                ca_dir = args.ca_name.replace(" ", "_") + "_catchment_data"
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