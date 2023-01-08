# python packages

from dataclasses import dataclass
from typing import Union, List
import requests
import urllib
import asyncio
import re
import functools
from functools import partial
from io import BytesIO
from zipfile import ZipFile
from glob import glob
from typing import Union, List
from os import getcwd, remove
from csv import DictReader
# for ascync requests
from aiohttp import ClientSession
# cfg
from CIF_Config import ACSConfig
# pandas / numpy
import pandas as pd
import numpy as np
# multi-processing
from joblib import Parallel, delayed
# import stateDF from utils
from utils import stateDf





def batchify_variables(config: ACSConfig):
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
        for i, res in enumerate(result):
            if i == 0:
                df = pd.DataFrame(res[1:], columns = res[0])
            else:
                df_1 = pd.DataFrame(res[1:], columns = res[0])
                df = df.merge(df_1, how = 'inner', on = df.columns[df.columns.str.isalpha()].tolist())
    df = pd.concat([df.loc[:, df.columns.str.isalpha()], df.loc[:, ~df.columns.str.isalpha()]], axis = 1)
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

###################################################################################################################
####################### census sdoh variables for CIFTools ########################################################
###################################################################################################################

@dataclass
class acs_sdoh:
    year: int
    state_fips: Union[str, int]
    query_level: str        
    key: str
    
    
    
    def cancer_infocus_download(self):
        cancer_infocus_funcs = [self.__getattribute__(x) for x in self.__dir__() if re.match('gen_\w.+_table', x)]
        for func in cancer_infocus_funcs:
            func()
        output = self.download_all()
        return output 

    def download_all(self):
        res = Parallel(n_jobs=-1)(delayed(fun)() for fun in self.functions.values())
        res = Parallel(n_jobs=-1)(delayed(self.cleanup_geo_col)(df, self.query_level) for df in res)
        return {key:val for key, val in zip(self.functions.keys(), res)}
    
    
    def clean_functions(self):
        if hasattr(self, 'functions'):
            self.functions = {}
        else:
            pass

    
    @staticmethod
    def cleanup_geo_col(df, level):
        if level in ['county subdivision','tract','block', 'county']:
            name_series = df.NAME.str.split(', ')
            county_name = [x[-2] for x in name_series]
            state_name  = [x[-1] for x in name_series]
            FIPS        = df[df.columns[df.columns.isin(
                ['county subdivision','tract','block', 'county', 'state'])]].apply(lambda x: ''.join(x), axis = 1)
            if level != "county":
                subgroup_name = [x[0] for x in name_series]
                columns = pd.DataFrame(zip(FIPS, subgroup_name, county_name, state_name), 
                                       columns = ['FIPS', level.title(), 'County','State'])
            else:
                columns = pd.DataFrame(zip(FIPS, county_name, state_name), columns = ['FIPS','County','State'])
                

        elif level == 'zip':
            zip_name = [x[-5:] for x in df.NAME]
            states = {x:stateDf.loc[stateDf.FIPS2.eq(str(x)),'State'].values[0] for x in df.state.unique()}
            state_name = df.state.apply(lambda x: states[x])
            columns = pd.DataFrame(zip(zip_name, state_name), columns = ['ZCTA5','State'])
            
        geo_name = ['county subdivision','tract','block', 'county', 'zip', 'state']
        df = df.drop(df.columns[df.columns.isin(geo_name)].tolist() + ['NAME'], axis = 1)
        return pd.concat([columns, df], axis = 1)
        
            
            
    def add_function(self, func, name):
        if hasattr(self, "functions"):
            pass
        else:
            self.functions = {}
        self.functions[name] = func

    
    def gen_acs_config(self, **kwargs):
        arguements = {'year': self.year,
                    'state_fips': self.state_fips,
                    'query_level': self.query_level,
                    'acs_group': kwargs['acs_group'],
                    'acs_type': kwargs['acs_type']}
        print(arguements)
        self.config = ACSConfig(**arguements)
        return self.config
    
    
#####################################################################
# Check this out!!!!!!!!!!!!!
#####################################################################
    def add_custom_table(self, group_id, acs_type, name):
        config = self.gen_acs_config(**{'acs_group': group_id, 'acs_type': acs_type})
        def transform_data(func):
            @functools.wraps(func)
            def wrapper(**kwargs):
                df = acs_data(self.key, config)
                df = func(df, **kwargs)
                return df
            self.add_function(wrapper, name)
            return wrapper
        
        return transform_data

    
    def gen_insurance_table(self, return_table = False):
        config = self.gen_acs_config(**{'acs_group': ["B27001", "C27007",], 'acs_type': ''})
        @custom_acs_data(self.key, config)
        def transform_df(df):
            insurance_col = [config.variables[config.labels.index(x)] for x in config.labels if re.match('.+With health insurance coverage', x)]
            medicaid_col  = [config.variables[config.labels.index(x)] for x in config.labels if re.match('.+With Medicaid/means-tested public coverage', x)]
            df['health_insurance_coverage_rate'] = df.loc[:, insurance_col].astype(int).sum(axis = 1)/df.B27001_001E.astype(int)
            df['medicaid'] = df.loc[:,medicaid_col].astype(int).sum(axis = 1)/df.C27007_001E.astype(int)
            df.drop(config.variables, axis = 1, inplace = True)
            return df
        self.add_function(transform_df, 'insurance')
        if return_table:
            return transform_df()
        

    def gen_vacancy_table(self, return_table = False):
        config = self.gen_acs_config(**{'acs_group': 'B25002', 'acs_type': ''})
        @custom_acs_data(self.key, config)
        def transform_df(df):
            df['vacancy_rate'] = df.B25002_003E.astype(int)/df.B25002_001E.astype(int)
            df.drop(df.columns[df.columns.str.contains(config.acs_group)], axis = 1, inplace = True)
            return df
        self.add_function(transform_df, 'vacancy')
        if return_table:
            return transform_df()

    

    def gen_poverty_table(self, return_table = False):
        config = self.gen_acs_config(**{'acs_group': 'B17026', 'acs_type': ''})
        @custom_acs_data(self.key, config)
        def transform_df(df):
            df['below_poverty_x.5'] = df.B17026_002E.astype(int)/df.B17026_001E.astype(int)
            df['below_poverty'] = df.loc[:, df.columns.str.match(re.compile('B17026_00[2-4]E'))].astype(int).sum(axis = 1).astype(int)/df.B17026_001E.astype(int)
            df['below_poverty_x2'] = (df.B17026_010E.astype(int) + df.loc[:, df.columns.str.match(re.compile('B17026_00[2-9]E'))].astype(int).sum(axis = 1)).astype(int)/df.B17026_001E.astype(int)
            df.drop(df.columns[df.columns.str.contains(config.acs_group)], axis = 1, inplace = True)
            return df
        self.add_function(transform_df, 'poverty')
        if return_table:
            return transform_df()
        
    def gen_transportation_table(self, return_table = False):
        config = self.gen_acs_config(**{'acs_group': 'B08141', 'acs_type': ''})
        @custom_acs_data(self.key, config)
        def transform_df(df):
            df['no_vehicle'] = df.B08141_002E.astype(int)/df.B08141_001E.astype(int)
            df['two_or_more_vehicle']  = (df.B08141_004E.astype(int) + df.B08141_005E.astype(int))/df.B08141_001E.astype(int)
            df['three_or_more_vehicle'] = df.B08141_005E.astype(int)/df.B08141_001E.astype(int)
            df.drop(df.columns[df.columns.str.contains(config.acs_group)], axis = 1, inplace = True)
            return df
        self.add_function(transform_df, 'transportation')
        if return_table:
            return transform_df()

    
    def gen_employment_table(self, return_table = False):
        config = self.gen_acs_config(**{'acs_group': 'B23025', 'acs_type': ''})
        @custom_acs_data(self.key, config)
        def transform_df(df):
            df['Labor Force Participation Rate'] = df.B23025_003E.astype(
                int)/(df.B23025_003E.astype(int) + df.B23025_007E.astype(int))
            df['Unemployment Rate'] = df.B23025_005E.astype(int)/df.B23025_003E.astype(int)
            df.drop(df.columns[df.columns.str.contains(config.acs_group)], axis = 1, inplace = True)
            return df
        self.add_function(transform_df, 'employment')
        if return_table:
            return transform_df()
        
        
    def gen_gini_index_table(self, return_table = False):
        config = self.gen_acs_config(**{'acs_group': 'B19083', 'acs_type': ''})
        @custom_acs_data(self.key, config)
        def transform_df(df):
            df = df.rename(columns = {'B19083_001E': 'Gini Index'})
            df['Gini Index'] = df['Gini Index'].astype(float).apply(lambda x: x if x>=0 else pd.NA)
            df.drop(df.columns[df.columns.str.contains(config.acs_group)], axis = 1, inplace = True)
            return df
        self.add_function(transform_df, 'gini_index')
        if return_table:
            return transform_df()

    def gen_rent_to_income_table(self, return_table = False):
        config = self.gen_acs_config(**{'acs_group': 'B25070', 'acs_type': ''})
        @custom_acs_data(self.key, config)
        def transform_df(df):
            df['rent_over_40'] = (df.B25070_009E.astype(int) + df.B25070_010E.astype(int))/df.B25070_001E.astype(int)
            df.drop(df.columns[df.columns.str.contains(config.acs_group)], axis = 1, inplace = True)
            return df
        self.add_function(transform_df, 'rent_to_income')
        if return_table:
            return transform_df()
        
        
    def gen_old_house_table(self, return_table = False):
        config = self.gen_acs_config(**{'acs_group': 'B25034', 'acs_type': ''})
        @custom_acs_data(self.key, config)
        def transform_df(df):
            df['houses_before_1960'] = df[['B25034_009E','B25034_010E','B25034_011E']].astype(int).sum(axis = 1)/df.B25034_001E.astype(int) ##### DOUBLE CHECK
            df.drop(df.columns[df.columns.str.contains(config.acs_group)], axis = 1, inplace = True)
            return df
        self.add_function(transform_df, 'houses_before_1960')
        if return_table:
            return transform_df()

        
    def gen_public_assistance_table(self, return_table = False):
        config = self.gen_acs_config(**{'acs_group': 'B19058', 'acs_type': ''})
        @custom_acs_data(self.key, config)
        def transform_df(df):
            df['public_assistance_recieved'] = df.B19058_002E.astype(int)/df.B19058_001E.astype(int)
            df.drop(df.columns[df.columns.str.contains(config.acs_group)], axis = 1, inplace = True)
            return df
        self.add_function(transform_df, 'public_assistance')
        if return_table:
            return transform_df()

               
    def gen_education_table(self, return_table = False):
        config = self.gen_acs_config(**{'acs_group': 'B15003', 'acs_type': ''})

        @custom_acs_data(self.key, config)
        def transform_df(df):
            # col1 is for below 9th grade
            col1_label = ['Nursery school', 'No schooling completed', '5th grade', '3rd grade', '4th grade', '2nd grade', '1st grade', 'Kindergarten','8th grade', '7th grade', '6th grade']
            col1       = [config.variables[config.labels.index(x)] for x in col1_label]
            # col4 is for advanced degree
            col4_label = ['Doctorate degree','Professional school degree', "Master's degree"]
            col4       = [config.variables[config.labels.index(x)] for x in col4_label]

            # col3 is for 4 years college and above: (it changes at the end, but for now, it includes any college to define col2, which is high school and above)
            col3_label = ["Bachelor's degree", "Associate's degree", "Some college, 1 or more years, no degree",'Some college, less than 1 year'] + col4_label
            col3       = [config.variables[config.labels.index(x)] for x in col3_label]
            # col2 is high school and above
            col2_label = ['Regular high school diploma'] + col3_label + col4_label
            col2       = [config.variables[config.labels.index(x)] for x in col2_label]
            # col5 is for completed college
            col5_label = ["Bachelor's degree"] + col4_label
            col5       = [config.variables[config.labels.index(x)] for x in col5_label]
            
            df['Total'] = df.B15003_001E.astype(int)
            df['Below 9th grade'] = df.loc[:, col1].astype(int).sum(axis = 1)/df.Total
            df['High School'] = df.loc[:, col2].astype(int).sum(axis = 1)/df.Total
            df['College'] = df.loc[:, col5].astype(int).sum(axis = 1)/df.Total
            df['Advanced Degree'] = df.loc[:, col4].astype(int).sum(axis = 1)/df.Total
            
            df.drop('Total', axis = 1, inplace = True)
            df.drop(df.columns[df.columns.str.contains(config.acs_group)], axis = 1, inplace = True)
            return df

        self.add_function(transform_df, 'education')
        if return_table:
            return transform_df()
        
        
        
        
    def gen_income_table(self, group_id = "B19013", race = 'all', return_table = False):
        config = self.gen_acs_config(**{'acs_group': group_id, 'acs_type': ''})
        @custom_acs_data(self.key, config)
        def transform_df(df):
            df[f'median_income_{race}'] = df[f'{config.acs_group}_001E'].astype(float)
            df.loc[df[f'median_income_{race}'].le(0), f'median_income_{race}'] = pd.NA
            df.drop(df.columns[df.columns.str.contains(config.acs_group)], axis = 1, inplace = True)
            return df
        if race == 'all':
            self.add_function(transform_df, "income")
        else:
            self.add_function(transform_df, f"income_{race}")
        if return_table:
            return transform_df()

        
    def gen_age_demographic_table(self, group_id = "B01001", age_groups: Union[str, dict] = '18-64', return_table = False):
        config = self.gen_acs_config(**{'acs_group': group_id, 'acs_type': ''})
        
        @custom_acs_data(self.key, config)
        def transform_df(df):
            if isinstance(age_groups, str):
                if age_groups in ['ten years', '18-64']:
                    if age_groups == 'ten years':
                        age_group_dict = ten_year_age_groups(config = config) # you can chage the age group defnition here
                    else:
                        age_group_dict = large_age_groups(config = config) # you can chage the age group defnition here
                else:
                    raise ValueError("you should choose between 'ten years' or '18-64'; otherwise, provide a custom age_groups in a dictionary format")
            elif isinstance(age_groups, dict):
                try:
                    age_group_dict = find_index_for_age_group(age_groups, config = config)
                except:
                    raise ValueError("Please follow the guideline for the custom age_groups")
                
            age_group_dict = large_age_groups(config = config) # you can chage the age group defnition here
            for key, val in age_group_dict.items():
                col = [x for x in config.variables if config.variables.index(x) in val]
                df[key] = df.loc[:, col].astype(int).apply(np.sum, axis = 1)
            df.drop(df.columns[df.columns.str.contains(config.acs_group)], axis = 1, inplace = True)
            return df
        self.add_function(transform_df, 'demographic_age')
        if return_table:
            return transform_df()
    
  
    def gen_race_demographic_table(self, return_table = False):
        config = self.gen_acs_config(**{'acs_group': 'B03002', 'acs_type': ''})
        
        @custom_acs_data(self.key, config)
        def transform_df(df):
            def gen_race_series(race_variable, df = df, config = config):
                newdf = df.copy()
                race_series = newdf[race_variable].astype(int)/newdf.B03002_001E.astype(int)
                del newdf
                return race_series
            variables = ['B03002_' + x for x in ['003E','004E','012E','006E']]
            race_names = ['White','Black','Hispanic','Asian']
            for v, n in zip(variables, race_names):
                df[n] = gen_race_series(v)
            df['Other_Races'] = (df.B03002_002E.astype(int) - df.loc[:,['B03002_003E','B03002_004E','B03002_006E']].astype(int).sum(1))/df.B03002_001E.astype(int)

            df.drop(df.columns[df.columns.str.contains(config.acs_group)], axis = 1, inplace = True)
            return df
        
        self.add_function(transform_df, 'demographic_race')
        if return_table:
            return transform_df()

        

        
        
        
        
#########################
# utils for Demographics
#########################

ten_years = dict(zip(['Under 5 years', '5 to 14 years','15 to 24 years',
                      '25 to 34 years','35 to 44 years','45 to 54 years',
                      '55 to 64 years','65 to 74 years','75 to 84 years',
                      '85 years and over'],
                     [(0, 4), (5, 14), (15, 24), (25, 34), (35, 44), (45, 54), (55, 64),
                      (65, 74), (75, 84), (85, 100)]))

total_years = dict(zip(['Under 18', '18 to 64', 'Over 64'],
                       [(0, 17), (18, 64), (65, 100)]))


def find_index_for_age_group(age_group_dict, config = None, **kwargs):
    if config:
        pass
    else:
        config = ACSConfig(**kwargs)
        
    def extract_age_range(text):
        """from the labels for B01001, this extracts age interval in a tuple:
        \d year old    -> (\d, \d)
        Under \d years -> (0, \d)
        \d_1 to \d_2   -> (\d_1, \d_2)
        \d and over    -> (\d, 100)
        """
        def check_integer(s):
            try:
                int(s)
                return True
            except:
                return False
        numbers = [int(x) for x in text.split(' ') if check_integer(x)]
        if len(numbers) == 1:
            numbers = numbers + numbers
        return tuple(numbers)
    
    one = [[x.replace('Under', '0 to').replace('over','100'), config.labels.index(x)] for x in config.labels if re.match('.*years.*', x)]
    two = [(extract_age_range(x[0]), x[1]) for x  in one]

    def check_in_between(t1, t2):
        if t1[0] >= t2[0] and t1[1] <= t2[1]:
            return True
        else:
            return False

    def find_age_group(test):
        for k, v in age_group_dict.items():
            if check_in_between(test[0], v):
                return k
        
# def find_index_for_age_group(new_def, two):
    index_by_age_group = {k: [] for k in age_group_dict.keys()}
    for t in two:
        index_by_age_group[find_age_group(t)].append(t[1])
    return index_by_age_group


large_age_groups = partial(find_index_for_age_group, age_group_dict = total_years)

ten_year_age_groups = partial(find_index_for_age_group, age_group_dict = ten_years)



###################################################################
###################################################################
# facilities    ###################################################
###################################################################
###################################################################


def gen_facility_data(location:Union[List[str], str], taxonomy:List[str] = ['Gastroenterology','colon','obstetrics']):
    data_dict = {}
    if isinstance(location, str):
        data_dict['nppes'] = nppes(location)
        functions = [mammography, hpsa, fqhc, lung_cancer_screening]
        dataset_names = ['mammography', 'hpsa','fqhc','lung_cancer_screening']
        datasets = Parallel(n_jobs=-1)(delayed(f)(location) for f in functions)
        for name, df in zip(dataset_names, datasets):
            data_dict[name] = df
        return data_dict
    else:
        data_dict['nppes'] = nppes(location)
        functions = [mammography, hpsa, fqhc]
        dataset_names = ['mammography', 'hpsa','fqhc']
        datasets = Parallel(n_jobs=-1)(delayed(f)(location) for f in functions)
        for name, df in zip(dataset_names, datasets):
            data_dict[name] = df
        data_dict['lung_cancer_screening'] = lung_cancer_screening(location)
        return data_dict
    
    

###################################################################
## mammography
###################################################################


def mammography(location: Union[str, List[str]]):

    url = urllib.request.urlopen("http://www.accessdata.fda.gov/premarket/ftparea/public.zip")
    
    with ZipFile(BytesIO(url.read())) as my_zip_file:
        df = pd.DataFrame(my_zip_file.open(my_zip_file.namelist()[0]).readlines(), columns= ['main'])
        df = df.main.astype(str).str.split('|', expand = True)
        df.columns = ['Name','Street','Street2','Street3','City','State','Zip_code','Phone_number', 'Fax']
        # state = state.upper()
        if isinstance(location, str):
            df = df.loc[df.State.eq(location)].reset_index(drop = True)
        else:
            df = df.loc[df.State.isin(location)].reset_index(drop = True)
        df.Name = df.Name.str.extract(re.compile('[bB].(.*)'))
        df['Address'] = df['Street'] + ', ' + df['City'] + ', ' +  df['State'] + ' ' + df['Zip_code']
        df['Type'] = 'Mammography'
        df['Notes'] = ''
    return df.loc[:,['Type','Name','Address','Phone_number', 'Notes']] #try to add FIPS and State


###################################################################
## hpsa
###################################################################

def download_hpsa_data():
    from tqdm import tqdm
    import os
    resp = requests.get('https://data.hrsa.gov/DataDownload/DD_Files/BCD_HPSA_FCT_DET_PC.xlsx', stream=True)
    total = int(resp.headers.get('content-length', 0))
    fname = os.path.join(getcwd(), 'hpsa.xlsx')
    chunk_size = 1024*10
    with open(fname, 'wb') as file, tqdm(
        desc="downloading hpsa data file",
        total=total,
        unit='iB',
        unit_scale=True,
        unit_divisor=1024,
        leave = True
    ) as bar:
        for data in resp.iter_content(chunk_size=chunk_size):
            size = file.write(data)
            bar.update(size)
    output = pd.read_excel(fname, engine = 'openpyxl')
    remove(fname)
    return output




def hpsa(location: Union[str, List[str]]):
    df= download_hpsa_data()
    df.columns = df.columns.str.replace(' ','_')
    if isinstance(location, str):
        df = df.loc[df.Primary_State_Abbreviation.eq(location)&
                    df.HPSA_Status.eq('Designated')&
                    df.Designation_Type.ne('Federally Qualified Health Center')].reset_index(drop = True)
    else:
        df = df.loc[df.Primary_State_Abbreviation.isin(location)&
                    df.HPSA_Status.eq('Designated')&
                    df.Designation_Type.ne('Federally Qualified Health Center')].reset_index(drop = True)
    df = df[['HPSA_Name','HPSA_ID','Designation_Type','HPSA_Score','HPSA_Address',
             'HPSA_City', 'State_Abbreviation', 'Common_State_County_FIPS_Code',
             'HPSA_Postal_Code','Longitude','Latitude']]
    pattern = re.compile('(\d+)-\d+')
    df['HPSA_Postal_Code']  = df.HPSA_Postal_Code.str.extract(pattern)
    df['HPSA_Street'] = df['HPSA_Address'] + ', ' + df['HPSA_City'] + \
        ', ' + df['State_Abbreviation'] + ' ' + df['HPSA_Postal_Code']
    df = df.drop_duplicates()
    df['Type'] = 'HPSA '+df.Designation_Type
    df = df.rename(columns = {'HPSA_Name' : 'Name', 'HPSA_Street':'Address', 
                              'Common_State_County_FIPS_Code': 'FIPS', 'State_Abbreviation': 'State',
                              'Longitude': 'longitude', 'Latitude': 'latitude'})
    df = df[['Type','Name','HPSA_ID','Designation_Type','HPSA_Score','Address',
             'FIPS', 'State', 'longitude','latitude']]
    df = df.loc[df.longitude.notnull()|df.Address.notnull()].reset_index(drop = True)
    df['Phone_number'] = pd.NA
    df['Notes'] = ''
    return df[['Type','Name', 'Address', 'Phone_number', 'Notes', 'latitude', 'longitude']] #try to add FIPS and State


###################################################################
## fqhc
###################################################################


def download_fqhc_data(location: Union[str, List[str]]):
    from tqdm import tqdm
    import os
    resp = requests.get('https://data.hrsa.gov//DataDownload/DD_Files/Health_Center_Service_Delivery_and_LookAlike_Sites.csv', stream = True)    
    total = int(resp.headers.get('content-length', 0))
    fname = os.path.join(getcwd(), 'fqhc.csv')
    chunk_size = 1024*10
    with open(fname, 'wb') as file, tqdm(
        desc="downloading fqhc data file",
        total=total,
        unit='iB',
        unit_scale=True,
        unit_divisor=1024,
        leave = True
    ) as bar:
        for data in resp.iter_content(chunk_size=chunk_size):
            size = file.write(data)
            bar.update(size)
            
    with open(fname, newline='') as csvfile:
        reader = DictReader(csvfile)
        colnames = ['Health_Center_Type', 'Site_Name','Site_Address','Site_City','Site_State_Abbreviation',
                 'Site_Postal_Code','Site_Telephone_Number', 
                 'Health_Center_Service_Delivery_Site_Location_Setting_Description',
                 'Geocoding_Artifact_Address_Primary_X_Coordinate',
                 'Geocoding_Artifact_Address_Primary_Y_Coordinate']
        csv_keys = [x.replace('_', ' ') for x in colnames]
        data_dict = dict(zip(colnames, [[] for _ in range(len(colnames))]))
        for row in reader:
            if isinstance(location, str):
                if (row['Site State Abbreviation'] == location.upper()) & (
                    row['Health Center Type'] == 'Federally Qualified Health Center (FQHC)') & (
                    row['Site Status Description'] == 'Active'):
                    for dict_key, row_key in zip(colnames, csv_keys):
                        data_dict[dict_key].append(row[row_key])   
            else:
                if (row['Site State Abbreviation'] in [x.upper() for x in location]) & (
                    row['Health Center Type'] == 'Federally Qualified Health Center (FQHC)') & (
                    row['Site Status Description'] == 'Active'):
                    for dict_key, row_key in zip(colnames, csv_keys):
                        data_dict[dict_key].append(row[row_key])   
    output = pd.DataFrame(data_dict)
    remove(fname)
    return output



def fqhc(location: Union[str, List[str]]):
    df= download_fqhc_data(location)
    df['Type'] = 'FQHC'
    df['Address'] = df['Site_Address'] + ', ' + df['Site_City'] + ', ' + \
                    df['Site_State_Abbreviation'] + ' ' + df['Site_Postal_Code']
    df = df.rename(columns = {'Site_Name':'Name', 'Site_Telephone_Number': 'Phone_number', 
                              'State_Abbreviation': 'State',
                              'Health_Center_Service_Delivery_Site_Location_Setting_Description': 'Notes',
                              'Geocoding_Artifact_Address_Primary_X_Coordinate': 'longitude',
                              'Geocoding_Artifact_Address_Primary_Y_Coordinate': 'latitude'})
    df = df.loc[df.Address.notnull()].reset_index(drop = True)
    return df[['Type', 'Name', 'Address', 'Phone_number', 'Notes', 'latitude', 'longitude']]


###################################################################
## nppes
###################################################################


def parse_basic(basic):
    if 'organization_name' in basic.keys():
        name = basic['organization_name'].title()
    else:
        if 'middle_name' in basic.keys():
            name = basic['first_name'].title() + ' ' + basic['middle_name'][0].upper() + ' ' + basic['last_name'].title()
        else:
            name = basic['first_name'].title() + ' ' + basic['last_name'].title()
        if 'credential' in basic.keys():
            name = name + ' ' + basic['credential'].upper()
    return name


def parse_address(address):
    address_dict = [x for x in address if x['address_purpose'] == 'LOCATION'][0]
    if 'address_2' in address_dict.keys():
        street = address_dict['address_1'].title() + ', ' + address_dict['address_2'].title() + ', ' + address_dict['city'].title() + ', ' + address_dict['state'].upper() + ' ' + address_dict['postal_code'][:5]
    else:
        street = address_dict['address_1'].title() + ', ' + address_dict['city'].title() + ', ' + address_dict['state'].upper() + ' ' + address_dict['postal_code'][:5]
    phone_number = address_dict['telephone_number']
    return street, phone_number


taxonomy = ['Gastroenterology','colon','obstetrics']
def gen_nppes_by_taxonomy(taxonomy: str, location: str):
    count = 0
    result_count = 200
    skip = 0
    datasets = []
    while result_count == 200:
        count += 1
        url = f'https://npiregistry.cms.hhs.gov/api/?version=2.1&address_purpose=LOCATION&number=&state={location}&taxonomy_description={taxonomy}&skip={200*(count -1)}&limit=200'
        resp = requests.get(url)
        output = resp.json()
        result_count = output['result_count']
        df = pd.DataFrame(output['results'])
        df['Name'] = df.basic.apply(parse_basic)
        df['Phone_number'] = df.addresses.apply(lambda x: parse_address(x)[1])
        df['Address'] = df.addresses.apply(lambda x: parse_address(x)[0])
        df['Type']    = taxonomy
        df['Notes']   = ''
        if result_count == 200:
            datasets.append(df[['Type','Name','Address','Phone_number', 'Notes']])
        elif count == 1:
            return df[['Type','Name','Address','Phone_number', 'Notes']]
        else:
            datasets.append(df[['Type','Name','Address','Phone_number', 'Notes']])
            result = pd.concat(datasets, axis = 0).reset_index(drop = True)
            return result
        
def nppes(location:Union[str, List[str]], taxonomy:List[str] = ['Gastroenterology','colon','obstetrics']) -> pd.DataFrame:
    if isinstance(location, str):
        res = Parallel(n_jobs=-1)(delayed(gen_nppes_by_taxonomy)(t, location) for t in taxonomy)
    else:
        from itertools import product
        res = Parallel(n_jobs=-1)(delayed(gen_nppes_by_taxonomy)(t, loc) for t, loc in product(taxonomy, location))
    return pd.concat(res, axis = 0)

###################################################################
## lung_cancer_screening
###################################################################

        
def setup_chrome_driver():
    glob_result = glob("./*/chromedriver", recursive = True)
    if len(glob_result) == 0:
        import chromedriver_autoinstaller
        fp = chromedriver_autoinstaller.install('.')
    else:
        fp = glob_result[0]
    return fp
    
def lung_cancer_screening_file_download(location:str, num_downloads = 1, wait = 0):
    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options
    from selenium.webdriver.chrome.service import Service
    from selenium.webdriver.common.by import By
    import os
    import time
    if wait:
        time.sleep(wait)
    
    chromeOptions = Options()
    prefs = {"download.default_directory" : getcwd()}
    chromeOptions.add_experimental_option("prefs",prefs)
    chromeOptions.add_argument(f"download.default_directory={getcwd()}")

    chrome_driver_path = setup_chrome_driver()
    driver = webdriver.Chrome(executable_path=chrome_driver_path, options=chromeOptions)
    url = 'https://report.acr.org/t/PUBLIC/views/NRDRLCSLocator/LCSLocator?:embed=y&:showVizHome=no&:host_url=https%3A%2F%2Freport.acr.org%2F&:embed_code_version=3&:tabs=no&:toolbar=no&:showAppBanner=no&:display_spinner=no&:loadOrderID=0'
    driver.get(url);  time.sleep(10)
    state = driver.find_elements(By.CLASS_NAME, 'tabComboBoxButtonHolder')[2]; state.click(); time.sleep(10)
    state2 = driver.find_elements(By.CLASS_NAME, 'tabMenuItemNameArea')[1]; state2.click(); time.sleep(10)
    download = driver.find_element(By.ID, 'tabZoneId422'); download.click()
    x = num_downloads
    t = 0
    while t < x:
        time.sleep(5)
        t = len(glob('./ACRLCSDownload*.csv'))
        print('Waiting on LCSR data...')
    else:
        print('LCSR data ready')
    driver.close()
    return None
    
def process_lcs_data(file_path, location):
    df = pd.read_csv(file_path)
    df.columns = ['Name','Street','City','State','Zip_code','Phone','Designation', 'Site ID', 'Facility ID', 'Registry Participant']
    df['Address'] = df['Street'].str.title() + ', ' + df['City'].str.title() + ', ' +  df['State'].str.upper() + ' ' + df['Zip_code'].apply(lambda x: x[:5])
    df['Type'] = 'Lung Cancer Screening'
    df['Phone_number'] = df['Phone']
    df['Notes'] = ''
    if isinstance(location, str):
        df = df.loc[df.State.eq(location)]
    else:
        df = df.loc[df.State.isin(location)]
    df = df[['Type','Name', 'Address', 'Phone_number', 'Notes']]
    return df

def remove_chromedriver(chrome_driver_path):
    import shutil
    from os import path
    shutil.rmtree(path.join(*chrome_driver_path.split('/')[:-1]))

    
def lung_cancer_screening(location: Union[str, List[str]]):
    if isinstance(location, str):
        lung_cancer_screening_file_download(location)
    else:
        Parallel(n_jobs=-1)(delayed(lung_cancer_screening_file_download)(loc, len(location), w) for loc, w in zip(location, [x*20 for x in range(len(location))]))
    downloads = glob('./ACRLCSDownload*.csv')
    if isinstance(location, list):
        assert len(downloads) == len(location)
    if len(downloads) > 1:
        datasets = Parallel(n_jobs=-1)(delayed(process_lcs_data)(path, location) for path in downloads)
        df = pd.concat(datasets, axis = 0)
    else:
        df = process_lcs_data(downloads[0], location)
    chrome_driver_path = setup_chrome_driver()
    remove_chromedriver(chrome_driver_path)
    df = df.reset_index(drop = True)
    for file in downloads:
        remove(file)
    return df


