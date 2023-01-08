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
from CIF_Config import ACSConfig, SocrataConfig
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
        functions = [mammography, hpsa, fqhc, lung_cancer_screening, toxRel_data, superfund]
        dataset_names = ['mammography', 'hpsa','fqhc','lung_cancer_screening', 'tri_facility', 'superfund_site']
        datasets = Parallel(n_jobs=-1)(delayed(f)(location) for f in functions)
        for name, df in zip(dataset_names, datasets):
            data_dict[name] = df
        return data_dict
    else:
        data_dict['nppes'] = nppes(location)
        functions = [mammography, hpsa, fqhc, toxRel_data, superfund]
        dataset_names = ['mammography', 'hpsa','fqhc','tri_facility', 'superfund_site']
        datasets = Parallel(n_jobs=-1)(delayed(f)(location) for f in functions)
        for name, df in zip(dataset_names, datasets):
            data_dict[name] = df
        data_dict['lung_cancer_screening'] = lung_cancer_screening(location)
        return data_dict

    
    
###################################################################
## toxRel_data
###################################################################
    
def toxRel_data(location:Union[str, List[str]]):
    from tqdm import tqdm
    import os
    resp = requests.get('https://data.epa.gov/efservice/downloads/tri/mv_tri_basic_download/2021_US/csv', stream=True)
    total = int(resp.headers.get('content-length', 0))
    fname = os.path.join(getcwd(), 'toxRel.csv')
    chunk_size = int(1024*1024/2)
    with open(fname, 'wb') as file, tqdm(
        desc="downloading toxRel data file",
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
        ############
        colnames = ['FRS ID', 'FACILITY NAME',    #   'STREET ADDRESS','CITY','ST','ZIP',
                    'LATITUDE', 'LONGITUDE', 'COUNTY', 'CHEMICAL']
        csv_keys = ['3. FRS ID', '4. FACILITY NAME', #. '5. STREET ADDRESS', '6. CITY', '8. ST', '9. ZIP',
                   '12. LATITUDE', '13. LONGITUDE', '7. COUNTY', '34. CHEMICAL']
        data_dict = dict(zip(colnames, [[] for _ in range(len(colnames))]))
        data_dict['Address'] = []
        if isinstance(location, str):
            assert len(location) == 2
            for row in reader:
                if (row['8. ST'] == location.upper()) & (
                    row['43. CARCINOGEN'] == 'YES'):
                    address = row['5. STREET ADDRESS'].title() + ', ' + row['6. CITY'].title() + ', ' + row['8. ST'].upper() + ' ' + str(row['9. ZIP'])
                    data_dict['Address'].append(address)
                    for dict_key, row_key in zip(colnames, csv_keys):
                        data_dict[dict_key].append(row[row_key]) 
        else:
            for loc in location:
                assert len(loc) == 2
            for row in reader:
                if (row['8. ST'] in [x.upper() for x in location]) & (
                    row['43. CARCINOGEN'] == 'YES'):
                    address = row['5. STREET ADDRESS'].title() + ', ' + row['6. CITY'].title() + ', ' + row['8. ST'].upper() + ' ' + str(row['9. ZIP'])
                    data_dict['Address'].append(address)
                    for dict_key, row_key in zip(colnames, csv_keys):
                        data_dict[dict_key].append(row[row_key])  
                    
    df = pd.DataFrame(data_dict)
    remove(fname)
    del data_dict
    df = df.groupby(["FRS ID", 'FACILITY NAME', 
                                 'Address', 'LATITUDE', 
                                 'LONGITUDE', 'COUNTY'])['CHEMICAL'].agg(lambda col: ', '.join(col)).reset_index()
    df['Notes'] = 'Chemicals relased: ' + df['CHEMICAL']
    df = df[['FACILITY NAME', 'Address', 'LATITUDE', 'LONGITUDE', 'COUNTY', 'Notes']]
    df = df.rename(columns = {'FACILITY NAME': 'Name', 'LATITUDE': 'latitude', 'LONGITUDE': 'longitude'})
    df['Type'] = 'Toxic Release Inventory Facility'
    df['Phone_number'] = pd.NA
    return df[['Type', 'Name', 'Address', 'Phone_number', 'Notes', 'latitude', 'longitude']]

    
    
    
    
###################################################################
## superfund
###################################################################

    
def gen_single_superfund(location: str):
    assert len(location) == 2
    url = f'https://data.epa.gov/efservice/SEMS_ACTIVE_SITES/SITE_STATE/{location.upper()}/CSV'
    sf = pd.read_csv(url)
    sf2 = sf.loc[sf.NPL.isin(['Currently on the Final NPL', 'Deleted from the Final NPL'])]
    sf3 = sf2[['SITE_NAME', 'SITE_STRT_ADRS1', 'SITE_CITY_NAME', 'SITE_STATE', 'SITE_ZIP_CODE',
             'SITE_FIPS_CODE', 'NPL', 'LATITUDE', 'LONGITUDE']]
    sf3 = sf3.assign(Address = sf3['SITE_STRT_ADRS1'] + ', ' + sf3['SITE_CITY_NAME'] + ', ' + sf3['SITE_STATE'] + ' ' + sf3['SITE_ZIP_CODE'].astype(str))
    sf3 = sf3.rename(columns = {'SITE_NAME':'Name', 'SITE_FIPS_CODE':'FIPS5', 'NPL':'Notes',
                              'LATITUDE':'latitude', 'LONGITUDE':'longitude'})
    sf3.drop(['SITE_STRT_ADRS1', 'SITE_CITY_NAME', 'SITE_STATE', 'SITE_ZIP_CODE'], axis=1, inplace=True)
    sf3['Type'] = 'Superfund Site'
    sf3['Phone_number'] = pd.NA
    del sf, sf2
    return sf3[['Type', 'Name', 'Address', 'Phone_number', 'Notes', 'latitude', 'longitude']]


def superfund(location: Union[str, List[str]]):
    if isinstance(location, str):
        df = gen_single_superfund(location)
    else:
        datasets = []
        for loc in location:
            datasets.append(gen_single_superfund(loc))
        df = pd.concat(datasets, axis = 0).reset_index(drop = True)
    return df
    
    

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
    del data_dict
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
## lung_cancer_screening ########################################## -> multiprocessing with multiple states
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


############################################################################
## BLS      ################################################################
############################################################################

@dataclass
class BLS:
    state_fips: Union[str, List[str]]
    most_recent: bool = True
    
    @property
    def bls_data(self):
        if hasattr(self, '_bls_data'):
            pass
        else:
            state = self.state_fips
            response = requests.get('https://www.bls.gov/web/metro/laucntycur14.txt')
            df = pd.DataFrame([x.strip().split('|') for x in response.text.split('\n')[6:-7]],
                              columns = ['LAUS Area Code','State','County','Area',
                                         'Period','Civilian Labor Force','Employed',
                                         'Unemployed','Unemployment Rate'] )
            df['State'] = df.State.str.strip().astype(str)
            if isinstance(state, str):
                assert len(state) == 2
                df = df.loc[df.State.eq(state), :]
            else:
                for s in state:
                    assert len(s) == 2
                df = df.loc[df.State.isin(state), :]
            df['County'] = df.County.str.strip().astype(str)
            df['County'] = ['0'+x if len(x) < 3 else x for x in df.County]
            df['County'] = ['0'+x if len(x) < 3 else x for x in df.County]
            df['Employed'] = pd.to_numeric(df.Employed.str.strip().str.replace(',',''), errors = 'coerce')
            df['Unemployed'] = pd.to_numeric(df.Unemployed.str.strip().str.replace(',',''), errors = 'coerce')
            df['Unemployment Rate'] = pd.to_numeric(df['Unemployment Rate'].str.strip().str.replace(',',''), errors = 'coerce')
            df['FIPS'] = df['State']+df['County']
            df['Period'] = df.Period.str.strip()
            if most_recent:
                df = df.loc[df.Period.str.match(re.compile('.*p\)$'))]
                df['Period'] = [x[:-3] for x in df.Period]
            self._bls_data = df.loc[df.State.eq(state),['FIPS','Unemployment Rate', 'Period']].sort_values('FIPS').reset_index(drop = True)
        return self._bls_data
        

        
############################################################################
## Water Violation      ####################################################  -> multiprocessing if with multiple states
############################################################################
        
@dataclass
class water_violation:
    location: Union[str, List[str]]
    start_year: int = 2016
        
    @property
    def water_violation_data(self):
        if hasattr(self, '_water_violation_data'):
            pass
        else:
            if isinstance(self.location, str):
                df = self.gen_water_violation(self.location)
                data_dict = {self.location: df}
            else:
                datasets = Parallel(n_jobs=-1)(delayed(self.gen_water_violation)(loc) for loc in self.location)
                data_dict = dict(zip(self.location, datasets))
            self._water_violation_data = data_dict
        return self._water_violation_data
        
        
    def gen_water_violation(self, state:str):
        assert len(state) == 2
        violation = self.gen_violation(state, self.start_year)
        profile   = self.gen_profile(state)
        violation_by_pws = violation[['PWSID','VIOLATION_ID','indicator']].groupby(['PWSID','VIOLATION_ID'], as_index = False).max().loc[:,['PWSID','indicator']].groupby('PWSID', as_index = False).sum()
        violation_by_pws.columns = ['PWSID','counts']
        df = profile.merge(violation_by_pws, on = 'PWSID', how='left')
        df = df[['COUNTY_SERVED', 'PRIMACY_AGENCY_CODE', 'counts']].groupby('COUNTY_SERVED', as_index = False).max()
        self.testing = df
        df['County'] = df.COUNTY_SERVED.astype(str) + ' County'
        df['StateAbbrev'] = df.PRIMACY_AGENCY_CODE.astype(str)
        df.drop(['COUNTY_SERVED', 'PRIMACY_AGENCY_CODE'], axis = 1, inplace  = True)
        df.loc[df.counts.isnull(),'counts'] = 0
        del profile, violation
        return df
        
    @staticmethod
    def gen_violation(state:str, start_year: int):
        url_violation = f'https://data.epa.gov/efservice/VIOLATION/IS_HEALTH_BASED_IND/Y/PRIMACY_AGENCY_CODE/{state}/CSV'
        violation = pd.read_csv(url_violation)
        violation.columns = violation.columns.str.replace(re.compile('.*\.'),"")
        violation = violation.loc[violation.COMPL_PER_BEGIN_DATE.notnull() ,:]
        violation['date'] = pd.to_datetime(violation.COMPL_PER_BEGIN_DATE)
        violation = violation.loc[violation.date.dt.year >= start_year, :].reset_index(drop = True)
        violation['indicator'] = 1
        return violation

    @staticmethod
    def gen_profile(state:str):
        url_systems = f'https://data.epa.gov/efservice/GEOGRAPHIC_AREA/PWSID/BEGINNING/{state}/CSV'
        profile = pd.read_csv(url_systems)
        if len(profile.index) == 10001:
            url_systems2 = f'https://data.epa.gov/efservice/GEOGRAPHIC_AREA/PWSID/BEGINNING/{state}/rows/10001:20000/CSV'
            profile2 = pd.read_csv(url_systems2)
            profile = pd.concat([profile,profile2]).reset_index(drop=True)
            del profile2
        profile.columns = profile.columns.str.replace(re.compile('.*\.'),"")
        profile = profile.loc[profile['PWS_TYPE_CODE'] == 'CWS']
        profile = profile.assign(COUNTY_SERVED = profile.COUNTY_SERVED.str.split(',')).explode('COUNTY_SERVED')
        return profile

        if isinstance(self.location, str):
            assert len(self.location) == 2
            self.profile = gen_profile(location)
            self.violation = gen_violation(location)
        else:
            self.profile = {}
            self.violation = {}
            for loc in self.location:
                assert len(loc) == 2
                self.profile[loc] = gen_profile(loc)
                self.violation[loc] = gen_violation(loc)

                
                
############################################################################
## Food Desert      ########################################################
############################################################################

class food_desert:
    def __init__(self, state_fips: Union[ str,  List[str]]):
        response = requests.get('https://www.ers.usda.gov/data-products/food-access-research-atlas/download-the-data/')
        soup = BeautifulSoup(response.content, "html.parser")
        hrefs = soup.find_all('a', href = True)
        url_path_series = pd.Series([x['href'] for x in hrefs])
        url = url_path_series[url_path_series.str.match(re.compile('.*FoodAccessResearchAtlasData.*', flags = re.I))].values[0]
        self.path = f'https://www.ers.usda.gov{url}'
        self.state_fips = state_fips
        
    @property
    def food_desert_data(self):
        if hasattr(self, '_food_desert_data'):
            pass
        else:
            self._food_desert_data = self.download_data(self.state_fips)
        return self._food_desert_data
    
    def download_data(self, state):
        from tqdm import tqdm
        import os
        resp = requests.get(self.path, stream=True)
        total = int(resp.headers.get('content-length', 0))
        fname = os.path.join(getcwd(), 'food_desert.xlsx')
        chunk_size = int(1024*1024/2)
        with open(fname, 'wb') as file, tqdm(
            desc="downloading food desert data file",
            total=total,
            unit='iB',
            unit_scale=True,
            unit_divisor=1024,
            leave = True
        ) as bar:
            for data in resp.iter_content(chunk_size=chunk_size):
                size = file.write(data)
                bar.update(size)

        df = pd.read_excel(fname, engine = 'openpyxl', sheet_name = 2)
        df['State'] = df.CensusTract.apply(lambda x: str(x)[:2])
        if isinstance(state, str):
            assert len(state) == 2
            df = df.loc[df.State.eq(state)].reset_index(drop = True)
        else:
            for s in state:
                assert len(s) == 2
            df = df.loc[df.State.isin(state)].reset_index(drop = True)
        df = df[['CensusTract', 'LILATracts_Vehicle', 'OHU2010']]
        df2 = df.copy()
        data_dictionary = {}
        # Tract
        df.rename(columns = {'CensusTract':'FIPS'}, inplace = True)
        df['FIPS'] = df.FIPS.astype(str)
        data_dictionary['Tract'] = df
        # County
        df2['FIPS'] = [str(x)[:5] for x in df2.CensusTract]
        df2 = df2[['FIPS','LILATracts_Vehicle','OHU2010']].groupby('FIPS', as_index = False).apply(lambda x: pd.Series(np.average(x['LILATracts_Vehicle'], weights=x['OHU2010'])))
        df2.columns = ['FIPS','LILATracts_Vehicle']
        df2['FIPS'] = df2.FIPS.astype(str)
        data_dictionary['County'] = df2
        remove(fname)
        
        return data_dictionary
    
    
    
############################################################################
## scp_cancer_data      ####################################################   -> multiprocessing
############################################################################

# reference (incidence) : https://www.statecancerprofiles.cancer.gov/incidencerates/index.php
# reference (mortality) : https://www.statecancerprofiles.cancer.gov/deathrates/index.php
    
@dataclass
class scp_cancer_data:
    state_fips : Union[str, List[str]] #
        
    @property
    def cancer_data(self):
        if hasattr(self, '_cancer_data'):
            pass
        else:
            data_dict = {}
            data_dict['incidence'] = self.scp_cancer_inc()
            data_dict['mortality'] = self.scp_cancer_mor()
            self._cancer_data = data_dict
        return self._cancer_data
        
        
    def scp_cancer_inc(self):
        sites = {'001': 'All Site', '071': 'Bladder', '076': 'Brain & ONS', '020': 'Colon & Rectum', '017': 'Esophagus', 
                 '072': 'Kidney & Renal Pelvis', '090': 'Leukemia', '035': 'Liver & IBD', '047': 'Lung & Bronchus',
                 '053': 'Melanoma of the Skin', '086': 'Non-Hodgkin Lymphoma', '003': 'Oral Cavity & Pharynx', '040': 'Pancreas',
                 '018': 'Stomach', '080': 'Thyroid'}

        sitesf = {'055': 'Female Breast', '057': 'Cervix', '061': 'Ovary', '058': 'Corpus Uteri & Uterus, NOS'}
        
        sitesm = {'066': 'Prostate'}
        
        gen_single_cancer_inc_all = partial(self.gen_single_cancer_inc, sex = '0')
        gen_single_cancer_inc_male = partial(self.gen_single_cancer_inc, sex = '1')
        gen_single_cancer_inc_female = partial(self.gen_single_cancer_inc, sex = '2')
        if isinstance(self.state_fips, str):
            incidence_all = Parallel(n_jobs=-1)(
                delayed(gen_single_cancer_inc_all)(
                    state, site[0], site[1]) for state, site in product([self.state_fips],sites.items()))
            incidence_female = Parallel(n_jobs=-1)(
                delayed(gen_single_cancer_inc_female)(
                    state, site[0], site[1]) for state, site in product([self.state_fips],sitesf.items()))
            incidence_male = Parallel(n_jobs=-1)(
                delayed(gen_single_cancer_inc_male)(
                    state, site[0], site[1]) for state, site in product([self.state_fips],sitesm.items()))
        else:
            incidence_all = Parallel(n_jobs=-1)(
                delayed(gen_single_cancer_inc_all)(
                    state, site[0], site[1]) for state, site in product(self.state_fips,sites.items()))
            incidence_female = Parallel(n_jobs=-1)(
                delayed(gen_single_cancer_inc_female)(
                    state, site[0], site[1]) for state, site in product(self.state_fips,sitesf.items()))
            incidence_male = Parallel(n_jobs=-1)(
                delayed(gen_single_cancer_inc_male)(
                    state, site[0], site[1]) for state, site in product(self.state_fips,sitesm.items()))
        df = pd.concat(incidence_all + incidence_female + incidence_male, axis = 0).reset_index(drop = True)
        return df

        
        
    @staticmethod
    def gen_single_cancer_inc(state:str, cancer_site_id:str, cancer_site:str, sex:int):
        assert len(state) == 2
        assert state.isnumeric()
        assert sex in list('012')
        assert len(cancer_site_id) == 3
        path = f'https://www.statecancerprofiles.cancer.gov/incidencerates/index.php?stateFIPS={state}&areatype=county&cancer={cancer_site_id}&race=00&sex={sex}&age=001&stage=999&year=0&type=incd&sortVariableName=rate&sortOrder=desc&output=1'
        df = pd.read_csv(path, skiprows=11, header=None, usecols=[0,1,2,8],  names=['County', 'FIPS', 'AAR', 'AAC'],
                         dtype={'County':str, 'FIPS':str}).dropna()
        df['County'] = df['County'].map(lambda x: x.rstrip('\(0123456789\)'))
        df['Site'] = cancer_site
        df['Type'] = 'Incidence'
        df = df[['FIPS', 'County', 'Site', 'Type', 'AAR', 'AAC']]
        return df
        

    def scp_cancer_mor(self):
        sites = {'001': 'All Site', '071': 'Bladder', '076': 'Brain & ONS', '020': 'Colon & Rectum', '017': 'Esophagus', 
                 '072': 'Kidney & Renal Pelvis', '090': 'Leukemia', '035': 'Liver & IBD', '047': 'Lung & Bronchus',
                 '053': 'Melanoma of the Skin', '086': 'Non-Hodgkin Lymphoma', '003': 'Oral Cavity & Pharynx', '040': 'Pancreas',
                 '018': 'Stomach', '080': 'Thyroid'}

        sitesf = {'055': 'Female Breast', '057': 'Cervix', '061': 'Ovary', '058': 'Corpus Uteri & Uterus, NOS'}
        
        sitesm = {'066': 'Prostate'}
        
        gen_single_cancer_mor_all = partial(self.gen_single_cancer_mor, sex = '0')
        gen_single_cancer_mor_male = partial(self.gen_single_cancer_mor, sex = '1')
        gen_single_cancer_mor_female = partial(self.gen_single_cancer_mor, sex = '2')
        if isinstance(self.state_fips, str):
            mortality_all = Parallel(n_jobs=-1)(
                delayed(gen_single_cancer_mor_all)(
                    state, site[0], site[1]) for state, site in product([self.state_fips],sites.items()))
            mortality_female = Parallel(n_jobs=-1)(
                delayed(gen_single_cancer_mor_female)(
                    state, site[0], site[1]) for state, site in product([self.state_fips],sitesf.items()))
            mortality_male = Parallel(n_jobs=-1)(
                delayed(gen_single_cancer_mor_male)(
                    state, site[0], site[1]) for state, site in product([self.state_fips],sitesm.items()))
        else:
            mortality_all = Parallel(n_jobs=-1)(
                delayed(gen_single_cancer_mor_all)(
                    state, site[0], site[1]) for state, site in product(self.state_fips,sites.items()))
            mortality_female = Parallel(n_jobs=-1)(
                delayed(gen_single_cancer_mor_female)(
                    state, site[0], site[1]) for state, site in product(self.state_fips,sitesf.items()))
            mortality_male = Parallel(n_jobs=-1)(
                delayed(gen_single_cancer_mor_male)(
                    state, site[0], site[1]) for state, site in product(self.state_fips,sitesm.items()))
        df = pd.concat(mortality_all + mortality_female + mortality_male, axis = 0).reset_index(drop = True)
        return df

        
    @staticmethod
    def gen_single_cancer_mor(state:str, cancer_site_id:str, cancer_site:str, sex:int):
        assert len(state) == 2
        assert state.isnumeric()
        assert sex in list('012')
        assert len(cancer_site_id) == 3
        path = f'https://www.statecancerprofiles.cancer.gov/deathrates/index.php?stateFIPS={state}&areatype=county&cancer={cancer_site_id}&race=00&sex={sex}&age=001&year=0&type=death&sortVariableName=rate&sortOrder=desc&output=1'
        df = pd.read_csv(path, skiprows=11, header=None, usecols=[0,1,2,8],  names=['County', 'FIPS', 'AAR', 'AAC'],
                         dtype={'County':str, 'FIPS':str}).dropna()
        df['County'] = df['County'].map(lambda x: x.rstrip('\(0123456789\)'))
        df['Site'] = cancer_site
        df['Type'] = 'Incidence'
        df = df[['FIPS', 'County', 'Site', 'Type', 'AAR', 'AAC']]
        return df

##################################################################
## CDC PlACES (county/tract level risk factors and screening data)
##################################################################


@dataclass
class places_data:
    state: Union[str, List[str]] # example: KY
    config: SocrataConfig
    
    
    
    @property
    def places_data(self):
        if hasattr(self, '_places_data'):
            pass
        else:
            self._places_data = {'county': self.places_county(), 'tract':self.places_tract()}
        return self._places_data
        
        
    def places_county(self):
        if isinstance(self.state, str):
            results = self.config.client.get("i46a-9kgh", where=f'stateabbr="{self.state}"')
        else:
            state = '("' + '","'.join(self.state) + '")'
            print(state)
            results = self.config.client.get("i46a-9kgh", where=f'stateabbr in {state}')
        results_df = pd.DataFrame.from_records(results)
        results_df2 = results_df.loc[:, results_df.columns.isin(['countyfips', 'countyname', 'stateabbr', 'cancer_crudeprev', 
                                  'cervical_crudeprev', 'colon_screen_crudeprev',
                                  'csmoking_crudeprev', 'mammouse_crudeprev', 'obesity_crudeprev'])]

        results_df3 = results_df2.rename(columns={'countyfips': 'FIPS', 'countyname': 'County', 'stateabbr': 'State', 
                                                  'cancer_crudeprev': 'Cancer_Prevalence','cervical_crudeprev': 'Met_Cervical_Screen',
                                                  'colon_screen_crudeprev': 'Met_Colon_Screen', 
                                                  'mammouse_crudeprev': 'Met_Breast_Screen', 'csmoking_crudeprev': 'Currently_Smoke',
                                                  'obesity_crudeprev': 'BMI_Obese'})
        del results_df, results_df2
        return results_df3
        
    def places_tract(self):
        if isinstance(self.state, str):
            results = self.config.client.get("yjkw-uj5s", where=f'stateabbr="{self.state}"', limit = "10000")
        else:
            state = '("' + '","'.join(self.state) + '")'            
            results = self.config.client.get("yjkw-uj5s", where=f'stateabbr in {state}', limit = "10000")

        results_df = pd.DataFrame.from_records(results)
        results_df2 = results_df.loc[:, results_df.columns.isin(['tractfips', 'countyfips', 'countyname', 
                                                                 'stateabbr', 'cancer_crudeprev', 
                                                                 'colon_screen_crudeprev', 'csmoking_crudeprev', 
                                                                 'mammouse_crudeprev', 'obesity_crudeprev'])]
        
        results_df3 = results_df2.rename(columns={'tractfips': 'FIPS', 'countyfips': 'FIPS5', 
                                                  'countyname': 'County',  'stateabbr': 'State', 
                                                  'cancer_crudeprev': 'Cancer_Prevalence',
                                                  'colon_screen_crudeprev': 'Met_Colon_Screen', 
                                                  'mammouse_crudeprev': 'Met_Breast_Screen', 
                                                  'csmoking_crudeprev': 'Currently_Smoke',
                                                  'obesity_crudeprev': 'BMI_Obese'})
        
        del results_df, results_df2
        return results_df3
