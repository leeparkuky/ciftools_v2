import re
import functools
import pandas as pd
import numpy as np
from CIF_Config import ACSConfig
from CIFTools_ACS import acs_data, custom_acs_data
from functools import partial
from dataclasses import dataclass
from typing import Union, List
from joblib import Parallel, delayed



sdoh_groups = {'demo_all': 'B01001', 'demo_White':'B01001H', 'demo_Black':'B01001B',
               'demo_Hispanic': 'B01001I', 'demo_Asian': 'B01001D', 'education':'B15003','employment':'B23025',
               'gini_index': 'B19083', 'rent_to_income':'B25070', 'tenure': 'B25008',
               'vacancy':'B25002','years_built':'B25034', 'median_income_all': 'B19013', 
               'median_income_white': 'B19013H', 'median_income_black': 'B19013B',
               'median_income_hispanic': 'B19013I','insurance':'B27001',
               'transportation_means': 'B08141', 'poverty': 'B17026', 
               'computer':'B28003', 'internet': 'B28011', 'public_assistance':'B19058',
               'medicaid':'C27007'
               } 

"""
    year: Union[str, int]
    state_fips: Union[str, int]
    query_level: str        
    acs_group: str
    acs_type: str = None
"""

key = '6888c47d7cd57defbcd47c6e6b71df8c70f6cccc'

@dataclass
class sdoh:
    year: Union[str, int]
    state_fips: Union[str, int]
    query_level: Union[List[str], str]        
    key: str
        
        
      
    
    def download_all(self):
        res = Parallel(n_jobs=-1)(delayed(fun)() for fun in self.functions.values())
        return {key:val for key, val in zip(self.functions.keys(), res)}
    
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
    
# 
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


