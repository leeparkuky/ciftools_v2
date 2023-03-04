import pickle
import pandas as pd
import geopandas as gpd
import numpy as np
from utils import stateDf, census_shape
from datetime import datetime as dt
from functools import partial




def open_pickle_file(data_file_path):
# data_file_path = 'cif_raw_data.pickle'
    with open(data_file_path, 'rb') as f:
        data_dictionary = pickle.load(f)
    return data_dictionary

def open_ca_file(ca_file_path):
# ca_file_path = './uky_ca.csv'
    ca = pd.read_csv(ca_file_path, dtype={'FIPS':str})
    ca['FIPS'] = ca.FIPS.str.zfill(5)
    if all(ca.County.str.contains('.+\sCounty')):
        pass
    elif all(ca.County.str.contains('.+\sParish')):
        pass
    else:
        ca['County'] = ca.County + ' County'
    return ca


def select_area_for_catchment_area_full(df, query_level, ca):
    if hasattr(df, 'County'):
        if df.County[0][-6:].lower() != 'county':
            df['County'] = df.County + ' County'
    
    if query_level == 'county':
        return df.loc[df.FIPS.isin(ca.FIPS), :].reset_index(drop = True)
    elif query_level in ['county subdivision','tract','block']:
        if hasattr(df, 'FIPS5'):
            df = df.loc[df.FIPS5.isin(ca.FIPS), :].reset_index(drop = True)
            df.drop('FIPS5', axis = 1, inplace = True)
        else:
            df = df.loc[df.FIPS.str[:5].isin(ca.FIPS), :].reset_index(drop = True)
        return df

def merge_all(*args, query_level = 'county'):
    if query_level == 'county':
        geo_col = ['FIPS','County','State']
    elif query_level == 'tract':
        geo_col = ['FIPS','Tract','County','State']
    datasets_to_merge = []
    datasets_not_to_merge = []
    columns_to_be_used_later = []
    for df in args:
        if all([hasattr(df, col) for col in geo_col]):
            datasets_to_merge.append(df)
        else:
            datasets_not_to_merge.append(df)
            columns_to_be_used_later.append(pd.Series(geo_col)[[hasattr(df, col) for col in geo_col]].tolist())
    for i, df in enumerate(datasets_to_merge):
        if i == 0:
            for col in geo_col:
                assert hasattr(df, col)
            output = df.copy()
        else:
            if df.shape[0] > output.shape[0]:
                if all([ hasattr(df, col) for col in geo_col ]):
                    output = output.merge(df, how= 'right', on = geo_col)
                else:
                    output = output.merge(df, how = 'left', on = geo_col)
            else:
                output = output.merge(df, how = 'left', on = geo_col)
    if len(datasets_not_to_merge):
        for df, merge_on_col in zip(datasets_not_to_merge, columns_to_be_used_later):
            output = output.merge(df, how = 'left', on = merge_on_col)
    return output

def organize_table(topic_variables, query_level, column_names_dict = None, columns_to_drop = None):
    topic_dataset = [select_area_for_catchment_area(data_dictionary[query_level][topic], query_level) for topic in topic_variables if topic in data_dictionary[query_level].keys()]
    df = merge_all(*topic_dataset, query_level = query_level)
    if column_names_dict:
        df = df.rename(columns = colnames)
    if columns_to_drop:
        df = df.drop(columns = columns_to_drop, axis = 1)
    return df



def write_excel_file(cdata, full_path, full_path2):
    from pandas import ExcelWriter
    from datetime import datetime as dt

    cdata_keys = ['rf_and_screening_county', 'rf_and_screening_county_long', 
                  'rf_and_screening_tract', 'rf_and_screening_tract_long', 
                  'cancer_incidence', 'cancer_incidence_long', 
                  'cancer_mortality', 'cancer_mortality_long', 
                  'economy_county', 'economy_county_long', 
                  'economy_tract', 'economy_tract_long', 
                  'ht_county', 'ht_county_long', 'ht_tract', 'ht_tract_long', 
                  'sociodemographics_county', 'sd_county_long', 
                  'sociodemographics_tract', 'sd_tract_long', 
                  'environment_county', 'environment_county_long', 
                  'environment_tract', 'environment_tract_long', 
                  'facilities_and_providers']
    
    for name in cdata.keys():
        assert name in cdata_keys
    
    with ExcelWriter(full_path, mode = 'w') as writer:
        print('Writing wide data to file...')
        pd.read_csv('CIFTools_Documentation.csv', 
                    header = None, encoding = "ISO-8859-1").to_excel(writer, header = None, 
                                                                     sheet_name = 'Variables and Sources', index = False)
        cdata['cancer_incidence'].to_excel(writer, sheet_name = 'Cancer Incidence', index = True)
        cdata['cancer_mortality'].to_excel(writer, sheet_name = 'Cancer Mortality', index = True)
        cdata['economy_county'].to_excel(writer, sheet_name = 'Economy (County)', index = False)
        cdata['economy_tract'].to_excel(writer, sheet_name = 'Economy (Tract)', index = False)
        cdata['environment_county'].to_excel(writer, sheet_name = 'Environment (County)', index = False)
        cdata['environment_tract'].to_excel(writer, sheet_name = 'Environment (Tract)', index = False)
        #cdata['broadband_speeds'].to_excel(writer, sheet_name = 'Broadband Speeds', index = False) #can be too long in some areas
        cdata['ht_county'].to_excel(writer, sheet_name = 'H and T (County)', index = False)
        cdata['ht_tract'].to_excel(writer, sheet_name= 'H and T (Tract)', index = False)
        cdata['rf_and_screening_county'].to_excel(writer, sheet_name= 'RF and Screening (County)', index=True)
        cdata['rf_and_screening_tract'].to_excel(writer, sheet_name= 'RF and Screening (Tract)', index=True)
        cdata['sociodemographics_county'].to_excel(writer, sheet_name = 'Sociodemographic (County)', index = False)
        cdata['sociodemographics_tract'].to_excel(writer, sheet_name = 'Sociodemographic (Tract)', index = False)
        cdata['facilities_and_providers'].to_excel(writer, sheet_name = 'Facilities', index = False)

    with ExcelWriter(full_path2, mode = 'w') as writer:
        print('Writing long data to file...')
        pd.read_csv('CIFTools_Documentation.csv', 
                    header = None, encoding = "ISO-8859-1").to_excel(writer, header = None, 
                                                                     sheet_name = 'Variables and Sources', index = False)
        cdata['cancer_incidence_long'].to_excel(writer, sheet_name = 'Cancer Incidence', index = True)
        cdata['cancer_mortality_long'].to_excel(writer, sheet_name = 'Cancer Mortality', index = True)
        cdata['economy_county_long'].to_excel(writer, sheet_name = 'Economy (County)', index = False)
        cdata['economy_tract_long'].to_excel(writer, sheet_name = 'Economy (Tract)', index = False)
        cdata['environment_county_long'].to_excel(writer, sheet_name = 'Environment (County)', index = False)
        cdata['environment_tract_long'].to_excel(writer, sheet_name = 'Environment (Tract)', index = False)
        #cdata['broadband_speeds'].to_excel(writer, sheet_name = 'Broadband Speeds', index = False) # can be too long in some areas
        cdata['ht_county_long'].to_excel(writer, sheet_name = 'H and T (County)', index = False)
        cdata['ht_tract_long'].to_excel(writer, sheet_name= 'H and T (Tract)', index = False)
        cdata['rf_and_screening_county_long'].to_excel(writer, sheet_name= 'RF and Screening (County)', 
                                                        index=True)
        cdata['rf_and_screening_tract_long'].to_excel(writer, sheet_name= 'RF and Screening (Tract)', 
                                                      index=True)
        cdata['sd_county_long'].to_excel(writer, sheet_name = 'Sociodemographic (County)', index = False)
        cdata['sd_tract_long'].to_excel(writer, sheet_name = 'Sociodemographic (Tract)', index = False)
        cdata['facilities_and_providers'].to_excel(writer, sheet_name = 'Facilities', index = False)
    
    return

def save_as_csvs(cdata, path2):
    cdata_keys = ['rf_and_screening_county', 'rf_and_screening_county_long', 
                  'rf_and_screening_tract', 'rf_and_screening_tract_long', 
                  'cancer_incidence', 'cancer_incidence_long', 
                  'cancer_mortality', 'cancer_mortality_long', 
                  'economy_county', 'economy_county_long', 
                  'economy_tract', 'economy_tract_long', 
                  'ht_county', 'ht_county_long', 'ht_tract', 'ht_tract_long', 
                  'sociodemographics_county', 'sd_county_long', 
                  'sociodemographics_tract', 'sd_tract_long', 
                  'environment_county', 'environment_county_long', 
                  'environment_tract', 'environment_tract_long', 
                  'facilities_and_providers']
    
    for name in cdata.keys():
        assert name in cdata_keys

    os.chdir(path2)

    cdata['cancer_incidence'].to_csv(ca_name + '_cancer_incidence_county_' + today + '.csv', encoding='utf-8', index=True)
    cdata['cancer_mortality'].to_csv(ca_name + '_cancer_mortality_county_' + today + '.csv', encoding='utf-8', index=True)
    cdata['cancer_incidence_long'].to_csv(ca_name + '_cancer_incidence_county_long_' + today + '.csv', encoding='utf-8', index=False)
    cdata['cancer_mortality_long'].to_csv(ca_name + '_cancer_mortality_county_long_' + today + '.csv', encoding='utf-8', index=False)
    cdata['economy_county'].to_csv(ca_name + '_economy_county_' + today + '.csv', encoding='utf-8', index=False)
    cdata['economy_county_long'].to_csv(ca_name + '_economy_county_long_' + today + '.csv', encoding='utf-8', index=False)
    cdata['economy_tract'].to_csv(ca_name + '_economy_tract_' + today + '.csv', encoding='utf-8', index=False)
    cdata['economy_tract_long'].to_csv(ca_name + '_economy_tract_long_' + today + '.csv', encoding='utf-8', index=False)
    cdata['environment_county'].to_csv(ca_name + '_environment_county_' + today + '.csv', encoding='utf-8', index=False)
    cdata['environment_county_long'].to_csv(ca_name + '_environment_county_long_' + today + '.csv', encoding='utf-8', index=False)
    cdata['environment_tract'].to_csv(ca_name + '_environment_tract_' + today + '.csv', encoding='utf-8', index=False)
    cdata['environment_tract_long'].to_csv(ca_name + '_environment_tract_long_' + today + '.csv', encoding='utf-8', index=False)
    cdata['ht_county'].to_csv(ca_name + '_housing_trans_county_' + today + '.csv', encoding='utf-8', index=False)
    cdata['ht_county_long'].to_csv(ca_name + '_housing_trans_county_long_' + today + '.csv', encoding='utf-8', index=False)
    cdata['ht_tract'].to_csv(ca_name + '_housing_trans_tract_' + today + '.csv', encoding='utf-8', index=False)
    cdata['ht_tract_long'].to_csv(ca_name + '_housing_trans_tract_long_' + today + '.csv', encoding='utf-8', index=False)
    cdata['rf_and_screening_county'].to_csv(ca_name + '_rf_and_screening_county_' + today + '.csv', encoding='utf-8', index=False)
    cdata['rf_and_screening_tract'].to_csv(ca_name + '_rf_and_screening_tract_' + today + '.csv', encoding='utf-8', index=False)
    cdata['rf_and_screening_county_long'].to_csv(ca_name + '_rf_and_screening_county_long_' + today + '.csv', encoding='utf-8', index=False)
    cdata['rf_and_screening_tract_long'].to_csv(ca_name + '_rf_and_screening_tract_long_' + today + '.csv', encoding='utf-8', index=False)
    cdata['sociodemographics_county'].to_csv(ca_name + '_sociodemographics_county_' + today + '.csv', encoding='utf-8', index=False)
    cdata['sd_county_long'].to_csv(ca_name + '_sociodemographics_county_long_' + today + '.csv', encoding='utf-8', index=False)
    cdata['sociodemographics_tract'].to_csv(ca_name + '_sociodemographics_tract_' + today + '.csv', encoding='utf-8', index=False)
    cdata['sd_tract_long'].to_csv(ca_name + '_sociodemographics_tract_long_' + today + '.csv', encoding='utf-8', index=False)
#     cdata['broadband_speeds'].to_csv(ca_name + '_broadband_speeds_' + today + '.csv', encoding='utf-8', index=False)
    cdata['facilities_and_providers'].to_csv(ca_name + '_facilities_and_providers_' + today + '.csv', encoding='utf-8', index=False)









if __name__ == '__main__':
    import argparse
    from tqdm import tqdm
    from glob import glob
    import time
    import os
    

    
    parser = argparse.ArgumentParser()
    # catchment_area_name
    parser.add_argument('--ca_name', help = 'catchment area name' ,required = True)

    # arguments for catchment_area_file
    parser.add_argument('--ca_file_path', help = 'catchment area csv file', required = True) #uky_ca.csv
    parser.add_argument('--pickle_data_path', help = 'The pickle CIF data is named as cif_raw_data.pickle', required = True)
    # --pickle_data_path 'cif_raw_data.pickle'
    # file type of the output
    parser.add_argument('--download_file_type', required = True, nargs = '+', choices = ['csv','pickle','excel'])

    args = parser.parse_args()
    
    ca_name = args.ca_name # args.cs_name will be used to create a folder to store cleaned data in a selected format
    ca_dir = args.ca_name.replace(" ", "_") + "_catchment_data"
    path2 = os.path.join(os.getcwd(), ca_dir)

    if os.path.exists(path2) == False:
        os.makedirs(path2) # if the folder has not yet been created, we create the folder

    pbar = tqdm(range(5), desc = "Transforming data to generate files for Cancer InFocus", leave = False)
    time.sleep(2)
    pbar.set_description("reading pickle data from CIF_Tool")

        
        
        
    if 'pickle' in args.download_file_type: # if pickle is the selected file format, we set the dataset name with pickle ext
        save_name = ca_name.replace(" ", "_") + '_catchment_data_' + dt.today().strftime('%m-%d-%Y') + '.pickle'
        save_name_spatial = ca_name.replace(" ", "_") + '_catchment_data_spatial_' + dt.today().strftime('%m-%d-%Y') + '.pickle'
        pickle_download_path = os.path.join(path2, save_name)
        pickle_download_path_spatial = os.path.join(path2, save_name_spatial)

    if 'excel' in args.download_file_type: # the same goes with excel, but it will create two separate excel files
        save_name = ca_name.replace(" ", "_") + '_catchment_data_' + dt.today().strftime('%m-%d-%Y') + '.xlsx'
        save_name2 = ca_name.replace(" ", "_") + '_catchment_data_long_' + dt.today().strftime('%m-%d-%Y') + '.xlsx'
        full_path = os.path.join(os.getcwd(), ca_dir, save_name)
        full_path2 = os.path.join(os.getcwd(), ca_dir, save_name2)
    if 'csv' in args.download_file_type: # if csv is chosen, it will create csvs by pandas DataFrame
        today = dt.today().strftime('%m-%d-%Y')

        
        
        
    #### importing data and ca_file (and shapes)
    data_dictionary = open_pickle_file(args.pickle_data_path) # First provide pickle data created by CIFTools
    ca = open_ca_file(args.ca_file_path)
    
    states_unique = ca.FIPS.str[:2].unique().tolist()
    years = [2015, 2019, 2021] # tract shape has changed in 2020
    shapes = census_shape(years, states_unique)
    
    
    # update tqdm
    pbar.update(1)
    pbar.set_description("transforming raw datasets")

    #### define select_area_for_catchment_area function
    select_area_for_catchment_area = partial(select_area_for_catchment_area_full, ca = ca)

    #### risk factor
    rfs_county = select_area_for_catchment_area(data_dictionary['county']['risk_and_screening'], 'county')
    rfs_tract = select_area_for_catchment_area(data_dictionary['tract']['risk_and_screening'], 'tract')    
    rfs_county_l = pd.melt(rfs_county, id_vars=['FIPS', 'County', 'State'], 
                         var_name='measure', value_name='value')
    rfs_tract_l = pd.melt(rfs_tract, id_vars=['FIPS', 'County', 'State'], 
                         var_name='measure', value_name='value')

    
    #### cancer data
    cancer_inc_l = data_dictionary['cancer']['incidence'].copy()
    cancer_inc_l = select_area_for_catchment_area(cancer_inc_l, 'county')
    cancer_inc_l = cancer_inc_l[['FIPS', 'County', 'State', 'Type', 'Site', 'AAR', 'AAC']]
    cancer_inc = pd.pivot(cancer_inc_l, index=['FIPS', 'County', 'State', 'Type'], columns='Site', values='AAR').reset_index()
    cancer_mor_l = data_dictionary['cancer']['mortality'].copy()
    cancer_mor_l = select_area_for_catchment_area(cancer_mor_l, 'county')
    cancer_mor_l = cancer_mor_l[['FIPS', 'County', 'State', 'Type', 'Site', 'AAR', 'AAC']]
    cancer_mor = pd.pivot(cancer_mor_l, index=['FIPS', 'County', 'State', 'Type'], columns='Site', values='AAR').reset_index()
    
    
    
    
    #### econ
    econ_topics = ['insurance','gini_index','income','employment','poverty','bls_unemployment']
    colnames = {'Labor Force Participation Rate': f'Annual Labor Force Participation Rate (2015-2019)',
            'Unemployment Rate' : f'Annual Unemployment Rate (2015-2019)',
            'health_insurance_coverage_rate': 'Insurance Coverage',
            'Gini Index': 'Gini Coefficient',
            'median_income_all': 'Household Income',
            'medicaid' : 'Medicaid Enrollment',
            'below_poverty' : 'Below Poverty'
            }
    drop_col = ['below_poverty_x.5', 'below_poverty_x2']
    econ_county = organize_table(econ_topics, 'county', colnames, drop_col)
    econ_tract  = organize_table(econ_topics, 'tract', colnames, drop_col)
    econ_county['Uninsured'] = 1 - econ_county['Insurance Coverage']
    econ_tract['Uninsured'] = 1 - econ_tract['Insurance Coverage']
    econ_county_l = pd.melt(econ_county, id_vars = ['FIPS', 'County', 'State'], 
                        var_name = 'measure', value_name = 'value')
    econ_tract_l = pd.melt(econ_tract, id_vars = ['FIPS', 'Tract', 'County','State'], 
                            var_name = 'measure', value_name = 'value')
    
    #### ht
    ht_topic = ['vacancy','transportation']
    colnames = {'vacancy_rate': 'Vacancy Rate', 
                'no_vehicle': 'No Vehicle',
                'rent_over_40':'Rent Burden (40% Income)'}
    cols_to_drop = ['two_or_more_vehicle','three_or_more_vehicle']
    ht_county = organize_table(ht_topic, 'county', colnames, cols_to_drop)
    ht_tract = organize_table(ht_topic, 'tract', colnames, cols_to_drop)
    ht_county_l = pd.melt(ht_county, id_vars = ['FIPS', 'County', 'State'], 
                            var_name = 'measure', value_name = 'value')
    ht_tract_l = pd.melt(ht_tract, id_vars = ['FIPS','Tract','County','State'], 
                            var_name = 'measure', value_name = 'value')
    
    #### sociodemographic
    socio_topic = ['demographic_age','demographic_race','education','urban_rural']
    sociodemo_county = organize_table(socio_topic, 'county')
    sociodemo_tract = organize_table(socio_topic, 'tract')
    sd_county_l = pd.melt(sociodemo_county, id_vars = ['FIPS', 'County', 'State'], 
                            var_name = 'measure', value_name = 'value')
    sd_tract_l = pd.melt(sociodemo_tract, id_vars = ['FIPS','Tract','County','State'], 
                            var_name = 'measure', value_name = 'value')
    
    #### env
    env_topic = ['water_violation','food_desert']
    data_dictionary['county']['water_violation'] = data_dictionary['county']['vacancy'].merge(data_dictionary['county']['water_violation'], on = ['County','State'], how = 'left').sort_values('FIPS').reset_index(drop = True)
    data_dictionary['county']['water_violation'] = data_dictionary['county']['water_violation'].drop('vacancy_rate', axis = 1)
    # food desert tracts are outdated (2010 ver.)
    data_dictionary['tract']['food_desert'].FIPS = data_dictionary['tract']['food_desert'].FIPS.str.zfill(11)
    data_dictionary['tract']['food_desert'] = select_area_for_catchment_area(data_dictionary['tract']['food_desert'], 'tract')
    all_counties = data_dictionary['county']['demographic_age'][['FIPS','County','State']]
    county_states = data_dictionary['tract']['food_desert'].FIPS.str[:5].apply(lambda x: all_counties.loc[all_counties.FIPS.eq(x),['County','State']].values.tolist())
    data_dictionary['tract']['food_desert'][['County','State']] = [x[0] for x in county_states.tolist()]
    env_tract = data_dictionary['tract']['food_desert'].merge(shapes['tract_shape'], how = 'left')
    env_tract = env_tract[['FIPS', 'Tract', 'County', 'State', 'LILATracts_Vehicle']].reset_index(drop = True)
#     data_dictionary['tract']['vacancy'].merge(data_dictionary['tract']['food_desert'], how = 'left').drop(['vacancy_rate'],axis = 1)
    env_county = organize_table(env_topic, 'county')
#     env_tract = organize_table(env_topic, 'tract')
    env_county_l = pd.melt(env_county, id_vars = ['FIPS', 'County', 'State'], 
                            var_name = 'measure', value_name = 'value')
    env_tract_l = pd.melt(env_tract, id_vars = ['FIPS','Tract','County','State'], 
                            var_name = 'measure', value_name = 'value')
    
    #### facility
    superfund = data_dictionary['facility']['superfund']
    superfund = select_area_for_catchment_area(superfund, 'tract') # it has FIPS5
    point_df = pd.concat([data_dictionary['facility']['all'], superfund], axis = 0).sort_values('Type').reset_index(drop = True)

    
    #### cdata
    cdata = {'rf_and_screening_county': rfs_county, 'rf_and_screening_county_long': rfs_county_l,
                'rf_and_screening_tract': rfs_tract, 'rf_and_screening_tract_long': rfs_tract_l,
                'cancer_incidence': cancer_inc, 'cancer_incidence_long': cancer_inc_l,
                'cancer_mortality': cancer_mor, 'cancer_mortality_long': cancer_mor_l,
                'economy_county': econ_county, 'economy_county_long': econ_county_l,
                'economy_tract': econ_tract, 'economy_tract_long': econ_tract_l,
                'ht_county': ht_county, 'ht_county_long': ht_county_l, 
                'ht_tract': ht_tract, 'ht_tract_long': ht_tract_l, 
                'sociodemographics_county': sociodemo_county, 'sd_county_long': sd_county_l,
                'sociodemographics_tract': sociodemo_tract, 'sd_tract_long': sd_tract_l,
                'environment_county': env_county, 'environment_county_long': env_county_l,
                'environment_tract': env_tract, 'environment_tract_long': env_tract_l,
                 'facilities_and_providers': point_df}
    
    #### cdata_with_spatial_information
    cdata_spatial = {}
    for table_name, df in cdata.items():
        if table_name[-6:] == 'county':
            df = df.merge(shapes['county_shape'], how = 'left')
            cdata_spatial[table_name] = df
        elif table_name[-5:] == 'tract':
            df = df.merge(shapes['tract_shape'], how = 'left')
            cdata_spatial[table_name] = df
    

    pbar.update(1)
    pbar.set_description("saving datasets")

    if 'pickle' in args.download_file_type:
        with open(pickle_download_path, 'wb') as dataset:
            pickle.dump(cdata, dataset, protocol=pickle.HIGHEST_PROTOCOL)
        print(f'dataset is stored at {pickle_download_path}')
        
        with open(pickle_download_path_spatial, 'wb') as dataset:
            pickle.dump(cdata_spatial, dataset, protocol=pickle.HIGHEST_PROTOCOL)
        print(f'spatial dataset is stored at {pickle_download_path_spatial}')
        
        pbar.update(1)
        pbar.set_description("pickle file is saved")
    else:
        pbar.update(1)

        
    if 'excel' in args.download_file_type:
        write_excel_file(cdata, full_path, full_path2)
        pbar.update(1)
        pbar.set_description("excel file is saved")
    else:
        pbar.update(1)

    if 'csv' in args.download_file_type:
        save_as_csvs(cdata, path2)
        pbar.update(1)
        pbar.set_description("csv file is saved")
    else:
        pbar.update(1)
        
    pbar.set_description("check the saved data file(s)")

        
        
    