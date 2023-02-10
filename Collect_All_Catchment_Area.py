import argparse
from tqdm import tqdm
from glob import glob
import time
import os
from utils import write_bash_script
import subprocess
import pandas as pd
import pickle
# multi-processing
from joblib import Parallel, delayed
from datetime import datetime as dt
import shutil
from functools import partial


today = dt.today().strftime('%m-%d-%Y')


def gen_zip_file(cancer_center_name_abb, catchment_area_df, data_dictionary):
    # make sure you have a folder "data" before calling this function
    directory_path = os.path.join(os.getcwd(), cancer_center_name_abb) # folder that will contain csv files
    ca_name = os.path.join(os.getcwd(), cancer_center_name_abb, cancer_center_name_abb) # will be used for file_name
    os.mkdir(cancer_center_name_abb) #create the folder
    cancer_center_fips = catchment_area_df.loc[catchment_area_df.name_short.eq(cancer_center_name_abb),:].FIPS.tolist()
    cancer_center_state = catchment_area_df.loc[catchment_area_df.name_short.eq(cancer_center_name_abb),:].State.unique().tolist()
    cdata = {}
    for table_name, df in data_dictionary.items():
        if 'tract' in table_name:
            df['FIPS'] = df.FIPS.str.zfill(10)
        elif 'county' in table_name:
            df['FIPS'] = df.FIPS.str.zfill(5)
        if 'facilities' in table_name:
            cdata[table_name] = df.loc[df.State.isin(cancer_center_state),:]
        else:
            cdata[table_name] = df.loc[df.FIPS.str[:5].isin(cancer_center_fips),:]
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
    
    shutil.make_archive(os.path.join(os.getcwd(), 'data', cancer_center_name_abb), 'zip', directory_path)
    shutil.rmtree(directory_path)
    return 1
    
    
    









if __name__ == '__main__':
    
    bash_script_kwargs = {
    "bash_file_name" : 'all_catchment_areas.sh', #the name of a bash file to run
    "catchment_area_name": "all", # the name of the catchment area name
    "ca_file_path": "all_catchment_areas.csv",
    "query_level" : ['county','tract'],
    "acs_year"    : 2019,
    "download_file_type": ['pickle'],
    "census_api_key": 'f1a4c4de1f35fe90fc1ceb60fd97b39c9a96e436',
    "generate_zip_file" : False,
    "install_packages" : False, # We already installed required packages above,
    "socrata_user_name": "ciodata@uky.edu",
    "socrata_password" : "MarkeyCancer123!"
}
    
    write_bash_script(**bash_script_kwargs)
    
    
    
    subprocess.run(["bash", "all_catchment_areas.sh"])

    ca_path = glob('*/all_catchment_areas.csv')[0]
    ca = pd.read_csv(ca_path, dtype = {"FIPS":str})
    ca['FIPS'] = ca.FIPS.str.zfill(5)
    pickle_path = glob('*/all_catchment_data_*.pickle')[0]
    with open(pickle_path, 'rb') as f:
        data_dictionary = pickle.load(f)
    if os.path.exists(os.path.join(os.getcwd(), 'data')):
        pass
    else:
        os.mkdir('data')
    
    gen_zip_file_partial = partial(gen_zip_file, catchment_area_df = ca, data_dictionary = data_dictionary)
    
    
    Parallel(n_jobs = -1)(delayed(gen_zip_file_partial)(abb) for abb in ca.name_short.unique().tolist())
    
    os.remove('cif_raw_data.pickle');
    os.remove('all_catchment_areas.sh')
    
#     for abb in ca.name_short:
#         gen_zip_file(abb, ca, data_dictionary)

