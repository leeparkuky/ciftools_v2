import urllib
from io import BytesIO
from zipfile import ZipFile
import pandas as pd
import re
from typing import Union, List
from joblib import Parallel, delayed
from glob import glob
from tqdm import tqdm

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


def hpsa(location: Union[str, List[str]]):
    df= pd.read_excel('https://data.hrsa.gov/DataDownload/DD_Files/BCD_HPSA_FCT_DET_PC.xlsx')
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


def fqhc(location: Union[str, List[str]]):
    df= pd.read_csv('https://data.hrsa.gov//DataDownload/DD_Files/Health_Center_Service_Delivery_and_LookAlike_Sites.csv')
    df.columns = df.columns.str.replace(' ','_')
    if isinstance(location, str):
        df = df.loc[df.State_and_County_Federal_Information_Processing_Standard_Code.eq(location)&
            df.Health_Center_Type.eq('Federally Qualified Health Center (FQHC)')&
            df.Site_Status_Description.eq('Active')].reset_index(drop = True)
    else:
        df = df.loc[df.State_and_County_Federal_Information_Processing_Standard_Code.isin(location)&
            df.Health_Center_Type.eq('Federally Qualified Health Center (FQHC)')&
            df.Site_Status_Description.eq('Active')].reset_index(drop = True)
    df = df[['Health_Center_Type', 'Site_Name','Site_Address','Site_City','Site_State_Abbreviation',
             'Site_Postal_Code','Site_Telephone_Number', 
             'Health_Center_Service_Delivery_Site_Location_Setting_Description',
             'Geocoding_Artifact_Address_Primary_X_Coordinate',
             'Geocoding_Artifact_Address_Primary_Y_Coordinate']]
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



def gen_nppes_by_taxonomy(taxonomy: str, location: str):
    count = 0
    result_count = 200
    skip = 0
    datasets = []
    while result_count == 200:
        count += 1
        url = f'https://npiregistry.cms.hhs.gov/api/?version=2.1&address_purpose=LOCATION&number=&state={location}&taxonomy_description={taxonomy}&skip={skip}&limit={limit}'
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
            result = pd.concat(datasets, axis = 0)
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
    shutil.rmtree(os.path.join(*chrome_driver_path.split('/')[:-1]))

    
def lung_cancer_screening(location: Union[str, List[str]]):
    if isinstance(location, str):
        lung_cancer_screening_file_download(location)
    else:
        Parallel(n_jobs=-1)(delayed(lung_cancer_screening_file_download)(loc, len(location), w) for loc, w in zip(location, [x*20 for x in range(len(location))]))
    downloads = glob('./ACRLCSDownload*.csv')
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
        os.remove(file)
    return df



