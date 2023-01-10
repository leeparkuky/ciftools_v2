catchment_area_name='Markey'
ca_file_path='uky_ca.csv'
download_file_type='pickle'
year=2019
census_api_key='f1a4c4de1f35fe90fc1ceb60fd97b39c9a96e436'

python CIFTools.py --ca_file_path $ca_file_path --query_level 'county' 'tract' --year $year --census_api_key $census_api_key

python CIF_pull_data.py --ca_name $catchment_area_name --ca_file_path $ca_file_path --pickle_data_path "cif_raw_data.pickle" --download_file_type $download_file_type