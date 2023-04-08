catchment_area_name="all"
ca_file_path="/Users/gradcheckout/Lee/ciftools_v2/catchment_area/all_catchment_areas.csv"
year=2021
census_api_key="f1a4c4de1f35fe90fc1ceb60fd97b39c9a96e436"


clear


python CIFTools.py --ca_file_path $ca_file_path --query_level "county" "tract" "puma" --year $year --census_api_key $census_api_key --socrata_user_name "ciodata@uky.edu" --socrata_password "MarkeyCancer123!"


