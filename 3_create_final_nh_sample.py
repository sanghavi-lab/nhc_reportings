## PROJECT: NURSING HOME REPORTING FOR FALL AND PRESSURE ULCER
## AUTHOR: ZOEY CHEN

## THIS SCRIPT MERGE MDS REPORTING DATA, MDS CHARACTERISTICS OUTPUT DATA FROM R SCRIPT AND OTHER DATA SOURCES
## TO CREATE THE FINAL DATASET FOR ANALYZING NURSING HOME CHARACTERISTICS ASSOCIATION WITH MDS REPORTING RATES;
## THE UNIT OF DATA IS NURISNG HOME - DATA ARE AVERAGED ACROSS YEARS FOR EACH NURSING HOME

import pandas as pd
from functools import reduce
import numpy as np
pd.set_option('display.max_columns', 500)

## this is the cleaned casper and ltcfocus data from last step
casper_variables = pd.read_csv('/gpfs/data/cms-share/duas/55378/Zoey/gardner/data/qm_all/initial_analysis/casper_and_ltc_clean2.csv')
## this is the reporting data on nursing home level
mds_reporting = pd.read_csv('/gpfs/data/cms-share/duas/55378/Zoey/gardner/data/qm_all/initial_analysis/all_mds_items_data_20220930.csv')
## this is the data with facility zip code info
facility = pd.read_csv('/gpfs/data/cms-share/data/mds/facility/csv/facility.csv')
## this is zipcode to rural indicator crosswalk
zipcode_rural = pd.read_csv('/gpfs/data/cms-share/duas/55378/Zoey/gardner/data/qm_all/initial_analysis/forhp-eligible-zips.csv')
## this is the count of Medicare residents for each nursing home year
nh_variables = pd.read_csv('/gpfs/data/cms-share/duas/55378/Zoey/gardner/data/mds/nh_population/nh_variables_capser.csv',
                             low_memory=False)
## this is the count of Medicare residents by resident type for each nursing home year
nh_shortlong_count = pd.read_csv('/gpfs/data/cms-share/duas/55378/Zoey/gardner/data/mds/nh_population/nh_medicare_res_count_shortlongstay.csv',
                                low_memory=False)
## this is the count of Medicare residents who had adrd for year nursing home year
nh_adrd = pd.read_csv('/gpfs/data/cms-share/duas/55378/Zoey/gardner/data/mds/nh_population/nh_adrd.csv')
## this is the average nursing home ratings and qm measure scores from 2011 - 2017
nh_ratings = pd.read_csv('/gpfs/data/cms-share/duas/55378/Zoey/gardner/data/qm_all/initial_analysis/nh_nhc_measures.csv')

## change the data type of the column for merging with other data later
casper_variables['PRVDR_NUM'] = casper_variables['PRVDR_NUM'].astype('str')

## merge facility zipcode with crosswalk to identify rural vs urban facilities
## the crosswalk defines rural area zip code; nursing homes not linked to a rural zip code would be considered non-rural
zipcode_rural['rural'] = 1
zipcode_rural = zipcode_rural.astype({'ZIP': 'str'})
facility = facility.astype({'FAC_ZIP': 'str'})
facility = facility.merge(zipcode_rural[['ZIP', 'rural']], left_on='FAC_ZIP', right_on='ZIP', how='left')

## replace missing rural with 0
facility['rural'] = facility['rural'].replace(np.nan, 0)

## merge nh variables with nh adrd to calcualte the percentage of adrd residents in nursing homes
nh_variables = nh_variables.merge(nh_adrd, on=["FAC_PRVDR_INTRNL_ID", "STATE_CD", "MEDPAR_YR_NUM"], how='left')

## fill missing value with 0
nh_variables['medicare_adrd_count'] = \
    nh_variables['medicare_adrd_count'].fillna(0)
nh_variables['adrd_percent'] = nh_variables['medicare_adrd_count']/nh_variables['medicare_count']

## calculate average dual_percent, adrd_percent, and white_percent for nursing home across years
nh_dual = nh_variables.groupby('MCARE_ID')[['dual_percent', 'adrd_percent', 'white_percent']].mean().reset_index()
nh_dual = nh_dual[nh_dual['MCARE_ID']!='************']

## change data type for later merging
nh_dual['MCARE_ID'] = nh_dual['MCARE_ID'].astype('str')

## count gives us the number of claims; sum fives us the number of claims reported on MDS
nh_reporting = mds_reporting.groupby(["MCARE_ID", "mds_item", "short_stay"]).mds_reporting.agg(['count', 'sum']).reset_index()
nh_reporting.columns = ["MCARE_ID", "mds_item", "short_stay", "nclaims", "nreported"]
## include only pressure ulcer and fall (uti was not used)
nh_reporting = nh_reporting[nh_reporting['mds_item'].isin(['fall', 'pu234', 'uti_readmission'])]

## reshape the data from long to wide
nh_reporting_wide = nh_reporting.pivot(index=["MCARE_ID", "short_stay"], columns="mds_item", values=['nreported', 'nclaims'])
nh_reporting_wide.columns = ['{0}_{1}'.format(x, y) for x,y in nh_reporting_wide.columns]
nh_reporting_wide = nh_reporting_wide.reset_index()

## add claims and reporting information across fall, pu adn uti
nh_reporting_wide['nclaims_all'] = nh_reporting_wide[[col for col in nh_reporting_wide.columns if col.startswith('nclaims')]].sum(axis=1)
nh_reporting_wide['nreported_all'] = nh_reporting_wide[[col for col in nh_reporting_wide.columns if col.startswith('nreported')]].sum(axis=1)

## change data type for merging
nh_reporting_wide['MCARE_ID'] = nh_reporting_wide['MCARE_ID'].astype('str')

## pivot data from long to wide for nursing home resident count data
nh_shortlong_count = nh_shortlong_count[nh_shortlong_count['MCARE_ID']!='************']
nh_percent_shortstay = nh_shortlong_count. \
    pivot(index=['MCARE_ID', 'MEDPAR_YR_NUM'], columns='short_stay', values='medicare_count'). \
    reset_index()
# print(nh_percent_shortstay.head())
nh_percent_shortstay.columns=['MCARE_ID', 'MEDPAR_YR_NUM', 'medicare_count_longstay', 'medicare_count_shortstay']
## calculate the percent short stay for each nh-year
nh_percent_shortstay['pct_shortstay'] = nh_percent_shortstay['medicare_count_shortstay']/(nh_percent_shortstay['medicare_count_shortstay'] + nh_percent_shortstay['medicare_count_longstay'])
## calculate the average percent short stay for each nh
nh_percent_shortstay = nh_percent_shortstay.groupby(['MCARE_ID'])['pct_shortstay'].mean().reset_index()

## calculate the total short stay and long stay residents for each nh from 2011 - 2017
nh_shortlong_count = nh_shortlong_count.groupby(['MCARE_ID', 'short_stay'])['medicare_count'].sum().reset_index()
nh_shortlong_count['MCARE_ID'] = nh_shortlong_count['MCARE_ID'].astype('str')

## merge the count of short vs long-stay residents in the nursing home with MDS reporting data
nh_reporting_wide = nh_reporting_wide.merge(nh_shortlong_count, on=['MCARE_ID', 'short_stay'], how='right')
nh_reporting_wide = nh_reporting_wide.merge(nh_percent_shortstay, on='MCARE_ID', how='left')

# ## calculate reporting rate for each nursing home across years
for mds_item in ['fall', 'pu234', 'uti_readmission', 'all']:
    nh_reporting_wide['reporting_{}'.format(mds_item)] = nh_reporting_wide['nreported_{}'.format(mds_item)] / nh_reporting_wide['nclaims_{}'.format(mds_item)]
    nh_reporting_wide['claims_based_rate_{}'.format(mds_item)] = nh_reporting_wide['nclaims_{}'.format(mds_item)] / nh_reporting_wide['medicare_count']

## CLEAN CASPER&LTCFOCUS DATA
print(casper_variables['PRVDR_NUM'].nunique()) #17003
## keep only data from 2011 - 2017
casper_variables = casper_variables[casper_variables['year']>2010]
casper_variables = casper_variables.drop(columns="name")
casper_variables['missing_casper'] = casper_variables['from_casper'].isna()
casper_variables['missing_ltc'] = casper_variables['from_ltc'].isna()

missing_corl = \
    casper_variables.groupby('PRVDR_NUM')[['missing_casper', 'missing_ltc']].sum().reset_index()
print(missing_corl.shape[0]) #16843
## remove nursing homes if more than 3 years of LTCFocus or Casper data is missing
missing_corl = missing_corl[(missing_corl['missing_casper'] < 4) & (missing_corl['missing_ltc'] < 4)]
# print(missing_corl.shape[0]) #16617
print(missing_corl['PRVDR_NUM'].nunique()) #16617

casper_variables_nomissing = casper_variables.merge(missing_corl, on='PRVDR_NUM')

## drop duplciated columns
casper_variables_nomissing = casper_variables_nomissing.drop(columns=[col for col in casper_variables_nomissing.columns if (col.endswith("_x") or col.endswith("_y"))])

## keep only nursing homes that have been in the data from 2011 - 2017
casper_variables_nomissing_years = casper_variables_nomissing.groupby("PRVDR_NUM")['year'].count().rename('nyears').reset_index()
casper_variables_nomissing = casper_variables_nomissing.merge(
    casper_variables_nomissing_years[casper_variables_nomissing_years['nyears']==7], on='PRVDR_NUM'
)

print(casper_variables_nomissing['PRVDR_NUM'].nunique()) #14665

## store numeric variable names to a list
num_columns = [col for col in casper_variables_nomissing.columns if not \
    ((col in ['PRVDR_NUM', 'year', 'nyears', 'from_casper', 'from_ltc', 'ownership', 'diff_ownership', 'ownership_common']) |
     col.endswith('_std') |
     col.endswith('_avg'))
               ]

## keep only unique nursing homes (data unit changed from nh-year to nh)
casper_variables_nh_unique = casper_variables_nomissing.drop_duplicates(subset='PRVDR_NUM')

## keep only columns for the average in casper data, except for ownership variables (ownership variables not used)
avg_columns = ['{}_avg'.format(col) for col in num_columns if not col.startswith(('GNRL_CNTL_TYPE_CD', 'STATE'))]
casper_variables_nh_unique = casper_variables_nh_unique[['PRVDR_NUM', 'ownership_common', 'diff_ownership', 'STATE'] + avg_columns]
## merge casper and nh reporting data
nh_characters = casper_variables_nh_unique.merge(nh_reporting_wide, left_on='PRVDR_NUM', right_on='MCARE_ID', how='left')

## merge for nursing home dual and adrd percent
nh_characters = nh_characters.merge(nh_dual, left_on='PRVDR_NUM', right_on='MCARE_ID', how='left', suffixes=['', '_drop'])
nh_characters = nh_characters.drop(columns=[col for col in nh_characters.columns if col.endswith('_drop')])

## merge for rural indicator
nh_characters = nh_characters.merge(facility[['FAC_ZIP', 'MCARE_ID', 'rural']],
                                    left_on='PRVDR_NUM',
                                    right_on='MCARE_ID', how='left', suffixes=['', '_drop'])
nh_characters = nh_characters.drop(columns=[col for col in nh_characters.columns if col.endswith('_drop')])


## merge with nursing home ratings data
nh_ratings = nh_ratings.astype({'provnum': 'str'})
nh_characters = nh_characters.merge(nh_ratings, left_on='PRVDR_NUM', right_on='provnum', how='left')

## read in complete list of nursing homes from MDS in 2011 - 2017
all_nh_fromMDS = pd.read_csv('/gpfs/data/cms-share/duas/55378/Zoey/gardner/data/mds/nh_population/complete_nursing_home_fromMDS.csv')
all_nh_fromMDS_unique = all_nh_fromMDS.drop_duplicates(subset='MCARE_ID')
all_nh_fromMDS_unique['MCARE_ID'] = all_nh_fromMDS_unique['MCARE_ID'].astype('str')
## keep nursing home if it is both in the casper sample and in the MDS sample
nh_characters_MDS = nh_characters[nh_characters['PRVDR_NUM'].isin(all_nh_fromMDS_unique['MCARE_ID'].tolist())]
print(nh_characters_MDS['PRVDR_NUM'].nunique()) #14658 28189

## drop duplicated nursing home
nh_characters_MDS = nh_characters_MDS.drop_duplicates(subset=['PRVDR_NUM', 'short_stay'])
print(nh_characters_MDS['PRVDR_NUM'].nunique())


# nh_characters_MDS.to_csv(
#     '/gpfs/data/cms-share/duas/55378/Zoey/gardner/data/qm_all/initial_analysis/nh_characteristics_20221020.csv',
#     index=False
# )



