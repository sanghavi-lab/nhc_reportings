## PROJECT: NURSING HOME REPORTING FOR FALL AND PRESSURE ULCER
## AUTHOR: ZOEY CHEN

####################################################################
## THIS CODE IS TO COUNT THE NUMBER OF MEDICARE FEE-FOR-SERVICE RESIDENTS BY RACE AND RESIDENT TYPE,
## AND RACE MIX IN EACH NURSING HOME-YEAR FROM 2011 TO 2017, USING MDS ASSESSMENTS AND MBSF BENEFICIARIES DATA
####################################################################

import pandas as pd
import dask.dataframe as dd
import yaml
import numpy as np
from dask.distributed import Client
import datetime
client = Client("10.50.86.251:38774")
pd.set_option('display.max_columns', 100)

years = list(range(2011, 2018))
## define input and output paths
yaml_path = '/gpfs/data/cms-share/duas/55378/Zoey/gardner/gitlab_code/nhc_pressure_ulcer/final_code/'
path = yaml.safe_load(open(yaml_path + 'data_path.yaml'))

## calculate nursing home level variables
for i in range(len(years)):
    # read in cross-walked mds with BENE_ID as index
    mds = dd.read_parquet(path['medicare_count']['input_mds'][i])
    if i >= 5:
        cols = mds.columns
        mds.columns = [col.upper() for col in cols]
    mds = mds.astype({'FAC_PRVDR_INTRNL_ID': 'str'})
    mds = mds.replace({'^': np.NaN, '-': 'not_applicable', '': np.NaN})
    mds = mds[['FAC_PRVDR_INTRNL_ID', 'STATE_CD', 'A1600_ENTRY_DT', 'A2000_DSCHRG_DT']]
    mds = mds.astype({'A2000_DSCHRG_DT': 'datetime64[ns]',
                      'A1600_ENTRY_DT': 'string'})
    ## change date columns to datetime format
    mds['A1600_ENTRY_DT'] = dd.to_datetime(mds['A1600_ENTRY_DT'], infer_datetime_format=True)

    ## create a proxy column for BENE_ID
    mds['BENE_ID_col'] = mds.index

    ## read in MBSF
    mbsf = dd.read_parquet(path['medicare_count']['input_mbsf'][i])

    mbsf = mbsf.astype({'RTI_RACE_CD': 'float64'})
    hmo = ['HMO_IND_{:02d}'.format(i) for i in range(1, 13)]
    ## include only FFS beneficiaries for the full year
    mbsf = mbsf[mbsf[hmo].isin(['0', '4']).all(axis=1)]

    dual = ['DUAL_STUS_CD_{:02d}'.format(i) for i in range(1, 13)]
    ## create binary indicators for black or white
    mbsf['white'] = mbsf['RTI_RACE_CD'] == 1
    mbsf['black'] = mbsf['RTI_RACE_CD'] == 2
    ## select columns
    mbsf = mbsf[['white', 'black'] + dual]
    mbsf = mbsf.astype(dict(zip(dual, ['float']*12)))

    ## merge mbsf FFS beneficiaries with mds on BENE_ID (index)
    merge = mds.merge(mbsf, left_index=True, right_index=True)

    ## calculate the number of days each bene stayed in a nursing home
    ## if discharge date is missing, fill in the last day of the year (assuming the resident was not discharged in that year)
    merge['A2000_DSCHRG_DT'] = merge['A2000_DSCHRG_DT'].fillna(datetime.datetime(years[i], 12, 31))
    merge['days'] = (merge['A2000_DSCHRG_DT'] - merge['A1600_ENTRY_DT']).dt.days
    ## For each entry date, choose whichever MDS assessments
    ## that has an earlier discharge date compared with the last date of the year we filled in
    merge_bene_entry = merge.groupby(['BENE_ID_col', 'FAC_PRVDR_INTRNL_ID', 'STATE_CD', 'A1600_ENTRY_DT', 'white', 'black'])['days'].min().reset_index()
    ## calcualte the total number of days each beneficiary spent in the nursing home
    merge_bene_stay = merge_bene_entry.groupby(['BENE_ID_col', 'FAC_PRVDR_INTRNL_ID', 'STATE_CD', 'white', 'black'])['days'].sum().reset_index()
    ## create an indicator for short stay
    merge_bene_stay['short_stay'] = merge_bene_stay['days']<100
    ## count the number of long stay and short stay residents in each nursing home
    merge_bene_stay_count = \
        merge_bene_stay.groupby(['FAC_PRVDR_INTRNL_ID', 'STATE_CD', 'short_stay'])['BENE_ID_col']. \
            count().rename('medicare_count').reset_index().compute()
    # write to csv
    merge_bene_stay_count.to_csv(path['medicare_count']['output'] + 'medicare_res_count_shortlongstay_{}.csv'.format(years[i]),
                          index=False)
    ## count the number of long stay and short stay residents by race in each nursing home
    merge_bene_stay_race_count = \
        merge_bene_stay.groupby(['FAC_PRVDR_INTRNL_ID', 'STATE_CD', 'short_stay'])[['white', 'black']]. \
            sum().reset_index().compute()
    merge_bene_stay_race_count.to_csv(path['medicare_count']['output'] + 'medicare_res_count_shortlongstay_race_{}.csv'.format(years[i]),
                          index=False)
   ## calculate nursing home race mix
    merge_unique = merge.drop_duplicates(subset=['FAC_PRVDR_INTRNL_ID', 'STATE_CD', 'BENE_ID_col'])
    race_mix = \
        merge_unique.groupby(['FAC_PRVDR_INTRNL_ID', 'STATE_CD', 'RTI_RACE_CD'])['BENE_ID_col']. \
            count().rename('nbene').reset_index().compute()

    race_mix.columns = ['FAC_PRVDR_INTRNL_ID', 'STATE_CD', 'race_rti', 'nbene']

    race_mix.to_csv(path['medicare_count']['output'] + 'nh_racemix_{}.csv'.format(years[i]),
                    index=False)

    ## count the percentage of dual eligibles in each nh
    merge['dual'] = merge[dual].isin([2, 4, 8]).any(axis=1)
    dual_percent = \
        merge_unique.groupby(['FAC_PRVDR_INTRNL_ID', 'STATE_CD'])['dual'].\
            mean().rename('dual_percent').reset_index().compute()
    dual_percent.columns = ['FAC_PRVDR_INTRNL_ID', 'STATE_CD', 'dual_percent']

    dual_percent.to_csv(path['medicare_count']['output'] + 'nh_dualpercent_{}.csv'.format(years[i]),
                        index=False)

## concat nh medicare count, dual percentage, and race mix from 2011 - 2017
## read in and concat percent of duals for each nursing home
dual_percent = [pd.read_csv(path['medicare_count']['output'] + 'nh_dualpercent_{}.csv'.format(y),
                              low_memory=False)
                  for y in years]
for i in range(len(dual_percent)):
    dual_percent[i]['MEDPAR_YR_NUM'] = years[i]
dual_percent = pd.concat(dual_percent)
dual_percent = dual_percent.astype({'FAC_PRVDR_INTRNL_ID': 'str'})

## concat nh race mix
race_mix = [pd.read_csv(path['medicare_count']['output'] + 'nh_racemix_{}.csv'.format(y),
                        low_memory=False)
            for y in years]
for i in range(len(race_mix)):
    race_mix[i]['MEDPAR_YR_NUM'] = years[i]
race_mix = pd.concat(race_mix)
race_mix = race_mix.astype({'FAC_PRVDR_INTRNL_ID': 'str',
                            'race_rti': 'float'})
## remove race with missing values
race_mix = race_mix[race_mix['race_rti']!=0]

## replace values for race_rti
vals_to_replace = {1: 'white', 2: 'black', 3:'other', 4: 'asian', 5:'hispanic', 6:'american_indian'}
race_mix['race_rti'] = race_mix['race_rti'].map(vals_to_replace)

## reshape race mix from long to wide
race_mix_wide = race_mix.pivot(index=['FAC_PRVDR_INTRNL_ID', 'STATE_CD', 'MEDPAR_YR_NUM'],
                               columns='race_rti',
                               values='nbene').reset_index()
## calculate the percentage of each race within each nh
race = list(vals_to_replace.values())

race_mix_wide['all_race'] = race_mix_wide[race].sum(axis=1)
for r in race:
    race_mix_wide[r + '_percent'] = race_mix_wide[r] / race_mix_wide['all_race']
race_mix_wide = race_mix_wide.drop(columns=race)

## merge race mix with medicare count by nh-year
nh_population = race_mix_wide.merge(dual_percent,
                                    on=['FAC_PRVDR_INTRNL_ID', 'STATE_CD', 'MEDPAR_YR_NUM'])

nh_population.to_csv(path['medicare_count']['output'] + 'nh_variables.csv', index=False)

## merge MCARE_ID from LTCFocus facility data with nurisng home medicare population data
facility = pd.read_csv(path['6_merge_mbsf_and_fac']['input_facility_path'])  ## read in facility data

## create new column -fac_state_id- that concatenates FACILITY_INTERNAL_ID and STATE_ID
facility = facility.astype({'FACILITY_INTERNAL_ID': 'str',
                            'STATE_ID': 'str'})
## fill FACILITY_INTERNAL_ID with leading zeros
facility['FACILITY_INTERNAL_ID'] = facility['FACILITY_INTERNAL_ID'].str.zfill(10)
## combine FACILITY_INTERNAL_ID and STATE_ID to create fac_state_id
facility['fac_state_id'] = facility['FACILITY_INTERNAL_ID'] + facility['STATE_ID']

# nursing homes with no Medicare FFS residents are not in the above sample (because I used inner merge);
# identify these nursing homes in the below code
nh_list = []
for i in range(len(years)):
    # read in cross-walked mds

    if i >= 5:
        mds = dd.read_csv('/gpfs/data/cms-share/data/mds/year/{0}/csv/MDS_{1}.csv'.format(years[i], years[i]),
                          dtype='object')
        cols = mds.columns
        mds.columns = [col.upper() for col in cols]
    else:
        mds = dd.read_csv('/gpfs/data/cms-share/data/mds/year/{0}/csv/mds{1}.csv'.format(years[i], years[i]),
                          dtype='object')
    mds = mds.astype({'FAC_PRVDR_INTRNL_ID': 'str'})
    mds = mds[['FAC_PRVDR_INTRNL_ID', 'STATE_CD']]
    nh_unique = mds.drop_duplicates(subset=['FAC_PRVDR_INTRNL_ID', 'STATE_CD'])
    nh_unique['MEDPAR_YR_NUM'] = years[i]
    nh_list.append(nh_unique.compute())

nh_all = pd.concat(nh_list)
nh_all.to_csv(
    '/gpfs/data/cms-share/duas/55378/Zoey/gardner/data/mds/nh_population/complete_nursing_home.csv',
    index=False
)

nh_all = pd.read_csv('/gpfs/data/cms-share/duas/55378/Zoey/gardner/data/mds/nh_population/complete_nursing_home.csv')
nh_all['FAC_PRVDR_INTRNL_ID'] = nh_all['FAC_PRVDR_INTRNL_ID'].astype('float').astype('Int64')
nh_all = nh_all.astype({'FAC_PRVDR_INTRNL_ID': 'str'})

## merge with nursing home medicare_count data to see how many nursing homes don't have Medicare FFS residents (very few)
nh_all = nh_all.merge(race_mix_wide, on=['FAC_PRVDR_INTRNL_ID', 'STATE_CD', 'MEDPAR_YR_NUM'], how='left')
nh_all['FAC_PRVDR_INTRNL_ID'] = nh_all['FAC_PRVDR_INTRNL_ID'].str.zfill(10)
nh_all['fac_state_id'] = nh_all['FAC_PRVDR_INTRNL_ID'] + nh_all['STATE_CD']

## merge nurisng home variables with facility data to get MCARE_ID
nh_all = nh_all.merge(facility[['fac_state_id', 'MCARE_ID', 'STATE_ID', 'FAC_ZIP']],
                          on='fac_state_id',
                          how='left')
nh_all = nh_all[~nh_all['MCARE_ID'].isna()]
nh_all.to_csv(
    '/gpfs/data/cms-share/duas/55378/Zoey/gardner/data/mds/nh_population/complete_nursing_home_fromMDS.csv',
    index=False
)
## aggregate nursing home population by short stay and long stay across years
nh_shortlong_count_lst = []
for year in years:
    nh_shortlong_count = pd.read_csv('/gpfs/data/cms-share/duas/55378/Zoey/gardner/data/mds/nh_population/medicare_res_count_shortlongstay_{}.csv'.format(year))

    nh_shortlong_count['MEDPAR_YR_NUM'] = year
    nh_shortlong_count_lst.append(nh_shortlong_count)
nh_shortlong_count_df = pd.concat(nh_shortlong_count_lst)

nh_shortlong_count_df['FAC_PRVDR_INTRNL_ID'] = nh_shortlong_count_df['FAC_PRVDR_INTRNL_ID'].astype('Int64')

nh_shortlong_count_df = nh_shortlong_count_df.astype({'FAC_PRVDR_INTRNL_ID': 'str'})
nh_shortlong_count_df['FAC_PRVDR_INTRNL_ID'] = nh_shortlong_count_df['FAC_PRVDR_INTRNL_ID'].str.zfill(10)
nh_shortlong_count_df['fac_state_id'] = nh_shortlong_count_df['FAC_PRVDR_INTRNL_ID'] + nh_shortlong_count_df['STATE_CD']
## merge nurisng home variables with facility data to get MCARE_ID
nh_shortlong_count_df = nh_shortlong_count_df.merge(facility[['fac_state_id', 'MCARE_ID']],
                                                      on='fac_state_id',
                                                      how='left')

# print(nh_shortlong_count_df['MCARE_ID'].isna().mean()) # 0.0001
nh_shortlong_count_df = nh_shortlong_count_df[~nh_shortlong_count_df['MCARE_ID'].isna()]
nh_shortlong_count_df.to_csv('/gpfs/data/cms-share/duas/55378/Zoey/gardner/data/mds/nh_population/nh_medicare_res_count_shortlongstay.csv',
                             index=False)

## aggregate nursing home population by short stay and long stay and by race across years
nh_shortlong_race_count_lst = []
for year in years:
    nh_shortlong_race_count = pd.read_csv('/gpfs/data/cms-share/duas/55378/Zoey/gardner/data/mds/nh_population/medicare_res_count_shortlongstay_race_{}.csv'.format(year))

    nh_shortlong_race_count['MEDPAR_YR_NUM'] = year
    nh_shortlong_race_count_lst.append(nh_shortlong_race_count)
nh_shortlong_race_count_df = pd.concat(nh_shortlong_race_count_lst)

nh_shortlong_race_count_df['FAC_PRVDR_INTRNL_ID'] = nh_shortlong_race_count_df['FAC_PRVDR_INTRNL_ID'].astype('Int64')
nh_shortlong_race_count_df = nh_shortlong_race_count_df.astype({'FAC_PRVDR_INTRNL_ID': 'str'})
nh_shortlong_race_count_df['FAC_PRVDR_INTRNL_ID'] = nh_shortlong_race_count_df['FAC_PRVDR_INTRNL_ID'].str.zfill(10)
nh_shortlong_race_count_df['fac_state_id'] = nh_shortlong_race_count_df['FAC_PRVDR_INTRNL_ID'] + nh_shortlong_race_count_df['STATE_CD']
nh_shortlong_race_count_df = nh_shortlong_race_count_df.drop(columns=['FAC_PRVDR_INTRNL_ID', 'STATE_CD'])
## merge with nursing home population by short and long stay
nh_shortlong_count_df = nh_shortlong_count_df.merge(nh_shortlong_race_count_df,
                                                    on=['fac_state_id', 'short_stay', 'MEDPAR_YR_NUM'])
## write to csv
nh_shortlong_count_df.to_csv('/gpfs/data/cms-share/duas/55378/Zoey/gardner/data/mds/nh_population/nh_medicare_res_count_shortlongstay_race.csv',
                             index=False)