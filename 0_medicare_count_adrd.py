## PROJECT: NURSING HOME REPORTING FOR FALL AND PRESSURE ULCER
## AUTHOR: ZOEY CHEN

## BASED ON HOW WE CALCULATED THE NUMBER OF FFS MEDICARE RESIDENTS IN NURSING HOMES AND PERCENTAGE OF DUALLY-ELIGIBLE
## RESIDENTS, WE FURTHER CREATE A VARIABLE FOR THE PERCENTAGE OF MEDICARE FFS BENEFICIARIES WHO HAD ADRD IN NURSING HOMES

import pandas as pd
import dask.dataframe as dd
import yaml
import numpy as np
from dask.distributed import Client
client = Client("10.50.86.250:40066")
pd.set_option('display.max_columns', 100)

years = list(range(2011, 2018))

for year in years:
    ## read in chronic conditions file
    cc = dd.read_parquet('/gpfs/data/cms-share/data/medicare/{}/mbsf/mbsf_cc/parquet/'.format(year))

    ## create adrd indicator which equals 1 or 3 if the ALZH_DEMEN indicates that the bene met the claims criteria
    cc['adrd'] = cc['ALZH_DEMEN'].isin(['1', '3'])
    ## subset benes to only those with adrd
    cc_adrd = cc[cc['adrd']==1]

    cc_adrd[['adrd', 'ALZH_DEMEN']].to_parquet(
        '/gpfs/data/cms-share/duas/55378/Zoey/gardner/data/mbsf/adrd_{}/'.format(year)
    )
## subset beneficiaries with ADRD to FFS beneficiaries with ADRD
for year in years:
    ## identify columns to use in MBSF
    hmo = ['HMO_IND_{:02d}'.format(i) for i in range(1, 13)]

    ## read in MBSF
    df_MBSF = dd.read_parquet('/gpfs/data/cms-share/data/medicare/{}/mbsf/mbsf_abcd/parquet/'.format(year))

    ## select FFS benes for a whole year
    df_MBSF = df_MBSF[df_MBSF[hmo].isin(['0', '4']).all(axis=1)]
    df_MBSF['ffs'] = 1

    ## read in adrd indicator from mbsfcc
    adrd = dd.read_parquet('/gpfs/data/cms-share/duas/55378/Zoey/gardner/data/mbsf/adrd_{}/'.format(year))

    ## merge with mbsf ffs bene
    adrd_ffs = adrd.merge(df_MBSF, left_index=True, right_index=True)

    adrd_ffs[['adrd', 'ffs']].to_parquet(
        '/gpfs/data/cms-share/duas/55378/Zoey/gardner/data/mbsf/adrd_ffs_{}/'.format(year)
    )
## calculate the number of residents with ADRD in nurisng homes
for year in years:
    if year>2011:
        # read in cross-walked mds with BENE_ID as index
        mds = dd.read_parquet('/gpfs/data/sanghavi-lab/DESTROY/MDS/cross_walked/xwalk_mds_unique_new/{}/'.format(year))

    if year>2015:
        cols = mds.columns
        mds.columns = [col.upper() for col in cols]
    mds = mds.astype({'FAC_PRVDR_INTRNL_ID': 'str'})
    mds = mds[['FAC_PRVDR_INTRNL_ID', 'STATE_CD']]
    ## create a proxy column for BENE_ID
    mds['BENE_ID_col'] = mds.index
    ## keep unique beneficiaries in each nursing home
    mds = mds.drop_duplicates(subset=['FAC_PRVDR_INTRNL_ID', 'STATE_CD', 'BENE_ID_col'])
    ## read in data for FFS benes with ADRD
    adrd = dd.read_parquet('/gpfs/data/cms-share/duas/55378/Zoey/gardner/data/mbsf/adrd_ffs_{}/'.format(year))
    ## merge with MDS data
    mds_adrd = mds.merge(adrd, left_index=True, right_index=True)

    ## within each nursing home, count the number of unique MEDICARE FFS residents with ADRD for each year
    medicare_count_adrd = \
        mds_adrd.groupby(['FAC_PRVDR_INTRNL_ID', 'STATE_CD'])['BENE_ID_col']. \
            count().rename('medicare_adrd_count').reset_index()
    medicare_count_adrd = medicare_count_adrd.compute()
    medicare_count_adrd.to_csv('/gpfs/data/cms-share/duas/55378/Zoey/gardner/data/mds/nh_population/' +
                               'medicare_res_adrd_count{}.csv'.format(year),
                          index=False)

## data across years were concatenated to nh_adrd.csv
adrd_percent = [pd.read_csv('/gpfs/data/cms-share/duas/55378/Zoey/gardner/data/mds/nh_population/' +
                               'medicare_res_adrd_count{}.csv'.format(year),
                              low_memory=False)
                  for year in years]
for i in range(len(adrd_percent)):
    adrd_percent[i]['MEDPAR_YR_NUM'] = years[i]
adrd_percent = pd.concat(adrd_percent)
adrd_percent.to_csv(
    '/gpfs/data/cms-share/duas/55378/Zoey/gardner/data/mds/nh_population/nh_adrd.csv',
    low_memory=False
)




