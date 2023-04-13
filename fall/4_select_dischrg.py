## PROJECT: NURSING HOME REPORTING FOR FALL AND PRESSURE ULCER
## AUTHOR: ZOEY CHEN

## THIS SCRIPT IDENTIFIES NURSING HOME RESIDENTS WITH DISCHARGE ASSESSMENT WITHIN 1 DAY PRIOR TO HOSPITALIZATION
## AND CRATE A DATASET CONSISTED OF LINKED HOSPITAL_CLAIMS-DISCHARGE_MDS_ASSESSMENT FOR ALL THESE RESIDENTS

import pandas as pd
import dask.dataframe as dd
import dask
import numpy as np

pd.set_option('display.max_columns', 500)
from dask.distributed import Client
client = Client("10.50.86.251:52781")

inputPath = '/gpfs/data/cms-share/duas/55378/Zoey/gardner/data/merge_output/fall/medpar_mds/CMDS_new/'
writePath = '/gpfs/data/cms-share/duas/55378/Zoey/gardner/data/merge_output/fall/medpar_mds/CDISCHRG_new/'
testPath = '/gpfs/data/cms-share/duas/55378/Zoey/gardner/data/merge_output/fall/test/'

years = [2016, 2017]
fall_cols = ['J1800_FALL_LAST_ASMT_CD','J1900A_FALL_NO_INJURY_CD','J1900B_FALL_INJURY_CD','J1900C_FALL_MAJ_INJURY_CD']
for year in years:
    ## read in claims-mds data
    df = dd.read_parquet(inputPath + '{}'.format(year))
    ## calculate days between MDS assessments and hospital admission
    df['days'] = (df['ADMSN_DT'] - df['TRGT_DT']).dt.days
    ## subset sample to claims merged with discharge assessment
    df_dischr = df[df['A0310F_ENTRY_DSCHRG_CD'].isin(["10", "11"])]

    ## subset sample to claims with a hospital admission date within 1 day of mds discharge date (which is target date on discharge assessment)
    df_dischr1day = df_dischr[(df_dischr['days']<=1) &
                              (df_dischr['days']>=0)]

    # ## select claims merged with an assessment whose discharge destination is a hospital
    df_dischr1day_tohospital = df_dischr1day[df_dischr1day['A2100_DSCHRG_STUS_CD'].isin(["03", "09"])]
    ## select the closest discharge assessment merged to a hospital claim
    closest = df_dischr1day_tohospital.groupby('MEDPAR_ID')['days'].min().reset_index()
    DFclosest = df_dischr1day_tohospital.merge(closest, on=['MEDPAR_ID', 'days'])

    for col in fall_cols:
        ## remove the observation if any MDS fall item has the value of '-' (changed to 'not_applicable' in merge_mds.py)
        DFclosest = \
            DFclosest[DFclosest[col]!='not_applicable']
        ## replace missing with 0, the MDS item is missing because nursing home can skip items after J1800_FALL_LAST_ASMT_CD
        ## if it is coded as 0
        DFclosest[col] = DFclosest[col].replace(np.nan, 0)
    ## specify the type of fall items to be integer
    DFclosest = \
        DFclosest.astype(dict(zip(fall_cols, ['int']*4)))
    ## for each fall claim, aggregate the reporting info on fall MDS tiems from all eligible MDS discharge assessments linked to the claim
    falls_info = \
        DFclosest.groupby('MEDPAR_ID') \
            [fall_cols]. \
        max().reset_index()
    ## keep unique claims
    DFclosest = DFclosest.drop_duplicates(subset='MEDPAR_ID')
    ## drop original MDS fall items
    DFclosest = DFclosest.drop(columns=fall_cols)
    ## merge the aggregated MDS fall items in
    DFclosest_final = DFclosest.merge(falls_info, on='MEDPAR_ID')
    ## write to parquet
    DFclosest_final.to_parquet(
        writePath + '{}'.format(year)
    )


