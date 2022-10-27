## PROJECT: NURSING HOME REPORTING FOR FALL AND PRESSURE ULCER
## AUTHOR: ZOEY CHEN

## THIS SCRIPT USES MDSF TO IDENTIFY FFS BENEFICIARIES FROM FALL CLAIMS, AND INTRODUCE PATIENT VARIABLES
## SUCH AS DUAL STATUS, RACE AND SO ON
import pandas as pd
import dask.dataframe as dd
import dask
import numpy as np

pd.set_option('display.max_columns', 500)
from dask.distributed import Client
client = Client("10.50.86.251:52781")

mbsfPath = '/gpfs/data/cms-share/data/medicare/{}/mbsf/mbsf_abcd/parquet/'
inputPath = '/gpfs/data/cms-share/duas/55378/Zoey/gardner/data/medpar/falls/'
writePath = '/gpfs/data/cms-share/duas/55378/Zoey/gardner/data/medpar/falls/MBSF/'

years = [2016, 2017]

for year in years:
    ## identify columns to use in MBSF
    dual = ['DUAL_STUS_CD_{:02d}'.format(i) for i in range(1, 13)]
    hmo = ['HMO_IND_{:02d}'.format(i) for i in range(1, 13)]
    col_use = ['BENE_ID', 'BENE_DEATH_DT', 'ESRD_IND', 'AGE_AT_END_REF_YR',
               'RTI_RACE_CD', 'BENE_ENROLLMT_REF_YR', 'SEX_IDENT_CD',
               'ENTLMT_RSN_ORIG', 'ENTLMT_RSN_CURR'] + dual + hmo
    ## read in MBSF
    df_MBSF = dd.read_parquet(mbsfPath.format(year))
    df_MBSF = df_MBSF.reset_index()
    df_MBSF = df_MBSF[col_use]

    ## select FFS benes for a whole year
    df_MBSF = df_MBSF[df_MBSF[hmo].isin(['0', '4']).all(axis=1)]

    ## rename columns
    df_MBSF = df_MBSF.rename(columns={'BENE_DEATH_DT': 'death_dt',
                                      'AGE_AT_END_REF_YR': 'age',
                                      'RTI_RACE_CD': 'race_RTI',
                                      'SEX_IDENT_CD': 'sex'})

    ## read in falls claims
    falls = dd.read_parquet(inputPath + 'qualified_falls{}_new'.format(year))
    ## merge in MBSF columns and subset to only fee-for-service benes
    falls = falls.merge(df_MBSF, on='BENE_ID', how='inner')

    ## write to parquet
    falls.to_parquet(writePath + '{}'.format(year))

