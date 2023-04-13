## PROJECT: NURSING HOME REPORTING FOR FALL AND PRESSURE ULCER
## AUTHOR: ZOEY CHEN

## THIS CODE MERGE FALLS CLAIM WITH MDS ASSESSMENTS TO IDENTIFY
## NURSING HOME RESIDENTS AND COLLECT THEIR MDS ASSESSMENTS

import pandas as pd
import dask.dataframe as dd
import dask
import numpy as np

pd.set_option('display.max_columns', 500)
from dask.distributed import Client
client = Client("10.50.86.251:52781")

mdsPath = '/gpfs/data/cms-share/data/mds/year/{}/xwalk/parquet/'
inputPath = '/gpfs/data/cms-share/duas/55378/Zoey/gardner/data/medpar/falls/MBSF_new/'
writePath = '/gpfs/data/cms-share/duas/55378/Zoey/gardner/data/merge_output/fall/medpar_mds/CMDS_new/'
testPath = '/gpfs/data/cms-share/duas/55378/Zoey/gardner/data/merge_output/fall/test/'

years = [2016, 2017]

for year in years:
    ## define MDS items to use
    fall_cols = ['J1800_FALL_LAST_ASMT_CD','J1900A_FALL_NO_INJURY_CD','J1900B_FALL_INJURY_CD','J1900C_FALL_MAJ_INJURY_CD']
    other_cols = ['MDS_ASMT_ID', 'BENE_ID', 'TRGT_DT', 'STATE_CD', 'FAC_PRVDR_INTRNL_ID', 'A0310A_FED_OBRA_CD',
                  'A0310B_PPS_CD', 'A0310C_PPS_OMRA_CD', 'A0310D_SB_CLNCL_CHG_CD', 'A0310E_FIRST_SINCE_ADMSN_CD',
                  'A0310F_ENTRY_DSCHRG_CD', 'A1600_ENTRY_DT', 'A1700_ENTRY_TYPE_CD', 'A1800_ENTRD_FROM_TXT',
                  'A1900_ADMSN_DT', 'A2000_DSCHRG_DT', 'A2100_DSCHRG_STUS_CD', 'A2300_ASMT_RFRNC_DT']
#     ## read in mds
    mds = dd.read_parquet(mdsPath.format(year))
    ## exclude mds with missing BENE_ID
    mds = mds[~mds.BENE_ID.isna()]
    ## turn all columns to upper case
    cols = [col.upper() for col in mds.columns]
    mds.columns = cols
    ## replace special characters
    mds = mds.replace({'^': np.NaN, '-': 'not_applicable', '': np.NaN})
    ## select columns to use
    mds_use = mds[other_cols + fall_cols]
    ## change data type
    mds_use = mds_use.astype({'A2000_DSCHRG_DT': 'datetime64[ns]',
                              'A1600_ENTRY_DT': 'string',
                              'TRGT_DT': 'string'})
    ## change date columns to datetime format
    mds_use['A1600_ENTRY_DT'] = dd.to_datetime(mds_use['A1600_ENTRY_DT'], infer_datetime_format=True)
    mds_use['TRGT_DT'] = dd.to_datetime(mds_use['TRGT_DT'], infer_datetime_format=True)

    ## read in falls claims
    df = dd.read_parquet(inputPath + '{}'.format(year))
    ## merge with MDS
    df_mds = df.merge(mds_use, on='BENE_ID', how='inner')
    df_mds = df_mds.astype({'ADMSN_DT': 'datetime64[ns]',
                            'DSCHRG_DT': 'datetime64[ns]'})
    ## write merged data to parquet
    df_mds.to_parquet(
        writePath + '{}'.format(year)
    )
