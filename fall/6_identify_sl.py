## PROJECT: NURSING HOME REPORTING FOR FALL AND PRESSURE ULCER
## AUTHOR: ZOEY CHEN

## THIS SCRIPT IDENTIFY LONG-STAY VERSUS SHORT-STAY NURSING HOME RESIDENTS
## IF THERE IS A 5DAY-PPS MDS FOR THE RESIDENT WITHIN 100 DAYS PRIOR TO THE MDS DISCHARGE ASSESSMENTS,
## THE RESIDENT IS A SHORT-STAY RESIDENT, AND LONG-STAY OTHERWISE

import pandas as pd
import dask.dataframe as dd
import dask
import numpy as np

pd.set_option('display.max_columns', 500)
from dask.distributed import Client
client = Client("10.50.86.251:52781")

def identify_sl_residents(analysisDF, mds, output_path=None):
    ## the function identifie long-stay vs short-stay residents

    ## merge mds with analytical sample
    merge = analysisDF.merge(mds, on='BENE_ID', how='left', suffixes=['', '_sl'])
    ## select mds assessments within 100 days before the discharge assessment
    merge100days = merge[((merge['TRGT_DT'] - merge['TRGT_DT_sl']).dt.days < 101) &
                          ((merge['TRGT_DT'] - merge['TRGT_DT_sl']).dt.days >= 0)]
    ##create binary indicator for the existence of a 5-day PPS assessment
    merge100days['short_stay'] = merge100days['A0310B_PPS_CD_sl']=='01'
    ## aggregate short-stay indicator for each MEDPAR_ID
    short_stay = merge100days.groupby('MEDPAR_ID')['short_stay'].max().reset_index()
    ## merge aggregate short_stay indicator to analytical sample data
    analysisDF = analysisDF.merge(short_stay, on='MEDPAR_ID', how='left')
    ## write to parquet
    analysisDF.to_parquet(output_path)

mdsPath = '/gpfs/data/cms-share/data/mds/year/{}/xwalk/parquet/'
inputPath = '/gpfs/data/cms-share/duas/55378/Zoey/gardner/data/merge_output/fall/medpar_mds/CDISCHRG_SAMENH/'
writePath = '/gpfs/data/cms-share/duas/55378/Zoey/gardner/data/merge_output/fall/medpar_mds/CDISCHRG_SL/'
testPath = '/gpfs/data/cms-share/duas/55378/Zoey/gardner/data/merge_output/fall/test/'

years = [2016, 2017]
for year in years:
    ## read in claims-mds data
    df = dd.read_parquet(inputPath + '{}'.format(year))
    ## read in MDS data of the year
    mdsyear = dd.read_parquet(mdsPath.format(year))
    ## read in MDS data of the previous year
    mdsyearprior = dd.read_parquet(mdsPath.format(year - 1))

    mdsyear.columns = [col.upper() for col in mdsyear.columns]
    mdsyearprior.columns = [col.upper() for col in mdsyearprior.columns]
    ## combine two years of data
    mds = dd.concat([mdsyear, mdsyearprior])
    #
    ## define MDS items to use
    use_cols = ['MDS_ASMT_ID', 'BENE_ID', 'TRGT_DT', 'A0310B_PPS_CD']
    ## exclude mds with missing BENE_ID
    mds = mds[~mds.BENE_ID.isna()]
    ## replace special characters
    mds = mds.replace({'^': np.NaN, '-': 'not_applicable', '': np.NaN})
    ## select columns to use
    mds_use = mds[use_cols]
    ## change data type
    mds_use = mds_use.astype({'TRGT_DT': 'str',
                              'A0310B_PPS_CD': 'str'})
    ## change date columns to datetime format
    mds_use['TRGT_DT'] = dd.to_datetime(mds_use['TRGT_DT'], infer_datetime_format=True)
    ## run the function to identify short- vs long-stay residents
    identify_sl_residents(df, mds_use, writePath + "{}".format(year))

