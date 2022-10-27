## PROJECT: NURSING HOME REPORTING FOR FALL AND PRESSURE ULCER
## AUTHOR: ZOEY CHEN

## THIS SCRIPT SUBSET THE SAMPLE TO ONLY CLAIMS FOR RESIDENTS WHO RETURNED TO THE SAME NURSING HOME WITHIN 1 DAY
## AFTER HOSPITALIZATION FOR FALL-RELATED INJURY
import pandas as pd
import dask.dataframe as dd
import dask
import numpy as np

pd.set_option('display.max_columns', 500)
from dask.distributed import Client
client = Client("10.50.86.251:52781")

mdsPath = '/gpfs/data/cms-share/data/mds/year/{}/xwalk/parquet/'
inputPath = '/gpfs/data/cms-share/duas/55378/Zoey/gardner/data/merge_output/fall/medpar_mds/CDISCHRG/'
writePath = '/gpfs/data/cms-share/duas/55378/Zoey/gardner/data/merge_output/fall/medpar_mds/CDISCHRG_SAMENH/'
testPath = '/gpfs/data/cms-share/duas/55378/Zoey/gardner/data/merge_output/fall/test/'

years = [2016, 2017]

for year in years:
    ## define MDS items to use
    use_cols = ['MDS_ASMT_ID', 'BENE_ID', 'TRGT_DT', 'STATE_CD', 'FAC_PRVDR_INTRNL_ID', 'A0310E_FIRST_SINCE_ADMSN_CD', 'A0310F_ENTRY_DSCHRG_CD']
    ## read in mds
    mds = dd.read_parquet(mdsPath.format(year), usecols=use_cols)
    ## exclude mds with missing BENE_ID
    mds = mds[~mds.BENE_ID.isna()]
    ## turn all columns to upper case
    cols = [col.upper() for col in mds.columns]
    mds.columns = cols
    mds = mds[use_cols]
    ## replace special characters
    mds = mds.replace({'^': np.NaN, '-': 'not_applicable', '': np.NaN})
    ## change data type
    mds = mds.astype({'TRGT_DT': 'str',
                      'A0310F_ENTRY_DSCHRG_CD': 'str',
                      'A0310E_FIRST_SINCE_ADMSN_CD': 'str'})
    ## change date columns to datetime format
    mds['TRGT_DT'] = dd.to_datetime(mds['TRGT_DT'], infer_datetime_format=True)
    ## select the entry tracking record completed after residents' (re)admission to nursing home
    mds_first = mds[mds['A0310F_ENTRY_DSCHRG_CD']=='01']
    ## concate nh provider id with state code to create the unique facility identifier as fac_state_id
    mds_first['FAC_PRVDR_INTRNL_ID'] = mds_first['FAC_PRVDR_INTRNL_ID'].astype('float').astype('Int64')

    mds_first = mds_first.astype({'FAC_PRVDR_INTRNL_ID': 'str'})
    mds_first['FAC_PRVDR_INTRNL_ID'] = mds_first['FAC_PRVDR_INTRNL_ID'].str.zfill(10)
    mds_first['fac_state_id'] = mds_first['FAC_PRVDR_INTRNL_ID'] + mds_first['STATE_CD']

    ## read in claims-mds data ###########################################################################
    Cdischrg = dd.read_parquet(inputPath + '{}'.format(year))
    ## create unique nursing home provider id: fac_state_id
    Cdischrg['FAC_PRVDR_INTRNL_ID'] = Cdischrg['FAC_PRVDR_INTRNL_ID'].astype('float').astype('Int64')

    Cdischrg = Cdischrg.astype({'FAC_PRVDR_INTRNL_ID': 'str'})
    Cdischrg['FAC_PRVDR_INTRNL_ID'] = Cdischrg['FAC_PRVDR_INTRNL_ID'].str.zfill(10)
    Cdischrg['fac_state_id'] = Cdischrg['FAC_PRVDR_INTRNL_ID'] + Cdischrg['STATE_CD']
    ## merge with mds assessments by bene_id
    Cdischrg_mds = Cdischrg.merge(mds_first, on='BENE_ID', how='left', suffixes=['', '_reenter'])
    ## calculate the number of days between assessment date and hospital discharge date
    Cdischrg_mds = Cdischrg_mds.astype({'DSCHRG_DT': 'datetime64[ns]'})
    Cdischrg_mds['days_reenter'] = (Cdischrg_mds['TRGT_DT_reenter'] - Cdischrg_mds['DSCHRG_DT']).dt.days
    ## select assessments performed within 1 day after hospital discharge
    Cdischrg_mds_after = Cdischrg_mds[(Cdischrg_mds['days_reenter'] >= 0) & (Cdischrg_mds['days_reenter'] < 2)]

    ## select the closest MDS assessment merged with claims after hospital admission
    closest_mds = Cdischrg_mds_after.groupby('MEDPAR_ID')['days_reenter'].min().reset_index()
    Cdischrg_mds_closest = Cdischrg_mds_after.merge(closest_mds, on=['MEDPAR_ID', 'days_reenter'], how='left')

    ## make sure all mds merged to the claims are from the same nursing home
    multiprvdr = Cdischrg_mds_closest.groupby('MEDPAR_ID')['fac_state_id_reenter'].nunique().rename('nfac').reset_index()
    multiprvdr = multiprvdr[multiprvdr['nfac']!=1]
    nmultiprvdr = multiprvdr.shape[0].compute()
    if nmultiprvdr==0:
        ## drop duplicate claims
        Cdischrg_mds_unique = Cdischrg_mds_closest.drop_duplicates(subset='MEDPAR_ID')
    else:
        print('nursing homes merged are not unique')
        print(nmultiprvdr)
        ## exclude claims merged with MDS from multiple nursing homes
        Cdischrg_mds_closest = Cdischrg_mds_closest.merge(multiprvdr, on='MEDPAR_ID', how='left')
        Cdischrg_mds_closest = Cdischrg_mds_closest[Cdischrg_mds_closest['nfac'].isna()]
        Cdischrg_mds_closest = Cdischrg_mds_closest.drop(columns='nfac')
        ## drop duplicate claims
        Cdischrg_mds_unique = Cdischrg_mds_closest.drop_duplicates(subset='MEDPAR_ID')

    ## select residents who returned to the same nursing home
    samenh = Cdischrg_mds_unique[Cdischrg_mds_unique['fac_state_id'] == Cdischrg_mds_unique['fac_state_id_reenter']]

    ## remove columns newly merged from re-enter MDS assessment
    samenh = samenh.drop(columns=[col for col in samenh.columns if col.endswith('_reenter')])
    ## write data to parquet
    samenh.to_parquet(
        writePath + '{}'.format(year)
    )


