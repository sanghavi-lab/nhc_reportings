## PROJECT: NURSING HOME REPORTING FOR FALL AND PRESSURE ULCER
## AUTHOR: ZOEY CHEN

## THIS SCRIPT MERGES NURISNG HOME CHARACTERISTICS FROM CASPER AND LTCFOCUS TO SAMPLE DATA
import pandas as pd
import dask.dataframe as dd
import dask
import numpy as np

pd.set_option('display.max_columns', 500)
from dask.distributed import Client
client = Client("10.50.86.251:52781")

inputPath = '/gpfs/data/cms-share/duas/55378/Zoey/gardner/data/merge_output/fall/medpar_mds/CDISCHRG_SL/'
writePath = '/gpfs/data/cms-share/duas/55378/Zoey/gardner/data/merge_output/fall/medpar_mds/CDISCHRG_FAC/'
testPath = '/gpfs/data/cms-share/duas/55378/Zoey/gardner/data/merge_output/fall/test/'

years = [2016, 2017]

### 1) use facility.csv from LTCFocus to crosswalk provider number
casper = pd.read_csv('/gpfs/data/cms-share/data/casper/April 2018/STRIP183/PART2.CSV', low_memory=False)  ## read in capser data
facility = pd.read_csv('/gpfs/data/sanghavi-lab/DESTROY/MDS/raw/mds/facility.csv')  ## read in facility data

## create new column -fac_state_id- that concatenates FACILITY_INTERNAL_ID and STATE_ID
facility = facility.astype({'FACILITY_INTERNAL_ID': 'str',
                            'STATE_ID': 'str'})
## fill FACILITY_INTERNAL_ID with leading zeros
facility['FACILITY_INTERNAL_ID'] = facility['FACILITY_INTERNAL_ID'].str.zfill(10)
## combine FACILITY_INTERNAL_ID and STATE_ID to create fac_state_id
facility['fac_state_id'] = facility['FACILITY_INTERNAL_ID'] + facility['STATE_ID']

## clean CASPER data
casper = casper.rename(columns={'PRVDR_NUM': 'MCARE_ID'})
casper['CRTFCTN_DT'] = pd.to_datetime(casper['CRTFCTN_DT'].astype(str), infer_datetime_format=True)
## create casper_year to indicate the year of the survey results
casper['casper_year'] = casper['CRTFCTN_DT'].dt.year

## keep the latest snap shot of NH characteristics for each year
casper_latest = casper.groupby(['MCARE_ID', 'casper_year'])['CRTFCTN_DT'].max().reset_index()
casper = casper.merge(casper_latest, on=['MCARE_ID', 'casper_year', 'CRTFCTN_DT'], how='inner')

def merge_facility(analysis_data, facility, casper, sameyear_path=None, notsameyear_path=None):
    ## merge facility data and mds-medpar data on the fac_state_id column
    analysis_data = analysis_data.merge(facility[
                                              ['fac_state_id', 'MCARE_ID', 'NAME', 'ADDRESS', 'FAC_CITY', 'STATE_ID', 'FAC_ZIP', 'CATEGORY',
                                               'CLOSEDDATE']],
                                        on='fac_state_id',
                                        how='left')
    print('what is the percentage of mds-medpar records didn not match to a MCARE_ID?')
    print(analysis_data['MCARE_ID'].isna().mean().compute())


    ## 2) merge the latest NH characteristics within a year from CASPER
    df_casper = analysis_data.merge(casper[['STATE_CD', 'MCARE_ID', 'CRTFCTN_DT', 'casper_year',
                                             'GNRL_CNTL_TYPE_CD', 'CNSUS_RSDNT_CNT', 'CNSUS_MDCD_CNT',
                                             'CNSUS_MDCR_CNT', 'CNSUS_OTHR_MDCD_MDCR_CNT']],
                                     on='MCARE_ID',
                                     how='left',
                                     suffixes=['', '_casper'])
    # check
    print('how many claims are not matched with casper records?')
    print(df_casper[df_casper.CRTFCTN_DT.isna()].MEDPAR_ID.unique().size.compute())

    ## drop missing values on casper_year
    df_casper = df_casper[~df_casper['casper_year'].isna()]

    ## select mds-medpar data matched with same year casper and write to csv
    df_casper = df_casper.astype({'MEDPAR_YR_NUM': 'int',
                                  'casper_year': 'int'})
    df_casper_matched = df_casper[df_casper.MEDPAR_YR_NUM == df_casper.casper_year]
    df_casper_matched.to_parquet(sameyear_path)


    ## calculate the number of years between casper and claim
    df_casper['days_casper'] = (df_casper['TRGT_DT'] - df_casper['CRTFCTN_DT']).dt.days
    df_casper['abs_days_casper'] = abs(df_casper['days_casper'])
    df_casper['years_casper'] = df_casper['days_casper'] / 365

    ## select casper within two years of the hospital/SNF claim
    df_casper_matched['sameyear'] = 1
    df_casper = df_casper.merge(df_casper_matched[['MEDPAR_ID', 'sameyear']], on='MEDPAR_ID', how='left')
    df_casper_nonmatched = df_casper[df_casper['sameyear'].isna()]
    df_casper_nonmatched = df_casper_nonmatched.drop(columns='sameyear')
    df_casper_nonmatched = df_casper_nonmatched[((df_casper_nonmatched['years_casper'] >= 0) &
                                                 (df_casper_nonmatched['years_casper'] <= 2))]

    ## select the closest casper linked to mds-medpar denominator
    df_casper_closest_casper = \
        df_casper_nonmatched. \
            groupby('MEDPAR_ID')['abs_days_casper']. \
            min(). \
            rename('closest_casper').reset_index()
    ## get all other columns for the closest casper
    df_casper_nonmatched = df_casper_nonmatched.merge(df_casper_closest_casper[['MEDPAR_ID', 'closest_casper']],
                                                      on='MEDPAR_ID',
                                                      how='inner')
    ## for claims not matched with casper at the year of hospitalization, select the closest previous casper within 2 years
    df_casper_nonmatched = df_casper_nonmatched[
        df_casper_nonmatched.abs_days_casper == df_casper_nonmatched.closest_casper]
    df_casper_nonmatched = df_casper_nonmatched.drop(
        columns=['days_casper', 'abs_days_casper', 'years_casper', 'closest_casper'])
    ## write to parquet
    df_casper_nonmatched.to_parquet(notsameyear_path)

## AGGREGATE 2016 AND 2017 SAMPLE DATA
df_lst = [dd.read_parquet(inputPath + '{}'.format(year)) for year in years]
df = dd.concat(df_lst)
## apply the function to merge facility characters with sample data
merge_facility(df, facility, casper, sameyear_path=writePath + 'sameyear/', notsameyear_path=writePath + 'not_sameyear/')
