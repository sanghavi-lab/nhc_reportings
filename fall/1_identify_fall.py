## PROJECT: NURSING HOME REPORTING FOR FALL AND PRESSURE ULCER
## AUTHOR: ZOEY CHEN

## THIS SCRIPT IDENTIFIES INPATIENT HOSPITAL CLAIMS USING THE SAME METHOD AS THIS PAPER - DOI: 10.1111/1475-6773.13247,
## BUT USING ICD-10 CODES (2016 - 2017 sample)
import pandas as pd
import dask.dataframe as dd
import dask
import numpy as np

pd.set_option('display.max_columns', 500)
from dask.distributed import Client
client = Client("10.50.86.250:53579")

def identify_fall_claims(row):
    ## function to identify whether the admission, or first (external) diagnosis code is related to fall diagnosis codes
    if any([row['ADMTG_DGNS_CD'].startswith(code) for code in icd['ecode']]):
        row['falls'] = 1
    elif any([row['DGNS_1_CD'].startswith(code) for code in icd['ecode']]):
        row['falls'] = 1
    elif any([row['DGNS_E_1_CD'].startswith(code) for code in icd['ecode']]):
        row['falls'] = 1

    return row

def identify_disqualified_falls(row):
    ## function to identify whether any diagnosis code is related to disqualifying diagnosis code;
    ## or whether any diagnosis code is related to major injury
    dcode = ['ADMTG_DGNS_CD'] + ['DGNS_{}_CD'.format(i) for i in list(range(1, 26))]
    ecode = ['DGNS_E_{}_CD'.format(i) for i in list(range(1, 13))]
    for code in dcode: ## search diagnosis code for disqualifying code or major injury code
        if any([row[code].startswith(d) for d in icd['disqualifying_code']]):
            row['disqualified'] = 1
        if any([row[code].startswith(d) for d in icd['fall_related']]):
            row['major'] = 1
    if row['disqualified'] != 1:
        for code in ecode: ## search external diagnosis code for major injury code
            if any([row[code].startswith(d) for d in icd['disqualifying_code']]):
                row['disqualified'] = 1
    if row['major'] != 1:
        for code in ecode: ## search external diagnosis code for  major injury code
            if any([row[code].startswith(d) for d in icd['fall_related']]):
                row['major'] = 1
    return row


years = [2016, 2017]
medparPath = "/gpfs/data/cms-share/data/medicare/{}/medpar/parquet"
writePath = '/gpfs/data/cms-share/duas/55378/Zoey/gardner/data/medpar/falls/'
testPath = '/gpfs/data/cms-share/duas/55378/Zoey/gardner/data/medpar/falls/test/'

## read in icd codes for falls
icd = pd.read_csv('/gpfs/data/cms-share/duas/55378/Zoey/gardner/gitlab_code/nhc_qm/falls/falls_icd.csv')
icd = icd.astype({'disqualifying_code': 'str',
                  'ecode': 'str',
                  'fall_related': 'str'})

for year in years:
    ## read in raw medpar data
    df = dd.read_parquet(medparPath.format(year))
    df = df.reset_index()

    dcode = ['DGNS_{}_CD'.format(i) for i in list(range(1, 26))]
    ecode = ['DGNS_E_{}_CD'.format(i) for i in list(range(1, 13))]

    ## define columns used for analysis
    col_use = ['BENE_ID', 'MEDPAR_ID', 'MEDPAR_YR_NUM', 'PRVDR_NUM', 'ADMSN_DT', 'DSCHRG_DT',
               'DSCHRG_DSTNTN_CD', 'SS_LS_SNF_IND_CD', 'BENE_DSCHRG_STUS_CD', 'DRG_CD',
               'ADMTG_DGNS_CD'] + dcode + ecode
    df = df[col_use]
    ## exclude SNF claims
    hospital = df[df.SS_LS_SNF_IND_CD.isin(['S', "L"])]
    ## create new columns with default value of 0
    hospital['disqualified'] = 0
    hospital['falls'] = 0
    hospital['major'] = 0

    ## apply function to identify fall related claims
    hospital = hospital.map_partitions(lambda ddf: ddf.apply(identify_fall_claims, axis=1))

    ## write fall related claims to file
    falls = hospital[hospital['falls']==1]
    falls.to_parquet(
        writePath + 'falls{}'.format(year)
    )

for year in years:
    df = dd.read_parquet(writePath + 'falls{}'.format(year))

    df = df.map_partitions(lambda ddf: ddf.apply(identify_disqualified_falls, axis=1))
    falls_qualified = df[df['disqualified']==0]
    ## write fall-related claims without disqualifying code to file
    falls_qualified.to_parquet(writePath + 'qualified_falls{}_new'.format(year))










