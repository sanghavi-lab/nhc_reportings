## PROJECT: NURSING HOME REPORTING FOR FALL AND PRESSURE ULCER
## AUTHOR: ZOEY CHEN

## THIS SCRIPT Restricts beneficiaries in fall final sample data to fee-for-services beneficiaries (2011 - 2015 sample)
import pandas as pd
import dask.dataframe as dd
from dask.distributed import Client
client = Client("10.50.86.250:49372")

mbsfPath = '/gpfs/data/cms-share/data/medicare/{}/mbsf/mbsf_abcd/parquet/'
## read in fall final data from 2011 - 2015
falls = pd.read_stata('/gpfs/data/cms-share/duas/55378/Pan/NH/datasets/stata/final/linprob/exhibit4_11212018.dta')
falls = falls.rename(columns={"bene_id": "BENE_ID"})

years = range(2011, 2016)

falls_lst = []
for year in years:
    falls_year = falls[falls['year']==year]
    ## read in MBSF data for the year
    mbsf = dd.read_parquet(mbsfPath.format(year))
    ## identify useful columns from MBSF
    hmo = ['HMO_IND_{:02d}'.format(i) for i in range(1, 13)]
    ## bene is FFS if the hmo indicator equals 0 or 4 for the whole year; keep only FFS benes
    ffs = mbsf[mbsf[hmo].isin(['0', '4']).all(axis=1)]
    ffs = ffs.reset_index()
    ffs['ffs'] = 1
    ## merge with original falls data to subset to only FFS benes with fall claims
    falls_year_ffs = ffs[['BENE_ID', 'ffs']].merge(falls_year, on="BENE_ID", how='inner')
    ## append years of data to a list
    falls_lst.append(falls_year_ffs)
## concat the list to a dataframe
falls_ffs = dd.concat(falls_lst)
## write to csv
falls_ffs.compute().to_csv(
    '/gpfs/data/cms-share/duas/55378/Zoey/gardner/data/qm_all/initial_analysis/falls_ffs.csv'
)
