## PROJECT: NURSING HOME REPORTING FOR FALL AND PRESSURE ULCER
## AUTHOR: ZOEY CHEN

## THIS SCRIPT CREATES FINAL DATA FOR FALL SAMPLE DATA IN 2016 AND 2017
import pandas as pd
import dask.dataframe as dd
import dask
import numpy as np

pd.set_option('display.max_columns', 500)
from dask.distributed import Client
client = Client("10.50.86.250:53579")

inputPath = '/gpfs/data/cms-share/duas/55378/Zoey/gardner/data/merge_output/fall/medpar_mds/CDISCHRG_FAC/'
writePath = '/gpfs/data/cms-share/duas/55378/Zoey/gardner/data/merge_output/fall/medpar_mds/'
testPath = '/gpfs/data/cms-share/duas/55378/Zoey/gardner/data/merge_output/fall/test/'
## read in the sample data from last step
df_lst = [dd.read_parquet(inputPath + '{}'.format(year)) for year in ['sameyear', 'not_sameyear']]
df = dd.concat(df_lst)

# ## keep only major injury falls
df = df[df['major']==1]

statecode = pd.read_csv('/gpfs/data/sanghavi-lab/Pan/NH/data/statexwalk/statecode.csv')

df = df.merge(statecode, left_on='STATE_CD', right_on='state', how='left')
df = df.astype({'state_code': 'int'})

df['state_code'] = df['state_code'].apply(lambda x: '{:02d}'.format(x))
dual = ['DUAL_STUS_CD_{:02d}'.format(i) for i in range(1, 13)]

## dual is True if the patient is a full dual in any month of the hospitalization year
df['dual'] = df[dual].isin(['02', '04', '08']).any(axis=1)

## create female and disability indicators
df['female'] = df['sex'] == '2'
df['disability'] = df['ENTLMT_RSN_CURR'] == '1'
## remove observations with missing race information
df = df[df['race_RTI'] != '0']
print(df.shape[0].compute()) #32942

replace_race = {'1': 'white', '2': 'black', '3': 'other', '4': 'asian', '5': 'hispanic', '6': 'american_indian'}

df = df.assign(race_name=df['race_RTI'].replace(replace_race))
## categorize nursing homes into northeast, midwest, south or west nursign homes by their location
replace_region = {"09": 'northeast', "23": "northeast", "25": 'northeast', "33": 'northeast', '34': 'northeast',
                  '36': 'northeast', '42': 'northeast', '44': 'northeast', '50': 'northeast',
                  '17': 'midwest', '18': 'midwest', '19': 'midwest', '26': 'midwest', '39': 'midwest', '55': 'midwest',
                  '20': 'midwest', '27': 'midwest', '29': 'midwest', '31': 'midwest', '38': 'midwest', '46': 'midwest',
                  '01': 'south', '05': 'south', '22': 'south', '10': 'south', '11': 'south', '12': 'south',
                  '13': 'south', '21': 'south', '24': 'south', '28': 'south', '37': 'south', '40': 'south',
                  '45': 'south', '47': 'south', '48': 'south', '51': 'south', '54': 'south', '78': 'south',
                  '02': 'west', '06': 'west', '04': 'west', '08': 'west', '15': 'west', '16': 'west', '30': 'west',
                  '32': 'west', '35': 'west', '41': 'west', '49': 'west', '53': 'west', '56': 'west'}
df = df.assign(region=df['state_code'].replace(replace_region))
# ## categorize nursing homes into for-profit, non-profit and government based on their ownership
replace_ownership = {1: 'for-profit', 2: 'for-profit', 3: 'for-profit', 13: 'for-profit',
                     4: 'non-profit', 5: 'non-profit', 6: 'non-profit',
                     7: 'government', 8: 'government', 9: 'government', 10: 'government', 11: 'government',
                     12: 'government'
                     }
df = df.assign(ownership=df['GNRL_CNTL_TYPE_CD'].replace(replace_ownership))
use_cols = ['BENE_ID', 'MEDPAR_ID', 'MDS_ASMT_ID', 'TRGT_DT', 'MEDPAR_YR_NUM', 'MCARE_ID', 'race_name', 'female', 'age',
            'dual', 'disability', 'short_stay', 'CNSUS_MDCR_CNT',
            'CNSUS_RSDNT_CNT', 'region', 'ownership']
fall_cols = ['J1800_FALL_LAST_ASMT_CD','J1900A_FALL_NO_INJURY_CD','J1900B_FALL_INJURY_CD','J1900C_FALL_MAJ_INJURY_CD']

df_final = df[use_cols + fall_cols]
df_final = df_final.dropna()

df_final.compute().to_csv(
    writePath + 'falls_final_data1617_new.csv'
)





