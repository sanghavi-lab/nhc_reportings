## PROJECT: NURSING HOME REPORTING FOR FALL AND PRESSURE ULCER
## AUTHOR: ZOEY CHEN

## THIS SCRIPT MERGES FINAL ANALYTICAL SAMPLE FOR MDS REPORTING ON FALL AND PRESSURE ULCER FROM 2011 - 2017 TOGETHER
## PNEUMONIA AND UTI WERE NOT INCLUDED IN FINAL ANALYSIS

import pandas as pd
import dask.dataframe as dd
import dask
import numpy as np
pd.set_option('display.max_columns', 500)
pd.set_option('display.max_rows', 500)

pd.set_option('display.max_columns', 500)
# from dask.distributed import Client
# client = Client("10.50.86.250:60534")

inputPath = '/gpfs/data/cms-share/duas/55378/Zoey/gardner/data/'
mdsPath = '/gpfs/data/cms-share/data/mds/year/{}/xwalk/parquet/'


## define the column names in MDS for creating QMs (I2000_PNEUMO_CD is not used fpr QM)
## m_pu is a binary indicator suggesting whether there is a pressure ulcer reported at any stage in discharge assessments
## m_pu_stage2_to_stage4 is a binary indicator suggesting whether there is a pressure ulcer reported at stage 2 - 4 in discharge assessments

mds_items = ['J1900C_FALL_MAJ_INJURY_CD', 'm_pu', 'm_pu_stage2_to_stage4', 'I2300_UTI_CD', 'I2300_UTI_CD_add_readmission', 'I2000_PNEUMO_CD']
## pu corresponds to m_pu, pu234 corresponds to m_pu_stage2_to_stage4
qm_names = ['fall', 'pu', 'pu234', 'uti', 'uti_readmission', 'pneu']
cols_use = ['BENE_ID', 'MEDPAR_YR_NUM', 'MCARE_ID', 'dual', 'race_name', 'short_stay', 'combinedscore']
## read in falls final data for 2016 - 2017
falls1617 = pd.read_csv(inputPath + 'merge_output/fall/medpar_mds/falls_final_data1617_new.csv')
## nursing homes can reporting 2 or more falls as 2; change 2 to 1 to make fall item a binary indicator
falls1617['J1900C_FALL_MAJ_INJURY_CD'] = falls1617['J1900C_FALL_MAJ_INJURY_CD'].replace({2: 1})
falls1617['combinedscore'] = np.nan
## read in falls final data for 2011 - 2015
falls1115 = pd.read_csv(inputPath + 'qm_all/initial_analysis/falls_ffs.csv')
## remove observations with missing race variable
falls1115 = falls1115[falls1115['bsf_rti'] != 0]
## rename race
replace_race = {1: 'white', 2: 'black', 3: 'other', 4: 'asian', 5: 'hispanic', 6: 'american_indian'}
falls1115 = falls1115.assign(race_name=falls1115['bsf_rti'].replace(replace_race))

## rename falls1115 columns to be consistent with fall1617 data
falls1115 = falls1115.rename(columns={'max_nj1900c': 'J1900C_FALL_MAJ_INJURY_CD',
                                      'shortstay': 'short_stay',
                                      'year': 'MEDPAR_YR_NUM',
                                      'm_prvdrnum': 'MCARE_ID'})

## include only useful columns
falls1617 = falls1617[cols_use + ['J1900C_FALL_MAJ_INJURY_CD']]
falls1115 = falls1115[cols_use + ['J1900C_FALL_MAJ_INJURY_CD']]
## concat data for falls
fall = pd.concat([falls1115, falls1617])
#
## read in other data for pressure ulcer (pu), uti, and pneumonia
pu = pd.read_csv(inputPath + 'merge_output/pu/medpar_mds_final/main_data_final.csv')
uti = pd.read_csv(inputPath + 'merge_output/infection/medpar_mds/FINAL/new/primaryUTI.csv')
uti_readmission = pd.read_csv(inputPath + 'merge_output/infection/medpar_mds/FINAL/new/primaryUTI_readmission.csv')
pneu = pd.read_csv(inputPath + 'merge_output/infection/medpar_mds/FINAL/new/primaryPNEU.csv')

## select MDS items for Stage 2 - 4 pressure ulcer
pucode_num = ['M0300B1_STG_2_ULCR_NUM', 'M0300C1_STG_3_ULCR_NUM', 'M0300D1_STG_4_ULCR_NUM']
## change data types for these columns
dtype = {col: 'float64' for col in pucode_num}
## create the reporting indicator of whether a pressure ulcer at Stage 2 - 4 was reported on MDS
pu['m_pu_stage2_to_stage4'] = (pu[pucode_num] > 0).any(axis=1)

## create a dictionary for data for different MDS items; we can reference them through the name of the MDS item
df_dict = {'fall': fall,
           'pu': pu,
           'pu234': pu, ## the same data set but different reporting indicator (this is what we used in the paper)
           'uti': uti,
           'uti_readmission': uti_readmission,
           'pneu': pneu}

df_final_lst = []
## clean and concat all MDS reporting data together
for n in range(0, 6):
    qm = qm_names[n] ## get the name for the clinical condition
    print(qm)
    df = df_dict.get(qm) ## get the sample data corresponding to the clinical condition
    if (qm=='pu') | (qm=='pu234'):
        ## include only hospital claims with a pressure ulcer diagnosed at Stage 2 to Stage 4
        df = df[df['highest stage'].isin([2, 3, 4])]
    if qm=='uti_readmission': ## this is not used in the paper
        ## merge UTI readmission data with UTI original data
        df = uti.merge(df[['MEDPAR_ID', 'I2300_UTI_CD']], on='MEDPAR_ID',
                                      suffixes=['', '_readmission'],
                                      how='left')
        ## fill missing value with 0 for readmission UTI
        df['I2300_UTI_CD_readmission'] = \
            df['I2300_UTI_CD_readmission'].fillna(0)
        ## the readmission reporting rate for UTI is the max value of UTI reporting on discharge or readmission MDS assessments
        df['I2300_UTI_CD_add_readmission'] = df[
            ['I2300_UTI_CD', 'I2300_UTI_CD_readmission']].max(axis=1)
        df = df.drop(columns='I2300_UTI_CD_readmission')
    if (qm=='pu') | (qm=='pu234'):
        df = df[cols_use + ['highest stage'] + [mds_items[n]]]
    else:
        df = df[cols_use + [mds_items[n]]]
    ## rename columns
    df = df.rename(columns={mds_items[n]: 'mds_reporting'})
    df['mds_item'] = qm_names[n]
    ## append data to a list
    df_final_lst.append(df)
## concat data together
df_final = pd.concat(df_final_lst)
print(df_final.head())
df_final.to_csv(inputPath + 'qm_all/initial_analysis/all_mds_items_data.csv', index=False)


