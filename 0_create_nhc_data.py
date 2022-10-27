## PROJECT: NURSING HOME REPORTING FOR FALL AND PRESSURE ULCER
## AUTHOR: ZOEY CHEN

## THIS SCRIPT CALCULATE THE AVERAGE OF STAR RATINGS AND QUALITY MEASURES OF FALL, PU AND UTI FOR EACH NURSING HOME
## ACROSS YEARS
import pandas as pd
import numpy as np
pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)

## read in five-star ratings file
star_09_15 = pd.read_sas('/gpfs/data/konetzka-lab/Data/5stars/NHCRatings/SAS/nhc_2009_2015.sas7bdat',
                         format='sas7bdat', encoding='latin-1')
star16_17 = [pd.read_csv(path) for path in
             ['/gpfs/data/cms-share/duas/55378/FiveStar/Rating/ProviderInfo_2016.csv',
              '/gpfs/data/cms-share/duas/55378/FiveStar/Rating/ProviderInfo_2017.csv']]

## define rating columns
rating_col = ['overall_rating', 'quality_rating', 'survey_rating', 'staffing_rating']

# ## pre-process Five-star ratings data #####################################
## separate time column into year and quarter columns
star_09_15[['year', 'quarter']] = star_09_15.time.str.split('_', expand=True)
## remove the character "Q" from quarter column
star_09_15['quarter'] = star_09_15['quarter'].str.replace('Q', '')

star_09_15['year'] = pd.to_numeric(star_09_15['year'], errors='coerce')
star_09_15['quarter'] = pd.to_numeric(star_09_15['quarter'], errors='coerce')
star_09_15 = star_09_15.astype({'provnum': 'str'})

## select ratings form 2011 - 2015
star_11_15 = star_09_15[star_09_15['year']>=2011]

## pre-process ratings in 2016-2017
star16_17 = pd.concat(star16_17)
star16_17.columns = [i.lower() for i in star16_17.columns]
## separate quarter column into year and quarter columns
star16_17[['year', 'quarter']] = star16_17.quarter.str.split('Q', expand=True)

star16_17['year'] = pd.to_numeric(star16_17['year'], errors='coerce')
star16_17['quarter'] = pd.to_numeric(star16_17['quarter'], errors='coerce')
star16_17 = star16_17.astype({'provnum': 'str'})

## combine ratings from 2011 - 2017
nh_star = pd.concat([star_11_15[['provnum', 'year', 'quarter'] + rating_col], star16_17[['provnum', 'year', 'quarter'] + rating_col]])
## calculate the average of quarterly star ratings
nh_star_avg = nh_star.groupby('provnum')[rating_col].mean().reset_index()

## read in Quality Measures (QMs) data
qm_11_17 = pd.read_csv('/gpfs/data/cms-share/duas/55378/FiveStar/QM/quality_measure_2011_2017.csv')
qm_11_17 = qm_11_17.drop(columns='Unnamed: 0')

## drop 2016 data - the original 2016 raw data in qm_11_17 have significant number of nursing homes missing
qm_11_17 = qm_11_17[qm_11_17['year']!=2016]
## read in another 2016 raw data - more complete
qm16 = pd.read_csv('/gpfs/data/cms-share/duas/55378/FiveStar/QM/QualityMsrMDS_12_2016.csv')
qm16 = qm16[['PROVNUM', 'MSR_CD', 'Q1_MEASURE_SCORE', 'Q2_MEASURE_SCORE', 'Q3_MEASURE_SCORE', 'Q4_MEASURE_SCORE']]
qm16.columns = ['provnum', 'msr_cd', 'q1', 'q2', 'q3', 'q4']
## reshape data from wide to long
qm16_long = pd.wide_to_long(qm16, stubnames='q', i=['provnum', 'msr_cd'], j='quarter').reset_index()
qm16_long = qm16_long.rename({'q': 'MEASURE_SCORE'})
qm16_long['year'] = 2016
## combine 2011 - 2017 quality measure
qm_11_17 = pd.concat([qm_11_17, qm16_long])

## select relevant QM
qm_11_17 = qm_11_17[qm_11_17['msr_cd'].isin(['403', '425', '410', '407'])]

qm_11_17 = qm_11_17.astype({'provnum': 'str',
                            'msr_cd': 'object',
                            'MEASURE_SCORE': 'object'})
qm_11_17['msr_cd'] = qm_11_17['msr_cd'].replace(to_replace=[403, 425, 410, 407],
                                                value=['lpu_qm', 'spu_qm', 'fall_qm', 'uti_qm'])
## 199 and 201 are special values for QM score before 2013
qm_11_17 = qm_11_17.replace({'MEASURE_SCORE': {199: np.nan, 201: np.nan}})

## calculate the average of QMs
qm_11_17_nh = qm_11_17.groupby(['provnum', 'msr_cd']).MEASURE_SCORE.mean().reset_index()
qm_nh_wide = qm_11_17_nh.pivot(index='provnum', columns='msr_cd', values='MEASURE_SCORE').reset_index()

## merge star ratings and quality measures together
nh_star_avg = nh_star_avg.astype({'provnum': 'str'})

nh_rating = nh_star_avg.merge(qm_nh_wide, on='provnum', how='outer')
## write to csv
nh_rating.to_csv(
    '/gpfs/data/cms-share/duas/55378/Zoey/gardner/data/qm_all/initial_analysis/nh_nhc_measures.csv',
    index=False
)