# Nursing Home Care Compare Reportings on Falls and Pressure Ulcers Code Description

## Software

We used Python 3.8 and R 4.2.1 for this analysis.

## 1. Setup sample data for fall hospitalizations in 2016 and 2017

We used the same methods from [fall](https://onlinelibrary.wiley.com/doi/abs/10.1111/1475-6773.13247) and [pressure ulcer](https://journals.lww.com/lww-medicalcare/Fulltext/2022/10000/Accuracy_of_Pressure_Ulcer_Events_in_US_Nursing.7.aspx) papers to build the sample data for fall hospitalizations in 2016 and 2017 (the previous study period in the fall paper was for 2011 - 2015). We identified major injury fall hospitalizations from MedPAR data, and linked the claims to MDS assessments for residents who were discharged from nursing homes, admitted to the hospital within one day, and returned to the same nursing home within one day after hospital discharge. 

Scripts used in this step were listed in the "fall" folder.

## 2. Create nursing home variables using MDS assessments data and MBSF data

By linking MDS data and MBSF data, we created several nursing home-level variables that were used in the paper: the number of Medicare fee-for-service residents in nursing homes stratified by short stay and long stay as well as by White and Non-White, the percent of White residents, the percent of residents dually eligible for Medicare and Medicaid, the percent of residents with ADRD. We also aggregated ratings and quality measures from Nursing Home Care Compare (NHCC) data for nursng homes. Below are the scripts in this step.

|Script name |Input files (File source) |Output files|
|-|-|-|
|0_create_nh_variables_mbsf.py|MDS and MBSF and MBSF Chronic Conditions from 2011 - 2017|nh_medicare_res_count_shortlongstay.csv<br>nh_medicare_res_count_shortlongstay_race.csv<br>nh_variables_capser.csv|
|0_medicare_count_adrd.py|MDS and MBSF and MBSF Chronic Conditions from 2011 - 2017|nh_adrd.csv|
|0_create_nhc_data.py|Ratings and quality measure data from NHCC website|nh_nhc_measures.csv|

## 3. Merge all MDS reporting data

This step combined MDS reporting data previously created for [falls](https://github.com/sanghavi-lab/nhc_falls/blob/master/README.md) and [pressure ulcers](https://github.com/sanghavi-lab/nhc_pressure_ulcer). Please see detailed descriptions and code in corresponding links. The script for this step is **1_merge_all_mds_item.py**, and the output from this script was **all_mds_items_data.csv**.

## 4. Create nursing home sample data

This step created nursing home sample from CASPER and LTCfocus, linked MDS reporting data to nursing home characteristics, and transformed all nurisng home-year variables to nursing home variables. Below are the scripts in this step.

|Script name |Input files (File source) |Output files|
|-|-|-|
|2_create_nh_sample_casper_ltcfocus.R|CASPER and LTCfocus data from 2011 - 2017|casper_and_ltc_clean2.csv|
|3_create_final_nh_sample.py|all_mds_items_data.csv<br>casper_and_ltc_clean2.csv<br>nh_nhc_measures.csv<br>nh_medicare_res_count_shortlongstay.csv<br>nh_medicare_res_count_shortlongstay_race.csv<br>nh_variables_capser.csv<br>forhp-eligible-zips.csv|nh_characteristics_20221020.csv|

## 5. Create exhibits

This script 4_create_exhibits.R used the final nurisng home sample data from last step and created all exhibits published in the paper.








