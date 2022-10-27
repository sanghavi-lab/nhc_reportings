library(dplyr)
library(readxl)

## 1: READ IN AND CLEAN CASPER DATA #####################################
## read raw casper data part2 (general info on survey) and part4 (deficiency data)
casper2 = read.csv('Z:/data/casper/April 2018/STRIP183/PART2.csv')
casper4 = read.csv('Z:/data/casper/April 2018/STRIP183/PART4.csv')
## read in casper variable names we selected for analysis 
variables = read.csv('Z:/duas/55378/Zoey/gardner/data/qm_all/initial_analysis/casper_variables.csv')

casper2_use_variables = colnames(casper2)[colnames(casper2) %in% variables$?..Variables]
casper4_use_variables = colnames(casper4)[colnames(casper4) %in% variables$?..Variables]
## subset casper data to selected variables
casper2_use = casper2 %>% select(casper2_use_variables, STATE_CD) ## add STATE_CD
casper4_use = casper4 %>% select(casper4_use_variables)

## 1.a: CLEAN DEFICIENCY DATA ############################################################
## these ftags represent different types of deficiencies that are related to MDS reporting or quality of care in nursing homes
## based on the description of ftags
ftags = c(514, 325, 314, 309, 353, 157, 278, 353, 315, 387, 389, 502, 503, 505, 507, 514, 515, 516)
## select deficiencies with specified ftags and calculate the number of deficiencies for each nursing home-survey
casper4_use_relavant = casper4_use %>% 
  filter(DFCNCY_TAG_NUM  %in% ftags) %>% 
  group_by(PRVDR_NUM, CRTFCTN_DT) %>% 
  count() %>% dplyr::rename(dfcn_count_relavant = n)
## select severe deficiencies: severity range from A to L, L is the most severe;
##and calculate the number of deficiencies for each nursing home-survey
casper4_use_severe = casper4_use %>% 
  filter(SCOPE_SVRTY_CD %in% LETTERS[7:12]) %>% 
  group_by(PRVDR_NUM, CRTFCTN_DT) %>% 
  count()%>% dplyr::rename(dfcn_count_severe = n)
##  and calculate the number of all deficiencies for each nursing home-survey
casper4_use_all = casper4_use %>% group_by(PRVDR_NUM, CRTFCTN_DT) %>% count()%>% dplyr::rename(dfcn_count_all = n)
## merge all deficiency data together
deficiency_count = 
  list(casper4_use_all, casper4_use_relavant, casper4_use_severe) %>% 
    purrr::reduce(full_join, by=c("PRVDR_NUM", "CRTFCTN_DT")) 
## replace missing with 0
deficiency_count[is.na(deficiency_count)] = 0
## create date variable for survey certification date - used for merging
deficiency_count$date = as.Date(as.character(deficiency_count$CRTFCTN_DT), format = "%Y%m%d")
  
## 1.b: CLEAN PROVIDER INFO DATA ############################################################
## create date variable for survey certification date - used for merging
casper2_use$date = as.Date(as.character(casper2_use$CRTFCTN_DT), format = "%Y%m%d")

## 1.c: MERGE PROVIDER INFO AND DEFICIENCY DATA ##########################################
casper = casper2_use  %>% dplyr::select(-CRTFCTN_DT) %>% left_join(deficiency_count, by=c("PRVDR_NUM", "date"))

## restrict to 2010 - 2017
casper$year = as.numeric(format(casper$date,'%Y'))
casper = casper %>% filter(year %in% c(2010:2017))


## mean, median, min, max, mode, time trend, the number of zeros (for some) of all numeric values 
## create binary indicators or replace missing values with 0 for some nursing home characters
casper_clean = casper %>% mutate(
  compliant = ifelse(CMPLNC_STUS_DESC=="IN COMPLIANCE", T, F),
  chain_owned = ifelse(MLT_OWND_FAC_ORG_SW=="Y", T, F),
  phy_onsite = ifelse(PHYSN_SRVC_ONST_RSDNT_SW=="Y", T, F),
  phy_offsite = ifelse(PHYSN_SRVC_OFSITE_RSDNT_SW=="Y", T, F),
  lab_onsite_res = ifelse(CL_SRVC_ONST_RSDNT_SW=="Y", T, F),
  lab_onsite_nores = ifelse(CL_SRVC_ONST_NRSDNT_SW == "Y", T, F),
  lab_offsite_res = ifelse(CL_SRVC_OFSITE_RSDNT_SW == "Y", T, F),
  lab_atall = ifelse(lab_onsite_res==F & lab_offsite_res==F, F, T),
  dfcn_count_all = replace_na(dfcn_count_all, 0),
  dfcn_count_relavant  = replace_na(dfcn_count_relavant , 0),
  dfcn_count_severe  = replace_na(dfcn_count_severe , 0),
  count_cmplt_def = replace_na(count_cmplt_def, 0)
)

## for nursing homes that had more than 1 survey within 1 year, take the average of 
## numeric variables such as the number of beds, take the max value of numeric variables
## related to deficiencies, and take the latest value of all binary variables.
## the unit of data turns from nh-survey to nh-year
casper_unique_summarise <- casper_clean %>%
  group_by(PRVDR_NUM, STATE_CD, year) %>% summarise(
    crtfd_bed = mean(CRTFD_BED_CNT),
    bed = mean(BED_CNT),
    rsdnt = mean(CNSUS_RSDNT_CNT),
    dfcn_count_all = max(dfcn_count_all ),
    dfcn_count_relavant = max(dfcn_count_relavant ),
    dfcn_count_severe = max(dfcn_count_severe ),
    count_cmplt_def = max(count_cmplt_def )
  )

casper_clean_sorted <- casper_clean %>% arrange(PRVDR_NUM, date)
## keep the latest record of CAPSER for each nurisng home wihtin a year
casper_unique_latest <- casper_clean_sorted[!duplicated(casper_clean_sorted[c("PRVDR_NUM", "year")], fromLast = TRUE),]

casper_unique <- casper_unique_summarise %>% left_join(
  casper_unique_latest %>% select(PRVDR_NUM, year, chain_owned, GNRL_CNTL_TYPE_CD, phy_onsite,phy_offsite, lab_onsite_res, lab_offsite_res, lab_atall, compliant),
  by = c("PRVDR_NUM", "year")
)

write.csv(casper_unique,
          "Z:/duas/55378/Zoey/gardner/data/qm_all/initial_analysis/casper_year_unique_new.csv")

## READ IN AND CLEAN LTCFOCUS DATA ##################################################
ltc2011 <- read_excel('Z:/data/mds/facility/csv/facility_2011.xls')
ltc2012 <- read_excel('Z:/data/mds/facility/csv/facility_2012.xls')
ltc2013 <- read_excel('Z:/data/mds/facility/csv/facility_2013.xls')
ltc2014 <- read_excel('Z:/data/mds/facility/csv/facility_2014.xls')
ltc2015 <- read_excel('Z:/data/mds/facility/csv/facility_2015.xls')
ltc2016 <- read_excel('Z:/data/mds/facility/Oct2019 Download/facility_2016.xlsx')
ltc2017 <- read_excel('Z:/data/mds/facility/Oct2019 Download/facility_2017.xlsx')


use_cols <- c("year", "PROV1680", "PROV0475", "PROV2905", "state", "pctblack_2011p", "pcthisp_2011p", "pctwhite_2011p", "paymcaid", "paymcare", "pctNHdaysSNF", "totbeds", 
              "hospbase", "multifac", "rnhrppd", "lpnhrppd", "cnahrppd", "occpct")
## include only useful columns for ltc data from 2011 - 2017; name the data as ltc{year}_use
for (year in c(2011:2017)){
  assign(paste0("ltc", year, "_use"),
         eval(as.name(paste0("ltc", year))) %>% 
           mutate(year=year) %>% select(use_cols)
  )
}

## concat years of ltc data
ltcyear <- rbind(ltc2011_use, ltc2012_use, ltc2013_use, ltc2014_use, ltc2015_use, ltc2016_use, ltc2017_use)
## rename columns
colnames(ltcyear)[2:4] <- c("PRVDR_NUM", "name", "zip")
## dropping duplicates from LTC
ltcyear = ltcyear[!duplicated(ltcyear[c("PRVDR_NUM", "year")]),]

## MERGE LTC AND CASPER DATA #############################################
## outer merge, keep nursing homes from both casper and ltc data
nh_variables <- casper_unique %>% mutate(from_casper=T) %>% 
  full_join(ltcyear %>% mutate(from_ltc=T), 
            by=c("PRVDR_NUM", "year"))
## if the state variable is missing from casper, assign the value from ltc to it
nh_variables$STATE = ifelse(is.na(nh_variables$STATE_CD), nh_variables$state, nh_variables$STATE_CD)
## remove state variable from casper(STATE_CD) or ltc(state), only keeping newly create variable STATE
nh_variables = nh_variables %>% select(-c(STATE_CD, state))
## write to csv
write.csv(nh_variables,
          "Z:/duas/55378/Zoey/gardner/data/qm_all/initial_analysis/casper_and_ltc_new.csv")

## INVESTIGATE MERGED DATA ################################################
## each year, about 1400 nursing home that have ltc values but don't have casper (casper is not assessed every year);
## each year except 2010, about 230 nursing homes don't have ltc but has casper (not sure why);
nh_variables <- read.csv("Z:/duas/55378/Zoey/gardner/data/qm_all/initial_analysis/casper_and_ltc_new.csv")
nh_variables <- nh_variables %>% mutate(
  chain_owned = as.numeric(chain_owned),
  phy_onsite = as.numeric(phy_onsite),
  phy_offsite = as.numeric(phy_offsite),
  lab_onsite_res = as.numeric(lab_onsite_res),
  lab_offsite_res = as.numeric(lab_offsite_res),
  lab_atall = as.numeric(lab_atall),
  compliant = as.numeric(compliant),
  pctblack_2011p = as.numeric(pctblack_2011p), ## some values are LNE; assuming it means the denominator is not large enough to display the exact number
  pcthisp_2011p = as.numeric(pcthisp_2011p),
  pctwhite_2011p = as.numeric(pctwhite_2011p),
  pctNHdaysSNF = as.numeric(pctNHdaysSNF),
  hospbase = case_when(hospbase=="No" ~ 0,
                       hospbase=="Yes" ~ 1,
                       is.na(hospbase) ~ NA_real_),
  multifac = case_when(multifac=="No" ~ 0,
                       multifac=="Yes" ~ 1,
                       is.na(multifac) ~ NA_real_),
  rnhrppd = as.numeric(rnhrppd),
  lpnhrppd = as.numeric(lpnhrppd),
  cnahrppd = as.numeric(cnahrppd),
  occpct = as.numeric(occpct)
)
## calculate percent missing for all variables
## 42.7% missing for pctblack_2011p; 
## 39.2% for pcthisp_2011p;
## 30.3% for pctNHdaysSNF
colMeans(is.na(nh_variables %>% filter(year!=2010)))

## get numeric variables from the data
num_variables = names(which(unlist(lapply(nh_variables, is.numeric))))

## calculate the average of numeric variables for each nursing home across years
group_avg <- 
  nh_variables %>% group_by(PRVDR_NUM) %>% 
  summarise_at(num_variables[3:length(num_variables)], mean, na.rm=TRUE)

old_group_colnames <- colnames(group_avg)
new_group_colnames <- paste(old_group_colnames[2: length(old_group_colnames)], "avg", sep="_")
colnames(group_avg) <- append(c("PRVDR_NUM"), new_group_colnames)

## merge group means to original data
nh_variables_avg <- nh_variables %>% left_join(
  group_avg, by="PRVDR_NUM"
)
for (col in new_group_colnames){
  hist(group_avg %>% select(col))
}

## drop index column
nh_variables_avg <- nh_variables_avg %>% 
  select(-c(X))

nh_variables_avg <- nh_variables_avg %>% mutate(
  ownership = case_when(GNRL_CNTL_TYPE_CD %in% c(1, 2, 3, 13) ~ 'for-profit',
                        GNRL_CNTL_TYPE_CD %in% c(4, 5, 6) ~ 'non-profit',
                        GNRL_CNTL_TYPE_CD %in% c(7:12) ~ 'government')
)


mode_ownership <- 
  nh_variables_avg  %>% group_by(PRVDR_NUM, ownership) %>% 
  count() %>% arrange(PRVDR_NUM, n)


# mode_ownership %>% filter(!is.na(ownership)) %>% 
#   group_by(PRVDR_NUM) %>% count() %>% filter(n>2) %>% nrow()

mode_ownership_unique <- mode_ownership %>% 
  group_by(PRVDR_NUM) %>% slice(which.max(n))

count_obs <- nh_variables_avg %>% group_by(PRVDR_NUM) %>% count()

nh_variables_avg_final <- nh_variables_avg %>% left_join(
  mode_ownership_unique %>% left_join(count_obs, by='PRVDR_NUM') %>% mutate(diff_ownership = n.y - n.x) %>% 
    select(PRVDR_NUM, ownership, diff_ownership),
  by = "PRVDR_NUM", suffix = c("", "_common")
)

write.csv(
  nh_variables_avg_final,
  "Z:/duas/55378/Zoey/gardner/data/qm_all/initial_analysis/casper_and_ltc_clean2.csv",
  row.names = F
)



