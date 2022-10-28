library(dplyr)
library(ggplot2)
library(tidyverse)

## read in and clean the data #########
df = read.csv("Z:/duas/55378/Zoey/gardner/data/qm_all/initial_analysis/nh_characteristics_20221020.csv")
## read in state code crosswalk
state_cw = read.csv("Y:/Pan/NH/data/statexwalk/statecode.csv")
df = df %>% left_join(state_cw, by=c("STATE"="state"))
df = df %>% mutate(FAC_ZIP=as.character(FAC_ZIP),
                   forprofit = ifelse(ownership_common=='for-profit', 1, 0),
                   northeast = ifelse(state_code %in% c("9", "23", "25", "33", "44" ,"50", "34" ,"36", "42"), 1, 0),
                   midwest = ifelse(state_code %in% c("18", "17", "26", "39", "55", "19", "20", "27", "29", "31", "38", "46"), 1, 0),
                   south = ifelse(state_code %in% c("10", "11", "12", "13", "24", "37", "45", "51", "54", "1", "21", "28", "47", "5", "22", "40", "48", '78'), 1, 0),
                   west = ifelse(state_code %in% c("4", "8", "16", "35", "30", "49", "32", "56", "2", "6", "15", "41","53"), 1, 0),
                   has_claims = ifelse(is.na(nclaims_all), 0, 1),
                   hospbase = case_when(hospbase_avg==1 ~ 1,
                                        hospbase_avg==0 ~ 0) ## only 1.7% of nursing homes's hospbase variable changed durign study period
)

## calculate the number of claims by pu stage
mds_reporting = read.csv('Z:/duas/55378/Zoey/gardner/data/qm_all/initial_analysis/all_mds_items_data_20220930.csv')
mds_pu_nh = mds_reporting %>% filter(mds_item=='pu234') %>% 
  group_by(MCARE_ID, short_stay, highest.stage) %>% summarise(nclaims=n(), nreported=sum(mds_reporting))
mds_pu_nh_wide = mds_pu_nh %>% pivot_wider(names_from = highest.stage, values_from = c("nclaims", "nreported"))
colnames(mds_pu_nh_wide) = c('PRVDR_NUM', 'short_stay', 'nclaims_pu4', 'nclaims_pu3', 'nclaims_pu2', 'nreported_pu4', 'nreported_pu3', 'nreported_pu2')
mds_pu_nh_wide = mds_pu_nh_wide %>% mutate(
  nclaims_pu2 = replace_na(nclaims_pu2, 0),
  nclaims_pu3 = replace_na(nclaims_pu3, 0),
  nclaims_pu4 = replace_na(nclaims_pu4, 0),
  nreported_pu2 = replace_na(nreported_pu2, 0),
  nreported_pu3 = replace_na(nreported_pu3, 0),
  nreported_pu4 = replace_na(nreported_pu4, 0)
)

df$short_stay = case_when(df$short_stay=='False' ~ 0,
                          df$short_stay=='True' ~ 1,
                          df$short_stay=='1.0' ~ 1,
                          df$short_stay=='0.0' ~ 0)

df = df %>% left_join(mds_pu_nh_wide, by=c("PRVDR_NUM", "short_stay")) 
df = df %>% 
  mutate(nclaims_pu34 = nclaims_pu3 + nclaims_pu4,
         nreported_pu34 = nreported_pu3 + nreported_pu4,
         claims_based_rate_pu34 = nclaims_pu34/medicare_count,
         reporting_pu34 = nreported_pu34/nclaims_pu34,
         nclaims_fall = ifelse(is.na(nclaims_fall), 0, nclaims_fall),
         claims_based_rate_fall = ifelse(nclaims_fall==0, 0, claims_based_rate_fall),
         nclaims_pu34 = ifelse(is.na(nclaims_pu34), 0, nclaims_pu34),
         claims_based_rate_pu34 = ifelse(nclaims_pu34==0, 0, claims_based_rate_pu34)
         ) 
df = df %>% drop_na(medicare_count)

## SUBSET BASED ON NURISNG HOME SIZE ##############
## exclude nursing homes under 10th percentile of resident count (excluding small nursing homes)
## rationale: very small nursing homes could have very few or no hospital claims, thus exclude them from our sample
## 13189 nursing homes left
small_rsdnt = quantile(df[!duplicated(df[,c('PRVDR_NUM','rsdnt_avg')]), 'rsdnt_avg'], 0.1)
df.9 = df %>% filter(rsdnt_avg > quantile(df$rsdnt_avg, 0.1))
## CREATE NURSING HOME CATEGORICAL VARIABLES #################################
reporting_cutoff = c(-Inf, 0.5, 0.8, Inf)
df.9$reporting_cat_pu <- cut(df.9$reporting_pu34, reporting_cutoff, labels=c("low_reporting", "medium_reporting", "high_reporting"))
df.9$reporting_cat_fall <- cut(df.9$reporting_fall, reporting_cutoff, labels=c("low_reporting", "medium_reporting", "high_reporting"))

level_reporting_cat = c("low_reporting", "medium_reporting", "high_reporting")

df.9$reporting_cat_pu = factor(df.9$reporting_cat_pu, levels=level_reporting_cat)
df.9$reporting_cat_fall = factor(df.9$reporting_cat_fall, levels=level_reporting_cat)

df.9 = df.9 %>% mutate(reported_rate_fall = nreported_fall/medicare_count,
                       reported_rate_pu34 = nreported_pu34/medicare_count)
# df.9$claims_based_rate_fall_cut = cut(df.9$claims_based_rate_fall,
#                                       breaks = c(seq(0, 0.05, by=0.00125), max(df.9$claims_based_rate_fall, na.rm=TRUE)), 
#                                       labels = c(seq(0.000625, 0.05, 0.00125), (0.05 + max(df.9$claims_based_rate_fall, na.rm=TRUE))/2), 
#                                       include.lowest = T)
# df.9$reported_rate_fall_cut = cut(df.9$reported_rate_fall,
#                                   breaks = c(seq(0, 0.05, by=0.00125), max(df.9$reported_rate_fall, na.rm=TRUE)), 
#                                   labels = c(seq(0.000625, 0.05, 0.00125), (0.05 + max(df.9$reported_rate_fall, na.rm=TRUE))/2), 
#                                   include.lowest = T)
# df.9$claims_based_rate_pu34_cut = cut(df.9$claims_based_rate_pu34,
#                                       breaks = c(seq(0, 0.05, by=0.00125), max(df.9$claims_based_rate_pu34, na.rm=TRUE)), 
#                                       labels = c(seq(0.000625, 0.05, 0.00125), (0.05 + max(df.9$claims_based_rate_pu34, na.rm=TRUE))/2), 
#                                       include.lowest = T)
# df.9$reported_rate_pu34_cut = cut(df.9$reported_rate_pu34,
#                                   breaks = c(seq(0, 0.05, by=0.00125), max(df.9$reported_rate_pu34, na.rm=TRUE)), 
#                                   labels = c(seq(0.000625, 0.05, 0.00125), (0.05 + max(df.9$reported_rate_pu34, na.rm=TRUE))/2), 
#                                   include.lowest = T)

## make the cut based on distribution of long-stay 
## this cutoff might change if we want to include only nursing homes with at least 1 claims
df.9 = df.9 %>% mutate(percent_white = case_when(white_percent>0.94 ~ 'high',
                                                 white_percent>0.80 & white_percent<=0.94 ~ 'medium',
                                                 white_percent<=0.80 ~ 'low'),
                       percent_dual = case_when(dual_percent>0.64 ~ 'high',
                                                dual_percent>0.44 & dual_percent<=0.64 ~ 'medium',
                                                dual_percent<=0.44 ~ 'low'),
                       claims_rate_fall_cut = case_when(claims_based_rate_fall<=0.0088 ~'low',
                                                        claims_based_rate_fall>0.0088 & claims_based_rate_fall<=0.014 ~ 'medium',
                                                        claims_based_rate_fall>0.014 ~ 'high'),
                       claims_rate_pu34_cut = case_when(claims_based_rate_pu34<=0.003 ~'low',
                                                        claims_based_rate_pu34>0.003 & claims_based_rate_pu34<=0.007 ~ 'medium',
                                                        claims_based_rate_pu34>0.007 ~ 'high'),
                       claims_rate_fall_cut_with0 = case_when(claims_based_rate_fall<=0.0085 ~'low',
                                                              claims_based_rate_fall>0.0085 & claims_based_rate_fall<=0.014 ~ 'medium',
                                                              claims_based_rate_fall>0.014 ~ 'high'),
                       claims_rate_pu34_cut_with0 = case_when(claims_based_rate_pu34<=0.0015 ~'low',
                                                              claims_based_rate_pu34>0.0015 & claims_based_rate_pu34<=0.0053 ~ 'medium',
                                                              claims_based_rate_pu34>0.0053 ~ 'high'))

df.9$claims_rate_fall_cut = factor(df.9$claims_rate_fall_cut, levels=c("low", "medium", "high"))
df.9$claims_rate_pu34_cut = factor(df.9$claims_rate_pu34_cut, levels=c("low", "medium", "high"))
df.9$claims_rate_fall_cut_with0 = factor(df.9$claims_rate_fall_cut_with0, levels=c("low", "medium", "high"))
df.9$claims_rate_pu34_cut_with0 = factor(df.9$claims_rate_pu34_cut_with0, levels=c("low", "medium", "high"))

df.9.l = df.9 %>% filter(short_stay==0)
df.9.s = df.9 %>% filter(short_stay==1)


## NURSING HOME CHARACTERISTICS BY HIGH, M, AND L REPORTING RATES ##########################################
calculate_cols = c("rsdnt_avg","totbeds_avg", "occpct_avg", "dfcn_count_all_avg", "phy_onsite_avg", 
                   "pctblack_2011p_avg", "white_percent","pctNHdaysSNF_avg", "hospbase",
                   "rnhrppd_avg" ,"lpnhrppd_avg","rural", "dual_percent", "adrd_percent", 
                  "overall_rating","quality_rating", "survey_rating","staffing_rating", 
                   "fall_qm","lpu_qm", "spu_qm", "paymcaid_avg", "northeast", "midwest", "south", "west")

df.9 %>% filter(nclaims_fall!=0) %>% 
  group_by(short_stay, reporting_cat_fall) %>% 
  select(reporting_fall, calculate_cols, nclaims_fall, claims_based_rate_fall, reported_rate_fall) %>% 
  summarise_all(mean, na.rm=T) %>% 
  write.csv("Z:/duas/55378/Zoey/gardner/data/qm_all/initial_analysis/tables/nh_characters_by_ls_fall_reporting_20221019.csv")
df.9 %>% filter(nclaims_pu34>2) %>% 
  group_by(short_stay, reporting_cat_pu) %>% 
  select(reporting_pu34, calculate_cols, nclaims_pu34, claims_based_rate_pu34, reported_rate_pu34) %>% 
  summarise_all(mean, na.rm=T) %>% 
  write.csv("Z:/duas/55378/Zoey/gardner/data/qm_all/initial_analysis/tables/nh_characters_by_ls_pu_reporting_20221027.csv")

df.9 %>% filter(short_stay==0) %>% 
  group_by(claims_rate_fall_cut_with0) %>% 
  select(reporting_fall, calculate_cols, nclaims_fall, claims_based_rate_fall, reported_rate_fall) %>% 
  summarise_all(mean, na.rm=T) %>% 
  write.csv("Z:/duas/55378/Zoey/gardner/data/qm_all/initial_analysis/tables/nh_characters_by_l_fall_claimsrate_with0_20221019.csv")
df.9 %>% filter(short_stay==0) %>% 
  group_by(claims_rate_pu34_cut_with0) %>% 
  select(reporting_pu34, calculate_cols, nclaims_pu34, claims_based_rate_pu34, reported_rate_pu34) %>% 
  summarise_all(mean, na.rm=T) %>% 
  write.csv("Z:/duas/55378/Zoey/gardner/data/qm_all/initial_analysis/tables/nh_characters_by_l_pu_claims_with0_20221019.csv")
df.9 %>% filter(short_stay==0) %>% group_by(claims_rate_fall_cut_with0) %>% count()

## plot by states #################
df.9.l.state = df.9.l %>% 
  group_by(STATE) %>% 
  summarise(reporting_pu34 = mean(reporting_pu34, na.rm=T),
            reporting_fall = mean(reporting_fall, na.rm=T),
            nreported_pu34 = sum(nreported_pu34, na.rm=T),
            nreported_fall = sum(nreported_fall, na.rm=T),
            white_percent = mean(white_percent, na.rm=T),
            n_nh = n()) %>% 
  mutate(percent_white_cat = ifelse(white_percent>0.83, 'high', 'low')) 

reg.l = lm(reporting_fall ~ reporting_pu34, 
           df.9.l.state %>% filter(nreported_pu34>10 & nreported_fall>10))


p.state.fallpu = 
       ggplot(df.9.l.state %>% filter(nreported_pu34>10 & nreported_fall>10), 
              aes(x= reporting_pu34, y= reporting_fall,  label=STATE, color=percent_white_cat))+
         geom_point() +geom_text(hjust=0, vjust=0, size=5) +
         xlab('MDS reporting rates of pressure ulcer hospitalizations for long-stay residents') + 
         ylab('MDS reporting rates of major injury fall hospitalizations \nfor long-stay residents') +
  scale_color_manual(values=c('red', 'blue'),
                     labels=c('Above average', 'Below average%'), #83%
                     name='Average percent of white residents \nin nursing homes',
                     aesthetics = 'color') + 
  geom_abline(slope = coef(reg.l)[["reporting_pu34"]], 
              intercept = coef(reg.l)[["(Intercept)"]])+
  annotate("text", size=5, x=0.43, y=0.78, label= paste0('Slope = ', round(coef(reg.l)[["reporting_pu34"]], digits=2))) +  
  theme_classic() +   
  theme(legend.position="none",
        text=element_text(size=15))+
  xlim(0.4, 0.8) + ylim(0.5, 0.9)


df.9.pu.state = df.9.l %>% select(MCARE_ID, STATE, nreported_pu34, reporting_pu34, white_percent) %>% 
  full_join(df.9.s %>% select(MCARE_ID, nreported_pu34, reporting_pu34),
              by = "MCARE_ID",
              suffix = c("_long", "_short")) %>% 
  group_by(STATE) %>% 
  summarise(reporting_pu34_short = mean(reporting_pu34_short, na.rm=T),
            reporting_pu34_long = mean(reporting_pu34_long, na.rm=T),
            nreported_pu34_short = sum(nreported_pu34_short, na.rm=T),
            nreported_pu34_long = sum(nreported_pu34_long, na.rm=T),
            white_percent = mean(white_percent, na.rm=T),
            n_nh = n()) %>% 
  mutate(percent_white_cat = ifelse(white_percent>0.83, 'high', 'low'))

reg.pu = lm(reporting_pu34_short ~ reporting_pu34_long, 
            df.9.pu.state %>% filter(nreported_pu34_short>10 & nreported_pu34_long>10))

p.state.pu = 
  ggplot(df.9.pu.state %>% filter(nreported_pu34_short>10 & nreported_pu34_long>10), 
         aes(x= reporting_pu34_long, y= reporting_pu34_short,  label=STATE, color=percent_white_cat))+
  geom_point() +geom_text(hjust=0, vjust=0, size=5) +
  xlab('MDS reporting rates of pressure ulcer hospitalizations for long-stay residents') + 
  ylab('MDS reporting rates of pressure ulcer hospitalizations \nfor short-stay residents') +
  scale_color_manual(values=c('red', 'blue'),
                     labels=c('Above average', 'Below average'),
                     name='State average of the percent of White residents \nin nursing homes',
                     aesthetics = 'color') +
  geom_abline(slope = coef(reg.pu)[["reporting_pu34_long"]], 
              intercept = coef(reg.pu)[["(Intercept)"]])+
  annotate("text", size=5, x=0.43, y=0.61, label= paste0('Slope = ', round(coef(reg.pu)[["reporting_pu34_long"]], digits=2))) +  
  theme_classic() +
  theme(legend.position = c(0.4, 0.88),
        legend.background = element_rect(fill = "transparent", color = "black"),
        text=element_text(size=15),
        plot.margin = unit(c(0.3,0.3,0.0,0.15), "cm"),
        axis.text.x =element_blank(),axis.title.x =element_blank())+
  xlim(0.4, 0.8) + ylim(0.5, 0.9)
aligned_plots <- cowplot::align_plots(p.state.pu, p.state.fallpu, 
                                       align="vh")
stacked_plots <- do.call(cowplot::plot_grid, c(aligned_plots, ncol=1))
ggsave(filename='Z:/duas/55378/Zoey/gardner/data/qm_all/initial_analysis/plots/reporting_states_20221021.pdf',
       width=8.5, height=16,
       stacked_plots)
ggsave(filename='Z:/duas/55378/Zoey/gardner/data/qm_all/initial_analysis/plots/reporting_states_20221021_2.pdf',
       width=8.5, height=16,
       ggpubr::ggarrange(p.state.pu, p.state.fallpu, nrow=2))

## TABLE 2: hospitalization rates in different nursing home populations ########
## caculate mds reporting rates for nursing homes by race
mds_reporting_by_race = 
  mds_reporting %>% 
  filter(race_name %in% c('white', 'black') %>% 
  filter(mds_item=='fall' | (mds_item=='pu234' & highest.stage %in% c(3, 4))) %>% 
  group_by(MCARE_ID, short_stay, mds_item, race_name) %>% 
  summarise(nreported = sum(mds_reporting),
            nclaims = n())
## reshape data from long to wide
mds_reporting_by_race_wide = 
  mds_reporting_by_race %>% 
  pivot_wider(names_from = race, values_from = c("nclaims", "nreported")) %>% 
  mutate(nclaims_white = replace_na(nclaims_white, 0), ## replace missing with 0
         nclaims_black = replace_na(nclaims_black, 0),
         nreported_white = replace_na(nreported_white, 0),
         nreported_black = replace_na(nreported_black, 0))

nh_variables = read.csv('Z:/duas/55378/Zoey/gardner/data/mds/nh_population/nh_variables_capser.csv')
nh_race = nh_variables %>% group_by(MCARE_ID) %>% 
  summarise(white_percent = mean(white_percent))

nh_shortlong_count = read.csv('Z:/duas/55378/Zoey/gardner/data/mds/nh_population/nh_medicare_res_count_shortlongstay_race.csv')
## for each nursing home, calculate the number of White and Non-White residents
nh_sl_pop = nh_shortlong_count %>% 
  group_by(MCARE_ID, short_stay) %>% 
  summarise(white_res_count = sum(white),
            black_res_count = sum(black)) 
nh_sl_race_pop = nh_sl_pop %>% inner_join(nh_race, by='MCARE_ID')
nh_sl_race_pop = nh_sl_race_pop %>% filter(MCARE_ID!='************') %>% 
  mutate(short_stay=ifelse(short_stay=='False', 0, 1))

## merge nursing home resident count by race data with MDS reporting by race data
nh_reporting_by_race_wide =
  mds_reporting_by_race_wide %>% full_join(nh_sl_race_pop, by=c('MCARE_ID', 'short_stay'))

## subset the data to our sample
nh_reporting_by_race_insample = nh_reporting_by_race_wide %>% filter(MCARE_ID %in% df.9.l$MCARE_ID)

df_analysis = nh_reporting_by_race_insample %>% mutate(
  claims_rate_white = nclaims_white/white_res_count,
  claims_rate_black = nclaims_black/black_res_count,
  reported_rate_white = nreported_white/white_res_count,
  reported_rate_black = nreported_black/black_res_count,
  reporting_white = nreported_white/nclaims_white,
  reporting_black = nreported_black/nclaims_black
)
## select only long stay 
df_analysis_l = df_analysis %>% filter(short_stay==0)
## reshape data from long to wide
df_analysis_l_wide = 
  df_analysis_l %>% 
  pivot_wider(names_from = mds_item, values_from = c("nclaims_white", "nclaims_black", "nreported_white", "nreported_black",
                                                     "claims_rate_white", "claims_rate_black", "reported_rate_white", "reported_rate_black",
                                                     "reporting_white", "reporting_black"))


df_analysis_l_wide = df_analysis_l_wide %>% 
  filter(white_res_count!=0 & black_res_count!=0) %>% #
  mutate(
    nclaims_white_fall=replace_na(nclaims_white_fall, 0),
    nclaims_black_fall=replace_na(nclaims_black_fall, 0),
    nclaims_white_pu234=replace_na(nclaims_white_pu234, 0),
    nclaims_black_pu234=replace_na(nclaims_black_pu234, 0),
    claims_rate_white_fall = replace_na(claims_rate_white_fall, 0),
    claims_rate_black_fall = replace_na(claims_rate_black_fall, 0),
    claims_rate_white_pu234 = replace_na(claims_rate_white_pu234, 0),
    claims_rate_black_pu234 = replace_na(claims_rate_black_pu234, 0)
  ) 

## create a subsample of nursing homes with at least 50 White and Non-White residents
df_analysis_l_wide_noextr = df_analysis_l_wide %>% filter(white_res_count>50 & black_res_count>50)
## categorize nursing homes based on the distribution of percent of white residents
df_analysis_l_wide_noextr$percent_white_cat = case_when(df_analysis_l_wide_noextr$white_percent>=0.82 ~ 'high',
                                                        df_analysis_l_wide_noextr$white_percent<=0.82 & df_analysis_l_wide_noextr$white_percent>=0.63 ~ 'medium',
                                                        df_analysis_l_wide_noextr$white_percent<0.63 ~ 'low')
df_analysis_l_wide_noextr$percent_white_cat = factor(df_analysis_l_wide_noextr$percent_white_cat, levels=c('high', 'medium', 'low'))

df_analysis_l_wide$percent_white_cat = case_when(df_analysis_l_wide$white_percent>=0.95 ~ 'high', #66 percentile
                                                 df_analysis_l_wide$white_percent<0.95 & df_analysis_l_wide$white_percent>=0.79 ~ 'medium', ## 33 percentile
                                                 df_analysis_l_wide$white_percent<0.79 ~ 'low') 
df_analysis_l_wide$percent_white_cat = factor(df_analysis_l_wide$percent_white_cat, levels=c('high', 'medium', 'low'))

## calculate overall fall rate
df_analysis_l_wide = df_analysis_l_wide %>% 
  mutate(claims_rate_fall = (nclaims_white_fall+nclaims_black_fall)/(white_res_count + black_res_count),
         claims_rate_pu234 = (nclaims_white_pu234+nclaims_black_pu234)/(white_res_count + black_res_count),
  )
df_analysis_l_wide_noextr = df_analysis_l_wide_noextr %>% 
  mutate(claims_rate_fall = (nclaims_white_fall+nclaims_black_fall)/(white_res_count + black_res_count),
         claims_rate_pu234 = (nclaims_white_pu234+nclaims_black_pu234)/(white_res_count + black_res_count),
  )
## calculate average, 25th and 75th percentile of hospitalization rate within each level of nursing home race mix
df_analysis_l_wide_noextr %>% filter(!is.na(white_percent)) %>% 
  group_by(percent_white_cat) %>% select(white_percent, claims_rate_fall,claims_rate_white_fall, claims_rate_black_fall, 
                                         claims_rate_pu234,claims_rate_white_pu234,claims_rate_black_pu234) %>% 
  summarise_all(list(mean=mean, standard_deviation=sd, lowerCI= ~ quantile(.x, probs=0.25), upperCI= ~ quantile(.x, probs=0.75))) %>% 
  write.csv('Z:/duas/55378/Zoey/gardner/data/qm_all/initial_analysis/tables/claims_rate_fall_pu_noextreme_with0_20221019.csv')

df_analysis_l_wide %>% filter(!is.na(white_percent)) %>% 
  group_by(percent_white_cat) %>% select(white_percent,claims_rate_fall,claims_rate_white_fall, claims_rate_black_fall, 
                                         claims_rate_pu234,claims_rate_white_pu234,claims_rate_black_pu234) %>% 
  summarise_all(list(mean=mean, standard_deviation=sd, lowerCI= ~ quantile(.x, probs=0.25), upperCI= ~ quantile(.x, probs=0.75))) %>% 
  write.csv('Z:/duas/55378/Zoey/gardner/data/qm_all/initial_analysis/tables/claims_rate_fall_pu_with0_20221019.csv')
