# prepare_dashboard.R

library(bigrquery)
library(stringr)
library(dplyr)
library(dbplyr)
source('../../lib/prediction.R')
source(file = '../../lib/scoring.R')

#### Config ####
project <- "moz-fx-dev-cdowhyglund-subbeta"
pred_tbl_prefix <- 'ccd_subbeta_prelease_subset_'
beta_tbl_prefix <- 'ccd_subbeta_prelease_cleaned_'
valid_tbl_prefix <- 'ccd_subbeta_validation_cleaned_'

#### Funcs ####
import_data <- function(tbl_prefix, version, dataset) {
  tbl <- paste(tbl_prefix, version, sep='')
  
  con <- dbConnect(
    bigrquery::bigquery(),
    project = project,
    dataset = dataset,
    billing = project
  )
  
  df <- tbl(con, tbl) %>%
    collect()
  
  return(df)
}

#### Step 1 ####
# query subbeta tables to get current version 
v_n1 <- max(
  as.numeric(
    str_match(list_tables(project, 'prerelease'), 
              paste(pred_tbl_prefix, '([0-9]+)', sep='' ))[, 2]),
  na.rm = TRUE
)

#### Step 2 ####
# pull current predictions from DB
pred_v_n1 <- import_data(pred_tbl_prefix, v_n1, 'prerelease')

beta_v_n1 <- import_data(beta_tbl_prefix, v_n1, 'prerelease')

#### Step 3 ####
# extract levels for use in dashboard
source('../../lib/attributes.R')

# os_version <- c('Other', '6.1', '6.2', '6.3', '10.0')
# cpu_cores_cat <- c("1",  "2", '< 4',  '< 8', "> 16" "< 8"  "< 4"  "< 16") # levels(predictions$cpu_cores_cat)
# cpu_speed_cat <- unique(predictions$cpu_speed_cat) # levels(predictions$cpu_speed_cat)

#### Step 4 ####
v_n <- v_n1 - 1

# pull previous predictions 
pred_v_n <- import_data(pred_tbl_prefix, v_n, 'prerelease')

# pull current validation (this is used for both Health metrics and validating)
valid_v_n <- import_data(valid_tbl_prefix, v_n, 'validation')

# pull all of current Beta
beta_v_n <- import_data(beta_tbl_prefix, v_n, 'prerelease')

#### Step 5 ####
# load up outcomes
metrics <- names(get_m2_metric_map())

#### Step 6 ####
# Create a full dataframe containing both validation and predictions
# df <- valid_v_n %>% 
#   bind_rows(pred_v_n1)
df <- pred_v_n1 %>%
  mutate(label = 'beta-matched') %>%
  bind_rows(beta_v_n) %>%
  bind_rows(valid_v_n)


#### Step 7 ####
# Calculate health metrics
health_metrics <- extract_health_metrics(pred_v_n1, valid_v_n, metrics)

#### Step 8 ####
# Save image for later loading when dashboard initializes
save(v_n1, v_n, pred_v_n1, beta_v_n1, pred_v_n, valid_v_n, beta_v_n, metrics, df, file='data/dashboard_prepped.RData')
