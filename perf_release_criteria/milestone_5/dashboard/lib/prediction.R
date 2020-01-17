library(tidyr)
library(purrr)
library(lubridate)
# library(boot)
source('lib/scoring.R')

extract_health_metrics <- function(predictions, release, outcomes, v_n1){
  agg_sets <- function(predictions){
    predictions %>% 
      select(outcomes) %>% 
      gather("metric", "value") %>%
      group_by(metric) %>%
      summarise(
        mean = mean(value),
        p5 = partial(quantile, probs = .05)(value),  
        p25 = partial(quantile, probs = .25)(value),  
        p50 = median(value),
        p75 = partial(quantile, probs = .75)(value),  
        p95 = partial(quantile, probs = .95)(value),
        p99 = partial(quantile, probs = .99)(value)
      )
  }
  
  pred_agg <- agg_sets(predictions)
  release_agg <-  agg_sets(release)
  
  abs_rel_diff <- (abs(pred_agg[-1]-release_agg[-1])/release_agg[-1])
  names(abs_rel_diff) <- paste('rel_diff_', names(abs_rel_diff), sep='')
  abs_rel_diff <- abs_rel_diff %>%
    mutate(metric = pred_agg$metric)
  
  cms_score <- sapply(outcomes, calc_cms, beta=predictions, release=release)
  cms_score <- data.frame(cms_score) %>% mutate(metric = rownames(.))
  
  if (missing(v_n1)) v_n1 <- unique(predictions$app_version)[1]
  
  health_metrics <- pred_agg %>% 
    inner_join(., abs_rel_diff, by='metric') %>%
    inner_join(., cms_score, by='metric') %>%
    # mutate(cms_score = cms_score) %>%
    mutate(date_created = now())  %>%
    mutate(version = v_n1)
  
  return(health_metrics)
}

# bootstrap_mean <- function(predictions, release, metrics){
#   agg_sets <- function(predictions){
#     predictions %>% 
#       select(metrics) %>% 
#       gather("metric", "value") %>%
#       group_by(metric) %>%
#       summarise(
#         mean = mean(value)
#       ) %>%
#       pull(mean)
#   }
#   
#   bs_stat <- function(df, indices) {
#     df_trim <- df[indices, ]
#     predictions <- df_trim %>% filter(label == 'beta')
#     release <- df_trim %>% filter(label == 'release')
#     pred_agg <- agg_sets(predictions)
#     # release_agg <- agg_sets(release[indices,])
#     release_agg <- agg_sets(release)
#     rel_diff <- (abs(pred_agg-release_agg)/release_agg)
#     return(rel_diff)
#   } 
#   return(boot(data=predictions %>% bind_rows(release), statistic=bs_stat, 
#               R=100, parallel = "multicore", ncpus = 2))
# }