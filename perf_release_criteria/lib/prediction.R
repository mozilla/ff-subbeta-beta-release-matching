library(tidyr)
library(purrr)
library(lubridate)

extract_health_metrics <- function(predictions, release, outcomes){
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
  
  health_metrics <- pred_agg %>% 
    inner_join(., abs_rel_diff, by='metric') %>%
    inner_join(., cms_score, by='metric') %>%
    # mutate(cms_score = cms_score) %>%
    mutate(date_created = now()) %>%
    mutate(version = v_n1)
  
  return(health_metrics)
}