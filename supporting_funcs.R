library(dplyr)
library(ggplot2)
library(corrplot)
library(magrittr)
library(tidyr)
library(tidyselect)
library(MatchIt)

#### Helpers
# TODO remove `tr_cov` arg
calc_means <- function(df.match, tr_cov, ho_cov, add_1 = FALSE){
  # Validation
  ## Means: train
  # tr_mean <- df.match %>% 
  #   select(tr_cov, label) %>% 
  #   group_by(label) %>% 
  #   summarise_all(mean)
  # print(tr_mean)
  ## Means: holdout
  ho_mean <- df.match %>% 
    select(ho_cov, label) %>% 
    mutate_all(function(x) if(is.numeric(x) && add_1) x+1 else x) %>%
    group_by(label) %>% 
    summarise_all(mean)
  return(ho_mean)
}

calc_medians <- function(df.match, tr_cov, ho_cov, add_1 = FALSE){
  # Validation
  ## medians: train
  # tr_median <- df.match %>% 
  #   select(tr_cov, label) %>% 
  #   group_by(label) %>% 
  #   summarise_all(median)
  # print(tr_median)
  ## medians: holdout
  ho_median <- df.match %>% 
    select(ho_cov, label) %>% 
    mutate_all(function(x) if(is.numeric(x) && add_1) x+1 else x) %>%
    group_by(label) %>% 
    summarise_all(median)
  # print(ho_median)
  # return(list(train = tr_median, ho = ho_median))
  return(ho_median)
}

calc_delta <- function(df.match, tr_cov, ho_cov){
  perc_diff <- function(x) abs(x[1]-x[2])/x[2]
  means <- calc_means(df.match, tr_cov, ho_cov) %>%
    select(-label) %>% 
    summarise_all(perc_diff)
  medians <- calc_medians(df.match, tr_cov, ho_cov) %>%
    select(-label) %>% 
    summarise_all(perc_diff)
  # return(list(means = means, medians = medians))
  df <- means %>% 
    rbind(medians) %>% 
    as.data.frame() %>%
    set_rownames(c('means', 'medians'))
  return(df)
}

# TODO: merge with `calc_delta`
calc_stats <- function(df.match, ho_cov, add_1 = FALSE){
  means <- calc_means(df.match, NULL, ho_cov, add_1 = add_1) %>%
    mutate(metric = 'mean')
  medians <- calc_medians(df.match, NULL, ho_cov, add_1 = add_1) %>%
    mutate(metric = 'median')
  # return(list(means = means, medians = medians))
  df <- means %>% 
    rbind(medians) %>% 
    as.data.frame()
    # set_rownames(c('means', 'medians'))
  return(df)
}


#### Modeling Pipeline

generate_formula <- function(tr_cov, label){
  # Focus on linear and no-interactions
  return(as.formula(paste(label, '~', paste(tr_cov, collapse="+"))))
}

model_pipeline <- function(df_tr, tr_cov, ho_cov, match_type, label, ...){
  #### WARNING: Doesn't work due to environmental issues of `match.data` not finding dataset. PTE
  # Matching
  print(paste('Matching:', formula))
  matcher <- matchit(formula = generate_formula(tr_cov, label), data = df_tr, method = match_type, ...)
  
  df.match <- match.data(matcher) # Environment issues, looking for df_tr in Global
  
  calc_means(df.match, tr_cov, ho_cov)
  return(list(matcher = matcher, matched = df.match))
}

#### Categorize continuous covariates
# Following prior art by David Zeber: https://metrics.mozilla.com/~dzeber/notebooks/esper/esper-main-summary_model.html

normalize_search_engine <- function(engine){
  engine <- as.character(engine)
  case_when(
    is.null(engine) | engine %in% c("nan", "NONE", "UNKNOWN") ~ 'missing',
    startsWith(engine, "other-") ~ 'other (non-bundled)',
    startsWith(engine, "google") ~ 'Google',
    startsWith(engine, "yahoo") ~ 'Yahoo',
    startsWith(engine, "bing") ~ 'Bing',
    engine %in% c("ddg", 'duckduckgo') ~ 'DuckDuckGo',
    TRUE ~ 'other (bundled)'
  )
}

normalize_distro_id <- function(distro_id){
  distro_id <- as.character(distro_id)
  case_when(
    startsWith(distro_id, "mozilla") ~ "Mozilla",
    startsWith(distro_id, "yahoo") ~ 'Yahoo',
    startsWith(distro_id, "acer") ~ 'acer',
    TRUE ~ 'other'
  )
}

normalize_profile_age <- function(profile_age){
  case_when(
    profile_age < 7 ~ "< 1 week",
    profile_age < 31 ~ "< 1 month",
    profile_age < 180 ~ "< 6 months",
    profile_age < (365 * 2) ~ "< 2 years",
    profile_age < (365 * 5) ~ "< 5 years",
    TRUE ~ "> 5 years"
  )
}

normalize_timezone <- function(timezone){
  return(cut_interval(timezone/60, length = 2))
}

normalize_memory <- function(memory){
  gb <- memory / 1024
  case_when(
    gb <= 1 ~ "< 1GB",
    gb <= 2 ~ "< 2GB",
    gb <= 4 ~ "< 4GB",
    gb <= 6 ~ "< 6GB",
    gb <= 16 ~ "< 16GB",
    TRUE ~ "> 16GB"
  )
}

normalize_cpu_speed <- function(cpu_speed){
  ghz <- cpu_speed / 1000
  case_when(
    ghz <= 1 ~ "< 1GHz",
    ghz <= 2 ~ "< 2GHz",
    ghz <= 3 ~ "< 3GHz",
    ghz <= 4 ~ "< 4GHz",
    TRUE ~ "> 16GHz"
  )
}

normalize_cpu_cores <- function(cpu_cores){
  case_when(
    cpu_cores <= 1 ~ "1",
    cpu_cores <= 2 ~ "2",
    cpu_cores <= 4 ~ "< 4",
    cpu_cores <= 8 ~ "< 8",
    cpu_cores <= 16 ~ "< 16",
    TRUE ~ "> 16"
  )
}

#### Diagnostics
compare_log_cont <- function(df, covariate, tr_mean = NULL, tr_median = NULL, ho_mean = NULL, print = TRUE){
  p <- ggplot(df %>% mutate(!!covariate := get(covariate)+1), aes(label, get(covariate))) +
    geom_violin(aes(fill = label)) +
    geom_boxplot(width=0.1) + 
    scale_y_log10() +
    theme_bw() +
    guides(fill = FALSE) +
    labs(x = 'Channel', y='Measure', title=covariate)
  
  # add in lines for treatment means/medians
  if (!is.null(tr_mean)){
    p <- p + 
      geom_hline(yintercept=tr_mean, linetype="dashed", color = "red", lwd = 1.5) + 
      geom_hline(yintercept=tr_median, linetype="dashed", color = "blue", lwd = 1.5) + 
      geom_hline(yintercept=ho_mean, linetype="dotted", color = "green", lwd = 1.5)
  }
  
  if(print) print(p)
  return(p)
}

compare_cont <- function(df, covariate){
  p <- ggplot(df , aes(label, get(covariate))) +
    geom_violin(aes(fill = label)) +
    geom_boxplot(width=0.1) + 
    theme_bw() +
    guides(fill = FALSE) +
    labs(x = 'channel', y=covariate)
  
  print(p)
}

compare_cat <- function(df, covariate, limit = NULL, plot=TRUE){
  # reshape for plotting
  df_c <- df %>% 
    count(get(covariate), label) %>%
    setNames(c('category', 'label', 'count')) %>%
    group_by(label) %>%
    mutate(density = count/sum(count))
  
  # limit to the most frequent categories
  if (!is.null(limit)){
    topn <- df %>%
      count(get(covariate)) %>%
      setNames(c('category', 'count')) %>%
      arrange(desc(count)) %>%
      head(n=limit)
    
    df_c <- df_c %>%
      filter(category %in% topn$category)
  }
  
  p <- ggplot(df_c, aes(category, density)) +
    geom_bar(aes(fill = label), position = "dodge", stat="identity") +
    theme_bw() + 
    labs(x = covariate)
  
  if (plot) print(p)
  return(p)
  # return(df_c)
}