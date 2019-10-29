library(dplyr)
library(ggplot2)
library(magrittr)
library(tidyr)
library(tidyselect)

#### Helpers ####
calc_means <- function(df.match, ho_cov, add_1 = FALSE){
  ho_mean <- df.match %>% 
    select(ho_cov, label) %>% 
    mutate_all(function(x) if(is.numeric(x) && add_1) x+1 else x) %>%
    group_by(label) %>% 
    summarise_all(mean)
  return(ho_mean)
}

calc_medians <- function(df.match, ho_cov, add_1 = FALSE){
  ho_median <- df.match %>% 
    select(ho_cov, label) %>% 
    mutate_all(function(x) if(is.numeric(x) && add_1) x+1 else x) %>%
    group_by(label) %>% 
    summarise_all(median)
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
  df <- means %>% 
    rbind(medians) %>% 
    as.data.frame()
  return(df)
}

#### Modeling Pipeline

generate_formula <- function(tr_cov, label, add_interactions=FALSE){
  if (add_interactions){
    formula <- paste(label, '~', paste(tr_cov, collapse="*"))
  }
  else {
    formula <- paste(label, '~', paste(tr_cov, collapse="+"))
  }
  return(as.formula(formula))
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
  levels <- c("< 1 week",  "< 1 month", "< 6 months", "< 2 years", "< 5 years", "> 5 years")
  factor(
    case_when(
      profile_age < 7 ~ levels[1],
      profile_age < 31 ~ levels[2],
      profile_age < 180 ~ levels[3],
      profile_age < (365 * 2) ~ levels[4], 
      profile_age < (365 * 5) ~ levels[5],
      TRUE ~ levels[6]
    ), levels = levels, ordered = TRUE
  )
}

normalize_timezone <- function(timezone){
  return(cut_interval(timezone/60, length = 2))
}

normalize_memory <- function(memory){
  levels <- c('< 1GB', '< 2GB', '< 4GB', '< 6GB', '< 16GB', '> 16GB')
  gb <- memory / 1024
  factor(
    case_when(
      gb <= 1 ~ levels[1],
      gb <= 2 ~ levels[2],
      gb <= 4 ~ levels[3],
      gb <= 6 ~ levels[4],
      gb <= 16 ~ levels[5],
      TRUE ~ levels[6]
    ), levels = levels, ordered = TRUE
  )
}

normalize_cpu_speed <- function(cpu_speed){
  levels <- c("< 1GHz", "< 2GHz", "< 3GHz", "< 4GHz", "> 16GHz")
  ghz <- cpu_speed / 1000
  factor(
    case_when(
      ghz <= 1 ~ levels[1],
      ghz <= 2 ~ levels[2],
      ghz <= 3 ~ levels[3],
      ghz <= 4 ~ levels[4],
      TRUE ~ levels[5]
  ), levels = levels, ordered = TRUE
  )
}

normalize_cpu_cores <- function(cpu_cores){
  levels <- c('1', '2', '< 4', '< 8', '< 16', '> 16')
  factor(
    case_when(
      cpu_cores <= 1 ~ levels[1],
      cpu_cores <= 2 ~ levels[2],
      cpu_cores <= 4 ~ levels[3],
      cpu_cores <= 8 ~ levels[4],
      cpu_cores <= 16 ~ levels[5],
      TRUE ~ levels[6]
  ), levels = levels, ordered = TRUE
  )
}

#### Diagnostics ####
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

compare_cat <- function(df, covariate, limit = NULL, plot=TRUE, add_legend=TRUE){
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
    theme(legend.position = c(0.2, 0.8), axis.text.x = element_text(angle = 45, hjust = 1)) +
    labs(x = covariate)
  
  if (!add_legend) p <- p + guides(fill=FALSE)
  if (plot) print(p)
  return(p)
  # return(df_c)
}