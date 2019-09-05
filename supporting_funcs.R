library(dplyr)
library(ggplot2)
library(corrplot)
library(tidyr)
library(tidyselect)
library(MatchIt)

model_pipeline <- function(df, tr_cov, ho_cov, match_type, label, ...){
  # Matching
  formula <- as.formula(paste(label, paste(tr_cov, collapse="+")))
  print(paste('Matching:', formula))
  matcher <- matchit(formula, df, method = match_type, ...)
  df.match <- match.data(matcher)
  
  # Validation
  ## Means: train
  tr_mean <- df.match %>% 
    select(tr_cov, label) %>% 
    group_by(label) %>% 
    summarise_all(mean)
  print(tr_mean)
  ## Means: holdout
  ho_mean <- df.match %>% 
    select(ho_cov, label) %>% 
    group_by(label) %>% 
    summarise_all(mean)
  print(ho_mean)
  return(c(matcher, df.match))
}

normalize_search_engine <- function(engine){
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

compare_log_cont <- function(df, covariate){
  p <- ggplot(df %>% mutate(!!covariate := get(covariate)+1), aes(label, get(covariate))) +
    geom_violin(aes(fill = label)) +
    geom_boxplot(width=0.01) + 
    scale_y_log10() +
    theme_bw() +
    guides(fill = FALSE) +
    labs(x = 'channel', y=covariate)
  
  print(p)
}

compare_cont <- function(df, covariate){
  p <- ggplot(df , aes(label, get(covariate))) +
    geom_violin(aes(fill = label)) +
    geom_boxplot(width=0.01) + 
    theme_bw() +
    guides(fill = FALSE) +
    labs(x = 'channel', y=covariate)
  
  print(p)
}

compare_cat <- function(df, covariate, limit = NULL){
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
  
  print(p)
  # return(df_c)
}