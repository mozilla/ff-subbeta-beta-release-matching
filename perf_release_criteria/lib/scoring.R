library(transport)
library(dplyr)
library(foreach)
# library(doMC)
library(doParallel)

get_m2_metric_map = function(){
  #' Get milestone 2 map of metrics to scoring weights
  #'
  #' @return A list of keys = metrics, values = weights
  #' @export
  #'
  #' @examples get_m2_metric_map()
  return(
    c(
      'FX_PAGE_LOAD_MS_2_PARENT' = 0.5,
      'TIME_TO_DOM_INTERACTIVE_MS' =  0.10,
      'TIME_TO_DOM_COMPLETE_MS' =  0.10,
      'TIME_TO_LOAD_EVENT_END_MS' =  0.10,
      'TIME_TO_DOM_CONTENT_LOADED_END_MS' =  0.10,
      'TIME_TO_NON_BLANK_PAINT_MS' = 0.10
    )
  )
}

calc_cms <- function(metric, beta, release){
  #' Calculate Cramér-von Mises distance 
  #'
  #' @param metric character. Field for distance calculation
  #' @param beta data.frame. Contains beta measurements
  #' @param release data.frame. Contains release measurements
  #' @return numeric. Distance measure
  #'
  #' @examples get_m2_metric_map
  cms <- wasserstein1d(beta[[metric]], release[[metric]], p=2)
}

calc_score <- function(df, metric_map){
  #' Calculate Cramér-von Mises distance across a range of metrics, and weighted average the results
  #' 
  #' The metrics are normalized (min-max) before distance is calculated. 
  #' Then the distances are averaged, with weights given by metric_map
  #'
  #' @param df data.frame. Contains measurements
  #' @param label character. Field containing the various channels. 
  #' @param metric_map list. Keys are metrics for distance calculation, values are weights for sum
  #' @return numeric. Aggregated distance measure
  #'
  #' @examples calc_score(df)
  if(missing(metric_map)) metric_map <- get_m2_metric_map()
  metrics <- names(metric_map)
  
  norm <- function(x) return((x-min(x))/(max(x) - min(x)))
  # norm <- function(x) return(x)
  
  df_norm <- df %>%
    select(metrics, label) %>%
    na.omit() %>%
    mutate_at(vars(metrics), norm)
  
  beta <- df_norm %>% filter(label == 'beta')
  release <- df_norm %>% filter(label == 'release')
  
  scores <- sapply(metrics, calc_cms, beta=beta, release=release)
  return(sum(scores*metric_map)/length(scores))
}


get_matches <- function(model, data, group = "all", distance = "distance", weights = "weights", 
                        subclass = "subclass") 
  #' Get the matches from a MatchIt model
  #' 
  #' Re-implements MatchIt's `match.data` method, as it has environment issues inside functions.
  #' Primary difference is the inclusion of data as an argument. 
  #'
  #' @param group character. This argument specifies for which matched group the user wants to extract the data. Available options are "all" (all matched units), "treat" (matched units in the treatment group), and "control" (matched units in the control group). The default is "all".
  #' @distance character. This argument specifies the variable name used to store the distance measure. The default is "distance".
  #' @weights character. This argument specifies the variable name used to store the resulting weights from matching. The default is "weights".
  #' @subclass character. This argument specifies the variable name used to store the subclass indicator. The default is "subclass".
  #' @return Returns a subset of the original data set sent to matchit(), with just the matched units. The data set also contains the additional variables distance, weights, and subclass. The variable distance gives the estimated distance measure, and weights gives the weights for each unit, generated in the matching procedure. The variable subclass gives the subclass index for each unit (if applicable). See the http://gking.harvard.edu/matchit/ for the complete documentation and type demo(match.data) at the R prompt to see a demonstration of the code.
  #'
  #' @examples get_matches(model, df_train)
{
  treat <- model$treat
  wt <- model$weights
  vars <- names(data)
  if (distance %in% vars) 
    stop("invalid input for distance. choose a different name.")
  else if (!is.null(model$distance)) {
    dta <- data.frame(cbind(data, model$distance))
    names(dta) <- c(names(data), distance)
    data <- dta
  }
  if (weights %in% vars) 
    stop("invalid input for weights. choose a different name.")
  else if (!is.null(model$weights)) {
    dta <- data.frame(cbind(data, model$weights))
    names(dta) <- c(names(data), weights)
    data <- dta
  }
  if (subclass %in% vars) 
    stop("invalid input for subclass. choose a different name.")
  else if (!is.null(model$subclass)) {
    dta <- data.frame(cbind(data, model$subclass))
    names(dta) <- c(names(data), subclass)
    data <- dta
  }
  if (group == "all") 
    return(data[wt > 0, ])
  else if (group == "treat") 
    return(data[wt > 0 & treat == 1, ])
  else if (group == "control") 
    return(data[wt > 0 & treat == 0, ])
  else stop("error: invalid input for group.")
}

#### Bootstramp Resampling
# The following are for feature selection and hyperparameter tuning

run_matchit_sample <- function(df_train, bt, model_covs, seed, add_interactions, size=50000, ...){
  # generate training and test dataset
  train <- df_train %>% 
    right_join(data.frame(client_id = bt$train, stringsAsFactors=FALSE), by='client_id', 'right')
  if (!is.null(size)){
    set.seed(seed)
    df_train <- df_train %>% 
      sample_n(size = size)
  }
  
  test <- df_train %>% 
    right_join(data.frame(client_id = bt$test, stringsAsFactors=FALSE), by='client_id', 'right') %>%
    filter(label == 'release')
  
  # train model 
  formula <- generate_formula(model_covs, label = 'is_release', add_interactions)
  model <- matchit(formula, train, ...)
  
  # extract beta subset
  df_matched <- get_matches(model, train) %>%
    select(-weights, -distance) %>%
    filter(label == 'beta')
  
  # build dataframe for scoring
  df_scored <- df_matched %>%
    bind_rows(test)
  
  #apply scoring
  return(calc_score(df_scored, get_m2_metric_map()))
}

perform_matchit_fs <- function(df_train, bts, model_covs, workers, seed, add_interactions=FALSE, 
                               size=50000, ...){
  if (missing(workers)) workers = detectCores()
  if (missing(seed)) seed <- 1984
  # registerDoMC(workers)
  cl <- makePSOCKcluster(workers) # number of cores to use
  registerDoParallel(cl)
  final <- tryCatch({
    scores <- foreach(i=1:length(bts), 
                      .packages = c('dplyr', 'MatchIt', 'transport'), 
                      .export=c('run_matchit_sample', 'generate_formula', 'get_matches', 
                                'calc_score', 'get_m2_metric_map',
                                'calc_cms')) %dopar% {
                                  bt <- bts[[i]]
                                  score <- run_matchit_sample(df_train, bt, model_covs, seed, add_interactions,
                                                              size = size, 
                                                              ...)
                                  score
                                  # scores[[i]] <- score
                                }
    scores <- unlist(scores)
    c(mean = mean(scores), median = median(scores))
    }, 
    error = function(cond){
      message(paste("Bootstrap matching failed: ", cond))
      return(NA)
    },
    finally = {
      stopCluster(cl)
    }
  )
  return(final)
}