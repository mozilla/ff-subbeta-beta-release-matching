train_matchit <- function(train, model_covs, add_interactions, ...){
  # train model
  formula <- generate_formula(model_covs, label = 'is_release', add_interactions)
  model <- matchit(formula, train, ...)
  
  # extract beta subset
  df_matched <- get_matches(model, train) %>%
    select(-weights, -distance) %>%
    filter(label == 'beta')
  
  return(list(model = model, matched = df_matched))
}

extract_predictions <- function(matched, validation){
  prediction <- validation %>%
    filter(client_id %in% matched$client_id)
  
  return(prediction)
}