library(ggplot2)
library(ggridges)
library(viridis)

apply_filters <- function(df, input){
  resp.df <- df %>%
    filter(case_when(!is.null(input$cpu_cores_cat) ~ cpu_cores_cat %in% input$cpu_cores_cat,
                     TRUE ~ cpu_cores_cat == cpu_cores_cat)) %>%
    filter(case_when(!is.null(input$cpu_speed_cat) ~ cpu_speed_cat %in% input$cpu_speed_cat,
                     TRUE ~ cpu_speed_cat == cpu_speed_cat)) %>%
    filter(case_when(!is.null(input$mem_cat) ~ memory_cat %in% input$mem_cat,
                     TRUE ~ memory_cat == memory_cat)) %>%
    filter(case_when(!is.null(input$profile_age_cat) ~ profile_age_cat %in% input$profile_age_cat,
                     TRUE ~ profile_age_cat == profile_age_cat)) %>%
    filter(case_when(!is.null(input$country_cat) ~ country %in% input$country_cat,
                     TRUE ~ country == country)) %>%
    filter(case_when(!is.null(input$timezone_cat) ~ timezone_cat %in% input$timezone_cat,
                     TRUE ~ timezone_cat == timezone_cat))
  
  return(resp.df)
}

calc_qq <- function(validation, matched, original, response){
  qq <- qqplot(validation[[response]], matched[[response]], plot.it = FALSE) %>% 
    bind_rows() %>%
    mutate(type = 'matched')
  qq_full <- qqplot(validation[[response]], original[[response]], plot.it = FALSE) %>% 
    bind_rows() %>%
    mutate(type = 'original') %>%
    bind_rows(qq) %>%
    mutate(metric = response)
  # qq_full$metric <- perf_metric
  # qq_df <- qqs %>% bind_rows() %>% rename(release = x, beta = y)
  qq_df <- qq_full %>% rename(release = x, beta = y)
  return(qq_df)
}
  
plot.qq <- function(qq.df, response, ranges=NULL){
  p_qq <- ggplot(qq.df %>% filter(metric == response), aes(x = release, y = beta)) +
    geom_point(aes(color = type, shape = type)) + 
    geom_abline(slope = 1, intercept = 0) + 
    theme_bw() + 
    theme(axis.text.x = element_text(angle = 45, hjust = 1),
          plot.title = element_text(size=10),
          legend.position = c(0.8, 0.2)) +
    # scale_x_continuous(limits = ranges$x) + 
    coord_cartesian(xlim = ranges$x)+
    ggtitle(response) 
  return(p_qq)
  
}

plot.ridges <- function(df, response, ranges=NULL){
  resp.df <- df
  plot_log <- FALSE
  if (plot_log) {
    resp.df[response] <- log(df[response])
  }

  p_ridge <- ggplot(resp.df, aes(x=!!sym(response), y=label, fill=factor(..quantile..))) +
    stat_density_ridges(
      geom = "density_ridges_gradient", calc_ecdf = TRUE,
      quantiles = 10, quantile_lines = TRUE
    ) +
    scale_fill_viridis(discrete = TRUE, name = "Quartiles") + 
    theme_bw() +
    xlab(response) + 
    # scale_x_continuous(limits = ranges$x) + 
    coord_cartesian(xlim = ranges$x)+
    guides(fill = FALSE)
  return(p_ridge)
}
