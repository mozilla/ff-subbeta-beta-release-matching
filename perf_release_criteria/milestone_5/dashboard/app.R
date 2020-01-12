library(shiny)
library(shinydashboard)
library(shinycssloaders)
library(dplyr)
library(tidyr)
library(purrr)

# source('prepare_dashboard.R')
load('data/dashboard_prepped.RData')
source('plotters.R')
source('../../lib/prediction.R')
source('../../lib/attributes.R')

ui <- dashboardPage(
  dashboardHeader(title = "subBeta"),
  dashboardSidebar(
    sidebarMenu(
      menuItem("Perf: Prediction",
               tabName = "performance_prediction",
               icon = icon("dashboard")),
      menuItem("Perf: Method Validation",
               tabName = "performance_validation",
               icon = icon("dashboard")),
      menuItem('Environment Covariates',
               menuSubItem(
                 selectInput(
                   'cpu_cores_cat',
                   '# of Cores',
                   cpu_cores_cat,
                   multiple = TRUE,
                   selectize = TRUE
                 )
               ),
               menuSubItem(
                 selectInput(
                   'cpu_speed_cat',
                   'CPU Speed',
                   cpu_speed_cat,
                   multiple = TRUE,
                   selectize = TRUE
                 )
               ),
               menuSubItem(
                 selectInput(
                   'mem_cat',
                   'Memory Size',
                   mem_cat,
                   multiple = TRUE,
                   selectize = TRUE
                 )
               )
      ),
      menuItem('Profile Covariates',
               menuSubItem(
                 selectInput(
                   'profile_age_cat',
                   'Profile Age',
                   profile_age_cat,
                   multiple = TRUE,
                   selectize = TRUE
                 )
               ),
               menuSubItem(
                 selectInput(
                   'country_cat',
                   'Country',
                   sort(unique(pred_v_n1$country)),
                   multiple = TRUE,
                   selectize = TRUE
                 )
               ),
               menuSubItem(
                 selectInput(
                   'timezone_cat',
                   'Timezone Offset',
                   sort(unique(pred_v_n1$timezone_cat)),
                   multiple = TRUE,
                   selectize = TRUE
                 )
               )
      )
    )
  ),
  dashboardBody(
    tabItems(
      #### Sidbar Tab: Prediction #### 
      tabItem(tabName = 'performance_prediction',
              fluidRow(box(
                column(
                  width = 12,
                  # dataTableOutput('summ_valid_tbl'),
                  tableOutput('release_health_tbl'),
                  style = "overflow-y: scroll;overflow-x: scroll;"
                ),
                width = 12
                # height = 12
              )),
              tabBox(
                # title = 'Metric',
                width = 12,
                tabPanel('FX_PAGE_LOAD_MS_2',
                         fluidRow(box(withSpinner(
                           plotOutput(
                             "fxpglms_pred_qq",
                             height = 300,
                             width = "100%"
                           )),
                           width = 12
                         )),
                         fluidRow(box(withSpinner(
                           plotOutput(
                             "fxpglms_pred_ridges",
                             height = 300,
                             width = "100%"
                           )),
                           width = 12
                         ))),
                tabPanel('TIME_TO_DOM_INTERACTIVE_MS',
                         fluidRow(box(withSpinner(
                           plotOutput(
                             "ttdims_pred_qq",
                             height = 300,
                             width = "100%"
                           )),
                           width = 12
                         )),
                         fluidRow(box(withSpinner(
                           plotOutput(
                             "ttdims_pred_ridges",
                             height = 300,
                             width = "100%"
                           )),
                           width = 12
                         ))),
                tabPanel('TIME_TO_DOM_COMPLETE_MS',
                         fluidRow(box(withSpinner(
                           plotOutput(
                             "ttdcms_pred_qq",
                             height = 300,
                             width = "100%"
                           )),
                           width = 12
                         )),
                         fluidRow(box(withSpinner(
                           plotOutput(
                             "ttdcms_pred_ridges",
                             height = 300,
                             width = "100%"
                           )),
                           width = 12
                         ))),
                tabPanel('TIME_TO_LOAD_EVENT_END_MS',
                         fluidRow(box(withSpinner(
                           plotOutput(
                             "ttleems_pred_qq",
                             height = 300,
                             width = "100%"
                           )),
                           width = 12
                         )),
                         fluidRow(box(withSpinner(
                           plotOutput(
                             "ttleems_pred_ridges",
                             height = 300,
                             width = "100%"
                           )),
                           width = 12
                         ))),
                tabPanel('TIME_TO_DOM_CONTENT_LOADED_END_MS',
                         fluidRow(box(withSpinner(
                           plotOutput(
                             "ttdclems_pred_qq",
                             height = 300,
                             width = "100%"
                           )),
                           width = 12
                         )),
                         fluidRow(box(withSpinner(
                           plotOutput(
                             "ttdclems_pred_ridges",
                             height = 300,
                             width = "100%"
                           )),
                           width = 12
                         ))),
                tabPanel('TIME_TO_NON_BLANK_PAINT_MS',
                         fluidRow(box(withSpinner(
                           plotOutput(
                             "ttnbpms_pred_qq",
                             height = 300,
                             width = "100%"
                           )),
                           width = 12
                         )),
                         fluidRow(box(withSpinner(
                           plotOutput(
                             "ttnbpms_pred_ridges",
                             height = 300,
                             width = "100%"
                           )),
                           width = 12
                         )))
              )
      ),
      
      #### Sidbar Tab: Validation #### 
      tabItem(tabName = 'performance_validation',
              fluidRow(box(
                column(
                  width = 12,
                  # dataTableOutput('summ_valid_tbl'),
                  tableOutput('summ_valid_tbl'),
                  style = "overflow-y: scroll;overflow-x: scroll;"
                ),
                width = 12
              )),
              tabBox(
                # title = 'Metric',
                width = 12,
                tabPanel('FX_PAGE_LOAD_MS_2',
                         fluidRow(box(withSpinner(
                           plotOutput(
                             "fxpglms_qq",
                             height = 300,
                             width = "100%"
                           )),
                           width = 12
                         )),
                         fluidRow(box(withSpinner(
                           plotOutput(
                             "fxpglms_ridges",
                             height = 300,
                             width = "100%"
                           )),
                           width = 12
                         ))),
                tabPanel('TIME_TO_DOM_INTERACTIVE_MS',
                         fluidRow(box(withSpinner(
                           plotOutput(
                             "ttdims_qq",
                             height = 300,
                             width = "100%"
                           )),
                           width = 12
                         )),
                         fluidRow(box(withSpinner(
                           plotOutput(
                             "ttdims_ridges",
                             height = 300,
                             width = "100%"
                           )),
                           width = 12
                         ))),
                tabPanel('TIME_TO_DOM_COMPLETE_MS',
                         fluidRow(box(withSpinner(
                           plotOutput(
                             "ttdcms_qq",
                             height = 300,
                             width = "100%"
                           )),
                           width = 12
                         )),
                         fluidRow(box(withSpinner(
                           plotOutput(
                             "ttdcms_ridges",
                             height = 300,
                             width = "100%"
                           )),
                           width = 12
                         ))),
                tabPanel('TIME_TO_LOAD_EVENT_END_MS',
                         fluidRow(box(withSpinner(
                           plotOutput(
                             "ttleems_qq",
                             height = 300,
                             width = "100%"
                           )),
                           width = 12
                         )),
                         fluidRow(box(withSpinner(
                           plotOutput(
                             "ttleems_ridges",
                             height = 300,
                             width = "100%"
                           )),
                           width = 12
                         ))),
                tabPanel('TIME_TO_DOM_CONTENT_LOADED_END_MS',
                         fluidRow(box(withSpinner(
                           plotOutput(
                             "ttdclems_qq",
                             height = 300,
                             width = "100%"
                           )),
                           width = 12
                         )),
                         fluidRow(box(withSpinner(
                           plotOutput(
                             "ttdclems_ridges",
                             height = 300,
                             width = "100%"
                           )),
                           width = 12
                         ))),
                tabPanel('TIME_TO_NON_BLANK_PAINT_MS',
                         fluidRow(box(withSpinner(
                           plotOutput(
                             "ttnbpms_qq",
                             height = 300,
                             width = "100%"
                           )),
                           width = 12
                         )),
                         fluidRow(box(withSpinner(
                           plotOutput(
                             "ttnbpms_ridges",
                             height = 300,
                             width = "100%"
                           )),
                           width = 12
                         )))
              )
      )
    ),
    fluidRow(
      fluidRow(
        # includeMarkdown("page_load_relds_desc.Rmd")),
        box(
          actionButton("go", "Recompute and Render?")
        )
      )
    )
  )
)

# Define server logic required to draw a histogram
server <- function(input, output) {
  # qq.df <- reactiveValues(qq.df = build_quantile_df(valid_v_n, pred_v_n1, beta_v_n, input))
  
  pred_v_n1_f <- eventReactive(input$go, {
    apply_filters(pred_v_n1, input)
  }) 
    
  pred_v_n_f <- eventReactive(input$go, {
    apply_filters(pred_v_n, input)
  })
  
  valid_v_n_f <- eventReactive(input$go, {
    apply_filters(valid_v_n, input)
  })
  
  beta_v_n_f <- eventReactive(input$go, {
    apply_filters(beta_v_n, input)
  })
  
  summ.valid <- eventReactive(input$go, {
    extract_health_metrics(pred_v_n_f(), valid_v_n_f(), metrics) %>%
             select(metric, cms_score, starts_with('rel_diff'), version
    )
  }
  )
  
  health_metrics <- eventReactive(input$go, {
    extract_health_metrics(pred_v_n1_f(), valid_v_n_f(), metrics) %>%
      select(metric, cms_score, starts_with('rel_diff'), version
      )
  }
  )
  
  df <- eventReactive(input$go, {
    pred_v_n_f() %>%
      mutate(label = 'beta-matched') %>%
      bind_rows(beta_v_n_f()) %>%
      bind_rows(valid_v_n_f())
  }
  )
  
  df_pred <- eventReactive(input$go, {
    pred_v_n1_f() %>%
      mutate(label = 'beta-forecast') %>%
      bind_rows(pred_v_n_f()) %>%
      bind_rows(valid_v_n_f())
  }
  )

  
  # Release Health
  output$release_health_tbl <- renderTable(health_metrics())
  
  # Summary metrics
  # output$summ_valid_tbl <- renderDataTable(summ.valid,
  output$summ_valid_tbl <- renderTable(summ.valid()
                                       # options = list(
                                       #   pageLength = 6,
                                       #   paging = FALSE
                                       #   # initComplete = I("function(settings, json) {alert('Done.');}")
                                       # )
  )
  
  # FX_PAGE_LOAD_MS_2_PARENT
  output$fxpglms_pred_qq <- renderPlot({
    qq.df = build_quantile_df(valid_v_n_f(), pred_v_n1_f(), pred_v_n_f(), metrics)
    p_ridge <- plot.qq(qq.df, 'FX_PAGE_LOAD_MS_2_PARENT')
    p_ridge
  })
  
  output$fxpglms_pred_ridges <- renderPlot({
    p_ridge <- plot.ridges(df_pred(), 'FX_PAGE_LOAD_MS_2_PARENT')
    p_ridge
  })
  
  output$fxpglms_qq <- renderPlot({
    qq.df = build_quantile_df(valid_v_n_f(), pred_v_n_f(), beta_v_n_f(), metrics)
    p_ridge <- plot.qq(qq.df, 'FX_PAGE_LOAD_MS_2_PARENT')
    p_ridge
  })
  
  output$fxpglms_ridges <- renderPlot({
    p_ridge <- plot.ridges(df(), 'FX_PAGE_LOAD_MS_2_PARENT')
    p_ridge
  })
  
  # TIME_TO_DOM_COMPLETE_MS
  output$ttdims_pred_qq <- renderPlot({
    qq.df = build_quantile_df(valid_v_n_f(), pred_v_n1_f(), pred_v_n_f(), metrics)
    p_ridge <- plot.qq(qq.df, 'TIME_TO_DOM_INTERACTIVE_MS')
    p_ridge
  })
  
  output$ttdims_pred_ridges <- renderPlot({
    p_ridge <- plot.ridges(df_pred(), 'TIME_TO_DOM_INTERACTIVE_MS')
    p_ridge
  })
  
  output$ttdims_qq <- renderPlot({
    qq.df = build_quantile_df(valid_v_n_f(), pred_v_n_f(), beta_v_n_f(), metrics)
    p_ridge <- plot.qq(qq.df, 'TIME_TO_DOM_INTERACTIVE_MS')
    p_ridge
  })
  output$ttdims_ridges <- renderPlot({
    p_ridge <- plot.ridges(df(), 'TIME_TO_DOM_INTERACTIVE_MS')
    p_ridge
  })

  # TIME_TO_DOM_COMPLETE_MS
  output$ttdcms_pred_qq <- renderPlot({
    qq.df = build_quantile_df(valid_v_n_f(), pred_v_n1_f(), pred_v_n_f(), metrics)
    p_ridge <- plot.qq(qq.df, 'TIME_TO_DOM_COMPLETE_MS')
    p_ridge
  })
  
  output$ttdcms_pred_ridges <- renderPlot({
    p_ridge <- plot.ridges(df_pred(), 'TIME_TO_DOM_COMPLETE_MS')
    p_ridge
  })
  
  output$ttdcms_qq <- renderPlot({
    qq.df = build_quantile_df(valid_v_n_f(), pred_v_n_f(), beta_v_n_f(), metrics)
    p_ridge <- plot.qq(qq.df, 'TIME_TO_DOM_COMPLETE_MS')
    p_ridge
  })

  output$ttdcms_ridges <- renderPlot({
    p_ridge <- plot.ridges(df(), 'TIME_TO_DOM_COMPLETE_MS')
    p_ridge
  })

  # TIME_TO_LOAD_EVENT_END_MS
  output$ttleems_pred_qq <- renderPlot({
    qq.df = build_quantile_df(valid_v_n_f(), pred_v_n1_f(), pred_v_n_f(), metrics)
    p_ridge <- plot.qq(qq.df, 'TIME_TO_LOAD_EVENT_END_MS')
    p_ridge
  })
  
  output$ttleems_pred_ridges <- renderPlot({
    p_ridge <- plot.ridges(df_pred(), 'TIME_TO_LOAD_EVENT_END_MS')
    p_ridge
  })
  
  output$ttleems_qq <- renderPlot({
    qq.df = build_quantile_df(valid_v_n_f(), pred_v_n_f(), beta_v_n_f(), metrics)
    p_ridge <- plot.qq(qq.df, 'TIME_TO_LOAD_EVENT_END_MS')
    p_ridge
  })

  output$ttleems_ridges <- renderPlot({
    p_ridge <- plot.ridges(df(), 'TIME_TO_LOAD_EVENT_END_MS')
    p_ridge
  })

  # TIME_TO_DOM_CONTENT_LOADED_END_MS
  output$ttdclems_pred_qq <- renderPlot({
    qq.df = build_quantile_df(valid_v_n_f(), pred_v_n1_f(), pred_v_n_f(), metrics)
    p_ridge <- plot.qq(qq.df, 'TIME_TO_DOM_CONTENT_LOADED_END_MS')
    p_ridge
  })
  
  output$ttdclems_pred_ridges <- renderPlot({
    p_ridge <- plot.ridges(df_pred(), 'TIME_TO_DOM_CONTENT_LOADED_END_MS')
    p_ridge
  })
  
  output$ttdclems_qq <- renderPlot({
    qq.df = build_quantile_df(valid_v_n_f(), pred_v_n_f(), beta_v_n_f(), metrics)
    p_ridge <- plot.qq(qq.df, 'TIME_TO_DOM_CONTENT_LOADED_END_MS')
    p_ridge
  })

  output$ttdclems_ridges <- renderPlot({
    p_ridge <- plot.ridges(df(), 'TIME_TO_DOM_CONTENT_LOADED_END_MS')
    p_ridge
  })

  # TIME_TO_NON_BLANK_PAINT_MS
  output$ttnbpms_pred_qq <- renderPlot({
    qq.df = build_quantile_df(valid_v_n_f(), pred_v_n1_f(), pred_v_n_f(), metrics)
    p_ridge <- plot.qq(qq.df, 'TIME_TO_NON_BLANK_PAINT_MS')
    p_ridge
  })
  
  output$ttnbpms_pred_ridges <- renderPlot({
    p_ridge <- plot.ridges(df_pred(), 'TIME_TO_NON_BLANK_PAINT_MS')
    p_ridge
  })
  
  output$ttnbpms_qq <- renderPlot({
    qq.df = build_quantile_df(valid_v_n_f(), pred_v_n_f(), beta_v_n_f(), metrics)
    p_ridge <- plot.qq(qq.df, 'TIME_TO_NON_BLANK_PAINT_MS')
    p_ridge
  })

  output$ttnbpms_ridges <- renderPlot({
    p_ridge <- plot.ridges(df(), 'TIME_TO_NON_BLANK_PAINT_MS')
    p_ridge
  })
}

# Run the application 
shinyApp(ui = ui, server = server)


# Thoughts

# 1. Table of breakdown of # of obs (dynamicly generated)
#   - total obs
#   - obs by segmenting category

