# Databricks notebook source
# MAGIC %md #Metrics

# COMMAND ----------

sum_int_metrics = {
                    'crashes_detected_content_sum': 'content_crashes'
                   }

mean_int_metrics = {
  'active_hours_sum': 'active_hours',
  'scalar_parent_browser_engagement_total_uri_count_sum': 'uri_count',
  'subsession_hours_sum': 'session_length',
  'search_count_all': 'search_count',
  'places_bookmarks_count_mean': 'num_bookmarks',                      
  'places_pages_count_mean': 'num_pages',            
  'scalar_parent_browser_engagement_unique_domains_count_mean': 'daily_unique_domains',
  'scalar_parent_browser_engagement_max_concurrent_tab_count_max': 'daily_max_tabs',
  'scalar_parent_browser_engagement_tab_open_event_count_sum': 'daily_tabs_opened',
  'first_paint_mean': 'startup_ms',
  'sessions_started_on_this_day': 'daily_num_sessions_started',
 }

max_int_metrics = {
  'active_hours_sum': 'active_hours_max',
  'scalar_parent_browser_engagement_total_uri_count_sum': 'uri_count_max',
  'subsession_hours_sum': 'session_length_max',
  'search_count_all': 'search_count_max',
  'places_pages_count_mean': 'num_pages_max',            
  'scalar_parent_browser_engagement_unique_domains_count_mean': 'daily_unique_domains_max',
  'scalar_parent_browser_engagement_max_concurrent_tab_count_max': 'daily_max_tabs_max',
  'scalar_parent_browser_engagement_tab_open_event_count_sum': 'daily_tabs_opened_max',
  'first_paint_mean': 'startup_ms_max',
  'sessions_started_on_this_day': 'daily_num_sessions_started_max',
}


probe_ms_map = {
  # page load
  'histogram_parent_fx_page_load_ms_2': 'FX_PAGE_LOAD_MS_2_PARENT',
  'histogram_content_time_to_dom_complete_ms': 'TIME_TO_DOM_COMPLETE_MS',
  'histogram_content_time_to_dom_content_loaded_end_ms': 'TIME_TO_DOM_CONTENT_LOADED_END_MS',
  'histogram_content_time_to_load_event_end_ms': 'TIME_TO_LOAD_EVENT_END_MS', 
  'histogram_content_time_to_dom_interactive_ms': 'TIME_TO_DOM_INTERACTIVE_MS',
  'histogram_content_time_to_non_blank_paint_ms': 'TIME_TO_NON_BLANK_PAINT_MS'
}

# COMMAND ----------

# MAGIC %md # Methods

# COMMAND ----------

def gen_hist_metrics(df):
  metrics = [F.collect_list(x).alias(y) for 
            x, y in probe_ms_map.items()]  
  
  return metrics

def gen_metrics(df):
  metrics = [F.count('*').alias('num_active_days')]
  
  # sum across client pings
  metrics = metrics + [F.sum(F.coalesce(df[x], F.lit(0))).alias(y) for 
            x, y in sum_int_metrics.items()]  
  
  # mean across client pings
  metrics = metrics + [F.mean(F.coalesce(df[x], F.lit(0))).alias(y) for 
            x, y in mean_int_metrics.items()]  
  
  # find max across client pings
  metrics = metrics + [F.max(F.coalesce(df[x], F.lit(0))).alias(y) for 
            x, y in max_int_metrics.items()]  
  
  return metrics

def get_df(tbl, channel, start_date, end_date, sample_ids, version, metrics, rename_cid=False, add_label=True):
  channel_client_end_date = end_date - dt.timedelta(NUM_DAYS)

  df = (
    tbl
    .filter(tbl.submission_date_s3 >= start_date.strftime('%Y%m%d'))
    .filter(tbl.submission_date_s3 < end_date.strftime('%Y%m%d'))
    .filter(tbl.app_version.startswith(version))
    .filter(tbl.normalized_channel == channel)
    .filter(F.col('sample_id').isin(*sample_ids))
    .filter(F.col('locale').isin('en-US', 'en-GB'))
    .filter(F.col('country').isin('US', 'GB'))
    .withColumn("row_num", F.row_number().over(Window.partitionBy(tbl.client_id).orderBy("submission_date_s3")))
    .withColumn('first_day', F.min("submission_date_s3").over(Window().partitionBy(tbl.client_id))) 
    .filter(F.col('first_day') <= channel_client_end_date.strftime('%Y%m%d'))
    .withColumn('first_date', F.to_date('first_day', "yyyyMMdd"))
    .withColumn('s_date', F.to_date("submission_date_s3", "yyyyMMdd"))
    .withColumn("n_days", F.datediff("s_date", "first_date"))  
    .filter(F.col('n_days') <= NUM_DAYS)
  )
  
  agg_df = (
    df
    .groupBy(df.client_id)
    .agg(*metrics(df))
  )
  
  if add_label:
    agg_df = agg_df.withColumn('label', F.lit(channel))
  
  if rename_cid:
    df = df.withColumnRenamed('client_id', 'cid')
    agg_df = agg_df.withColumnRenamed('client_id', 'cid')
  
  return {'df': df, 'agg': agg_df}

def get_meta(df):
  df_meta = (
  df
  .filter(df.row_num == 1)
  .select(
    # profile
    df.client_id.alias('cid'),
    df.install_year,
    df.profile_age_in_days.alias('profile_age'), 
    df.fxa_configured,
    F.when(F.isnull(df.sync_configured), False).otherwise(df.sync_configured).alias('sync_configured'),
    F.when(F.isnull(df.is_default_browser), False).otherwise(df.is_default_browser).alias('is_default_browser'),    
    # app 
    df.locale,
    df.normalized_channel,
    df.app_version,
    F.when(F.isnull(df.distribution_id), 'mozilla-stock').otherwise(df.distribution_id).alias('distribution_id'),    
    # search
    df.default_search_engine,
    # geo 
    df.country,
    df.city,
    df.geo_subdivision1,
    df.geo_subdivision2,
    df.timezone_offset,
    # add-ons
    df.active_addons_count_mean.alias('num_addons'),
    # cpu 
    df.cpu_cores,
    df.cpu_speed_mhz,
    df.cpu_family,
    df.cpu_l2_cache_kb,
    df.cpu_l3_cache_kb,
    df.cpu_stepping,
    df.cpu_model,
    df.cpu_vendor,
    # memory
    df.memory_mb,   
    # os
    df.os,
    df.os_version,
    df.is_wow64               
  )
)
  return df_meta

def add_meta(df, df_meta, reduce_size=False):  
  df_f = (
    df
    .join(df_meta, [df.client_id == df_meta.cid], 'left')
    .drop('cid')    
  )
  if reduce_size:
    df_f = df_f.withColumn('client_id', F.dense_rank().over(Window().orderBy(F.col('client_id'))))
  return df_f

def filter_covariates(df, thresh = 0.999):
  thresholds = {}
  for covariate in {**sum_int_metrics, **mean_int_metrics}.values():
    thresholds[covariate] = df.approxQuantile(covariate, [thresh], relativeError=0)[0]
    df = df.filter(F.col(covariate) <= thresholds[covariate])  
  return df

def mean_hist(x):  
  s, count = 0, 0 
  for hist in x:    
    for key, value in hist.items():
      s += key*value
      count += value
  result = None if not count else s/float(count)  
  return result

def mean_probes(df):  
  for probe in probe_ms_map.values():
      mean_udf = F.udf(mean_hist, T.DoubleType())
      df = df.withColumn(probe, mean_udf(F.col(probe)))      
  return df