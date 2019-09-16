# Databricks notebook source
# MAGIC %md # Imports

# COMMAND ----------

import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql import Window
import datetime as dt
from io import StringIO
import boto3
import pandas as pd

# COMMAND ----------

# MAGIC %md Lots of prior art from [David Zeber's ESPER project](https://metrics.mozilla.com/~dzeber/notebooks/esper/esper-main-summary_model.html)

# COMMAND ----------

# MAGIC %md
# MAGIC * Beta and Release subsets
# MAGIC   - Training: V67
# MAGIC   - Validation: V68
# MAGIC   - Beta: Last four weeks 
# MAGIC   - Release: First four weeks
# MAGIC   - en-US, en-GB locales
# MAGIC   - US, GB country

# COMMAND ----------

NUM_DAYS = 14
NUM_WEEKS  = 4

DATA_BUCKET = "net-mozaws-prod-us-west-2-pipeline-analysis"
DATA_PATH = 'cdowhygelund/beta_release_matching/4week_US/Validation'

# COMMAND ----------

cd = spark.table('clients_daily')
ms = spark.table('main_summary')

# COMMAND ----------

# MAGIC %md # Metrics

# COMMAND ----------

sum_int_metrics = {'active_hours_sum': 'active_hours',
                   'scalar_parent_browser_engagement_total_uri_count_sum': 'uri_count',
                   'subsession_hours_sum': 'session_length',
                   'search_count_all': 'search_count',
                   'crashes_detected_content_sum': 'content_crashes'
                  }

mean_int_metrics = {'places_bookmarks_count_mean': 'num_bookmarks',                      
                    'places_pages_count_mean': 'num_pages',            
                    'scalar_parent_browser_engagement_unique_domains_count_mean': 'daily_unique_domains',
                    'scalar_parent_browser_engagement_max_concurrent_tab_count_max': 'daily_max_tabs',
                    'scalar_parent_browser_engagement_tab_open_event_count_sum': 'daily_tabs_opened',
                    'first_paint_mean': 'startup_ms',
                    'sessions_started_on_this_day': 'daily_num_sessions_started',
                   }

probe_ms_map = {
  # page load
  'histogram_parent_fx_page_load_ms_2': 'FX_PAGE_LOAD_MS_2_PARENT',
  'histogram_content_time_to_dom_complete_ms': 'TIME_TO_DOM_COMPLETE_MS',
  'histogram_content_time_to_dom_content_loaded_end_ms': 'TIME_TO_DOM_CONTENT_LOADED_END_MS',
  'histogram_content_time_to_load_event_end_ms': 'TIME_TO_LOAD_EVENT_END_MS',  
  # responsiveness
  'histogram_parent_fx_tab_switch_total_e10s_ms': 'FX_TAB_SWITCH_TOTAL_E10S_MS', 
  # memory
  'histogram_parent_memory_total': 'MEMORY_TOTAL',   
  # graphics
  'histogram_parent_content_paint_time': 'CONTENT_PAINT_TIME_PARENT',  
  'histogram_content_content_paint_time': 'CONTENT_PAINT_TIME_CONTENT',  
  'histogram_parent_content_frame_time': 'CONTENT_FRAME_TIME_PARENT',
  'histogram_gpu_content_frame_time': 'CONTENT_FRAME_TIME_GPU',  
  'histogram_parent_composite_time': 'COMPOSITE_TIME_PARENT',
  'histogram_gpu_composite_time': 'COMPOSITE_TIME_GPU',
  'histogram_content_composite_time': 'COMPOSITE_TIME_CONTENT'  
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
                      
  metrics = metrics + [F.sum(F.coalesce(df[x], F.lit(0))).alias(y) for 
            x, y in sum_int_metrics.items()]  
  
  metrics = metrics + [F.mean(F.coalesce(df[x], F.lit(0))).alias(y) for 
            x, y in mean_int_metrics.items()]  

  return metrics

def get_df(tbl, channel, start_date, sample_ids, version, metrics, rename_cid=False, add_label=True):
  channel_end_date = start_date + dt.timedelta(weeks=NUM_WEEKS)
  channel_client_end_date = channel_end_date - dt.timedelta(NUM_DAYS)

  df = (
    tbl
    .filter(tbl.submission_date_s3 >= start_date.strftime('%Y%m%d'))
    .filter(tbl.submission_date_s3 < channel_end_date.strftime('%Y%m%d'))
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
    df.client_id.alias('cid'),
    df.app_version,
    df.country,
    df.city,
    df.geo_subdivision1,
    df.geo_subdivision2,
    df.active_addons_count_mean.alias('num_addons'),
    df.cpu_cores,
    df.cpu_speed_mhz,
    df.default_search_engine,
    df.memory_mb,    
    df.os,
    df.os_version,
    df.is_wow64,
    df.e10s_enabled,
    df.fxa_configured,
    df.profile_age_in_days.alias('profile_age'),
    F.when(F.isnull(df.sync_configured), False).otherwise(df.sync_configured).alias('sync_configured'),
    F.when(F.isnull(df.is_default_browser), False).otherwise(df.is_default_browser).alias('is_default_browser'),
    df.timezone_offset,
    F.when(F.isnull(df.distribution_id), 'mozilla-stock').otherwise(df.distribution_id).alias('distribution_id')    
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

# COMMAND ----------

# MAGIC %md # V68: Validation Set

# COMMAND ----------

BETA_VALIDATION_START_DATE = dt.datetime(2019, 6, 11)
BETA_VALIDATION_VERSION = '68'

RELEASE_VALIDATION_START_DATE = dt.datetime(2019, 7, 9)
RELEASE_VALIDATION_VERSION = BETA_VALIDATION_VERSION

# COMMAND ----------

# MAGIC %md ## Beta 

# COMMAND ----------

sample_frac = 99

beta = get_df(cd, 'beta', BETA_VALIDATION_START_DATE, (str(x) for x in range(1, 1+sample_frac)), BETA_VALIDATION_VERSION, gen_metrics)
beta_meta = get_meta(beta['df'])
beta_df = add_meta(beta['agg'], beta_meta)
beta_ms = beta_ms = get_df(ms, 'beta', BETA_VALIDATION_START_DATE, (str(x) for x in range(1, 1+sample_frac)), BETA_VALIDATION_VERSION,
                           gen_hist_metrics, rename_cid=True, add_label=False)['agg'] 
beta_ms = mean_probes(beta_ms)
beta_df = add_meta(beta_df, beta_ms, reduce_size=True)

# COMMAND ----------

beta_df.cache()
beta_df.count()

# COMMAND ----------

beta_df = filter_covariates(beta_df)
beta_df.count()

# COMMAND ----------

beta_df.first()

# COMMAND ----------

s3_path = "s3://{}/{}_{}".format(DATA_BUCKET, DATA_PATH, BETA_VALIDATION_VERSION)

beta_df.write.parquet(s3_path + '/beta_f', mode='overwrite')
beta['df'].write.parquet(s3_path + '/beta',  mode='overwrite')

# COMMAND ----------

beta_pd = beta_df.toPandas()

# COMMAND ----------

# MAGIC %md ## Release 

# COMMAND ----------

sample_frac = ('42', '43', '44')

release = get_df(cd, 'release', RELEASE_VALIDATION_START_DATE, sample_frac, RELEASE_VALIDATION_VERSION, gen_metrics)
release_meta = get_meta(release['df'])
release_df = add_meta(release['agg'], release_meta)
release_ms = get_df(ms, 'release', RELEASE_VALIDATION_START_DATE, sample_frac, RELEASE_VALIDATION_VERSION, 
                    gen_hist_metrics, rename_cid=True, add_label=False)['agg'] 
release_ms = mean_probes(release_ms)
release_df = add_meta(release_df, release_ms, reduce_size=True)

# COMMAND ----------

release_df.cache()
release_df.count()

# COMMAND ----------

release_df = filter_covariates(release_df)
release_df.count()

# COMMAND ----------

release_df.first()

# COMMAND ----------

release_df.write.parquet(s3_path + '/release_f', mode='overwrite')
release['df'].write.parquet(s3_path + '/release',  mode='overwrite')

# COMMAND ----------

release_pd = release_df.toPandas()

# COMMAND ----------

# MAGIC %md ## Export 

# COMMAND ----------

# MAGIC %md Build the final data frame for modeling

# COMMAND ----------

df_pd = pd.concat([beta_pd, release_pd])

# COMMAND ----------

df_pd.shape

# COMMAND ----------

s3_resource = boto3.resource('s3')
with StringIO() as f:
  df_pd.to_csv(f, index=False)
  file_path = '{}/df_pd_{}.csv'.format(DATA_PATH, BETA_VALIDATION_VERSION)
  s3_resource.Object(DATA_BUCKET, file_path).put(Body=f.getvalue())    

# COMMAND ----------

# MAGIC %md # V67: Training Set

# COMMAND ----------

# MAGIC %md ## Beta

# COMMAND ----------

BETA_TRAINING_VERSION = '67'

BETA_TRAINING_START_DATE = dt.datetime(2019, 4, 23)

RELEASE_TRAINING_START_DATE = dt.datetime(2019, 5, 21)

# COMMAND ----------

sample_frac = 99

beta = get_df(cd, 'beta', BETA_TRAINING_START_DATE, (str(x) for x in range(1, 1+sample_frac)), BETA_TRAINING_VERSION, gen_metrics)
beta_meta = get_meta(beta['df'])
beta_df = add_meta(beta['agg'], beta_meta)
beta_ms = beta_ms = get_df(ms, 'beta', BETA_TRAINING_START_DATE, (str(x) for x in range(1, 1+sample_frac)), BETA_TRAINING_VERSION,
                           gen_hist_metrics, rename_cid=True, add_label=False)['agg'] 
beta_ms = mean_probes(beta_ms)
beta_df = add_meta(beta_df, beta_ms, reduce_size=True)

beta_df.cache()
beta_df.count()

# COMMAND ----------

beta_df = filter_covariates(beta_df)
beta_df.count()

# COMMAND ----------

s3_path = "s3://{}/{}_{}".format(DATA_BUCKET, DATA_PATH, BETA_TRAINING_VERSION)

beta_df.write.parquet(s3_path + '/beta_f', mode='overwrite')
beta['df'].write.parquet(s3_path + '/beta',  mode='overwrite')
beta_pd = beta_df.toPandas()

# COMMAND ----------

# MAGIC %md ## Release

# COMMAND ----------

sample_frac = ('42', '43', '44')

release = get_df(cd, 'release', RELEASE_TRAINING_START_DATE, sample_frac, RELEASE_TRAINING_VERSION, gen_metrics)
release_meta = get_meta(release['df'])
release_df = add_meta(release['agg'], release_meta)
release_ms = get_df(ms, 'release', RELEASE_TRAINING_START_DATE, sample_frac, RELEASE_TRAINING_VERSION, 
                    gen_hist_metrics, rename_cid=True, add_label=False)['agg'] 
release_ms = mean_probes(release_ms)
release_df = add_meta(release_df, release_ms, reduce_size=True)

# COMMAND ----------

release_df = filter_covariates(release_df)
release_df.count()

# COMMAND ----------

release_df.write.parquet(s3_path + '/release_f', mode='overwrite')
release['df'].write.parquet(s3_path + '/release',  mode='overwrite')
release_pd = release_df.toPandas()

# COMMAND ----------

# MAGIC %md ## Export

# COMMAND ----------

df_pd = pd.concat([beta_pd, release_pd])
df_pd.shape

# COMMAND ----------

s3_resource = boto3.resource('s3')
with StringIO() as f:
  df_pd.to_csv(f, index=False)
  file_path = '{}/df_pd_{}.csv'.format(DATA_PATH, BETA_TRAINING_VERSION)
  s3_resource.Object(DATA_BUCKET, file_path).put(Body=f.getvalue())    