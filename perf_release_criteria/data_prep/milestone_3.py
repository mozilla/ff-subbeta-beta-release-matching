# Databricks notebook source
# MAGIC %md # Description
# MAGIC 
# MAGIC Generates training, prediction, and validation datasets to determing [modeling framework](https://docs.google.com/document/d/1SfuanvmYmvmEFAdQ7Z5djDeLezdNB1TESqVmj93O8to/edit#) for this [PRD](https://docs.google.com/document/d/1Ygz6MkudYHZjnDnD9Z97kUyFrvV3KGWsjXyPjddhHq0/edit#heading=h.x5t09uq91x6z). 
# MAGIC 
# MAGIC * Utilize a two-week window. During a four week release cycle, this ensures the model framework is optimal for over half the cycle.
# MAGIC * Focuses on US/GB countries, and en-US/en-GB locales. 
# MAGIC   - Subsequent training sets will generated in separate notebooks.
# MAGIC   - This implies that their will be multiple modeling frameworks for full coverage of all countries and locales. 

# COMMAND ----------

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

# MAGIC %md
# MAGIC * Beta and Release subsets
# MAGIC   - Training: V67
# MAGIC   - Validation: V68
# MAGIC   - Beta: Last two weeks 
# MAGIC   - Release: First two weeks
# MAGIC   - en-US, en-GB locales
# MAGIC   - US, GB country

# COMMAND ----------

NUM_DAYS = 7
NUM_WEEKS  = 2

DATA_BUCKET = "net-mozaws-prod-us-west-2-pipeline-analysis"
DATA_PATH = 'cdowhygelund/subBeta/US_GB_country/US_GB_locale'

# COMMAND ----------

cd = spark.table('clients_daily')
ms = spark.table('main_summary')

# COMMAND ----------

# MAGIC %md # Metrics and Methods

# COMMAND ----------

# MAGIC %md Load up the notebook containing the metric (covariates and responses) and data preparation methods.

# COMMAND ----------

# MAGIC %run ./subBeta_lib

# COMMAND ----------

# MAGIC %md # V68: Validation Set

# COMMAND ----------

BETA_VALIDATION_START_DATE = dt.datetime(2019, 6, 25)
BETA_VALIDATION_END_DATE = BETA_VALIDATION_START_DATE + dt.timedelta(weeks=NUM_WEEKS)
BETA_VALIDATION_VERSION = '68'

RELEASE_VALIDATION_START_DATE = dt.datetime(2019, 7, 9)
RELEASE_VALIDATION_END_DATE = RELEASE_VALIDATION_START_DATE + dt.timedelta(weeks=NUM_WEEKS)
RELEASE_VALIDATION_VERSION = BETA_VALIDATION_VERSION

# COMMAND ----------

# MAGIC %md ## Beta 

# COMMAND ----------

sample_frac = 99

beta = get_df(cd, 'beta', BETA_VALIDATION_START_DATE, BETA_VALIDATION_END_DATE, 
              (str(x) for x in range(1, 1+sample_frac)), BETA_VALIDATION_VERSION, gen_metrics)
beta_meta = get_meta(beta['df'])
beta_df = add_meta(beta['agg'], beta_meta)
beta_ms = get_df(ms, 'beta', BETA_VALIDATION_START_DATE, BETA_VALIDATION_END_DATE,
                 (str(x) for x in range(1, 1+sample_frac)), BETA_VALIDATION_VERSION,
                 gen_hist_metrics, rename_cid=True, add_label=False)['agg'] 
beta_ms = mean_probes(beta_ms)
beta_df = add_meta(beta_df, beta_ms, reduce_size=False)
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

release = get_df(cd, 'release', RELEASE_VALIDATION_START_DATE, RELEASE_VALIDATION_END_DATE, 
                 sample_frac, RELEASE_VALIDATION_VERSION, gen_metrics)
release_meta = get_meta(release['df'])
release_df = add_meta(release['agg'], release_meta)
release_ms = get_df(ms, 'release', RELEASE_VALIDATION_START_DATE, RELEASE_VALIDATION_END_DATE, 
                    sample_frac, RELEASE_VALIDATION_VERSION, 
                    gen_hist_metrics, rename_cid=True, add_label=False)['agg'] 
release_ms = mean_probes(release_ms)
release_df = add_meta(release_df, release_ms, reduce_size=False)
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
  file_path = '{}/df_pd_{}_{}.csv'.format(DATA_PATH, BETA_VALIDATION_VERSION, 
                                          dt.date.today().strftime('%Y%m%d'))
  s3_resource.Object(DATA_BUCKET, file_path).put(Body=f.getvalue())    

# COMMAND ----------

# MAGIC %md # V67: Training Set

# COMMAND ----------

# MAGIC %md ## Beta

# COMMAND ----------

BETA_TRAINING_START_DATE = dt.datetime(2019, 5, 7)
BETA_TRAINING_END_DATE = BETA_TRAINING_START_DATE + dt.timedelta(weeks=NUM_WEEKS)
BETA_TRAINING_VERSION = '67'

RELEASE_TRAINING_START_DATE = dt.datetime(2019, 5, 21)
RELEASE_TRAINING_END_DATE = RELEASE_TRAINING_START_DATE + dt.timedelta(weeks=NUM_WEEKS)
RELEASE_TRAINING_VERSION = BETA_TRAINING_VERSION

# COMMAND ----------

sample_frac = 99

beta = get_df(cd, 'beta', BETA_TRAINING_START_DATE, BETA_TRAINING_END_DATE, 
              (str(x) for x in range(1, 1+sample_frac)), BETA_TRAINING_VERSION, gen_metrics)
beta_meta = get_meta(beta['df'])
beta_df = add_meta(beta['agg'], beta_meta)
beta_ms = get_df(ms, 'beta', BETA_TRAINING_START_DATE, BETA_TRAINING_END_DATE,
                 (str(x) for x in range(1, 1+sample_frac)), BETA_TRAINING_VERSION,
                 gen_hist_metrics, rename_cid=True, add_label=False)['agg'] 
beta_ms = mean_probes(beta_ms)
beta_df = add_meta(beta_df, beta_ms, reduce_size=False)

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

release = get_df(cd, 'release', RELEASE_TRAINING_START_DATE, RELEASE_TRAINING_END_DATE,
                 sample_frac, RELEASE_TRAINING_VERSION, gen_metrics)
release_meta = get_meta(release['df'])
release_df = add_meta(release['agg'], release_meta)
release_ms = get_df(ms, 'release', RELEASE_TRAINING_START_DATE, RELEASE_TRAINING_END_DATE, 
                    sample_frac, RELEASE_TRAINING_VERSION, 
                    gen_hist_metrics, rename_cid=True, add_label=False)['agg'] 
release_ms = mean_probes(release_ms)
release_df = add_meta(release_df, release_ms, reduce_size=False)

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
  file_path = '{}/df_pd_{}_{}.csv'.format(DATA_PATH, BETA_TRAINING_VERSION, 
                                          dt.date.today().strftime('%Y%m%d'))
  s3_resource.Object(DATA_BUCKET, file_path).put(Body=f.getvalue())    