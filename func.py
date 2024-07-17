# Databricks notebook source
!pip install sqlparse
!pip install azure-cosmos
!pip install adal
!pip install databricks-sql-connector==2.7.0
#DATAX
!pip install confluent-kafka==1.8.2
!pip install pythainlp
!pip install jellyfish

# COMMAND ----------

# DBTITLE 1,Import library, function and set global setting
import datetime
import time
import os
import uuid
import base64
import requests
import sqlparse
import json
import secrets
import re
from pyspark.sql.functions import col, explode, lit
#DATAX
from datetime import date, timedelta
import ast
from pyspark.sql.functions import split, substring, upper, trim, length, regexp_replace, when, desc, concat, coalesce, expr, countDistinct, row_number, udf
import pyspark.sql.functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StringType,DateType
import pyspark.sql.utils
from random import randint
import pandas as pd
import random
import numpy as np

os.environ['TZ'] = 'Asia/Bangkok'

# Set spark insert overwrite
spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")
spark.conf.set("spark.sql.legacy.storeAnalyzedPlanForView", "true")

# COMMAND ----------

# DBTITLE 1,Get global variable from configuration file
# MAGIC %run ../../../cmmn/config/environment_config

# COMMAND ----------

# DBTITLE 1,Generate temp view from config json
# dp packaged
##dpadd
##todo:
##      - raise exceptions
#def create_config_views():
#  configs = [            
#     "cmmn_area"                
#    ,"cmmn_dpdc"                
#    ,"ingt_job"        
#    ,"tnfm_job"        
#    ,"outbnd_job"      
#    ,"schm_sync_config"   
#    ,"delta_hskp_config"  
#  ]
#
#  for c in configs:
#    configPath = JSON_CONFIG_PATH + c + "/*.json"
#    viewName = "json_" + c
#
#    if (c == "cmmn_area"):
#      try:
#        jsonDF = spark.read.option("multiLine","true").json(configPath)
#        df = jsonDF.select(
#          col('area_nm'),
#          explode('job').alias('job')
#        ).select(
#          col('area_nm'),
#          col('job.job_type'),
#          col('job.job_nm'),
#          col('job.job_seq'),
#          col('job.job_hldy_flag')
#        )
#        df.createOrReplaceTempView(viewName)
#        print_std("INFO", "Created view\t"+configPath+'\t--> '+viewName)
#      except:
#        print_std("WARN", "Not found\t"+configPath+'\t--> '+viewName)
#    elif (c == "cmmn_dpdc"):
#      try:
#        jsonDF = spark.read.option("multiLine","true").json(configPath)
#        df = jsonDF.select(
#          col('job_nm'),
#          explode('pndng_job').alias('pndng_job')
#        ).select(
#          col('job_nm'),
#          col('pndng_job.pndng_type'),
#          col('pndng_job.pndng_db'),
#          col('pndng_job.pndng_tbl'),
#          col('pndng_job.pndng_job_nm'),
#          col('pndng_job.alw_zero_rec_flag'),
#          col('pndng_job.script_chck_dpdc'),
#        )
#        df.createOrReplaceTempView(viewName)
#        print_std("INFO", "Created view\t"+configPath+'\t--> '+viewName)
#      except:
#        print_std("WARN", "Not found\t"+configPath+'\t--> '+viewName)
#    else:
#      try:
#        df = spark.read.option("multiLine","true").json(configPath)
#        df.createOrReplaceTempView(viewName)
#        print_std("INFO", "Created view\t"+configPath+'\t--> '+viewName)
#      except:
#        print_std("WARN", "Not found\t"+configPath+'\t--> '+viewName)

# COMMAND ----------

# DBTITLE 1,Retry decorator
def retry(max_retries=5, wait_time=60):
    def decorator(func):
        def wrapper(*args, **kwargs):
            for attempt in range(max_retries):
                try:
                    result = func(*args, **kwargs)
                    return result
                except Exception as e:
                    print("ERROR", f"Attempt {attempt + 1} failed with error: {str(e)}")
                    if attempt < max_retries - 1: 
                        time.sleep(wait_time)
                    else:
                        raise
        return wrapper
    return decorator

# COMMAND ----------

# DBTITLE 1,Get current date / datetime
def get_current(dt_type):
  if dt_type == 'date':
    return str(datetime.datetime.today().strftime('%Y-%m-%d'))
  elif dt_type == 'datetime':
    return str(datetime.datetime.today().strftime('%Y-%m-%d %H:%M:%S.%f')[0:-3])
  elif dt_type == 'datetimelog':
    return str(datetime.datetime.today().strftime('%Y%m%d_%H%M%S'))
  else:
    return ""

# COMMAND ----------

# DBTITLE 1,Validate & Return formatted date
def chck_bsns_dt(date_text):
  try:
      if "-" in date_text:
        return datetime.datetime.strptime(date_text, '%Y-%m-%d').date()
      else:
        return datetime.datetime.strptime(date_text, '%Y%m%d').date()
  except ValueError:
      raise ValueError("[Value Error] Incorrect value found",date_text,"format should be YYYY-MM-DD or YYYYMMDD")

# COMMAND ----------

# DBTITLE 1,Check Y/N
def chck_y_n(y_n_val,msg,full_raise=True):
  if y_n_val.upper() not in ['Y','N']:
    if full_raise:
      raise ValueError("[Value Error] Incorrect value found",msg,y_n_val,"format should be Y or N")
    else:
      raise Exception(msg,y_n_val,"format should be Y or N")
  elif y_n_val not in ['Y','N']:
    if full_raise:
      raise ValueError("[Value Error] Incorrect value found",msg,y_n_val,"format should be upper case")
    else:
      raise Exception(msg,y_n_val,"format should be upper case")

# COMMAND ----------

# DBTITLE 1,Print standard
def print_std(lvl,msg,rtn = False,printout = True):
  rtn_str = "["+get_current('datetime')+"]"+" ["+lvl+"] "+msg
  if printout == True: print(rtn_str)
  if rtn == True: return rtn_str

# COMMAND ----------

# DBTITLE 1,Print information
def print_info(tuple_info,rtn = False):
  rtn_str = []
  for key in tuple_info:
    val = key[1]
    if not val: val = 'NULL'
    if rtn == False: 
      print_std("INFO",key[0]+": "+val)
    else:
      rtn_str.append(print_std("INFO",key[0]+": "+val, True, False))
  if rtn == True: return '\n'.join(rtn_str)

# COMMAND ----------

# DBTITLE 1,Clean up log
def cleanup_log(tbl_nm, dict_value):
  job_nm = dict_value['job_nm']
  delete_stmt = "DELETE FROM {}.{} WHERE job_nm = '{}'".format(CONFIG_DB_NM, tbl_nm, job_nm)
  print_std("INFO", "Query: {}".format(delete_stmt))
  spark.sql(delete_stmt)

# COMMAND ----------

# DBTITLE 1,Insert log job configuration
def insert_log_job_configuration(tbl_nm, dict_values, multi_values=False):
  print_std("INFO","Log inserting...")
  list_log_col = spark.read.table(CONFIG_DB_NM+"."+tbl_nm).columns
  if not multi_values:
    insert_val = []
    del_key_list =[]
    for key in dict_values:
      if not dict_values[key]:
        del_key_list.append(key)
    for key in del_key_list:    
      del dict_values[key]

    for col in list_log_col:
      if col in dict_values:
        insert_val.append("'"+dict_values[col]+"'")
      else:
        insert_val.append("NULL")
    query = "INSERT INTO "+CONFIG_DB_NM+"."+tbl_nm+" VALUES ("+",".join(insert_val)+")"
#     query = "INSERT INTO "+CONFIG_DB_NM+"."+tbl_nm+" PARTITION("+partition+") VALUES ("+",".join(insert_val)+")"
  else:
    insert_row = []
    for row in dict_values:
      del_key_list =[]
      insert_val = []
      for key in row:
        if not row[key]:
          del_key_list.append(key)
      for key in del_key_list:    
        del row[key]

      for col in list_log_col:
        if col in row:
          insert_val.append("'"+str(row[col]).replace("'","\"")+"'")
        else:
          insert_val.append("NULL")
      insert_row.append("(%s)" % (",".join(insert_val)))
    query = "INSERT INTO %s.%s VALUES %s" % (CONFIG_DB_NM,tbl_nm,partition,",".join(insert_row))
#     query = "INSERT INTO %s.%s PARTITION(%s) VALUES %s" % (CONFIG_DB_NM,tbl_nm,partition,",".join(insert_row)) 
  print_std("INFO","Query: "+query)
  spark.sql(query)

# COMMAND ----------

# DBTITLE 1,Cast none to string
def none2str(str_val):
  if str_val is None:
    return ''
  return str(str_val)

# COMMAND ----------

# DBTITLE 1,Check duplicated job running
def chck_dup_job(tbl_nm,where_cond,flag=None):
  print_std("INFO","Duplicated job running checking...")
  query = "SELECT * FROM "+CONFIG_DB_NM+"."+tbl_nm + " " + where_cond
  print_std("INFO","Query: "+query)
  dup_df = spark.sql(query)
  if not flag:
    if dup_df.count() > 0:
      raise SystemError("[System Error] Duplicate running found",", ".join([tbl_nm,where_cond]))
    else:
      print_std("INFO","Duplicated job running not found")
  else:
    if dup_df.filter("job_sts = 'TRIGGER-RUNNING'").count() == 1 and dup_df.filter("job_sts <> 'TRIGGER-RUNNING'").count()== 0:
      print_std("INFO","Duplicated job trigger-running not found")
    else:
      raise SystemError("[System Error] Duplicate running found",", ".join([tbl_nm,where_cond]))

# COMMAND ----------

# DBTITLE 1,Get job id
def get_job_id(job_id_prfx = None, **kwargs):
  if job_id_prfx:
    return_uuid = f"{job_id_prfx}_{uuid.uuid4()}"
  else:
    return_uuid = f"MANUAL_{uuid.uuid4()}"
  return return_uuid

# COMMAND ----------

# DBTITLE 1,Get configuration
def get_config(tbl_nm,where_cond,tbl_area=False):
  print_std("INFO","Getting configuration...")
  query = "SELECT * FROM "+CONFIG_DB_NM+"."+tbl_nm + " " + where_cond
  
  #dpadjust
#   view_nm = 'global_temp.json' + tbl_nm[3:]
#   query = "SELECT * FROM "+ view_nm + " " + where_cond
  
  print_std("INFO","Query: "+query)
  conf_df = spark.sql(query)
  if tbl_area == False:
    actv_y_cnt = conf_df.filter("UPPER(actv_flag) = 'Y'").count()
    actv_n_cnt = conf_df.filter("UPPER(actv_flag) = 'N'").count()
    if actv_y_cnt == 0:
      if actv_n_cnt == 0:
        raise SystemError("[System Error] Configuration not found",", ".join([tbl_nm,where_cond]))
      else:
        raise SystemError("[System Error] Active configuration not found (Inactive found)",", ".join([tbl_nm,where_cond]))
    elif actv_y_cnt > 1:
      raise SystemError("[System Error] Duplicate active configuration found",", ".join([tbl_nm,where_cond]))
    else:
      return conf_df
  else:
    if conf_df.count() == 0:
      raise SystemError("[System Error] Configuration not found",", ".join([tbl_nm,where_cond]))
    else:
      return conf_df

# COMMAND ----------

# DBTITLE 1,Validate configuration
def vld_config(df_conf,list_col):
  print_std("INFO","Validating configuration...")
  list_not_conf=[]
  for col in list_col:
    for row in df_conf.collect():
      conf_val = row[col]
      if not conf_val or str(conf_val).upper() == 'NULL':
        list_not_conf.append(col)
    if list_not_conf: 
      raise ValueError("[Value Error] Null value in configuration found",", ".join(set(list_not_conf)))

# COMMAND ----------

# DBTITLE 1,Validate flag configuration
def vld_flag_config(df_conf,list_col):
  print_std("INFO","Validating flag value of configuration...")
  list_not_conf=[]
  list_err=[]
  for col in list_col:
    for row in df_conf.collect():
      conf_val = row[col]
      if conf_val:
        try:
          chck_y_n(conf_val,col,False)
        except Exception as e:
          list_not_conf.append(col)
          list_err.append(str(e))
  if list_not_conf: 
    raise ValueError("[Value Error] Incorrect value found",", ".join(set(list_err)))

# COMMAND ----------

# DBTITLE 1,Validate path configuration
def vld_path_config(df_conf,list_col):
  print_std("INFO","Validating path value of configuration...")
  list_not_conf=[]
  for col in list_col:
    conf_val = df_conf.collect()[0][col]
    if conf_val and conf_val[-1] not in ['/','\\']:
      list_not_conf.append(col)
  if list_not_conf: 
    raise ValueError("[Value Error] Incorrect value found",", ".join(list_not_conf),"format should end with / or \\")

# COMMAND ----------

# DBTITLE 1,Validate list of value in configuration
def vld_val_config(df_conf,dict_col_list):
  print_std("INFO","Validating list of value in configuration...")
  list_not_conf=[]
  for col in dict_col_list:
    for row in df_conf.collect():
      conf_val = row[col]
      if conf_val.upper() in dict_col_list[col]:
        if conf_val not in dict_col_list[col]:
          list_not_conf.append("%s: %s format should be upper case" % (col,conf_val))
      else:
        list_not_conf.append("%s: %s is not in list %s" % (col,conf_val,str(dict_col_list[col])))
  if list_not_conf: 
    raise ValueError("[Value Error] Incorrect value found",", ".join(set(list_not_conf)))

# COMMAND ----------

# DBTITLE 1,Insert data to log
def insert_log(tbl_nm,dict_values,partition="dl_data_dt,job_nm",multi_values=False):
  print_std("INFO","Log inserting...")
  partition = 'monoline' + "," + partition
  list_log_col = spark.read.table(CONFIG_DB_NM+"."+tbl_nm).columns
  # print_std("INFO","Log columns: "+",".join(list_log_col))
  
  # Remove empty string from dict_values
  if not multi_values:
    insert_val = []
    del_key_list =[]
    for key in dict_values:
      if not dict_values[key]:
        del_key_list.append(key)
    for key in del_key_list:    
      del dict_values[key]

    for col in list_log_col:
      if col in dict_values:
        insert_val.append("'"+dict_values[col]+"'")
      elif col == 'monoline':
        insert_val.append("'"+MONOLINE.upper()+"'")
      else:
        insert_val.append("NULL")
    query = "INSERT INTO "+CONFIG_DB_NM+"."+tbl_nm+" PARTITION("+partition+") VALUES ("+",".join(insert_val)+")"
  else:
    insert_row = []
    for row in dict_values:
      del_key_list =[]
      insert_val = []
      for key in row:
        if not row[key]:
          del_key_list.append(key)
      for key in del_key_list:    
        del row[key]

      for col in list_log_col:
        if col in row:
          insert_val.append("'"+str(row[col]).replace("'","\"")+"'")
        elif col == 'monoline':
          insert_val.append("'"+MONOLINE.upper()+"'")
        else:
          insert_val.append("NULL")
      insert_row.append("(%s)" % (",".join(insert_val)))
        
    query = "INSERT INTO %s.%s PARTITION(%s) VALUES %s" % (CONFIG_DB_NM,tbl_nm,partition,",".join(insert_row))
    
  
  print_std("INFO","Query: "+query)
  spark.sql(query)
    
  if not multi_values:
    if "job_nm" in dict_values.keys() and "dl_data_dt" in dict_values.keys():
      job_nm = dict_values['job_nm']
      dl_data_dt = dict_values['dl_data_dt']
      print_std("INFO","Job_nm: " + job_nm + ", DL_DATA_DT: "+ dl_data_dt)

      sccs_ppty = get_sccs_ppty_nm(job_nm)
      zone = sccs_ppty['zone']
      title_nm = sccs_ppty['title_nm']

      if zone != "" and zone != "skip":
        print_std("INFO","Removing success file(s)...")
        rm_prev_sccs_file(job_nm,dl_data_dt,zone,title_nm)

# COMMAND ----------

# DBTITLE 1,Update data to log
def update_log(tbl_nm,where_cond,dict_upd,vld_dup_flag):
  print_std("INFO","Log updating...")
  where_cond += "AND monoline = '{}'".format(MONOLINE.upper())
  # Remove empty string from dict_values
  del_key_list =[]
  for key in dict_upd:
    if not dict_upd[key]:
      del_key_list.append(key)
  for key in del_key_list:    
    del dict_upd[key]
  
  if vld_dup_flag.upper() == "Y":
    query_vld = "SELECT * FROM "+CONFIG_DB_NM+"."+tbl_nm+" "+where_cond
    print_std("INFO","Query validate duplicated update: "+query_vld)
    vld_df = spark.sql(query_vld)
    if vld_df.count() > 1:
      raise SystemError("[System Error] Duplicate updating log found",", ".join([tbl_nm,where_cond]))

  set_cond = " SET "+', '.join([(x+"="+str(dict_upd[x]) if 'CASE WHEN' in str(dict_upd[x]) else x+"='"+str(dict_upd[x])+"'") for x in dict_upd])+" "
  query_upd = "UPDATE "+CONFIG_DB_NM+"."+tbl_nm+set_cond+where_cond
  print_std("INFO","Query log updated: "+query_upd)
  spark.sql(query_upd)
  
  if 'job_sts' in dict_upd.keys():
    if dict_upd['job_sts'] == "SUCCESS":
      job_nm = re.findall("job_nm ='(.*?)'", where_cond)
      dl_data_dt = re.findall("dl_data_dt ='(.*?)'",where_cond)
      if job_nm and dl_data_dt:
        job_nm = job_nm[0]
        dl_data_dt = dl_data_dt[0]
        print_std("INFO","Job_nm: " + job_nm + ", DL_DATA_DT: "+ dl_data_dt)


        sccs_ppty = get_sccs_ppty_nm(job_nm)
        zone = sccs_ppty['zone']
        title_nm = sccs_ppty['title_nm']

        if zone != "" and title_nm != "" and zone != "skip":
          crte_sccs_file(job_nm,dl_data_dt,zone,title_nm)

# COMMAND ----------

# DBTITLE 1,Run notebook
def run_notebook(notebook_path,dict_params):
  try:
    rtn_run_nb = dbutils.notebook.run(notebook_path, DEFAULT_TIMEOUT, dict_params)
    return rtn_run_nb
  except Exception as e:
    if 'Notebook not found' in str(e):
      raise SystemError("[System Error] Notebook not found",notebook_path)
    else:
      raise SystemError("[System Error] Cannot execute notebook",notebook_path)

# COMMAND ----------

# DBTITLE 1,Get notebook content
def get_notebook_content(notebook_path,encd_char='utf-8'):
  response = requests.get(
    'https://%s/api/2.0/workspace/export' % (DOMAIN_URL),
    headers={'Authorization': 'Bearer %s' % API_TOKEN},
    json={
      "path": notebook_path,
      "format": "SOURCE"
    }
  )
  # Validate response code
  response.raise_for_status()
  # Decode notebook content
  data_byte = base64.b64decode(response.json()['content'])
  data = data_byte.decode(encd_char)
  return data

# COMMAND ----------

# DBTITLE 1,Run notebook via API
def api_run_notebook(notebook_nm,token_id,param={}):
  response = requests.post(
    'https://%s/api/2.0/jobs/runs/submit' % (DOMAIN_URL),
    headers={'Authorization': 'Bearer %s' % API_TOKEN},
    json={
      "run_name": token_id,
      "existing_cluster_id": CLUSTER_ID,
      "notebook_task": {
        "notebook_path" :  notebook_nm,
        "base_parameters": param
      },
      "idempotency_token": token_id
    }
  )
  # Validate response code
  response.raise_for_status()
  return response

# COMMAND ----------

# DBTITLE 1,Run job via API
def api_run_job_now(job_id,job_param):
  response = requests.post('https://%s/api/2.0/jobs/run-now' % (DOMAIN_URL),
    headers ={'Authorization': 'Bearer %s' % API_TOKEN},
    json= {"job_id": job_id,
          "notebook_params": job_param
          })
    # Validate response code
  response.raise_for_status()
  return response

# COMMAND ----------

# DBTITLE 1,API Get run information
def api_get_run_info(run_id):
  response = requests.get(
    'https://%s/api/2.0/jobs/runs/get' % (DOMAIN_URL),
    headers={'Authorization': 'Bearer %s' % API_TOKEN},
    json={
      "run_id": run_id
    }
  )
  # Validate response code
  response.raise_for_status()
  return response

# COMMAND ----------

def api_expt_html_file(run_id):
  response = requests.get(
    'https://%s/api/2.0/jobs/runs/export' % (DOMAIN_URL),
    headers={'Authorization': 'Bearer %s' % API_TOKEN},
    json={"run_id": run_id}
  )
  return response.json()

# COMMAND ----------

def api_imprt_file(content,path,file_lang,encd_char='utf-8'):
  print_std("INFO","Import file: "+path)
  response = requests.post(
    'https://%s/api/2.0/workspace/import' % (DOMAIN_URL),
    headers={'Authorization': 'Bearer %s' % API_TOKEN},
    json={
      "content": base64.b64encode(content.encode(encd_char)).decode("utf-8"),
      "path": path,
      "language": file_lang,
      "overwrite": True,
      "format": "SOURCE"
    }
  )
  response.raise_for_status()
  return response.json()

# COMMAND ----------

#list
def api_list_files(dir_path):
  print_std("INFO","List files: "+dir_path)
  response = requests.get(
    'https://%s/api/2.0/workspace/list' % (DOMAIN_URL),
    headers={'Authorization': 'Bearer %s' % API_TOKEN},
    json={"path": dir_path}
  )
  list_rslt = response.json()
  return list_rslt

# COMMAND ----------

#delete
def api_del_file(dir_path):
  print_std("INFO","Delete: "+dir_path)
  response = requests.post(
    'https://%s/api/2.0/workspace/delete' % (DOMAIN_URL),
    headers={'Authorization': 'Bearer %s' % API_TOKEN},
    json={"path": dir_path, "recursive": "true"}
  )
  response.raise_for_status()
  return response.json()

# COMMAND ----------

#mkdirs
def api_mkdirs(dir_path):
  print_std("INFO","Create folder: "+dir_path)
  response = requests.post(
    'https://%s/api/2.0/workspace/mkdirs' % (DOMAIN_URL),
    headers={'Authorization': 'Bearer %s' % API_TOKEN},
    json={"path": dir_path}
  )

# COMMAND ----------

#get_status
def api_get_status(dir_path):
  response = requests.get(
    'https://%s/api/2.0/workspace/get-status' % (DOMAIN_URL),
    headers={'Authorization': 'Bearer %s' % API_TOKEN},
    json={"path": dir_path}
  )
  list_rslt = response.json()
  return list_rslt

# COMMAND ----------

# DBTITLE 1,Get trigger detail
@retry()
def get_adf_trigger_info(trigger_name):
  url_trigger_detail = "%s/triggers/%s?api-version=%s" % (AZURE_ADF_URL,trigger_name,AZURE_API_VERSION)
  response = requests.get(
    url_trigger_detail,
    headers={'Authorization': 'Bearer %s' % get_access_token()}
  )
  # Validate response code
  response.raise_for_status()
  return response.json()

# COMMAND ----------

# DBTITLE 1,Get Pipeline detail
@retry()
def get_adf_pipeline_info(trigger_name):
  url_trigger_detail = "%s/pipelines/%s?api-version=%s" % (AZURE_ADF_URL,trigger_name,AZURE_API_VERSION)
  response = requests.get(
    url_trigger_detail,
    headers={'Authorization': 'Bearer %s' % get_access_token()}
  )
  # Validate response code
  response.raise_for_status()
  return response.json()

# COMMAND ----------

# DBTITLE 1,Get run information
def get_run_info(token_id="DUMMY_RUNS"):
  # Return output
  dict_output = {'run_id':'','run_type':'','run_url':'','job_id':'','root_run_id':''}
  
  try:
    # Get run ID by normal
    run_info = json.loads(dbutils.notebook.entry_point.getDbutils().notebook().getContext().toJson())
    run_id = run_info['currentRunId']['id']
    root_run_id = run_info['rootRunId']['id']
    job_id = run_info['tags']['jobId']
    nb_path = run_info['extraContext']['notebook_path']
    if run_id:
      dict_output['run_id'] = run_id
      dict_output['job_id'] = job_id
      dict_output['root_run_id'] = root_run_id
      dict_output['nb_path'] = nb_path
      # Validate run ID
      response = api_get_run_info(str(run_id))
      dict_output['run_url'] = response.json()['run_page_url']
    else:
        dict_output['run_type'] = 'MANUAL'
    return dict_output
  except:
    # Get run ID by API
    if token_id == 'DUMMY_RUNS':
      token_id = token_id + get_current("datetimelog")
    response = api_run_notebook(DUMMY_NOTEBOOK,token_id)
    run_id = response.json()['run_id']
    dict_output['run_id'] = run_id
    # Validate run ID
    response = api_get_run_info(run_id)
    dict_output['run_url'] = response.json()['run_page_url']
    rtn_nb = response.json()['task']['notebook_task']['notebook_path']
    if rtn_nb == DUMMY_NOTEBOOK:
      dict_output['run_type'] = 'MANUAL'
      return dict_output
    else:
      return dict_output


# COMMAND ----------

# DBTITLE 1,Get execution user
def get_exec_usr():
  try:
    return dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().apply('user')
  except:
    return ""

# COMMAND ----------

# DBTITLE 1,Get authenticate with Azure AD
def get_access_token():
  response = requests.post(
    AZURE_URL_GET_TOKEN,
    data={
      "grant_type":"client_credentials",
      "client_id":AZURE_APP_ID,
      "client_secret":AZURE_CLIENT_SECRETS,
      "resource":AZURE_URL_RESOURCE
    }
  )
  # Validate response code
  response.raise_for_status()
  return response.json()['access_token']

# COMMAND ----------

# DBTITLE 1,Run ADF pipeline
@retry()
def run_adf_pipeline(pipline_nm,dict_param={}):
  url_run_pipeline = "%s/pipelines/%s/createRun?api-version=%s" % (AZURE_ADF_URL,pipline_nm,AZURE_API_VERSION)
  response = requests.post(
    url_run_pipeline,
    headers={'Authorization': 'Bearer %s' % get_access_token()},
    json=dict_param
  )
  # Validate response code
  response.raise_for_status()
  return response.json()['runId']

# COMMAND ----------

# DBTITLE 1,Get pipeline run information
@retry()
def get_adf_run_info(run_id):
  url_monitor_pipeline = "%s/pipelineruns/%s?api-version=%s" % (AZURE_ADF_URL,run_id,AZURE_API_VERSION)
  response = requests.get(
    url_monitor_pipeline,
    headers={'Authorization': 'Bearer %s' % get_access_token()}
  )
  # Validate response code
  response.raise_for_status()
  return response.json()

# COMMAND ----------

# DBTITLE 1,Get pipeline run activity detail
@retry()
def get_adf_run_dtl(run_id):
  url_run_detail = "%s/pipelineruns/%s/queryActivityruns?api-version=%s" % (AZURE_ADF_URL,run_id,AZURE_API_VERSION)
  response = requests.post(
    url_run_detail,
    headers={'Authorization': 'Bearer %s' % get_access_token()}
  )
  # Validate response code
  response.raise_for_status()
  return response.json()

# COMMAND ----------

# DBTITLE 1,Validate File After Copy (multiple copy activities)
# update: support if there are more than one transfer activity
# update: support if data file and control file are separated by two activities
def vld_adf_copy_file_outbnd(adf_run_id,adf_actv_nm,list_blob_file,list_blob_ctl_file):
  # Get activity detail
  print_std("INFO","Validating ADF activity...")
  actv_details = get_adf_run_dtl(adf_run_id)['value']
  dict_adf_file_info = {}
  # Non's edit: added total_file_write for pipeline with more than one copy activity
  num_total_file_write = 0  
  retry_cnt = 0
  for actv_info in actv_details:
    # Validate read = write on ADF
    if actv_info['activityType'] == "Copy":
      try:
        num_file_read = actv_info['output']['filesRead']
        num_file_write = actv_info['output']['filesWritten']
        # Non's edit: added total_file_write for pipeline with more than one copy activity
        num_total_file_write = num_total_file_write + num_file_write  
        print_std("INFO","Num file read: %d, Num file write: %d" % (num_file_read,num_file_write))
        if num_file_read != num_file_write:
          raise Exception("[Exception] Other exception","Data transfer read not equal to data transfer write","Read: %d" % num_file_read, "Write: %d" % num_file_write)
      except:
        if retry_cnt == ADF_TRNF_MAX_RETRY:
          raise Exception("[Exception] Other exception","Data transfer failed")
        retry_cnt += 1
        continue
    # Get adf file information
    if actv_info['activityType'] == "GetMetadata" and actv_info['activityName'] == adf_actv_nm:
      print_std("INFO","CHECKING GET METADATA")
      curr_file_nm = actv_info['output']['itemName']
      curr_file_size = actv_info['output']['size']
      dict_adf_file_info[curr_file_nm] = curr_file_size

  # Validate number of file
  print_std("INFO","---------------------------------------------------------")
  print_std("INFO","Validating number of transfer file...")
  num_blob_file = len(list_blob_file)
  # Non's edit: added number of control file (separated list)
  num_blob_ctl_file = len(list_blob_ctl_file)
  num_blob_total_file = num_blob_file + num_blob_ctl_file
  print_std("INFO","Num ADF file: %d, Num blob file: %d" % (num_total_file_write,num_blob_total_file))
  if num_total_file_write != num_blob_total_file:
    raise Exception("[Exception] Other exception","Number of transfer file from ADF not equal to blob","ADF: %d" % num_total_file_write, "BLOB: %d" % num_blob_total_file)
#   print_std("INFO","Num ADF file: %d, Num blob file: %d" % (num_file_write,num_blob_file))
#   if num_file_write != num_blob_file:
#     raise Exception("[Exception] Other exception","Number of transfer file from ADF not equal to blob","ADF: %d" % num_file_write, "BLOB: %d" % num_blob_file)

  # Validate size file
  print_std("INFO","---------------------------------------------------------")
  print_std("INFO","Validating size transfer file...")
  # Get list of dict file information
  dict_blob_file_info = {}
  for file in list_blob_file:
    file_nm = dbutils.fs.ls(file)[0][1]
    file_size = dbutils.fs.ls(file)[0][2]
    dict_blob_file_info[file_nm] = file_size
  for file in list_blob_ctl_file:
    file_nm = dbutils.fs.ls(file)[0][1]
    file_size = dbutils.fs.ls(file)[0][2]
    dict_blob_file_info[file_nm] = file_size
  print_std("INFO",f"Dict Blob File: {str(dict_blob_file_info)}")
  print_std("INFO",f"Dict ADF File: {str(dict_adf_file_info)}")
  # Compare ADF transfer file with blob file
  for blob_file in dict_blob_file_info:
    size_adf =dict_adf_file_info[blob_file]
    size_blob = dict_blob_file_info[blob_file]
    print_std("INFO","File name: %s, ADF size: %d, Blob size: %d" % (blob_file,size_adf,size_blob))
    if size_adf != size_blob:
      raise Exception("[Exception] Other exception","Size transfer file from ADF not equal to blob","ADF: %d" % size_adf, "Blob: %d" % size_blob)


# COMMAND ----------

# DBTITLE 1,Validate File After Copy - Total Num/Size (multiple copy activities)
def vld_adf_copy_file_outbnd_total(adf_run_id,adf_actv_nm,list_blob_file,list_blob_ctl_file):
  # This is used to reduce overheads caused 'Get Metadata Activity'
  # The ADF pipeline that use this function should not contain 'Get Metadata Activity'
  # This function validates total number and size of files transferred insteal of each file
  
  print_std("INFO","Validating ADF activity...")
  actv_details = get_adf_run_dtl(adf_run_id)['value']
  num_total_file_write = 0  
  retry_cnt = 0
  adf_total_size_write = 0
  for actv_info in actv_details:
      # Validate if read = write on ADF
      if actv_info['activityType'] == "Copy":
        print_std("INFO",f"Validating Activity: {actv_info['activityName']}")
        try:
          num_file_read = actv_info['output']['filesRead']
          num_file_write = actv_info['output']['filesWritten']
          num_total_file_write = num_total_file_write + num_file_write  
          print_std("INFO","Num file read: %d, Num file write: %d" % (num_file_read, num_file_write))
          # Validate total number of files read and written from blob to target
          if num_file_read != num_file_write:
            raise Exception("[Exception] Other exception", \
                            "Data transfer read not equal to data transfer write", \
                            "Read: %d" % num_file_read, "Write: %d" % num_file_write)
            
          size_file_read = actv_info['output']['dataRead']
          size_file_write = actv_info['output']['dataWritten']
          adf_total_size_write = adf_total_size_write + size_file_write
          print_std("INFO","File size read: %d, File size write: %d" % (size_file_read, size_file_write))
          # Validate total size of files read and written from blob to target
          if size_file_read != size_file_write:
            raise Exception("[Exception] Other exception", \
                            "Size of data read is not equal to size of data written", \
                            "Read: %d" % size_file_read, "Write: %d" % size_file_write)
        except:
          if retry_cnt == ADF_TRNF_MAX_RETRY:
            raise Exception("[Exception] Other exception","Data transfer failed")
          retry_cnt += 1
          continue

  num_total_file_blob = len(list_blob_file) + len(list_blob_ctl_file)
  total_file_size_blob = 0
  for file in list_blob_file:
  #   file_nm = dbutils.fs.ls(file)[0][1]
    file_size = dbutils.fs.ls(file)[0][2]
    total_file_size_blob = total_file_size_blob + file_size
  for file in list_blob_ctl_file:
  #   file_nm = dbutils.fs.ls(file)[0][1]
    file_size = dbutils.fs.ls(file)[0][2]
    total_file_size_blob = total_file_size_blob + file_size
  # Validate total size of files from blob and target location
  print_std("INFO","File size from blob: %d, File size from ADF write: %d" % (total_file_size_blob, adf_total_size_write))
  if adf_total_size_write != total_file_size_blob:
    raise Exception("[Exception] Other exception", \
                    "Total size of files transferred is not equal to blob", \
                    "ADF: %d" % size_adf, "Blob: %d" % size_blob)


# COMMAND ----------

# DBTITLE 1,Validate file after copy
def vld_adf_copy_file(adf_run_id,adf_actv_nm,list_blob_file):
  # Get activity detail
  print_std("INFO","Validating ADF activity...")
  actv_details = get_adf_run_dtl(adf_run_id)['value']
  dict_adf_file_info = {}
  retry_cnt = 0
  for actv_info in actv_details:
    # Validate read = write on ADF
    if actv_info['activityType'] == "Copy":
      try:
        num_file_read = actv_info['output']['filesRead']
        num_file_write = actv_info['output']['filesWritten']
        print_std("INFO","Num file read: %d, Num file write: %d" % (num_file_read,num_file_write))
        if num_file_read != num_file_write:
          raise Exception("[Exception] Other exception","Data transfer read not equal to data transfer write","Read: %d" % num_file_read, "Write: %d" % num_file_write)
      except:
        if retry_cnt == ADF_TRNF_MAX_RETRY:
          raise Exception("[Exception] Other exception","Data transfer failed")
        retry_cnt += 1
        continue
    # Get adf file information
    if actv_info['activityType'] == "GetMetadata" and actv_info['activityName'] == adf_actv_nm:
      curr_file_nm = actv_info['output']['itemName']
      curr_file_size = actv_info['output']['size']
      dict_adf_file_info[curr_file_nm] = curr_file_size

  # Validate number of file
  print_std("INFO","---------------------------------------------------------")
  print_std("INFO","Validating number of transfer file...")
  num_blob_file = len(list_blob_file)
  print_std("INFO","Num ADF file: %d, Num blob file: %d" % (num_file_write,num_blob_file))
  if num_file_write != num_blob_file:
    raise Exception("[Exception] Other exception","Number of transfer file from ADF not equal to blob","ADF: %d" % num_file_write, "BLOB: %d" % num_blob_file)

  # Validate size file
  print_std("INFO","---------------------------------------------------------")
  print_std("INFO","Validating size transfer file...")
  # Get list of dict file information
  dict_blob_file_info = {}
  for file in list_blob_file:
    file_nm = dbutils.fs.ls(file)[0][1]
    file_size = dbutils.fs.ls(file)[0][2]
    dict_blob_file_info[file_nm] = file_size
    
  # Compare ADF transfer file with blob file
  for blob_file in dict_blob_file_info:
    size_adf =dict_adf_file_info[blob_file]
    size_blob = dict_blob_file_info[blob_file]
    print_std("INFO","File name: %s, ADF size: %d, Blob size: %d" % (blob_file,size_adf,size_blob))
    if size_adf != size_blob:
      raise Exception("[Exception] Other exception","Size transfer file from ADF not equal to blob","ADF: %d" % size_adf, "Blob: %d" % size_blob)


# COMMAND ----------

# DBTITLE 1,Validate File After Copy (dbutils.fs.cp)
def vld_copy_file(orig_file_list,land_file_list):
    # Validate number of file
    print_std("INFO","---------------------------------------------------------")
    print_std("INFO","Validating number of transfer file...")
    num_inb_file = len(orig_file_list)
    num_land_file = len(land_file_list)
    print_std("INFO","Num inb file: %d, Num raw file: %d" % (num_inb_file,num_land_file))
    if num_inb_file != num_land_file:
        raise Exception("[Exception] Other exception","Number of transfer file from inb not equal to raw","inb: %d" % num_inb_file, "raw: %d" % num_land_file)

    # Validate size file
    print_std("INFO","---------------------------------------------------------")
    print_std("INFO","Validating size transfer file...")
    # Get list of dict file information
    dict_inb_file_info = {}
    for file in orig_file_list:
        file_nm = dbutils.fs.ls(file)[0][1]
        file_size = dbutils.fs.ls(file)[0][2]
        dict_inb_file_info[file_nm] = file_size
    dict_land_file_info = {}
    for land_file in land_file_list:
        file_nm = dbutils.fs.ls(land_file)[0][1]
        file_size = dbutils.fs.ls(land_file)[0][2]
        dict_land_file_info[file_nm] = file_size
    
    # Compare inb transfer file with raw
    for file in dict_land_file_info:
        size_inb = dict_inb_file_info[file]
        size_land = dict_land_file_info[file]
        print_std("INFO","File name: %s, inb size: %d, raw size: %d" % (file,size_inb,size_land))
        if size_inb != size_land:
            raise Exception("[Exception] Other exception","Size transfer file from inb not equal to raw","inb: %d" % size_inb, "raw: %d" % size_land)

# COMMAND ----------

# DBTITLE 1,Get encrypt key
def get_encpt_key():
  wrap_key = ENCPT_KEY_1
  data_key = ENCPT_KEY_2
  encpt_key = wrap_key+":"+data_key
  return encpt_key

# COMMAND ----------

# DBTITLE 1,Rotate encrypt key
def rotate_encpt_key():
  wrap_base64_string = ENCPT_KEY_1
  wrap_base64_bytes = wrap_base64_string.encode("UTF-8")
  wrap_sample_string_bytes = base64.b64decode(wrap_base64_bytes)
  wrap_sample_string = wrap_sample_string_bytes.decode("UTF-8")

  data_base64_string = ENCPT_KEY_2
  data_base64_bytes = data_base64_string.encode("UTF-8")
  data_sample_string_bytes = base64.b64decode(data_base64_bytes)
  data_sample_string = data_sample_string_bytes.decode("UTF-8")

  encpt_key = spark.sql(f"select decrypt('{wrap_sample_string}','{data_sample_string}')").collect()[0][0]
  new_wrap_key = secrets.token_urlsafe(32)
  new_data_key = spark.sql(f"select encrypt('{new_wrap_key}','{encpt_key}')").collect()[0][0]

  new_wrap_string_bytes = new_wrap_key.encode("UTF-8")
  new_wrap_base64_bytes = base64.b64encode(new_wrap_string_bytes)
  new_wrap_base64 = new_wrap_base64_bytes.decode("UTF-8")

  new_data_string_bytes = new_data_key.encode("UTF-8")
  new_data_base64_bytes = base64.b64encode(new_data_string_bytes)
  new_data_base64 = new_data_base64_bytes.decode("UTF-8")
  return new_wrap_base64, new_data_base64

# COMMAND ----------

# DBTITLE 1,Check Holiday
def chck_is_hldy(hldy_type,dl_data_dt):
  query = "SELECT * FROM " + CONFIG_DB_NM + ".tbl_hldy where dt_en = '" + dl_data_dt + "'"
  hldy_info = spark.sql(query)
  
  if (hldy_type == "WEEKEND" or hldy_type == "HOLIDAY") and get_wkdy_nm(dl_data_dt) in ("Saturday","Sunday"):
    return True
  elif (hldy_type == "HOLIDAY" and hldy_info.count() > 0):
    return True

# COMMAND ----------

def get_wkdy_nm(dl_data_dt):
  yr, mnth, day = (int(x) for x in dl_data_dt.split('-'))
  answr = datetime.date(yr, mnth, day)
  return answr.strftime("%A")

# COMMAND ----------

def crte_sccs_file(job_nm,dl_data_dt,zone,trgt_tbl_nm):
  
  ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
  sccs_file_nm = trgt_tbl_nm.lower() + '.' + job_nm + '.' + dl_data_dt.replace("-","") + '.' + ts + '.success'
  sccs_file_path = DATAX_SHARED_FILE_LCN + "/" + MONOLINE + "/evtg/" + zone + "/" + dl_data_dt.replace("-","") + "/"
  
  try:
    dbutils.fs.ls(sccs_file_path)
  except:
    dbutils.fs.mkdirs(sccs_file_path)
  
  try:
    dbutils.fs.put(sccs_file_path + sccs_file_nm, "", True)
    print_std("INFO","Put a success file to a proper location completely: job_nm: {}, dl_data_dt: {}".format(job_nm,dl_data_dt))
  except:
    raise SystemError("[SystemError] Path directory issue found", "Cannot write a success file: " + sccs_file_path)

# COMMAND ----------

def rm_prev_sccs_file(job_nm,dl_data_dt,zone,title_nm):
  sccs_file_path = DATAX_SHARED_FILE_LCN + "/" + MONOLINE + "/evtg/" + zone + "/" + dl_data_dt.replace("-","") + "/"
  rm_file_nm = []
  
  try:
    file_list = [file[1] for file in dbutils.fs.ls(sccs_file_path)]
    key_wrd = title_nm.lower() + "." +job_nm + "." + dl_data_dt.replace("-","")
    
    for file in file_list:
      if key_wrd in file:
        rm_file_nm.append(file)
        print_std("INFO","File needs to be removed: " + file)
  except:
    print_std("INFO","No file needs to be removed")
  
  try:
    if len(rm_file_nm) > 0:
      for file in rm_file_nm:
        dbutils.fs.rm(sccs_file_path + file, True)
      
      print_std("INFO","Removed file(s) successfully")
    else:
      print_std("INFO","No file needs to be removed")
  except:
    raise SystemError("[SystemError] File cannot be removed","Cannot remove file: " + sccs_file_path)

# COMMAND ----------

def get_sccs_ppty_nm(job_nm):
  title_nm = ""
  zone = ""
  if job_nm.startswith("pi"):
    config_df = get_config("tbl_ingt_job","WHERE job_nm ='"+job_nm+"'")
    zone = "ingt"
    title_nm = none2str(config_df.collect()[0]['trgt_tbl_nm'])
  elif job_nm.startswith("pt"):
    config_df = get_config("tbl_tnfm_job","WHERE job_nm ='"+job_nm+"'")
    zone = "curated"
    title_nm = none2str(config_df.collect()[0]['trgt_tbl_nm'])
  elif job_nm.startswith("po"):
    try:
      config_df = get_config("tbl_outbnd_job","WHERE job_nm ='"+job_nm+"'")
      zone = "outbnd"
      expt_data_type = none2str(config_df.collect()[0]['expt_data_type'])

      if expt_data_type == 'cosmos_db':
        expt_file_ppty = none2str(config_df.collect()[0]['expt_file_ppty'])
        expt_file_ppty = json.loads(expt_file_ppty)
        title_nm = expt_file_ppty['trgt_container_nm']
      else:
        title_nm = none2str(config_df.collect()[0]['expt_file_nm_fmt'])
    except:
      zone = "skip"
  
  sccs_ppty = {"zone":zone,"title_nm":title_nm}
  
  return sccs_ppty

# COMMAND ----------

def read_ctl_file(ctl_file_path,dict_spec):
  
  dict_spec = eval(dict_spec)
  
  #control header
  dict_header = {"0":"false","1":"true"}
  try:
    if dict_spec['ctl_header']:
      ctl_header = dict_header[dict_spec['ctl_header']]
  except:
    ctl_header = "false"

  #control delim
  try:
    if dict_spec['ctl_delimiter']:
      ctl_delim = dict_spec['ctl_delimiter']
  except:
    ctl_delim = "|"
    
  #control count position
  try:
    if dict_spec['ctl_cnt_pstn']:
      ctl_cnt_pstn = dict_spec['ctl_cnt_pstn']
  except:
    ctl_cnt_pstn = "6"
  
  #validate if position is integer
  try:
    ctl_cnt_pstn = int(ctl_cnt_pstn)
  except:
    raise Exception("[Exception] Other exception","Control position is not an integer: " + str(dict_spec))
  
  ctl_df = spark.read.format("csv").option("header", ctl_header).option("delimiter",ctl_delim).load(ctl_file_path)
  ctl_data = ctl_df.collect()[0][ctl_cnt_pstn-1]
  
  try:
    return int(ctl_data)
  except:
    raise Exception("[Exception] Other exception","Position "+str(ctl_cnt_pstn)+" in control file is not an integer: " + str(ctl_data))
  
  return ctl_df

#DATAX Functions

def check_job_exists(job_nm: str):
    try:
        job_dict_ls =  get_job_list_api().json()['jobs']
        print(job_dict_ls)
        for job_dict in job_dict_ls:
            if job_dict['settings']['name'] == job_nm:
                return True
    except Exception as e:
        print(e)
        return False
    return False

# COMMAND ----------

# DBTITLE 1,API Create JOB
def api_create_job(job_config_json):
  response = requests.post(
    'https://%s/api/2.1/jobs/create' % (DOMAIN_URL),
    headers={'Authorization': 'Bearer %s' % API_TOKEN},
    json= job_config_json
  )  
  # Validate response code
  response.raise_for_status()
  return response

# COMMAND ----------

# DBTITLE 1,API Delete JOB
def api_delete_job(token_id, job_config_json):
  response = requests.post(
    'https://%s/api/2.1/jobs/delete' % (DOMAIN_URL),
    headers={'Authorization': 'Bearer %s' % API_TOKEN},
    json= job_config_json
  )  
  # Validate response code
  response.raise_for_status()
  return response

# COMMAND ----------

def fin_hldy_rqst(year:int) -> dict:
  response = requests.get(
    f'https://apigw1.bot.or.th/bot/public/financial-institutions-holidays/?year={year}',
    headers={'X-IBM-Client-Id': f'{BOT_TOKEN}',
             'accept' : 'application/json'
            }
  )
  # Validate response code
  response.raise_for_status()
  return response.json()

# COMMAND ----------

# DBTITLE 1,Find workspace
def find_wkspc(db_mnl):
  if re.search('cardx',db_mnl.lower()) :
    wrkspc = 'CARDX'
    DL_MAIN_SRC_SYS = 'MOBIUS'
  elif re.search('card2',db_mnl.lower()) :
    wrkspc = 'CARDX'
    DL_MAIN_SRC_SYS = 'MOBIUS'
  elif re.search('autox',db_mnl.lower()):
    wrkspc = 'AUTOX'
    DL_MAIN_SRC_SYS = 'MOBIUS'
  elif re.search('dl',db_mnl.lower()):
    wrkspc = 'SCB'
    DL_MAIN_SRC_SYS = 'RM'
  else:
    wrkspc = 'OTHERX'
    DL_MAIN_SRC_SYS = 'MOBIUS'
  return wrkspc,DL_MAIN_SRC_SYS

# COMMAND ----------

def get_html_file_content(run_id):
  html_content = api_expt_html_file(run_id)['views'][0]['content']
  return html_content

# COMMAND ----------

def insr_arch_nb_log(dict_values):
  print_std("INFO","Log inserting...")
  list_log_col = spark.read.table(CONFIG_DB_NM+".tbl_arch_nb_log").columns
  insert_val = []
  del_key_list =[]
  for key in dict_values:
    if not dict_values[key]:
      del_key_list.append(key)
  for key in del_key_list:    
    del dict_values[key]
 
  for col in list_log_col:
    if col in dict_values:
      insert_val.append("'"+dict_values[col]+"'")
    else:
      if col == "expt_sts":
        insert_val.append("'NOT STARTED'")
      else:
        insert_val.append("NULL")
        
  query = "INSERT INTO "+CONFIG_DB_NM+".tbl_arch_nb_log"+" VALUES ("+",".join(insert_val)+")"
  print_std("INFO","Query: "+query)
  spark.sql(query)

# COMMAND ----------

def get_insr_arch_nb_struct(dict_run_info,adb_root_run_id,root_job_id,root_job_strt_dt,html_lctn):
  adb_run_id = str(dict_run_info['run_id'])
  adb_job_id = str(dict_run_info['job_id'])
  nb_path = str(dict_run_info['nb_path'])
  log_dt = get_current("date")
  log_dttm = get_current("datetime")
  run_log = dict_run_info['run_url']
  html_file_path = f"{html_lctn}/{PRFX_EXPT_NB_NM}_{adb_root_run_id}_{adb_job_id}_{adb_run_id}.HTML"
  
  insr_arch_nb_struct = {
    "adb_run_id": adb_run_id,
    "adb_job_id": adb_job_id,
    "nb_path": nb_path,
    "adb_root_run_id": adb_root_run_id,
    "root_job_id": root_job_id,
    "root_job_strt_dt": root_job_strt_dt,
    "run_log": run_log,
    "html_file_lctn": html_file_path,
    "log_dt": log_dt,
    "log_dttm": log_dttm,
    "monoline": MONOLINE.upper()
  }
  
  return insr_arch_nb_struct

# COMMAND ----------

# DBTITLE 1,Insert DSR data to log
def insert_dsr_log(tbl_nm,dict_values,partition="dl_data_dt,job_nm",multi_values=False):
  print_std("INFO","Log inserting...")
  list_log_col = spark.read.table(CONFIG_DB_NM+"."+tbl_nm).columns
  print_std("INFO","Log columns: "+",".join(list_log_col))
  
  # Remove empty string from dict_values
  if not multi_values:
    insert_val = []
    del_key_list =[]
    for key in dict_values:
      if not dict_values[key]:
        del_key_list.append(key)
    for key in del_key_list:    
      del dict_values[key]

    for col in list_log_col:
      if col in dict_values:
        insert_val.append("'"+dict_values[col]+"'")
      else:
        insert_val.append("NULL")
    query = "INSERT INTO "+CONFIG_DB_NM+"."+tbl_nm+" PARTITION("+partition+") VALUES ("+",".join(insert_val)+")"
  else:
    insert_row = []
    for row in dict_values:
      del_key_list =[]
      insert_val = []
      for key in row:
        if not row[key]:
          del_key_list.append(key)
      for key in del_key_list:
        del row[key]

      for col in list_log_col:
        if col in row:
          insert_val.append("'"+str(row[col]).replace("'","\"")+"'")
        else:
          insert_val.append("NULL")
      insert_row.append("(%s)" % (",".join(insert_val)))
        
    query = "INSERT INTO %s.%s PARTITION(%s) VALUES %s" % (CONFIG_DB_NM,tbl_nm,partition,",".join(insert_row))
    
  print_std("INFO","Query: "+query)
  spark.sql(query)
    
  if not multi_values:
    if "job_nm" in dict_values.keys() and "dl_data_dt" in dict_values.keys():
      job_nm = dict_values['job_nm']
      dl_data_dt = dict_values['dl_data_dt']
      print_std("INFO","Job_nm: " + job_nm + ", DL_DATA_DT: "+ dl_data_dt)

      sccs_ppty = get_sccs_ppty_nm(job_nm)
      zone = sccs_ppty['zone']
      title_nm = sccs_ppty['title_nm']

      if zone != "" and zone != "skip":
        print_std("INFO","Removing success file(s)...")
        rm_prev_sccs_file(job_nm,dl_data_dt,zone,title_nm)

# COMMAND ----------

# DBTITLE 1,Update DSR data to log
def update_dsr_log(tbl_nm,where_cond,dict_upd,vld_dup_flag):
  print_std("INFO","Log updating...")
  # Remove empty string from dict_values
  del_key_list =[]
  for key in dict_upd:
    if not dict_upd[key]:
      del_key_list.append(key)
  for key in del_key_list:    
    del dict_upd[key]
  
  if vld_dup_flag.upper() == "Y":
    query_vld = "SELECT * FROM "+CONFIG_DB_NM+"."+tbl_nm+" "+where_cond
    print_std("INFO","Query validate duplicated update: "+query_vld)
    vld_df = spark.sql(query_vld)
    if vld_df.count() > 1:
      raise SystemError("[System Error] Duplicate updating log found",", ".join([tbl_nm,where_cond]))

  set_cond = " SET "+', '.join([(x+"="+str(dict_upd[x]) if 'CASE WHEN' in str(dict_upd[x]) else x+"='"+str(dict_upd[x])+"'") for x in dict_upd])+" "
  query_upd = "UPDATE "+CONFIG_DB_NM+"."+tbl_nm+set_cond+where_cond
  print_std("INFO","Query log updated: "+query_upd)
  spark.sql(query_upd)
  
  if 'job_sts' in dict_upd.keys():
    if dict_upd['job_sts'] == "SUCCESS":
      job_nm = re.findall("job_nm ='(.*?)'", where_cond)
      dl_data_dt = re.findall("dl_data_dt ='(.*?)'",where_cond)
      if job_nm and dl_data_dt:
        job_nm = job_nm[0]
        dl_data_dt = dl_data_dt[0]
        print_std("INFO","Job_nm: " + job_nm + ", DL_DATA_DT: "+ dl_data_dt)

        sccs_ppty = get_sccs_ppty_nm(job_nm)
        zone = sccs_ppty['zone']
        title_nm = sccs_ppty['title_nm']

        if zone != "" and title_nm != "" and zone != "skip":
          crte_sccs_file(job_nm,dl_data_dt,zone,title_nm)

# COMMAND ----------

# DBTITLE 1,Cleanup DSR Log
def cleanup_dsr_log(tbl_nm, where_cond):
    delete_stmt = "DELETE FROM {}.{} WHERE {}".format(CONFIG_DB_NM, tbl_nm, where_cond)
    print_std("INFO", "Query: {}".format(delete_stmt))
    spark.sql(delete_stmt)

# COMMAND ----------

# DBTITLE 1,Get Unique Dict
def get_uniq_dict(dict_value):
    df = pd.DataFrame(dict_value)
    uniq_df = df.drop_duplicates()
    uniq_dict = uniq_df.to_dict(orient='records')
    return uniq_dict
