# Databricks notebook source
# MAGIC %md ##　02_AutoMLによる機械学習モデルの構築：本ノートブックで実施すること
# MAGIC Databricksに格納されているサンプルの与信データを使用し、貸し倒れ予測のモデルの作成からスコアリングまで実施します。

# COMMAND ----------

# MAGIC %pip install "mlflow-skinny[databricks]>=2.4.1"
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import re
from pyspark.sql.types import * 
import pandas as pd
from pyspark.sql.types import StringType

# COMMAND ----------

catalog = "catalog_techbookfest"
db_name = "db_loan_status"

# COMMAND ----------

spark.sql(f"USE catalog {catalog}")
spark.sql(f"USE {db_name}")

print("database:　" + db_name)

# COMMAND ----------

# MAGIC %md 
# MAGIC ## 1. AutoMLの実行
# MAGIC
# MAGIC ここからは、AutoMLを使用してモデルの分散トレーニングを実行し、同時にハイパーパラメータのチューニングを実行します。</br>
# MAGIC またMLflowにより、学習に使用されたハイパーパラメータ設定とそれぞれの精度をトラッキングし、モデルアーティファクトとともに保存します。</br>
# MAGIC 参考資料: [AutoMLとは](https://learn.microsoft.com/ja-jp/azure/databricks/machine-learning/automl/)</br>

# COMMAND ----------

# MAGIC %md
# MAGIC **注意：AutoMLの実行は全てUI上の操作で実施します**

# COMMAND ----------

# MAGIC %md
# MAGIC #### AutoMLエクスペリエントの作成画面に遷移する
# MAGIC <br>
# MAGIC <img src="https://sajpstorage.blob.core.windows.net/kitaoka/Shared/experiment.png">

# COMMAND ----------

# MAGIC %md #### モデル構築に必要な情報をUI上で入力する
# MAGIC <br>
# MAGIC <img src="https://sajpstorage.blob.core.windows.net/kitaoka/Shared/automl2.png">

# COMMAND ----------

# MAGIC %md
# MAGIC #### 5分間の学習後、最適なval_roc_auc_scoreのモデルを確認する
# MAGIC <br>
# MAGIC <img src="https://sajpstorage.blob.core.windows.net/kitaoka/Shared/automl3.png">

# COMMAND ----------

# MAGIC %md ## 2. Unity Catalogへの登録とステータス管理

# COMMAND ----------

# MAGIC %md
# MAGIC #### 最も精度の高いモデルをUnity Catalogに登録する

# COMMAND ----------

# モデルレジストリ名を指定
model_name = 'loan_prediction_model'
print('登録するモデル名:　' + model_name)

# COMMAND ----------

# DBTITLE 1,Unity Catalogにモデルを登録する
import mlflow
mlflow.set_registry_uri("databricks-uc")
mlflow.register_model(
    model_uri="runs:/8961af6643a240ee882ab70943be0042/model",
    name=f"{catalog}.{db_name}.{model_name}"
)

# COMMAND ----------

# MAGIC %md
# MAGIC <img src="https://sajpstorage.blob.core.windows.net/kitaoka/Shared/automl4.png">

# COMMAND ----------

# MAGIC %md
# MAGIC <img src="https://sajpstorage.blob.core.windows.net/kitaoka/Shared/catalog_registry.png">

# COMMAND ----------

# MAGIC %md
# MAGIC #### Unity Catalogにモデルが登録されていることを確認する
# MAGIC <br>
# MAGIC <img src="https://sajpstorage.blob.core.windows.net/kitaoka/Shared/model.png">

# COMMAND ----------

# MAGIC %md
# MAGIC <img src="https://sajpstorage.blob.core.windows.net/kitaoka/Shared/model_on_unitycatalog.png">

# COMMAND ----------

# MAGIC %md ## 3. 予測の実行

# COMMAND ----------

# MAGIC %md #### Unity Catalogに登録したモデルを取得する

# COMMAND ----------

import mlflow.pyfunc

model_version_uri = "models:/{model_name}/1".format(model_name=model_name)
model_version_1 = mlflow.pyfunc.load_model(model_version_uri)
print("Loading registered model version from URI: '{model_uri}'".format(model_uri=model_version_uri))

# COMMAND ----------

model_baseline_uri = "models:/{model_name}@Baseline_model".format(model_name=model_name)
model_baseline = mlflow.pyfunc.load_model(model_baseline_uri)
print("Loading registered model version from URI: '{model_uri}'".format(model_uri=model_baseline_uri))

# COMMAND ----------

import mlflow

# load_model関数を使用して、モデルレジストリから指定したモデルを取得
model = model_baseline
print("--Metadata--")
print(model.metadata)

# COMMAND ----------

# MAGIC %md #### 予測対象データの確認

# COMMAND ----------

df_test = spark.table('tbl_loan_test')
display(df_test)

# COMMAND ----------

# MAGIC %md #### 取得したモデルをUDFとして実装する

# COMMAND ----------

#　取得したモデルをUDFとして実装
import mlflow
from pyspark.sql.functions import struct

#　モデルをモデルレジストリから取得
model_uri = model_baseline_uri

# create spark user-defined function for model prediction
loaded_model = mlflow.pyfunc.spark_udf(spark, model_uri, result_type="double")

# COMMAND ----------

# MAGIC %md
# MAGIC #### 予測を実行する（バッチスコアリング）

# COMMAND ----------

pred_df = df_test.withColumn('prediction', loaded_model(*df_test.columns))
display(pred_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ####　予測結果をDeltaテーブルに格納する

# COMMAND ----------

# output_table = "tbl_loan_prediction" 
# pred_df.write.mode('overwrite').option("overwriteSchema", "True").saveAsTable(output_table)

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC   *
# MAGIC from
# MAGIC   tbl_loan_prediction;

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <head>
# MAGIC   <link href="https://fonts.googleapis.com/css2?family=Kosugi+Maru&display=swap" rel="stylesheet">
# MAGIC   <style>
# MAGIC     h1,h2,h3,p,span,td, div {font-family: "Kosugi Maru", sans-serif !important;}
# MAGIC   </style>
# MAGIC </head>
# MAGIC
# MAGIC <h1>END</h1>  
