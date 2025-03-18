# Databricks notebook source
# MAGIC %md
# MAGIC <img src='https://sajpstorage.blob.core.windows.net/kitaoka/Shared/functions.png'/>
# MAGIC

# COMMAND ----------

# MAGIC %md ##　01_データ作成：本ノートブックで実施すること
# MAGIC databricksに格納されているサンプルの与信データを使用し、トレーニングデータを作成します。

# COMMAND ----------

# MAGIC %md ## トレーニングデータの準備
# MAGIC ##### クラウドストレージ上のファイルからトレーニングデータ(ml_loan_status_workテーブル)を作成
# MAGIC - databricksに格納されているサンプルデータを使用：LendingClub社が公開している与信データ(US)

# COMMAND ----------

import re
from pyspark.sql.types import *
import pandas as pd
from pyspark.sql.types import StringType

# COMMAND ----------

# DBTITLE 1,カタログの設定
# データベース名を生成
catalog_name = "catalog_techbookfest"
db_name = "db_loan_status"

# データベースの準備
spark.sql(f"USE catalog {catalog_name}")
spark.sql(f"CREATE DATABASE IF NOT EXISTS {db_name}")
spark.sql(f"USE {db_name}")

print("database:　" + db_name)

# COMMAND ----------

# MAGIC %md
# MAGIC #### ブロンズテーブルの作成

# COMMAND ----------

# パスの指定
source_path = "dbfs:/databricks-datasets/samples/lending_club/parquet/"

# ソースディレクトリにあるParquetファイルをデータフレームとして読込む
bronze_df = spark.read.parquet(source_path)

# 読込まれたデータを参照
display(bronze_df)

# レコード件数確認
print("レコード件数:", bronze_df.count())

# COMMAND ----------

# ブロンズテーブルの作成
bronze_df.write.mode("overwrite").saveAsTable("tbl_loan_bronze")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE
# MAGIC OR REPLACE VIEW view_loan_bronze AS (
# MAGIC   SELECT
# MAGIC     *
# MAGIC   FROM
# MAGIC     tbl_loan_bronze
# MAGIC   LIMIT
# MAGIC     50
# MAGIC )

# COMMAND ----------

# raw_data_path = "s3://one-env-uc-external-location/ktksk/db_external_location/"
# bronze_df.write.format("delta").mode("overwrite").option("path", raw_data_path).saveAsTable("demo_ktksk.db_external_location.tbl_loan_unmanaged")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ブロンズテーブルのレコード件数を確認
# MAGIC select
# MAGIC   count(1)
# MAGIC from
# MAGIC   tbl_loan_bronze;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 主要なカラムにコメントを追加する
# MAGIC ALTER TABLE
# MAGIC   tbl_loan_bronze
# MAGIC ALTER COLUMN
# MAGIC   annual_inc COMMENT "借り手が登録時に提示した自己申告年収";
# MAGIC ALTER TABLE
# MAGIC   tbl_loan_bronze
# MAGIC ALTER COLUMN
# MAGIC   addr_state COMMENT "ローン申込時に借り手から提供された居住地情報(州)";
# MAGIC ALTER TABLE
# MAGIC   tbl_loan_bronze
# MAGIC ALTER COLUMN
# MAGIC   chargeoff_within_12_mths COMMENT "12ヶ月以内の償却件数";
# MAGIC ALTER TABLE
# MAGIC   tbl_loan_bronze
# MAGIC ALTER COLUMN
# MAGIC   delinq_2yrs COMMENT "借り手の信用ファイルの過去2年間の30日以上の延滞の発生数";
# MAGIC ALTER TABLE
# MAGIC   tbl_loan_bronze
# MAGIC ALTER COLUMN
# MAGIC   delinq_amnt COMMENT "債務者が現在滞納している口座の過去の延滞金";
# MAGIC ALTER TABLE
# MAGIC   tbl_loan_bronze
# MAGIC ALTER COLUMN
# MAGIC   dti COMMENT "住宅ローンと希望するＬＣローンを除いた債務総額の月々の支払額を、債務者の自己申告月収で割って算出した比率";
# MAGIC ALTER TABLE
# MAGIC   tbl_loan_bronze
# MAGIC ALTER COLUMN
# MAGIC   emp_title COMMENT "融資申込時に借入人から提供された職種";
# MAGIC ALTER TABLE
# MAGIC   tbl_loan_bronze
# MAGIC ALTER COLUMN
# MAGIC   emp_length COMMENT "雇用期間を年単位で表します。可能な値は0から10の間で、0は1年未満を意味し、10は10年以上を意味します";
# MAGIC ALTER TABLE
# MAGIC   tbl_loan_bronze
# MAGIC ALTER COLUMN
# MAGIC   grade COMMENT "LCが設定したローン申請者の信用グレード";
# MAGIC ALTER TABLE
# MAGIC   tbl_loan_bronze
# MAGIC ALTER COLUMN
# MAGIC   home_ownership COMMENT "登録時に借り手から提供された、または信用調査書から取得した自宅の所有状況。 賃貸、所有、モーゲージ、その他";
# MAGIC ALTER TABLE
# MAGIC   tbl_loan_bronze
# MAGIC ALTER COLUMN
# MAGIC   int_rate COMMENT "ローンの金利";
# MAGIC ALTER TABLE
# MAGIC   tbl_loan_bronze
# MAGIC ALTER COLUMN
# MAGIC   installment COMMENT "ローンを組んだ場合、借り手が毎月負担する支払い額";
# MAGIC ALTER TABLE
# MAGIC   tbl_loan_bronze
# MAGIC ALTER COLUMN
# MAGIC   loan_amnt COMMENT "借り手が申し込んだローンの記載金額です。ある時点で信用部が融資額を減額した場合は、この値に反映されます";
# MAGIC ALTER TABLE
# MAGIC   tbl_loan_bronze
# MAGIC ALTER COLUMN
# MAGIC   open_acc COMMENT "借り手のクレジットファイルのオープンクレジットラインの数です";
# MAGIC ALTER TABLE
# MAGIC   tbl_loan_bronze
# MAGIC ALTER COLUMN
# MAGIC   pub_rec COMMENT "借り手に対する軽蔑的な記録の数";
# MAGIC ALTER TABLE
# MAGIC   tbl_loan_bronze
# MAGIC ALTER COLUMN
# MAGIC   purpose COMMENT "借り手のローン目的";
# MAGIC ALTER TABLE
# MAGIC   tbl_loan_bronze
# MAGIC ALTER COLUMN
# MAGIC   pub_rec_bankruptcies COMMENT "公的な貸し倒れ件数";
# MAGIC ALTER TABLE
# MAGIC   tbl_loan_bronze
# MAGIC ALTER COLUMN
# MAGIC   revol_bal COMMENT "リボルビング払い残高";
# MAGIC ALTER TABLE
# MAGIC   tbl_loan_bronze
# MAGIC ALTER COLUMN
# MAGIC   revol_util COMMENT "リボルビングラインの利用率、または借り手が利用可能なすべてのリボルビングクレジットと相対的に使用しているクレジットカードの量";
# MAGIC ALTER TABLE
# MAGIC   tbl_loan_bronze
# MAGIC ALTER COLUMN
# MAGIC   sub_grade COMMENT "LCが設定したローン申請者の信用サブグレード";
# MAGIC ALTER TABLE
# MAGIC   tbl_loan_bronze
# MAGIC ALTER COLUMN
# MAGIC   term COMMENT "ローンの支払い回数。値は月数であり、36または60のいずれか";
# MAGIC ALTER TABLE
# MAGIC   tbl_loan_bronze
# MAGIC ALTER COLUMN
# MAGIC   total_acc COMMENT "借り手のクレジットカードの現在の総数";
# MAGIC ALTER TABLE
# MAGIC   tbl_loan_bronze
# MAGIC ALTER COLUMN
# MAGIC   verification_status COMMENT "収入がLCによって確認されたか、確認されていないか、または収入源が確認されたかを示す";
# MAGIC ALTER TABLE
# MAGIC   tbl_loan_bronze
# MAGIC ALTER COLUMN
# MAGIC   zip_code COMMENT "借り手から提供された居住地情報(エリア)";
# MAGIC -- テーブルコメント
# MAGIC ALTER TABLE
# MAGIC   tbl_loan_bronze
# MAGIC SET
# MAGIC   TBLPROPERTIES ('comment' = '与信デモ用テーブル');

# COMMAND ----------

# MAGIC %md
# MAGIC #### シルバーテーブルの作成

# COMMAND ----------

# MAGIC %md
# MAGIC ##### SQLによるデータクレンジング(不要なカラム/レコードの削除)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- クレンジングを実施
# MAGIC DROP TABLE IF EXISTS tbl_loan_silver;
# MAGIC CREATE TABLE tbl_loan_silver AS
# MAGIC SELECT
# MAGIC   annual_inc,
# MAGIC   addr_state,
# MAGIC   chargeoff_within_12_mths,
# MAGIC   delinq_2yrs,
# MAGIC   delinq_amnt,
# MAGIC   dti,
# MAGIC   emp_title,
# MAGIC   grade,
# MAGIC   home_ownership,
# MAGIC   cast(replace(int_rate, '%', '') as float) as int_rate,
# MAGIC   installment,
# MAGIC   loan_amnt,
# MAGIC   open_acc,
# MAGIC   pub_rec,
# MAGIC   purpose,
# MAGIC   pub_rec_bankruptcies,
# MAGIC   revol_bal,
# MAGIC   cast(replace(revol_util, '%', '') as float) as revol_util,
# MAGIC   sub_grade,
# MAGIC   total_acc,
# MAGIC   verification_status,
# MAGIC   zip_code,
# MAGIC   case
# MAGIC     when loan_status = 'Fully Paid' then 0
# MAGIC     else 1
# MAGIC   end as bad_loan
# MAGIC FROM
# MAGIC   tbl_loan_bronze
# MAGIC WHERE
# MAGIC   loan_status in (
# MAGIC     'Fully Paid',
# MAGIC     'Default',
# MAGIC     'Charged Off',
# MAGIC     'Late (31-120 days)',
# MAGIC     'Late (16-30 days)'
# MAGIC   )
# MAGIC   AND addr_state is not null
# MAGIC   AND annual_inc >= 120000;
# MAGIC --　新規作成のカラムにコメントを付与
# MAGIC ALTER TABLE
# MAGIC   tbl_loan_silver
# MAGIC ALTER COLUMN
# MAGIC   int_rate COMMENT "ローンの金利";
# MAGIC ALTER TABLE
# MAGIC   tbl_loan_silver
# MAGIC ALTER COLUMN
# MAGIC   revol_util COMMENT "リボルビングラインの利用率、または借り手が利用可能なすべてのリボルビングクレジットと相対的に使用しているクレジットカードの量";
# MAGIC ALTER TABLE
# MAGIC   tbl_loan_silver
# MAGIC ALTER COLUMN
# MAGIC   bad_loan COMMENT "債務不履行(貸し倒れ)　0=完済 or 1=貸し倒れ";

# COMMAND ----------

# MAGIC %sql
# MAGIC -- シルバーテーブルのレコード件数を確認
# MAGIC select
# MAGIC   count(1)
# MAGIC from
# MAGIC   tbl_loan_silver;

# COMMAND ----------

# DBTITLE 1,テーブル定義の確認
# MAGIC %sql
# MAGIC describe extended tbl_loan_silver;

# COMMAND ----------

# DBTITLE 1,SQLによるデータの確認
# MAGIC %sql
# MAGIC select
# MAGIC   *
# MAGIC from
# MAGIC   catalog_techbookfest.db_loan_status.tbl_loan_silver;

# COMMAND ----------

# DBTITLE 1,データの可視化
# MAGIC %sql
# MAGIC --特定の特徴量が予測対象にどの程度影響しているかをBoxチャートで確認
# MAGIC select
# MAGIC   case
# MAGIC     when bad_loan = 1 then '貸し倒れ'
# MAGIC     else '完済'
# MAGIC   end as bad_loan,
# MAGIC   purpose
# MAGIC from
# MAGIC   tbl_loan_silver

# COMMAND ----------

# DBTITLE 1,UnityCatalogによるデータリネージュの確認
# MAGIC %md
# MAGIC <img src="https://sajpstorage.blob.core.windows.net/kitaoka/Shared/uc1.png">

# COMMAND ----------

# MAGIC %md
# MAGIC <img src="https://sajpstorage.blob.core.windows.net/kitaoka/Shared/uc2.png">

# COMMAND ----------

# MAGIC %md
# MAGIC <img src="https://sajpstorage.blob.core.windows.net/kitaoka/Shared/uc3.png">

# COMMAND ----------

# MAGIC %md 
# MAGIC ##### SQLによるデータ加工

# COMMAND ----------

# DBTITLE 1,ACIDトランザクションサポート(SQL文レベル)とデータ変更履歴管理機能の確認
# MAGIC %sql
# MAGIC --(1) emp_titleの種類が多いので職業についている場合は"UNEMPLOYED"を代入
# MAGIC update
# MAGIC   tbl_loan_silver
# MAGIC set
# MAGIC   emp_title = 'EMPLOYED'
# MAGIC where
# MAGIC   emp_title is not null

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 全レコード削除
# MAGIC truncate table tbl_loan_silver;

# COMMAND ----------

# MAGIC %sql
# MAGIC --　Trincateを実行したため、クエリ結果は返却されません。(データは０件)
# MAGIC select
# MAGIC   *
# MAGIC from
# MAGIC   tbl_loan_silver;

# COMMAND ----------

# MAGIC %sql
# MAGIC --　変更履歴の確認
# MAGIC -- Version ４の時点がTrincateする前の最新データになっています。
# MAGIC describe history tbl_loan_silver;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Trincateする前のデータにアクセス
# MAGIC select
# MAGIC   *
# MAGIC from
# MAGIC   tbl_loan_silver version as of 4

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 別VersionのデータをUnion
# MAGIC select
# MAGIC   *
# MAGIC from
# MAGIC   tbl_loan_silver version as of 4
# MAGIC union
# MAGIC select
# MAGIC   *
# MAGIC from
# MAGIC   tbl_loan_silver version as of 3;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Trincateする前のデータをリストア
# MAGIC restore tbl_loan_silver to version as of 4;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- データがリストアされていることを確認
# MAGIC select
# MAGIC   *
# MAGIC from
# MAGIC   ktksk_catalog_demo.db_loan_status.tbl_loan_silver;

# COMMAND ----------

# MAGIC %md
# MAGIC ##### データを訓練用とテスト用に分割

# COMMAND ----------

# randomSplit()を使用して、データを分割
silver_df = spark.table("tbl_loan_silver")
(train_df, test_df) = silver_df.randomSplit([0.80, 0.20], seed=123)

# テーブルとして保存する
# トレーニングデータ
train_df.write.mode("overwrite").saveAsTable("tbl_loan_train")

# テストデータ:bad_loanカラムを除去する
test_df.drop("bad_loan").write.mode("overwrite").option(
    "overwriteSchema", "True"
).saveAsTable("tbl_loan_test")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 非構造化データのサンプル
# MAGIC - 画像: https://sajpstorage.blob.core.windows.net/kitaoka/Shared/functions.png
# MAGIC - CSVファイル: https://sajpstorage.blob.core.windows.net/kitaoka/Rakuten/lending_club.csv

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
