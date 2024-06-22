hdfs dfs -rm -R -skipTrash /user/hadoop/retail_db/result/*

spark-submit \
    --deploy-mode cluster \
    --conf spark.yarn.appMasterEnv.SRC_BASE_DIR=/user/`whoami`/retail_db \
    --conf spark.yarn.appMasterEnv.TGT_BASE_DIR=/user/`whoami`/retail_db/result \
    --packages io.delta:delta-spark_2.12:3.2.0 \
    --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
    --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
    /home/`whoami`/emr/app.py >> /home/hadoop/emr/app.log

hdfs dfs -ls ${TGT_BASE_DIR}/daily_revenue