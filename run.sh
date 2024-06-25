hdfs dfs -rm -R -skipTrash ${TGT_BASE_DIR}/*

spark-submit \
    --deploy-mode cluster \
    --conf spark.yarn.appMasterEnv.SRC_BASE_DIR=/user/`whoami`/retail_db \
    --conf spark.yarn.appMasterEnv.TGT_BASE_DIR=/user/`whoami`/retail_db/result \
    --jars /home/hadoop/emr/jars/delta-spark_2.12-3.2.0.jar,/home/hadoop/emr/jars/delta-storage-3.2.0.jar,/home/hadoop/emr/jars/antlr4-runtime-4.9.3.jar \
    --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
    --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
    /home/`whoami`/emr/app.py

hdfs dfs -ls ${TGT_BASE_DIR}/daily_revenue
