spark-submit \
    --deploy-mode cluster \
    --conf spark.yarn.appMasterEnv.SRC_BASE_DIR=/user/`whoami`/retail_db \
    --conf spark.yarn.appMasterEnv.TGT_BASE_DIR=/user/`whoami`/retail_db/result \
    /home/`whoami`/emr/app.py