from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, round
import os
import logging

logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s', 
                    level=logging.INFO,
                    datefmt='%y/%m/%d %H:%M:%S')

src_base_dir=os.environ.get('SRC_BASE_DIR')
tgt_base_dir=os.environ.get('TGT_BASE_DIR')

try:
        logging.info('Spark session is being initialized')
        spark = SparkSession. \
                builder. \
                appName('computr_daily_revenue'). \
                master('yarn'). \
                getOrCreate()

        logging.info('Reading orders data into dataframe')
        orders = spark. \
                read. \
                csv(
                f'{src_base_dir}/orders',
                schema='''
                        order_id INT, order_date STRING,
                        order_customer_id INT, order_status STRING
                        '''
                )

        logging.info('Reading order_items data into dataframe')
        order_items = spark. \
                        read. \
                        csv(
                        f'{src_base_dir}/order_items',
                        schema='''
                                order_item_id INT,order_item_order_id INT, order_item_product_id INT,
                                order_item_quantity INT,order_item_subtotal FLOAT, order_item_product_price FLOAT
                                '''
                        )

        logging.info('Computing daily revenue')
        daily_rev = orders. \
                filter("order_status IN ('COMPLETE','CLOSED')"). \
                join(order_items, orders['order_id'] == order_items['order_item_order_id']). \
                groupBy('order_date'). \
                agg(round(sum('order_item_subtotal'), 2).alias('revenue')). \
                orderBy(('order_date'))

        logging.info('Writing processed data into filrs')
        daily_rev. \
                write. \
                mode('overwrite'). \
                format('delta'). \
                save(f'{tgt_base_dir}/daily_revenue', header=True)
except:
        logging.error('Exception raised with running spark application')
        raise