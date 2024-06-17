from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, round

spark = SparkSession. \
            builder. \
            appName('compute_daily_revenue'). \
            master('yarn'). \
            getOrCreate()

orders = spark. \
        read. \
        csv(
            '/user/hadoop/retail_db/orders',
            schema='''
                order_id INT, order_date STRING,
                order_customer_id INT, order_status STRING
                '''
        )

order_items = spark. \
                read. \
                csv(
                    '/user/hadoop/retail_db/order_items',
                    schema='''
                            order_item_id INT,order_item_order_id INT, order_item_product_id INT,
                            order_item_quantity INT,order_item_subtotal FLOAT, order_item_product_price FLOAT
                            '''
                )

daily_rev = orders. \
        filter("order_status IN ('COMPLETE','CLOSED')"). \
        join(order_items, orders['order_id'] == order_items['order_item_order_id']). \
        groupBy('order_date'). \
        agg(round(sum('order_item_subtotal'), 2).alias('revenue')). \
        orderBy('order_date')