# from pyspark.sql.session import SparkSession
#
# if __name__=="__main__":
#     spark= SparkSession().builder\
#             .appName("Spark App")\
#             .master("local[3]")\
#             .getOrCreate()
#
#     spark.stop()
import sys
from pyspark.sql import SparkSession
from pyspark import SparkConf
from utils import get_spark_app_config, load_customer_df
from pyspark.sql.functions import col, avg

if __name__ == "__main__":
    # Initialize SparkSession
    conf = get_spark_app_config()
    spark = SparkSession.builder \
        .config(conf=conf)\
        .getOrCreate()

    if len(sys.argv) != 2:
        print("File does not exist")
        sys.exit(-1)

    print("Starting Hello Spark")
    # conf_out=spark.sparkContext.getConf()
    # print(conf_out.toDebugString())

    customer_df= load_customer_df(spark, sys.argv[1])

    #customer_df.show()

    filtered_df = customer_df.groupBy("gender")\
                             .agg(avg(col("income")).alias("avg_income"))

    filtered_df.show()

    print("Finished Hello Spark")
    spark.stop()



