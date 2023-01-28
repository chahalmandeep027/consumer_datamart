from pyspark.sql import SparkSession
import yaml
import os.path
# import utils.aws_utils as ut

if __name__ == "__main__":

    os.environ["PYSPARK_SUBMIT_ARGS"] = (
        '--packages "mysql:mysql-connector-java:8.0.15" pyspark-shell'
    )

    spark = SparkSession \
        .builder \
        .appName('mysqltostg') \
        .master('local[*]') \
        .getOrCreate()
    spark.sparkContext.setLogLevel('ERROR')

    current_dir = os.path.abspath(os.path.dirname(__file__))
    app_config_path = os.path.abspath(current_dir + '/' + 'application.yml')
    app_secret_path = os.path.abspath(current_dir + '/' + '.secrets')

    conf = open(app_config_path)
    app_conf = yaml.load(conf, Loader=yaml.FullLoader)
    secret = open(app_secret_path)
    app_secret = yaml.load(secret, Loader=yaml.FullLoader)

    # print(app_conf['s3_conf']['s3_bucket'])

    def get_mysql_jdbc_url(mysql_config: dict):
        host = mysql_config["mysql_conf"]["hostname"]
        port = mysql_config["mysql_conf"]["port"]
        database = mysql_config["mysql_conf"]["database"]
        return "jdbc:mysql://{}:{}/{}?autoReconnect=true&useSSL=false".format(host, port, database)

    for src in app_conf['SRC']['MYSQL']:
        # print(app_conf['SRC']['MYSQL'][src]['dbtable'])
        # print(app_conf['SRC']['MYSQL'][src]['partition_column'])
        # print(app_conf['SRC']['MYSQL'][src]['query'])
        jdbc_params = {
            "url": get_mysql_jdbc_url(app_secret),
            "lowerBound": "1",
            "upperBound": "100",
            "dbtable": app_conf['SRC']['MYSQL'][src]['query'],
            "numPartitions": "2",
            "partitionColumn": app_conf['SRC']['MYSQL'][src]['partition_column'],
            "user": app_secret["mysql_conf"]["username"],
            "password": app_secret["mysql_conf"]["password"]
        }

    # print(jdbcParams)

    # use the ** operator/un-packer to treat a python dictionary as **kwargs
        print("\nReading data from MySQL DB using SparkSession.read.format(),")
        srcDF = spark\
            .read.format("jdbc")\
            .option("driver", "com.mysql.cj.jdbc.Driver")\
            .options(**jdbc_params)\
            .load()

        src.show()

        src.printSchema()
        src.show(5, False)

        # src.write \
        #     .mode("overwrite") \
        #     .partitionBy("ins_dt") \
        #     .parquet(
        #         "s3a://" + app_conf["s3_conf"]["s3_bucket"]+app_conf["s3_conf"]["staging_dir"]+"/" + src)

        src.write \
            .mode("overwrite") \
            .parquet(
                "s3a://" + app_conf["s3_conf"]["s3_bucket"]+app_conf["s3_conf"]["staging_dir"]+"/" + src)

    print("Writing SRC data to s3 staging dir complete")

# spark-submit --packages "mysql:mysql-connector-java:8.0.15" dataframe/ingestion/others/systems/mysql_df.py
