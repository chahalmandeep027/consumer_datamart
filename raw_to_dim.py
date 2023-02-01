from pyspark.sql import SparkSession
import yaml
import os.path
import hashlib

if __name__ == "__main__":

    os.environ["PYSPARK_SUBMIT_ARGS"] = (
        '--jars "https://s3.amazonaws.com/redshift-downloads/drivers/jdbc/1.2.36.1060/RedshiftJDBC42-no-awssdk-1.2.36.1060.jar"\
         --packages "org.apache.spark:spark-avro_2.11:2.4.2,io.github.spark-redshift-community:spark-redshift_2.11:4.0.1,org.apache.hadoop:hadoop-aws:2.7.4" pyspark-shell'
    )

    spark = SparkSession \
        .builder \
        .appName('stgtodim') \
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

    def get_redshift_jdbc_url(redshift_config: dict):
        host = redshift_config["redshift_conf"]["host"]
        port = redshift_config["redshift_conf"]["port"]
        database = redshift_config["redshift_conf"]["database"]
        username = redshift_config["redshift_conf"]["username"]
        password = redshift_config["redshift_conf"]["password"]
        return "jdbc:redshift://{}:{}/{}".format(host, port, database)

    def udfMD5Python(colList):
        concatenatedString = ''
        for index, stringValue in enumerate(colList):
            # Concatenate all the strings together. Check if stringValue is NULL and insert a blank if true
            concatenatedString += ('' if stringValue is None else stringValue)
            # Check if its not the last element in the list. If true, then append a '|' to uniquely identify column values
            if index < len(colList)-1:
                concatenatedString += '|'
        # Convert the string to a binary string and then hash the binary string and output a hexidecimal value
        return hashlib.md5(concatenatedString.encode('utf-8')).hexdigest()

    spark.udf.register("udfMD5withPython", udfMD5Python)

    spark.read \
        .parquet("s3://consumer-datamart/stg/customer_test/") \
        .createOrReplaceTempView("CUSTOMER")
    spark.read \
        .parquet("s3://consumer-datamart/stg/individual_test/") \
        .createOrReplaceTempView("INDIVIDUAL")
    spark.read \
        .parquet("s3://consumer-datamart/stg/ccard/") \
        .createOrReplaceTempView("CARD")
    spark.read \
        .parquet("s3://consumer-datamart/stg/specialoffer/") \
        .createOrReplaceTempView("OFFER")
    spark.read \
        .parquet("s3://consumer-datamart/stg/salesorderheader/") \
        .createOrReplaceTempView("ORDERDETAILS")

    cus_stg_DF = spark.sql(
        app_conf["redshift_conf"]["dim"]["CUSTOMERDIM"]["srcQuery"])

    cus_stg_DF.printSchema()
    cus_stg_DF.createOrReplaceTempView('CustomerStg')

    exit()

    redshift_jdbc_url = get_redshift_jdbc_url(app_secret)

    cus_dim_df = spark.read\
        .format("io.github.spark_redshift_community.spark.redshift")\
        .option("url", redshift_jdbc_url) \
        .option("query", app_conf["redshift_conf"]["dim"]["CUSTOMERDIM"]["readDimDataQuery"]) \
        .option("forward_spark_s3_credentials", "true")\
        .option("tempdir", "s3a://" + app_conf["s3_conf"]["s3_bucket"] + "/temp")\
        .load() \
        .createOrReplaceDimView('CustomerDim')

    update_df = spark.sql(
        app_conf["redshift_conf"]["dim"]["CUSTOMERDIM"]["updatesQuery"])

    new_df = spark.sql(
        app_conf["redshift_conf"]["dim"]["CUSTOMERDIM"]["newQuery"])

    update_df.coalesce(1).write\
        .format("io.github.spark_redshift_community.spark.redshift") \
        .option("url", redshift_jdbc_url) \
        .option("driver", 'com.amazon.redshift.jdbc42.Driver') \
        .option("tempdir", "s3a://" + app_conf["s3_conf"]["s3_bucket"] + "temp") \
        .option("forward_spark_s3_credentials", "true") \
        .option("dbtable", "UPDATE_CUSTOMERDIM") \
        .option("user", app_secret["redshift_conf"]["username"]) \
        .option("password", app_secret["redshift_conf"]["password"]) \
        .mode("overwrite")\
        .save()

    spark.read\
        .format("io.github.spark_redshift_community.spark.redshift")\
        .option("url", redshift_jdbc_url) \
        .option("query", app_conf["redshift_conf"]["dim"]["CUSTOMERDIM"]["bulkUpdateQuery"]) \
        .option("forward_spark_s3_credentials", "true")\
        .option("tempdir", "s3a://" + app_conf["s3_conf"]["s3_bucket"] + "/temp")\
        .load()

    print("stg to dim load completed")

# spark-submit --jars "https://s3.amazonaws.com/redshift-downloads/drivers/jdbc/1.2.36.1060/RedshiftJDBC42-no-awssdk-1.2.36.1060.jar" --packages "org.apache.spark:spark-avro_2.11:2.4.2,io.github.spark-redshift-community:spark-redshift_2.11:4.0.1,org.apache.hadoop:hadoop-aws:2.7.4" dataframe/provision/df_redshift.py
