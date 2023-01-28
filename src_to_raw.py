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

    jdbc_params = {
        "url": get_mysql_jdbc_url(app_secret),
        "lowerBound": "1",
        "upperBound": "100",
        "dbtable": app_conf["mysql_conf"]["dbtable"],
        "numPartitions": "2",
        "partitionColumn": app_conf["mysql_conf"]["partition_column"],
        "user": app_secret["mysql_conf"]["username"],
        "password": app_secret["mysql_conf"]["password"]
    }
