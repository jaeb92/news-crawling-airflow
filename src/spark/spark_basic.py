import yaml
from pyspark.sql import SparkSession

with open('db/config.yaml', 'r') as f:
    dbconfig = yaml.load(f, Loader=yaml.FullLoader)

spark = SparkSession.builder \
    .appName("spark") \
    .config("spark.jars", "/opt/airflow/postgresql-42.6.0.jar") \
    .getOrCreate()

sql = """ select * from news order by date desc """

news = spark.read.format("jdbc") \
    .option("url", f"jdbc:postgresql://{dbconfig['host']}:{dbconfig['port']}/{dbconfig['database']}") \
    .option("driver", dbconfig['driver']) \
    .option("query", sql) \
    .option("user", dbconfig['user']) \
    .option("password", dbconfig['password']) \
    .load()
    
print(news.show())