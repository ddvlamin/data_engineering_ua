#!/usr/bin/python


from pyspark import SparkContext
from pyspark.sql import SparkSession


if __name__ == "__main__":
  spark = SparkSession.builder.appName("selection").getOrCreate()

  rdd = spark.sparkContext.parallelize([
        (1,'Tom', 'Antwerpen', 'M'),                  
        (2,'Els', 'Antwerpen', 'F'),
        (3,'Kevin', 'Gent', 'M'),
        (4,'An', 'Hasselt', 'F')])
