#!/usr/bin/python

from pyspark import SparkContext
from pyspark.sql import SparkSession

def mapper(tuple_person):
  """
  Only return City and Gender
  """
  tpl = (tuple_person[2], tuple_person[3])
  return (tpl, tpl)

def reducer(tpl):
  key, tpl_list = tpl
  return key

if __name__ == "__main__":
  spark = SparkSession.builder.appName("selection").getOrCreate()

  rdd = spark.sparkContext.parallelize([
        (1,'Tom', 'Antwerpen', 'M'),                  
        (2,'Els', 'Antwerpen', 'F'),
        (3,'Joe', 'Antwerpen', 'M'),
        (4,'Kevin', 'Gent', 'M'),
        (5,'An', 'Hasselt', 'F')])

  rdd2 = rdd.map(mapper)

  print "tuples after exection of first mapper"
  for tpl in rdd2.collect():
    print(tpl)
  print "-----------------------"

  rdd3 = rdd2.groupByKey().mapValues(lambda x: list(x)).map(reducer)

  print "tuples after exection of reducer"
  for tpl in rdd3.collect():
    print tpl
  print "-----------------------"

