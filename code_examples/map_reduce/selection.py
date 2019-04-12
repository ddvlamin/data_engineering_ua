#!/usr/bin/python


from pyspark import SparkContext
from pyspark.sql import SparkSession

def mapper(tuple_person):
  """
  Only return female tuples
  """
  if tuple_person[2] == 'F':
    return (tuple_person, tuple_person)
  else:
    return (None, None)

def reducer(tpl):
  key, records = tpl
  return records

def flatmapper(tuple_person):
  if tuple_person[2] == 'F':
    return [tuple_person]
  else:
    return []

def postprocessing_mapper(tpl):
  """
  Removes key and returns the full original tuple
  """
  return tpl[0]

def female_filter(tuple_person):
  if tuple_person[2] == 'F':
    return True
  else:
    return False

if __name__ == "__main__":
  spark = SparkSession.builder.appName("selection").getOrCreate()

  rdd = spark.sparkContext.parallelize([
        ('Tom', 'Antwerpen', 'M'),                  
        ('Els', 'Antwerpen', 'F'),                  
        ('Els', 'Antwerpen', 'F'),
        ('Kevin', 'Gent', 'M'),
        ('An', 'Hasselt', 'F')])

  rdd2 = rdd.map(mapper)

  print "records after execution of mapper"
  for tpl in rdd2.collect():
    print(tpl)
  print "-----------------------"

  rdd3 = rdd2.groupByKey().mapValues(lambda x: list(x)).flatMap(reducer) #transform Spark's ResultIterable value returned by groupByKey into a list (to simulated reduce function with key and list as input arguments)

  print "records after execution of reducer"
  for tpl in rdd3.collect():
    print tpl
  print "-----------------------"
  
  print "records after executing just the flatMap"
  for tpl in rdd.flatMap(flatmapper).collect():
    print tpl
  print "-----------------------"

  print "records after executing filter"
  for tpl in rdd.filter(female_filter).collect():
    print tpl
  print "-----------------------"

