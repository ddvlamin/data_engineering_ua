#!/usr/bin/python

from pyspark import SparkContext
from pyspark.sql import SparkSession

def to_dict_person(tpl):
  return { "PersonId": tpl[0], "Name": tpl[1], "City": tpl[2] }

def to_dict_zipcode(tpl):
  return { "City": tpl[0], "ZipCode": tpl[1] }

def mapper(record, relation):
  return (record["City"], (relation, record))

def reducer(tpl):
  city, records = tpl
  person_records = [rec for rec in records if rec[0]=="person"]
  zipcode_records = [rec for rec in records if rec[0]=="zipcode"]

  for person_rec in person_records:
    for zipcode_rec in zipcode_records:
      person_rec[1]["ZipCode"] = zipcode_rec[1]["ZipCode"]
      yield person_rec[1]

def city_as_key(record):
  return (record["City"], record)

def map_after_join(tpl):
  city, (person_record, city_record) = tpl
  person_record["ZipCode"] = city_record["ZipCode"]
  return person_record

if __name__ == "__main__":
  spark = SparkSession.builder.appName("selection").getOrCreate()

  rdd_person = spark.sparkContext.parallelize([
        (1,'Tom', 'Antwerpen'),                  
        (2,'Els', 'Antwerpen'),
        (3,'Joe', 'Antwerpen'),
        (4,'Kevin', 'Gent'),
        (5,'An', 'Hasselt')]).map(to_dict_person)
  
  rdd_zipcode = spark.sparkContext.parallelize([
        ('Antwerpen', 2000),
        ('Gent', 9000),
        ('Hasselt', 3500)]).map(to_dict_zipcode)

  rdd_person2 = rdd_person.map(lambda x: mapper(x, relation="person"))
  rdd_zipcode2 = rdd_zipcode.map(lambda x: mapper(x, relation="zipcode"))

  rdd = rdd_person2.union(rdd_zipcode2)

  print "after mapper"
  for rec in rdd.collect():
    print rec
  print "---------------------------"

  rdd = rdd.groupByKey().mapValues(lambda x: list(x)).flatMap(reducer)

  print "after reducer"
  for rec in rdd.collect():
    print rec
  print "---------------------------"

  rdd = rdd_person.map(city_as_key).leftOuterJoin(rdd_zipcode.map(city_as_key))

  print "leftOuterJoin"
  for rec in rdd.collect():
    print rec
  print "---------------------------"

  print "after join mapper"
  for rec in rdd.map(map_after_join).collect():
    print rec
