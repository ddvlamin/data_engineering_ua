import os

from pyspark import SparkContext

index2events_column = {
  0: "GlobalEventId",
  37: "Actor1Geo_CountryCode",
  60: "url",
  40: "lat",
  41: "long",
  59: "DateAdded"
}

index2mentions_column = {
  0: "GlobalEventId",
  3: "MentionType",
  5: "MentionIdentifier",
  1: "EventTimeDate",
  2: "MentionTimeDate"
}

sc = SparkContext("GDELT")

basepath = "/home/ubuntu/data/project/gdelt"
mentions_path_english = os.path.join(basepath,"english/mentions/")
mentions_path_multi = os.path.join(basepath,"multilinugal/mentions/")
events_path_english = os.path.join(basepath,"english/events/")
events_path_multi = os.path.join(basepath,"multilingual/events/")

mentions_english_rdd = sc.textFiles(mentions_path_english)
mentions_multi_rdd = sc.textFiles(mentions_path_multi)
mentions_rdd = mentions_english_rdd.union(mentions_multi_rdd)
