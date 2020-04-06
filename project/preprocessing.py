"""
Storing GDELT mentions

Example use:

python ./preprocessing.py corona --events /home/ubuntu/data/project/gdelt/sample/english/events/ /home/ubuntu/data/project/gdelt/sample/multilingual/events/ --mentions /home/ubuntu/data/project/gdelt/sample/english/mentions/ /home/ubuntu/data/project/gdelt/sample/multilingual/mentions/ --regex "(?=.*[ck]orona)(?=.*virus)|[Cc][Oo][Vv][Ii][Dd]-?19" --dbHost localhost --dbPort 3306 --dbUser root --dbPassword testtest --dbName GDELT --dbTable GeoEvents --fips /home/ubuntu/data/project/gdelt/fips2iso_country_codes.tsv
"""

import argparse
import sys
from datetime import datetime
from functools import partial
import re
import os
import json

import mysql.connector
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

tab_split_func = lambda input_line: input_line.split("\t")

def transform_to_json(record, index2column):
  return_value = dict()
  for index, column_name in index2column.items():
      return_value[column_name] = record[index]
  return return_value

def convert_dtstr_dt(dtstr):
  format_string = "%Y%m%d%H%M%S"
  dt = datetime.strptime(dtstr, format_string)
  return dt

def convert_to_float(floatstr):
  try:
      return float(floatstr)
  except ValueError:
      return None

type_converters = {
    "DateAdded": convert_dtstr_dt,
    "lat": convert_to_float,
    "long": convert_to_float
}

def convert_types(record, converters):
  for col_name, convert_func in converters.items():
      record[col_name] = convert_func(record[col_name])
  return record

def matches_regex(record, compiled_regex):
  url = record["url"]
  return compiled_regex.search(url) != None

def add_mentions(tpl):
  event_id, (event_record, number_of_mentions) = tpl
  event_record["NumberOfMentions"] = number_of_mentions
  return event_record

def convert_fips2iso(record):
  record["Actor1Geo_CountryCode"] = broadcast_fips2iso.value.get(record["Actor1Geo_CountryCode"],
                                                                 record["Actor1Geo_CountryCode"])
  return record

def connection_factory(user, password, host, database):
  cnx = mysql.connector.connect(
                  user=user, 
                  password=password,
                  host=host,
                  database=database
  )
  cursor = cnx.cursor()
  return cnx, cursor

def store_records(records, connection_factory, db_table):
  cnx, cursor = connection_factory()
  
  insert_statement_str = f"insert into {db_table} (EventName,DateAdded,CountryCode,NumberOfEvents,NumberOfMentions) VALUES (%s, %s, %s, %s, %s);"
  
  record_list = list()
  for record in records:
    record_list.append((
        record["EventName"],
        record["DateAdded"],
        record["CountryCode"],
        record["NumberOfEvents"],
        record["NumberOfMentions"]
    ))
      
  cursor.executemany(insert_statement_str, record_list)
  
  cnx.commit()
  cnx.close()
  
def load_fips2iso(filepath):
  with open(filepath,"r") as fin:
      fips2iso = dict()
      for line in fin:
          _, fips_code, iso_code = line.strip().split("\t")
          fips2iso[fips_code] = iso_code
  return fips2iso

if __name__ == "__main__":

  parser = argparse.ArgumentParser(description=__doc__)
  parser.add_argument("event_name", help="name of the event")
  parser.add_argument("--events", nargs="*", help="input folders for events")
  parser.add_argument("--mentions", nargs="*", help="input folders for mentions")
  parser.add_argument("--regex", help="regex", default="(?=.*[ck]orona)(?=.*virus)|[Cc][Oo][Vv][Ii][Dd]-?19")
  parser.add_argument("--dbHost", default="localhost")
  parser.add_argument("--dbPort", default=3306)
  parser.add_argument("--dbUser", default="root")
  parser.add_argument("--dbName", default="Corona")
  parser.add_argument("--dbTable", default="GeoEventMentions")
  parser.add_argument("--dbPassword")
  parser.add_argument("--fips", help="path to fips2iso code file")
  args = parser.parse_args(sys.argv[1:])

  event_name = args.event_name
  dbHost = args.dbHost
  dbPort = args.dbPort
  dbUser = args.dbUser
  dbPassword = args.dbPassword
  dbName = args.dbName
  dbTable = args.dbTable
  events = args.events
  mentions = args.mentions
  regex = re.compile(args.regex)
  fips = args.fips

  sc = SparkContext()

  events_rdd = sc.textFile(",".join(events))
  mentions_rdd = sc.textFile(",".join(mentions))

  #
  # process events
  #

  events_split_rdd = events_rdd.map(tab_split_func)
  events_json_rdd = events_split_rdd.map(partial(transform_to_json, index2column=index2events_column))
  events_converted_rdd = events_json_rdd.map(partial(convert_types, converters=type_converters))
  corona_events_rdd = events_converted_rdd.filter(partial(matches_regex,compiled_regex=regex))
  corona_forjoin_rdd = corona_events_rdd.map(lambda record: (record["GlobalEventId"], record))

  #
  # process mentions
  #

  mentions_count_rdd = (mentions_rdd
       .map(tab_split_func)
       .map(partial(transform_to_json, index2column=index2mentions_column))
       .map(lambda json_record: (json_record["GlobalEventId"],1))
       .reduceByKey(lambda x, y: x+y)
  )

  joined_rdd = corona_forjoin_rdd.join(mentions_count_rdd)
  event_mentions_rdd = joined_rdd.map(add_mentions)

  fips2iso = load_fips2iso(fips)
  broadcast_fips2iso = sc.broadcast(fips2iso)
  event_mentions_iso_rdd = event_mentions_rdd.map(convert_fips2iso) 

  #
  # group by country code and date
  #

  country_date_count_rdd = (event_mentions_iso_rdd
      .map(lambda x: (
            (x["Actor1Geo_CountryCode"], x["DateAdded"].date()), 
            (1, x["NumberOfMentions"])
          )
      )
      .reduceByKey(lambda x, y: (x[0]+y[0],x[1]+y[1]))
      .map(lambda x:
        {
          "EventName": event_name,
          "DateAdded": x[0][1],
          "CountryCode": x[0][0],
          "NumberOfEvents": x[1][0],
          "NumberOfMentions": x[1][1]
        }
      )
      .filter(lambda x: x["CountryCode"])
  )

  connection_factory = partial(connection_factory, 
                               user=dbUser, 
                               password=dbPassword, 
                               host=dbHost,
                               database=dbName
                      )
  country_date_count_rdd.foreachPartition(partial(store_records, connection_factory=connection_factory, db_table=dbTable))

  sc.stop()
