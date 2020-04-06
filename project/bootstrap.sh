#!/bin/bash -xe

#copy bootstrap info from s3 to local machines
aws s3 cp s3://ua-data-engineering-datasets/project/resources/fips2iso_country_codes.tsv /home/hadoop/

sudo easy_install-3.6 pip
sudo /usr/local/bin/pip3 install mysql-connector==2.2.9
