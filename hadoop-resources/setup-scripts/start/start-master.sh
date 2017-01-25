#!/bin/bash

#This is used to start a master of dev clusters of both AWS and docker

#Start HDFS and YARN
sudo service hadoop-hdfs-namenode restart
sudo service hadoop-hdfs-datanode restart
sudo service hadoop-hdfs-secondarynamenode restart
sudo service hadoop-yarn-resourcemanager restart
sudo service hadoop-yarn-nodemanager restart

#Setup the staging dir (which is /user)
#This is defined by yarn.app.mapreduce.am.staging-dir
#and follows instruction of setting up YARN (MR2) CDH5 staging dir.

#Why do this here instead of a dockerFile?
#When docker instance-layer gets built via a command in dockerFile,
#They dont retain non-transient state (like processes), hence all services are down including HDFS
sudo -u hdfs hdfs dfs -chmod 777 /
sudo -u hdfs hdfs dfs -mkdir -p /user
sudo -u hdfs hdfs dfs -chmod 777 /user
sudo -u hdfs hadoop fs -mkdir -p /user/history
sudo -u hdfs hadoop fs -chmod -R 1777 /user/history
sudo -u hdfs hadoop fs -chown mapred:hadoop /user/history

#Following need the above directories to exist and be at those permissions.
sudo service hadoop-mapreduce-historyserver restart

#Start Hive metastore
sudo service mysql restart
sudo service hive-metastore restart

#Spark history server
sudo -u hdfs hdfs dfs -mkdir -p hdfs:///user/spark/applicationHistory
sudo -u hdfs hadoop fs -chown -R spark:spark /user/spark
sudo -u hdfs hdfs dfs -chmod 1777 hdfs:/user/spark/applicationHistory