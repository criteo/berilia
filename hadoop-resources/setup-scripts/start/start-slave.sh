#!/bin/bash

#This is used to start a slave of dev clusters in AWS.  (Docker dev clusters have only one node)

#Start HDFS and YARN
sudo service hadoop-hdfs-datanode restart
sudo service hadoop-yarn-nodemanager restart