#!/bin/bash

# Works from vanilla AWS Ubuntu Trusty 14.04 image
# It is for setting up a Hadoop master.

# CDH5 minor version is passed as argument to this script.
CDH5_VERSION=$1

# Check if valid cdh5 version for this script
wget http://archive.cloudera.com/cdh5/ubuntu/trusty/amd64/cdh/dists/trusty-$CDH5_VERSION/ &> /dev/null
if [ $? -ne 0 ]
then
  echo "Invalid CDH version.  Must be valid CDH5 minor version"
  exit 1
fi


##Prevent startup of services on install
sudo touch /usr/sbin/policy-rc.d
echo "#!/bin/sh" | sudo tee -a /usr/sbin/policy-rc.d
echo "exit 101" | sudo tee -a /usr/sbin/policy-rc.d
sudo chmod a+x /usr/sbin/policy-rc.d


## From docker/base/cluster-java

sudo apt-get update
sudo apt-get install -y software-properties-common
sudo apt-get install -y awscli

sudo echo oracle-java8-installer shared/accepted-oracle-license-v1-1 select true | sudo debconf-set-selections
sudo add-apt-repository -y ppa:webupd8team/java
sudo apt-get update
sudo apt-get install -y oracle-java8-installer
sudo rm -rf /var/lib/apt/lists/*
sudo rm -rf /var/cache/oracle-jdk8-installer

# Define commonly used JAVA_HOME variable
export JAVA_HOME=/usr/lib/jvm/java-8-oracle

#install some useful tools
sudo apt-get -y install vim

#install cdh5.5
wget https://archive.cloudera.com/cdh5/ubuntu/trusty/amd64/cdh/archive.key -O archive.key
sudo apt-key add archive.key

#CDH5 not officially supported on Ubuntu 14.04, due to zookeeper conflict.
#Following steps from: http://blog.cloudera.com/blog/2014/11/guidelines-for-installing-cdh-packages-on-unsupported-operating-systems/

sudo touch /etc/apt/preferences.d/cloudera.pref
echo "Package: *" | sudo tee -a /etc/apt/preferences.d/cloudera.pref
echo "Pin: release o=Cloudera, l=Cloudera" | sudo tee -a /etc/apt/preferences.d/cloudera.pref
echo "Pin-Priority: 501" | sudo tee -a /etc/apt/preferences.d/cloudera.pref

#This section will be different if need to add support for different Ubuntu/Debian versions.
sudo wget "https://archive.cloudera.com/cdh5/ubuntu/trusty/amd64/cdh/cloudera.list" -O /etc/apt/sources.list.d/cloudera.list
sudo sed -i -e "s/trusty-cdh5/trusty-$CDH5_VERSION/g" /etc/apt/sources.list.d/cloudera.list

#install Namenode, Secondary Namenode, ResourceManager
#TODO- install some of these on one of the slave nodes.
sudo apt-get update
sudo apt-get install -y hadoop-hdfs-namenode
sudo apt-get install -y hadoop-hdfs-secondarynamenode
sudo apt-get install -y hadoop-yarn-resourcemanager
sudo apt-get install -y hadoop-mapreduce-historyserver

#install NodeManager, DataNode, MapReduce (also on slave nodes)
sudo apt-get install -y hadoop-yarn-nodemanager hadoop-hdfs-datanode

#Format namenode
sudo -u hdfs hdfs namenode -format -force

#Make HDFS directories
sudo mkdir -p /var/lib/hadoop-hdfs/cache/hdfs/dfs/name
sudo chown hdfs /var/lib/hadoop-hdfs/cache/hdfs/dfs/name
sudo mkdir -p /var/lib/hadoop-hdfs/cache/hdfs/dfs/data
sudo chown hdfs /var/lib/hadoop-hdfs/cache/hdfs/dfs/data

#Make YARN directories
sudo mkdir -p /var/lib/hadoop-yarn/cache
sudo chown yarn /var/lib/hadoop-yarn/cache
#sudo mkdir -p /var/log/hadoop-yarn/containers
#sudo chown yarn /var/log/hadoop-yarn/containers

#Install Hive (at end of this, it is only in embedded metastore db mode)
sudo apt-get -y install hive


# From docker/local-cluster/cluser-hive-metastore

sudo apt-get -y install hive-metastore

echo "mysql-server mysql-server/root_password password root" | sudo debconf-set-selections
echo "mysql-server mysql-server/root_password_again password root" | sudo debconf-set-selections
sudo apt-get -y install mysql-server

sudo apt-get -y install libmysql-java
sudo ln -s /usr/share/java/mysql-connector-java.jar /usr/lib/hive/lib/mysql-connector-java.jar

#setup hive
sudo touch create-metastore.sql
echo "CREATE DATABASE metastore;" | sudo tee --append create-metastore.sql
echo "USE metastore;" | sudo tee --append create-metastore.sql
echo "SOURCE /usr/lib/hive/scripts/metastore/upgrade/mysql/hive-schema-1.1.0.mysql.sql;" | sudo tee --append create-metastore.sql
echo "CREATE USER 'hive'@localhost IDENTIFIED BY 'hive';" | sudo tee --append create-metastore.sql
echo "GRANT ALL PRIVILEGES ON metastore.* TO 'hive'@localhost;" | sudo tee --append create-metastore.sql
echo "FLUSH PRIVILEGES;" | sudo tee --append create-metastore.sql
sudo mv create-metastore.sql /usr/lib/hive/scripts/metastore/upgrade/mysql/create-metastore.sql

#we disabled start earlier of the services.
sudo service mysql start

cd /usr/lib/hive/scripts/metastore/upgrade/mysql && mysql --user=root --password=root --protocol=tcp < "create-metastore.sql"