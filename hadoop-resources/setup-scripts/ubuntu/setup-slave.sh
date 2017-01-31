#!/bin/bash

# Works from vanilla AWS Ubuntu Trusty 14.04 image
# It is for setting up a Hadoop slave.

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

#CDH5.5 not officially supported on Ubuntu 14.04, due to zookeeper conflict.
#Following steps from: http://blog.cloudera.com/blog/2014/11/guidelines-for-installing-cdh-packages-on-unsupported-operating-systems/

sudo touch /etc/apt/preferences.d/cloudera.pref
echo "Package: *" | sudo tee -a /etc/apt/preferences.d/cloudera.pref
echo "Pin: release o=Cloudera, l=Cloudera" | sudo tee -a /etc/apt/preferences.d/cloudera.pref
echo "Pin-Priority: 501" | sudo tee -a /etc/apt/preferences.d/cloudera.pref

#This section will be different if need to add support for different Ubuntu/Debian versions.
sudo wget "https://archive.cloudera.com/cdh5/ubuntu/trusty/amd64/cdh/cloudera.list" -O /etc/apt/sources.list.d/cloudera.list
sudo sed -i -e "s/trusty-cdh5/trusty-$CDH5_VERSION/g" /etc/apt/sources.list.d/cloudera.list

#install NodeManager, DataNode
sudo apt-get update
sudo apt-get install -y hadoop-yarn-nodemanager hadoop-hdfs-datanode

#Prepare disks (if configured in aws)
if [ -d "/data" ]; then sudo chown -R hdfs /data; fi

#Make YARN directories
sudo mkdir -p /var/lib/hadoop-yarn/cache
sudo chown yarn /var//lib/hadoop-yarn/cache
#sudo mkdir -p /log/hadoop-yarn/containers
#sudo chown yarn /log/hadoop-yarn/containers