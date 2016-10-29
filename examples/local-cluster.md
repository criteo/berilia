# Create and run some tests on local-docker cluster
<pre>
$ dev cluster $ dev-cluster create-local
...
Instance id: [75de8f1f3699], State: [Running], Created: [2016-09-27 16:40:20 -0700 PDT]
SSH command:        ssh -o StrictHostKeyChecking=no root@192.168.99.100 -p 32797
Ports:
NodeManager:        192.168.99.100:32796
ResourceManager:    192.168.99.100:32795
HistoryServer:      192.168.99.100:32794
NameNode:           192.168.99.100:32793
DataNode:           192.168.99.100:32792

# List cluster, will display info of all local clusters.
$ dev-cluster list-local
…
Instance id: [75de8f1f3699], State: [Running], Created: [2016-09-27 16:40:20 -0700 PDT]
SSH command:        ssh -o StrictHostKeyChecking=no root@192.168.99.100 -p 32797
Ports:
NodeManager:        192.168.99.100:32796
ResourceManager:    192.168.99.100:32795
HistoryServer:      192.168.99.100:32794
NameNode:           192.168.99.100:32793
DataNode:           192.168.99.100:32792

# Copy configured Hive table from configured prod cluster to local cluster.
$ kinit -f
{user.name}@ORG.LAN's Password:

$ dev-cluster copy-local 75de8f1f3699 -Psample-1
...

# Run a simple query on the sample data
$ ssh -o StrictHostKeyChecking=no root@192.168.99.100 -p 32797
Welcome to Ubuntu 14.04.4 LTS (GNU/Linux 4.4.12-boot2docker x86_64)

 * Documentation:  https://help.ubuntu.com/
root@75de8f1f3699:~# hive
Logging initialized using configuration in file:/etc/hive/conf.dist/hive-log4j.properties
WARNING: Hive CLI is deprecated and migration to Beeline is recommended.
hive> use mydb;
OK
Time taken: 0.448 seconds
hive> show tables;
OK
mytable
Time taken: 0.257 seconds, Fetched: 1 row(s)
hive> show partitions mytable;
OK
day=2016-08-04/hour=04/host_platform=CN
day=2016-08-04/hour=04/host_platform=EU
Time taken: 0.213 seconds, Fetched: 2 row(s)
hive> select count(*) from mytable where hour=04 and host_platform='EU';
...
Stage-Stage-1: Map: 1  Reduce: 1   Cumulative CPU: 3.41 sec   HDFS Read: 1379762 HDFS Write: 7 SUCCESS
Total MapReduce CPU Time Spent: 3 seconds 410 msec
OK
129297
Time taken: 30.152 seconds, Fetched: 1 row(s)
hive> exit;
root@75de8f1f3699:~# exit
logout
Connection to 192.168.99.100 closed.

# Stop the local-docker dev-cluster
$ dev-cluster stop-local 75de8f1f3699
...

$ dev-cluster list-local
...
Instance id: [75de8f1f3699], State: [Stopped], Created: [2016-09-27 16:40:20 -0700 PDT]

# Restart it, it will have new ports.  Check the data is still there.
Instance id: [75de8f1f3699], State: [Running], Created: [2016-09-27 16:40:20 -0700 PDT]
SSH command:        ssh -o StrictHostKeyChecking=no root@192.168.99.100 -p 32803
Ports:
NodeManager:        192.168.99.100:32802
ResourceManager:    192.168.99.100:32801
HistoryServer:      192.168.99.100:32800
NameNode:           192.168.99.100:32799
DataNode:           192.168.99.100:32798


$ ssh -o StrictHostKeyChecking=no root@192.168.99.100 -p 32803
Welcome to Ubuntu 14.04.4 LTS (GNU/Linux 4.4.12-boot2docker x86_64)

 * Documentation:  https://help.ubuntu.com/
Last login: Tue Sep 27 23:46:43 2016 from 192.168.99.1
root@75de8f1f3699:~# hive
Logging initialized using configuration in file:/etc/hive/conf.dist/hive-log4j.properties
WARNING: Hive CLI is deprecated and migration to Beeline is recommended.
hive> show partitions mydb.mytable;
OK
day=2016-08-04/hour=04/host_platform=CN
day=2016-08-04/hour=04/host_platform=EU
Time taken: 0.905 seconds, Fetched: 2 row(s)
hive> exit;
root@75de8f1f3699:~# exit;
logout
Connection to 192.168.99.100 closed.


# Remove docker container
$ dev-cluster destroy-local 75de8f1f3699
...
$ dev-cluster list-local
...
</pre>

### Minumum Configuration

Configure ./conf/source.xml

<pre>
source.address=/url/of/hadoop/access/node

In profile "sample-1"
source.tables=mydb.mytable
default.partition.count=2
</pre>