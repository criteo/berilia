# Create and run some tests on S3 Bucket

<pre>

# create a S3 bucket
$ dev-cluster create-bucket demo
...
11:44:53.994 [main] c.c.d.c.aws.CreateBucketCliAction$ - Created bucket berilia-dev-cluster-demo
$ dev-cluster list-bucket
...
berilia-dev-cluster-demo


# Copy over a large amount of test data (two days worth)
$ dev-cluster copy-bucket berilia-dev-cluster-demo -Psample-2
...
12:16:55.912 [Thread-30] com.criteo.dev.cluster.SshAction$ - 16/09/28 19:16:53 INFO mapreduce.Job: Job job_1473932492402_2237191 completed successfully
12:16:55.982 [Thread-30] com.criteo.dev.cluster.SshAction$ - 16/09/28 19:16:53 INFO mapreduce.Job: Counters: 38
12:16:55.982 [Thread-30] com.criteo.dev.cluster.SshAction$ - 	File System Counters
12:16:55.982 [Thread-30] com.criteo.dev.cluster.SshAction$ - 		FILE: Number of bytes read=0
12:16:55.982 [Thread-30] com.criteo.dev.cluster.SshAction$ - 		FILE: Number of bytes written=3433973
12:16:55.982 [Thread-30] com.criteo.dev.cluster.SshAction$ - 		FILE: Number of read operations=0
12:16:55.982 [Thread-30] com.criteo.dev.cluster.SshAction$ - 		FILE: Number of large read operations=0
12:16:55.982 [Thread-30] com.criteo.dev.cluster.SshAction$ - 		FILE: Number of write operations=0
12:16:55.982 [Thread-30] com.criteo.dev.cluster.SshAction$ - 		HDFS: Number of bytes read=8523989095
12:16:55.982 [Thread-30] com.criteo.dev.cluster.SshAction$ - 		HDFS: Number of bytes written=0
12:16:55.982 [Thread-30] com.criteo.dev.cluster.SshAction$ - 		HDFS: Number of read operations=7896
12:16:55.982 [Thread-30] com.criteo.dev.cluster.SshAction$ - 		HDFS: Number of large read operations=0
12:16:55.982 [Thread-30] com.criteo.dev.cluster.SshAction$ - 		HDFS: Number of write operations=42
12:16:55.982 [Thread-30] com.criteo.dev.cluster.SshAction$ - 		S3A: Number of bytes read=0
12:16:55.982 [Thread-30] com.criteo.dev.cluster.SshAction$ - 		S3A: Number of bytes written=8523268237
12:16:55.982 [Thread-30] com.criteo.dev.cluster.SshAction$ - 		S3A: Number of read operations=55439
12:16:55.982 [Thread-30] com.criteo.dev.cluster.SshAction$ - 		S3A: Number of large read operations=0
12:16:55.982 [Thread-30] com.criteo.dev.cluster.SshAction$ - 		S3A: Number of write operations=7793
12:16:55.982 [Thread-30] com.criteo.dev.cluster.SshAction$ - 	Job Counters
12:16:55.982 [Thread-30] com.criteo.dev.cluster.SshAction$ - 		Launched map tasks=21
12:16:55.982 [Thread-30] com.criteo.dev.cluster.SshAction$ - 		Other local map tasks=21
12:16:55.982 [Thread-30] com.criteo.dev.cluster.SshAction$ - 		Total time spent by all maps in occupied slots (ms)=18473590
12:16:55.982 [Thread-30] com.criteo.dev.cluster.SshAction$ - 		Total time spent by all reduces in occupied slots (ms)=0
12:16:55.982 [Thread-30] com.criteo.dev.cluster.SshAction$ - 		Total time spent by all map tasks (ms)=9236795
12:16:55.982 [Thread-30] com.criteo.dev.cluster.SshAction$ - 		Total vcore-seconds taken by all map tasks=18473590
12:16:55.982 [Thread-30] com.criteo.dev.cluster.SshAction$ - 		Total megabyte-seconds taken by all map tasks=9458478080
12:16:55.982 [Thread-30] com.criteo.dev.cluster.SshAction$ - 	Map-Reduce Framework
12:16:55.982 [Thread-30] com.criteo.dev.cluster.SshAction$ - 		Map input records=3219
12:16:55.982 [Thread-30] com.criteo.dev.cluster.SshAction$ - 		Map output records=0
12:16:55.982 [Thread-30] com.criteo.dev.cluster.SshAction$ - 		Input split bytes=2646
12:16:55.982 [Thread-30] com.criteo.dev.cluster.SshAction$ - 		Spilled Records=0
12:16:55.982 [Thread-30] com.criteo.dev.cluster.SshAction$ - 		Failed Shuffles=0
12:16:55.982 [Thread-30] com.criteo.dev.cluster.SshAction$ - 		Merged Map outputs=0
12:16:55.982 [Thread-30] com.criteo.dev.cluster.SshAction$ - 		GC time elapsed (ms)=41904
12:16:55.982 [Thread-30] com.criteo.dev.cluster.SshAction$ - 		CPU time spent (ms)=1622570
12:16:55.982 [Thread-30] com.criteo.dev.cluster.SshAction$ - 		Physical memory (bytes) snapshot=17864019968
12:16:55.982 [Thread-30] com.criteo.dev.cluster.SshAction$ - 		Virtual memory (bytes) snapshot=86573834240
12:16:55.982 [Thread-30] com.criteo.dev.cluster.SshAction$ - 		Total committed heap usage (bytes)=42618847232
12:16:55.983 [Thread-30] com.criteo.dev.cluster.SshAction$ - 	File Input Format Counters
12:16:55.983 [Thread-30] com.criteo.dev.cluster.SshAction$ - 		Bytes Read=718212
12:16:55.983 [Thread-30] com.criteo.dev.cluster.SshAction$ - 	File Output Format Counters
12:16:55.983 [Thread-30] com.criteo.dev.cluster.SshAction$ - 		Bytes Written=0
12:16:55.983 [Thread-30] com.criteo.dev.cluster.SshAction$ - 	org.apache.hadoop.tools.mapred.CopyMapper$Counter
12:16:55.983 [Thread-30] com.criteo.dev.cluster.SshAction$ - 		BYTESCOPIED=8523268237
12:16:55.983 [Thread-30] com.criteo.dev.cluster.SshAction$ - 		BYTESEXPECTED=8523268237
12:16:55.983 [Thread-30] com.criteo.dev.cluster.SshAction$ - 		COPY=3219
...
$ dev-cluster describe-bucket criteo-dev-cluster-demo
Hive tables copied by {user} at 28/Sep/2016 19:17:32 (UTC)

 CREATE EXTERNAL TABLE IF NOT EXISTS `mydb.bigtable` ...
alter table bigtable add if not exists partition (day='2016-09-27', hour='00')
alter table bigtable add if not exists partition (day='2016-09-27', hour='01')
alter table bigtable add if not exists partition (day='2016-09-27', hour='02')
alter table bigtable add if not exists partition (day='2016-09-27', hour='03')
alter table bigtable add if not exists partition (day='2016-09-27', hour='04')
alter table bigtable add if not exists partition (day='2016-09-27', hour='05')
alter table bigtable add if not exists partition (day='2016-09-27', hour='06')
alter table bigtable add if not exists partition (day='2016-09-27', hour='07')
alter table bigtable add if not exists partition (day='2016-09-27', hour='08')
alter table bigtable add if not exists partition (day='2016-09-27', hour='09')
alter table bigtable add if not exists partition (day='2016-09-27', hour='10')
alter table bigtable add if not exists partition (day='2016-09-27', hour='11')
alter table bigtable add if not exists partition (day='2016-09-27', hour='12')
alter table bigtable add if not exists partition (day='2016-09-27', hour='13')
alter table bigtable add if not exists partition (day='2016-09-27', hour='14')
alter table bigtable add if not exists partition (day='2016-09-27', hour='15')
alter table bigtable add if not exists partition (day='2016-09-27', hour='16')
alter table bigtable add if not exists partition (day='2016-09-27', hour='17')
alter table bigtable add if not exists partition (day='2016-09-27', hour='18')
alter table bigtable add if not exists partition (day='2016-09-27', hour='19')
alter table bigtable add if not exists partition (day='2016-09-27', hour='20')
alter table bigtable add if not exists partition (day='2016-09-27', hour='21')
alter table bigtable add if not exists partition (day='2016-09-27', hour='22')
alter table bigtable add if not exists partition (day='2016-09-27', hour='23')
alter table bigtable add if not exists partition (day='2016-09-28', hour='00')
alter table bigtable add if not exists partition (day='2016-09-28', hour='01')
alter table bigtable add if not exists partition (day='2016-09-28', hour='02')
alter table bigtable add if not exists partition (day='2016-09-28', hour='03')
alter table bigtable add if not exists partition (day='2016-09-28', hour='04')
alter table bigtable add if not exists partition (day='2016-09-28', hour='05')
alter table bigtable add if not exists partition (day='2016-09-28', hour='06')
alter table bigtable add if not exists partition (day='2016-09-28', hour='07')
alter table bigtable add if not exists partition (day='2016-09-28', hour='08')
alter table bigtable add if not exists partition (day='2016-09-28', hour='09')
alter table bigtable add if not exists partition (day='2016-09-28', hour='10')
alter table bigtable add if not exists partition (day='2016-09-28', hour='11')
alter table bigtable add if not exists partition (day='2016-09-28', hour='12')
alter table bigtable add if not exists partition (day='2016-09-28', hour='13')
alter table bigtable add if not exists partition (day='2016-09-28', hour='14')
alter table bigtable add if not exists partition (day='2016-09-28', hour='15')
alter table bigtable add if not exists partition (day='2016-09-28', hour='16')


# We happen to have an AWS cluster lying around, use it for testing this data
$ dev-cluster list-aws
...
Cluster [i-797c91f6], size = 4 node(s)
Master Instance id: [i-797c91f6], state: RUNNING, hostName: [dev-host], ip: [52.211.37.229], created: 28/Sep/2016 21:00:37 (UTC), expires: 01/Oct/2016 21:00:34 (UTC)
SSH command:        ssh -i ./conf/devcluster_eu.pem ubuntu@52.211.37.229
NodeManager:        52.211.37.229:8042
ResourceManager:    52.211.37.229:8088
HistoryServer:      52.211.37.229:19888
NameNode:           52.211.37.229:50070
DataNode:           52.211.37.229:50075
	Slave Instance id: [i-537f92dc], state: RUNNING, hostName: [dev-slave-0], ip: [52.210.254.69]
	SSH command:        ssh -i ./conf/devcluster_eu.pem ubuntu@52.210.254.69
	NodeManager:        52.210.254.69:8042
	DataNode:           52.210.254.69:50075

	Slave Instance id: [i-a67e9329], state: RUNNING, hostName: [dev-slave-1], ip: [52.210.198.241]
	SSH command:        ssh -i ./conf/devcluster_eu.pem ubuntu@52.210.198.241
	NodeManager:        52.210.198.241:8042
	DataNode:           52.210.198.241:50075

	Slave Instance id: [i-527f92dd], state: RUNNING, hostName: [dev-slave-2], ip: [52.211.1.104]
	SSH command:        ssh -i ./conf/devcluster_eu.pem ubuntu@52.211.1.104
	NodeManager:        52.211.1.104:8042
	DataNode:           52.211.1.104:50075

# Point the AWS cluster to the data/metadata in S3 cluster
$ dev-cluster attach-bucket-aws berilia-dev-cluster-demo i-797c91f6
...

# Login and verify the data (notice it is pointing to S3 bucket).
$ ssh -i ./conf/devcluster_eu.pem ubuntu@52.211.37.229
  Welcome to Ubuntu 14.04.4 LTS (GNU/Linux 3.13.0-92-generic x86_64)

ubuntu@ip-10-0-71-23:~$ hive

Logging initialized using configuration in file:/etc/hive/conf.dist/hive-log4j.properties
WARNING: Hive CLI is deprecated and migration to Beeline is recommended.
hive> describe formatted mydb.bigtable;
OK
# col_name            	data_type           	comment
...
# Partition Information
# col_name            	data_type           	comment

day                 	string
hour                	int

# Detailed Table Information
Database:           	mydb
Owner:              	ubuntu
CreateTime:         	Wed Sep 28 21:10:04 UTC 2016
LastAccessTime:     	UNKNOWN
Protect Mode:       	None
Retention:          	0
Location:           	s3a://berilia-dev-cluster-demo/user/myuser/mydb/bigtable
Table Type:         	EXTERNAL_TABLE
Table Parameters:
EXTERNAL            	TRUE
transient_lastDdlTime	1475097004

...
Time taken: 0.283 seconds, Fetched: 109 row(s)
hive> show partitions mydb.bigtable;
OK
day=2016-09-27/hour=00
day=2016-09-27/hour=01
day=2016-09-27/hour=02
day=2016-09-27/hour=03
day=2016-09-27/hour=04
day=2016-09-27/hour=05
day=2016-09-27/hour=06
day=2016-09-27/hour=07
day=2016-09-27/hour=08
day=2016-09-27/hour=09
day=2016-09-27/hour=10
day=2016-09-27/hour=11
day=2016-09-27/hour=12
day=2016-09-27/hour=13
day=2016-09-27/hour=14
day=2016-09-27/hour=15
day=2016-09-27/hour=16
day=2016-09-27/hour=17
day=2016-09-27/hour=18
day=2016-09-27/hour=19
day=2016-09-27/hour=20
day=2016-09-27/hour=21
day=2016-09-27/hour=22
day=2016-09-27/hour=23
day=2016-09-28/hour=00
day=2016-09-28/hour=01
day=2016-09-28/hour=02
day=2016-09-28/hour=03
day=2016-09-28/hour=04
day=2016-09-28/hour=05
day=2016-09-28/hour=06
day=2016-09-28/hour=07
day=2016-09-28/hour=08
day=2016-09-28/hour=09
day=2016-09-28/hour=10
day=2016-09-28/hour=11
day=2016-09-28/hour=12
day=2016-09-28/hour=13
day=2016-09-28/hour=14
day=2016-09-28/hour=15
day=2016-09-28/hour=16
Time taken: 0.075 seconds, Fetched: 41 row(s)
hive> select count(*) from glup.bigtable where day="2016-09-28" and hour=16;
MapReduce Total cumulative CPU time: 5 minutes 59 seconds 590 msec
Ended Job = job_1475096774204_0001
MapReduce Jobs Launched:
Stage-Stage-1: Map: 38  Reduce: 1   Cumulative CPU: 359.59 sec   HDFS Read: 615922 HDFS Write: 7 SUCCESS
Total MapReduce CPU Time Spent: 5 minutes 59 seconds 590 msec
OK
955939
Time taken: 67.027 seconds, Fetched: 1 row(s)
hive> exit;
ubuntu@ip-10-0-71-23:~$ exit
logout
Connection to 52.211.37.229 closed.

# Destroy the bucket
$ dev-cluster destroy-bucket berilia-dev-cluster-demo

# Destroy cluster (which now points to non-existent cluster).
$ dev-cluster destroy-aws i-797c91f6

</pre>

### Minimum Configuration

Configiure ./conf/target-aws.xml

<pre>
target.aws.access.id
target.aws.access.key
</pre>

Configure ./conf/source.xml

<pre>
source.address=/url/of/hadoop/access/node

In profile "sample-2"
source.tables=mydb.bigtable
default.distcp.top.partition.count=2
</pre>