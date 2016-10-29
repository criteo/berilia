# Create and run some tests on 4 node AWS cluster
<pre>

$ dev-cluster create-aws 4
...
17:11:36.471 [main] c.c.d.c.aws.CreateClusterAction$ - Creating 4 node(s) in parallel.
...
Cluster [i-911bf91e], size = 4 node(s)
Master Instance id: [i-911bf91e], state: RUNNING, hostName: [dev-host], ip: [52.211.216.167], created: 28/Sep/2016 00:11:39 (UTC), expires: 01/Oct/2016 00:11:37 (UTC)
SSH command:        ssh -i ./conf/devcluster_eu.pem ubuntu@52.211.216.167
NodeManager:        52.211.216.167:8042
ResourceManager:    52.211.216.167:8088
HistoryServer:      52.211.216.167:19888
NameNode:           52.211.216.167:50070
DataNode:           52.211.216.167:50075
	Slave Instance id: [i-b61bf939], state: RUNNING, hostName: [dev-slave-0], ip: [52.211.216.226]
	SSH command:        ssh -i ./conf/devcluster_eu.pem ubuntu@52.211.216.226
	NodeManager:        52.211.216.226:8042
	DataNode:           52.211.216.226:50075

	Slave Instance id: [i-931bf91c], state: RUNNING, hostName: [dev-slave-1], ip: [52.211.216.240]
	SSH command:        ssh -i ./conf/devcluster_eu.pem ubuntu@52.211.216.240
	NodeManager:        52.211.216.240:8042
	DataNode:           52.211.216.240:50075

	Slave Instance id: [i-921bf91d], state: RUNNING, hostName: [dev-slave-2], ip: [52.16.232.193]
	SSH command:        ssh -i ./conf/devcluster_eu.pem ubuntu@52.16.232.193
	NodeManager:        52.16.232.193:8042
	DataNode:           52.16.232.193:50075



# Copy sample data
$ dev-cluster copy-aws i-911bf91e -Psample-1

# SSH into cluster master (where Hive is installed).  Run test queries
$ ssh -i ./conf/devcluster_eu.pem ubuntu@52.211.216.167
Welcome to Ubuntu 14.04.4 LTS (GNU/Linux 3.13.0-92-generic x86_64)
...

ubuntu@ip-10-0-31-169:~$ hive
Logging initialized using configuration in file:/etc/hive/conf.dist/hive-log4j.properties
WARNING: Hive CLI is deprecated and migration to Beeline is recommended.
hive> show partitions mydb.mytable;
OK
day=2016-08-04/hour=04/host_platform=CN
day=2016-08-04/hour=04/host_platform=EU
Time taken: 0.091 seconds, Fetched: 2 row(s)
hive> select count(*) from mydb.mytable where hour=04 and host_platform="EU";
...
Stage-Stage-1: Map: 1  Reduce: 1   Cumulative CPU: 4.41 sec   HDFS Read: 1379655 HDFS Write: 7 SUCCESS
Total MapReduce CPU Time Spent: 4 seconds 410 msec
OK
129297
Time taken: 25.645 seconds, Fetched: 1 row(s)
hive> exit;
ubuntu@ip-10-0-31-169:~$ exit
logout
Connection to 52.211.216.167 closed.

# Extend the expiry time to prevent instance from being purged.  Test that it cannot be extended too much.
$ dev-cluster extend-aws i-911bf91e
...
17:49:42.314 [main] c.c.d.c.aws.ExtendAwsCliAction$ - Updating expiration time for master i-911bf91e
17:49:42.314 [main] c.c.d.c.aws.ExtendAwsCliAction$ - Current expiration time is : 01/Oct/2016 00:11:37 (UTC)
17:49:42.405 [main] c.c.d.c.aws.ExtendAwsCliAction$ - Updating node to new expiration time : 04/Oct/2016 00:11:37 (UTC)
17:49:43.417 [main] c.c.d.c.aws.ExtendAwsCliAction$ - Updating expiration time for slave i-b61bf939
17:49:43.417 [main] c.c.d.c.aws.ExtendAwsCliAction$ - Current expiration time is : 01/Oct/2016 00:11:37 (UTC)
17:49:43.417 [main] c.c.d.c.aws.ExtendAwsCliAction$ - Updating node to new expiration time : 04/Oct/2016 00:11:37 (UTC)
17:49:44.205 [main] c.c.d.c.aws.ExtendAwsCliAction$ - Updating expiration time for slave i-921bf91d
17:49:44.206 [main] c.c.d.c.aws.ExtendAwsCliAction$ - Current expiration time is : 01/Oct/2016 00:11:37 (UTC)
17:49:44.206 [main] c.c.d.c.aws.ExtendAwsCliAction$ - Updating node to new expiration time : 04/Oct/2016 00:11:37 (UTC)
17:49:45.065 [main] c.c.d.c.aws.ExtendAwsCliAction$ - Updating expiration time for slave i-931bf91c
17:49:45.066 [main] c.c.d.c.aws.ExtendAwsCliAction$ - Current expiration time is : 01/Oct/2016 00:11:37 (UTC)
17:49:45.066 [main] c.c.d.c.aws.ExtendAwsCliAction$ - Updating node to new expiration time : 04/Oct/2016 00:11:37 (UTC)

$ dev-cluster extend-aws i-911bf91e
...
17:52:31.549 [main] c.c.d.c.aws.ExtendAwsCliAction$ - Updating expiration time for master i-911bf91e
17:52:31.549 [main] c.c.d.c.aws.ExtendAwsCliAction$ - Current expiration time is : 04/Oct/2016 00:11:37 (UTC)
java.lang.IllegalArgumentException: Cannot update time to 07/Oct/2016 00:11:37 (UTC), as it is must be less than: 04/Oct/2016 00:52:31 (UTC)
	at com.criteo.dev.cluster.aws.ExtendAwsCliAction$.extendTimeMetadata(ExtendAwsCliAction.scala:70)
...


# Try stopping and restarting the instance.  Note that the IP address changes.  Note expiration time is reset.  Check data is still there.
$ dev-cluster stop-aws i-911bf91e
...

$ dev-cluster list-aws
...
Cluster [i-911bf91e], size = 4 node(s)
Master Instance id: [i-911bf91e], state: SUSPENDED, hostName: [dev-slave-1]
	Slave Instance id: [i-921bf91d], state: SUSPENDED, hostName: [dev-slave-2]

	Slave Instance id: [i-b61bf939], state: SUSPENDED, hostName: [dev-slave-0]

	Slave Instance id: [i-931bf91c], state: SUSPENDED, hostName: [dev-slave-1]


$ dev-cluster start-aws i-911bf91e
...
Cluster [i-911bf91e], size = 4 node(s)
Master Instance id: [i-911bf91e], state: RUNNING, hostName: [dev-slave-1], ip: [52.210.197.32], created: 28/Sep/2016 00:11:40 (UTC), expires: 01/Oct/2016 00:57:45 (UTC)
SSH command:        ssh -i ./conf/devcluster_eu.pem ubuntu@52.210.197.32
NodeManager:        52.210.197.32:8042
ResourceManager:    52.210.197.32:8088
HistoryServer:      52.210.197.32:19888
NameNode:           52.210.197.32:50070
DataNode:           52.210.197.32:50075
	Slave Instance id: [i-921bf91d], state: RUNNING, hostName: [dev-slave-2], ip: [52.208.158.49]
	SSH command:        ssh -i ./conf/devcluster_eu.pem ubuntu@52.208.158.49
	NodeManager:        52.208.158.49:8042
	DataNode:           52.208.158.49:50075

	Slave Instance id: [i-b61bf939], state: RUNNING, hostName: [dev-slave-0], ip: [52.208.73.201]
	SSH command:        ssh -i ./conf/devcluster_eu.pem ubuntu@52.208.73.201
	NodeManager:        52.208.73.201:8042
	DataNode:           52.208.73.201:50075

	Slave Instance id: [i-931bf91c], state: RUNNING, hostName: [dev-slave-1], ip: [52.209.138.248]
	SSH command:        ssh -i ./conf/devcluster_eu.pem ubuntu@52.209.138.248
	NodeManager:        52.209.138.248:8042
	DataNode:           52.209.138.248:50075

ssh -i ./conf/devcluster_eu.pem ubuntu@52.210.197.32
Welcome to Ubuntu 14.04.4 LTS (GNU/Linux 3.13.0-92-generic x86_64)
...
Last login: Wed Sep 28 00:23:41 2016 from 199.204.169.242
ubuntu@ip-10-0-31-169:~$  hive

Logging initialized using configuration in file:/etc/hive/conf.dist/hive-log4j.properties
WARNING: Hive CLI is deprecated and migration to Beeline is recommended.
hive> show partitions mydb.mytable;
OK
day=2016-08-04/hour=04/host_platform=CN
day=2016-08-04/hour=04/host_platform=EU
Time taken: 0.759 seconds, Fetched: 2 row(s)
hive> select count(*) from mydb.mytable where hour=04 and host_platform="EU";
...
Stage-Stage-1: Map: 1  Reduce: 1   Cumulative CPU: 3.73 sec   HDFS Read: 1379670 HDFS Write: 7 SUCCESS
Total MapReduce CPU Time Spent: 3 seconds 730 msec
OK
129297
Time taken: 25.295 seconds, Fetched: 1 row(s)
hive> exit;
ubuntu@ip-10-0-31-169:~$ exit
logout
Connection to 52.210.197.32 closed.

# We are done.  Destroy this cluster.
$ dev-cluster destroy-aws i-911bf91e
...

$ dev-cluster list-aws
...
</pre>

### Minimum Configuration

* Configure [./conf/target-aws.xml](./conf/target-aws.xml) (values or hints provided where not confidential):

<pre>

target.aws.access.id
target.aws.access.key
target.aws.instance.type=m4.xlarge
target.aws.user=ubuntu
target.aws.region=eu-west-1
target.aws.subnet=subnet-xxxx
target.aws.security.group=sg-xxxx
target.aws.key.pair
target.aws.key.file=/path/to/key/file/of/key.pair
target.aws.base.image.id=ami-ed82e39e

</pre>

* Configure [./conf/source.xml] (./conf/source.xml)

<pre>
source.address=/url/of/hadoop/access/node

In profile "sample-1"
source.tables=mydb.mytable
default.partition.count=2
</pre>

