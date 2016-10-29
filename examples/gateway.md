# Create-gateway

Create a gatway to a configured production cluster.

<pre>
# Create gateway in local docker container
$ dev-cluster create-gateway mycluster
...
Successfully built bf0844aea002
docker run -P -it dev_cluster/gateway:final
Attaching terminal.  Type 'exit' to quit.  If you have specified a mount directory, it will be under '/mount'

# To run any Hadoop client against PA4, we must login via Kerberos inside this gateway.  It's preconfigured to authenticate on KDC.
{user}@1d61657f8c71:~$ kinit -f
Password for {user.name}@ORG.LAN:

# Now we can run some queries.
{user}@1d61657f8c71:~$ hive


Logging initialized using configuration in file:/etc/hive/conf.dist/hive-log4j.properties
WARNING: Hive CLI is deprecated and migration to Beeline is recommended.
hive> use glup;
OK
Time taken: 0.9 seconds
hive> show tables;
...
table1
table2
table3
table4
Time taken: 0.497 seconds, Fetched: 89 row(s)

hive> show partitions table4;
...
day=2016-08-22/hour=21
day=2016-08-22/hour=22
Time taken: 1.941 seconds, Fetched: 5639 row(s)
hive> select count(*) from table4 where day="2016-08-22" and hour=22;
Query ID = sz.ho_20160823003838_4a860a3b-92ee-4d3f-90c9-16e0d4f166e9
Total jobs = 1
Launching Job 1 out of 1
Number of reduce tasks determined at compile time: 1
In order to change the average load for a reducer (in bytes):
  set hive.exec.reducers.bytes.per.reducer=<number>
In order to limit the maximum number of reducers:
  set hive.exec.reducers.max=<number>
In order to set a constant number of reducers:
  set mapreduce.job.reduces=<number>
Starting Job = job_1469622382533_4315995, Tracking URL = http://e4-1d-2d-19-6c-50.pa4.hpc.criteo.prod:8088/proxy/application_1469622382533_4315995/
Kill Command = /usr/lib/hadoop/bin/hadoop job  -kill job_1469622382533_4315995
Hadoop job information for Stage-1: number of mappers: 34; number of reducers: 1
2016-08-23 00:46:57,366 Stage-1 map = 0%,  reduce = 0%
2016-08-23 00:47:08,907 Stage-1 map = 3%,  reduce = 0%, Cumulative CPU 6.5 sec
2016-08-23 00:47:11,153 Stage-1 map = 32%,  reduce = 0%, Cumulative CPU 52.86 sec
2016-08-23 00:47:13,255 Stage-1 map = 53%,  reduce = 0%, Cumulative CPU 136.99 sec
2016-08-23 00:47:15,725 Stage-1 map = 68%,  reduce = 0%, Cumulative CPU 199.11 sec
2016-08-23 00:47:17,824 Stage-1 map = 74%,  reduce = 0%, Cumulative CPU 217.3 sec
2016-08-23 00:47:19,929 Stage-1 map = 81%,  reduce = 0%, Cumulative CPU 249.78 sec
2016-08-23 00:47:22,020 Stage-1 map = 88%,  reduce = 0%, Cumulative CPU 271.56 sec
2016-08-23 00:47:24,119 Stage-1 map = 95%,  reduce = 0%, Cumulative CPU 307.73 sec
2016-08-23 00:47:26,217 Stage-1 map = 97%,  reduce = 0%, Cumulative CPU 310.02 sec
2016-08-23 00:47:30,564 Stage-1 map = 99%,  reduce = 0%, Cumulative CPU 318.3 sec
2016-08-23 00:47:33,625 Stage-1 map = 100%,  reduce = 0%, Cumulative CPU 323.04 sec
2016-08-23 00:47:44,749 Stage-1 map = 100%,  reduce = 100%, Cumulative CPU 323.04 sec
MapReduce Total cumulative CPU time: 5 minutes 23 seconds 40 msec
Ended Job = job_1469622382533_4315995
MapReduce Jobs Launched:
Stage-Stage-1: Map: 34  Reduce: 1   Cumulative CPU: 327.93 sec   HDFS Read: 164079145 HDFS Write: 7 SUCCESS
Total MapReduce CPU Time Spent: 5 minutes 27 seconds 930 msec
OK
708482
Time taken: 559.866 seconds, Fetched: 1 row(s)
hive> exit;



# You can always resume the gateway from the last state.
{user}@1d61657f8c71:~$ exit
exit
Exited docker container.  Run './dev-cluster resume-gateway' to resume.
</pre>

# Minumum Configuration

* Copy "mycluster" cluster configuration under ./hadoop-resources/hadoop-conf/mycluster to point to your real cluster.
* Configure [./gateway.xml] (./gateway.xml):
<pre>
gateway.mycluster.conf=mycluster
</pre>