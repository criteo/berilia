# Table of Contents
1. [Introduction] (#introduction)
2. [Setup] (#setup)
3. [Quickstart Examples] (#quickstart-examples)
4. [Documentation] (#documentation)
 1. [Local Cluster] (#local-cluster)
 2. [AWS Cluster] (#aws-cluster)
 3. [Copy Operations] (#copy-operations)
 4. [S3 Buckets] (#s3-buckets)
 5. [Gateways] (#gateways)
 6. [Scala API] (#scala-api)
5. [Configuration] (#configuration-guide)
6. [How to Contribute] (#how-to-contribute)
7. [Known Issues] (#known-issues)

# Introduction
While developing on Hadoop, it is nice to test on a private sandbox before pushing to a production cluster. 
However, it can be hard for developers to get access to a cluster, and Hadoop is difficult to set up locally.  
Berilia allows developers to quickly set up and manage a variety of simple Hadoop clusters and environments,
as well as sample and manage test data accessible to these clusters.

Berilia  Specific Functionalities:

1. Create, stop, start, destroy single-node Hadoop/Hive dev cluster on local Docker, with ability to mount local artifacts.
2. Create, stop, start, destroy multi-node Hadoop/Hive dev cluster on AWS cloud.
3. Expiry policy and auto-purge of dev clusters on AWS, ability to renew expiration time.
4. Create and manage a ‘gateway node’ on local Docker with Hadoop/Hive client access to any remote cluster with ability to mount local artifacts (AWS dev clusters mentioned above, or any configurable production cluster). 
5. Copy test data, in the form of Hive tables or HDFS files, from a prod cluster directly to dev cluster, with ability to sample partitioned Hive tables 
6. Create, manage, destroy S3 buckets for shared test data/metadata for multiple dev clusters, in the form of Hive tables or HDFS files, and allow dev clusters to access them locally.
7. Access via CLI or Scala library.

Comparing to existing solutions:

1. VM/Docker Hadoop Quickstart images: Non-customizable, fixed components/environment.  Berilia has options to create/customize dev clusters, also provides cluster management and other utilities.
2. Custom scripts (Bash/Vagrant): Hard to maintain.  Berilia is well-defined for customization, and also provides cluster management and other utilities.
3. Hadoop management tools:  Installed on each node of bare-metal cluster.  Berilia does not have to be installed on many nodes, as dev clusters are either local/cloud based (no bare metal required).
4. Cloud Hadoop management services: Subscription based, accessed as a service, fixed components/environment.  Berilia is open source, all scripts are viewable and changable, and is run locally as library or CLI tool.

# Setup

1. Clone this project.
2. mvn clean install -DskipTests
3. "cd target/berilia-{version}-bin" or unzip target/berilia-{version}-bin.tar.gz to a location and cd into it.
4. Run "./dev-cluster help" or just "./dev-cluster" to get a list of commands
5. Run "./dev-cluster [command] [args]" to run an action
6. Configure commands via config files in [./conf] (./conf) directory.
<br>

# Quickstart Examples

* [Local Cluster] (./examples/local-cluster.md)
* [AWS Cluster] (./examples/aws-cluster.md)
* [S3 Bucket] (./examples/s3-bucket.md)
* [Gateway] (./examples/gateway.md)
* [Local Cluster Custom] (./examples/local-cluster-custom.md)


# Documentation

## Local Cluster

Create and manage single-node Hadoop/Hive dev cluster on local Docker, with ability to mount local artifacts.  Used for limited functional testing.

##### Requirements
* Docker is installed and running.  Type 'docker version' to verify.
* Supported on Mac using Docker for Mac (newer) or Docker Toolbox (docker-machine).

##### Configuration
Files: Samples provided in [./conf/source.conf] (./conf/target.conf]
* In general, source is for data source (copy data to dev cluster)
* In general, target is for dev cluster (here, put settings for AWS account, docker, etc)
* dev-cluster will look in local directory for file named "source.conf" and "target.conf"
* To run with other configuration for source, run --source=./path/to/conf/file.
* To run with other configuration for target, run --target=./path/to/conf/file.

##### Notes
* Local clusters run in background and use the resources of the local machine
  * If docker clusters are resource-starved, you may increase the memory of the docker-machine, see [Known Issues] (#known-issues)
  * Stop clusters with "stop-local" command to free memory and cpu, and resumed by "start-local" when needed.
  * State of clusters is retained by docker until "destroy-local" command.
* Note that first time 'create-local' will take longer, as docker is building the intermediate images.  Subsequent invokations will be much faster due to docker cache.
* If mounting a directory in 'create-local', note that all changes done in the container to those files *will* be reflected on local machine.

##### Operation list
<pre>
* create-local
Creates a cluster using a local docker container and starts Hadoop services on it.  If mount.dir is provided, directory will be mounted under /mount.
Usage: dev-cluster create-local [(Optional) mountDir]

* destroy-local
Destroys a local cluster docker container. If instanceId specified, only destroy that one container, else destroy all.
Usage: dev-cluster destroy-local [(Optional) instanceId]

* list-local
Lists all running and stopped local dev-cluster docker containers.
Usage: dev-cluster list-local

* start-local
Starts a local cluster docker container. If instanceId specified, only start that one container, else starts them all.
Usage: dev-cluster start-local [(Optional) instanceId]

* stop-local
Stops a local cluster docker container. If instanceId specified, only stop that one container, else stops them all.
Usage: dev-cluster stop-local [(Optional) instanceId]
</pre>

## AWS Cluster

Runs a multi-node hadoop dev cluster on powerful AWS instances.  Used for testing at a larger scale with more compute power/cluster storage.

##### Requirements
* AWS account, basic artifacts created in target AWS data-center.  [See Configuring AWS](#configuring-aws)

##### Configuration
Files: [./conf/target-aws.xml] (./conf/target-aws.xml) and [./conf/target-common.xml] (./conf/target-common.xml).
* CDH Version, OS, Setup Scripts, Hadoop Configuration (defaults provided)
* AWS account information, region, VPC, security group, compatible base OS image.  [See Configuring AWS](#configuring-aws)

##### Notes
* Dev clusters run on EC2, be careful of usage costs.
  * All dev clusters start with an expire time set to 3 days after the creation time,
    that may be extended via 'extend-aws' command to a maximum of 6 days from the current time.
  * Once an instance is expired, it is eligible to be killed by a hidden command "dev-cluster purge" that can be run as a cron job.
  * Stop instances by 'stop-aws' command to reduce cost.  Stopped instances are exempt from purge, and have expiration time reset when restarted.
  * Destroy instances by 'destroy-aws' command after they are not needed anymore.
* The scope of nodes that are accessed these AWS command is limited to nodes owned by you in the specified AWS region (to improve performance).
  * Be careful, you may have had created instances in a regions that are not listed by the tool if it is pointing to another region.
* The cluster comes with an already configured hadoop and hive environment, as well as a spark installation. This allows the user to specify Spark as execution engine for Hive.
To do so, the properties related to Spark in the `hadoop-resources/hadoop-conf/cluster-default/hive/conf/hive-site.xml` configuration should be uncommented.


##### Operation List
<pre>
* create-aws
Creates an AWS multi-node cluster from the base OS image provided by AWS.  Runs all the install scripts and custom overrides.
Usage: dev-cluster create-aws [nodes]

* destroy-aws
Destroys AWS cluster with given cluster.id.  If no cluster.id is given, destroy all clusters for this user.
Usage: dev-cluster destroy-aws [(Optional) cluster.id]

* list-aws
Lists all clusters owned by this user, and details of nodes within the cluster.
Usage: dev-cluster list-aws

* start-aws
Starting a stopped cluster with given cluster.id.  If no cluster.id is given, start all stopped clusters owned by this user. Note that AWS assigns new public ips for restarted nodes in the cluster.  Expiration countdown is extended for restarted clusters.
Usage: dev-cluster start-aws [(Optional) cluster.id]

* stop-aws
Stopping a running cluster with given cluster.id.  If no cluster.id is given, stop all running clusters owned by this user.  Stopping a cluster prevents it from being purged due to expiration.
Usage: dev-cluster stop-aws [(Optional) cluster.id]

* configure-aws
Copies the hadoop-configuration in /hadoop-resources/hadoop-conf/${target.hadoop.conf.dir} to the specified AWS cluster.  Restart-services-aws may be required.
Usage: dev-cluster configure-aws [cluster.id]

* restart-services-aws
Restart all Hadoop services on all nodes in given cluster.
Usage: dev-cluster restart-services-aws [cluster.id]

* extend-aws
Extends expiry time of cluster with given cluster.id
Usage: dev-cluster extend-aws [cluster.id]

</pre>

## Copy Operations

Copy data from configured production clusters to your dev clusters.  Data may be Hive tables or HDFS directories.
Sampling can be configured if specified Hive tables are partitioned.  Use for ad-hoc or small data testing.

##### Requirements

* The tool must be able to access the configured cluster without password.
* You may check by running Hdfs and Hive commands successfully via "ssh -K", without password.  For example, if kerberos is enabled on the cluster, make sure the current user has obtained a valid kerberos ticket via kinit before running the tool.

##### Configuration
Files in [./conf/source.xml] (./conf/source.xml).
* List of Hive tables (copies Hive metadata and data)
* List of HDFS directories (only needed if need files unassociated with Hive tables)
* Number of partitions to sample per table (can be customized per table)
* Source-Cluster node URL

##### Notes

* Copy operations are idempotent and will not override existing Hive tables/partitions on the dev cluster.  Newer tables/partitions are copied over.  Sampling algorithm copies last n partitions.
* Due to security restrictions on both production (if within Kerberos network) and dev clusters (ip-based firewall that
doesn't work for data center without public ip), the copy is done on a single-file copy tunnel through the local machine and is slow.

##### Operation List

<pre>
* copy-aws
Copies sample data from gateway to AWS cluster identified by cluster.id
Usage: dev-cluster copy-aws [cluster.id]

* copy-local
Copies sample data from gateway to node identified by container.id
Usage: dev-cluster copy-local [container.id]
</pre>

## S3 Buckets

S3 buckets provide shared storage for test data/metadata between dev clusters.  S3 storage is more cost-efficient than cluster storage, and bucket
security allows faster copy.  Use for larger-scale testing on large test data sets.

##### Requirements
* AWS account only.

##### Configuration
Files: [./conf/target-aws.xml] (./conf/target-aws.xml) and [./conf/source.xml] (./conf/source.xml).
* List of Hive tables (copies Hive metadata and data)
* List of HDFS directories (only needed if need files unassociated with Hive tables)
* Number of top-level partitions to sample per table (can be customized per table)
* Source-Cluster access node URL
* AWS account id/key

##### Notes
* S3 bucket incur costs for storage.
* DistCP is the default copy algorithm, and tool intelligently falls back to single file-copy if conditions exist.
* Data/metadata is not overriden if it exists.  New data/metadata is copied over.  Sampling algorithm copies last N top-level partitions.
* Running "attach-bucket-aws" or "attach-bucket-local" on AWS or local dev cluster creates Hive tables pointing to the tables copied in the given S3 bucket.
  * After attaching a dev cluster, the tables can be directly queried from it.
  * It's recommended to copy Hive tables over to S3.  To access HDFS files (copied without a Hive table) to a dev cluster, refer to them by their full S3 Path (shown in "describe-bucket").
  * You may choose to reconfigure the dev-cluster to point to the S3 cluster using fs.default.name=s3a://bucket-name/, and then
  you may access S3 files by their relative paths.  But use caution, as all HDFS operations of the dev cluster will be reflected on the bucket
  (even writing).

##### Operation List
<pre>
* create-bucket
Creates a bucket in S3
Usage: dev-cluster create-bucket [(Optional) Name]

* destroy-bucket
Deletes a bucket in S3
Usage: dev-cluster destroy-bucket [Bucket-id]

* copy-bucket
Copies data from configured source to a bucket.
Usage: dev-cluster copy-bucket [bucket-id]

* describe-bucket
Shows contents of bucket.
Usage: dev-cluster describe-bucket [bucket-id]

* attach-bucket-aws
Attaches the given AWS cluster to Hive tables located in given S3 bucket.  Any existing Hive metadata on cluster is not overriden, be aware to maintain consistency.
Usage: dev-cluster attach-bucket-aws [bucket-id] [instance.id]

* attach-bucket-local
Attaches the given local docker cluster to Hive tables located in given S3 bucket.  Any existing Hive metadata on cluster is not overriden, be aware to maintain consistency.
Usage: dev-cluster attach-bucket-local [bucket-id] [container.id]
</pre>

## Gateways

Create and manage a ‘gateway node’ on local Docker with Hadoop/Hive client access to any remote cluster with ability to mount local artifacts
 (AWS dev clusters, or any configurable production cluster). 

##### Requirements
Docker is installed and running.  Type 'docker version' to verify.  All docker versions are supported.

##### Configuration
Files in [./conf/gateway.xml] (./conf/gateway.xml)
* Hadoop configuration files to configure client (one set provided for AWS clusters created by this tool)
* Additional Docker configuration (ports to expose, additional docker-files to install custom tools)

##### Notes
* The first time 'create-gateway' might take longer, as docker is building the intermediate images.  Subsequent invokations will be much faster due to docker cache.
* Commands 'create-gateway' or 'resume-gateway' switches terminal to the gateway.  Type 'exit' to quit and stop the container.
* As long as docker containers have not been destroyed via "destroy-gateway", their state is saved by docker and still accessible.  You may resume in last state via
'resume-gateway'.
* If mounting a directory in 'create-gateway', note that all changes done in the container to those files *will* be reflected on local machine.

##### Operation List
<pre>
* create-gateway
Spawns a local docker container as a gateway to the given cluster.  This can be the instance.id of the cluster, or custom cluster defined in ./conf/gateway.xml.  Also mounts the given mount.directory under /mount if provided.
Usage: dev-cluster create-gateway [cluster] [(Optional) mount.directory]

* list-gateway
Lists recently exited gateway docker containers, which can be resumed.
Usage: dev-cluster list-gateway

* resume-gateway
Resumes the docker gateway container, resuming from the state it was exited.
Usage: dev-cluster resume-gateway [container.id]

* destroy-gateway
Destroys local gateway docker containers. If container.id specified, only destroy that one container, else destroy all.
Usage: dev-cluster destroy-gateway [(Optional) container.id]
</pre>

## Scala API

All these berilia functionalities can be accessed as a Scala library, instead of via CLI commands, for automation purpose.
* Install:    Build and unzip berilia as mentioned in [Setup] (#setup)
* Dependency: Add project as a dependency to your project, using groupId=com.criteo.hadoop, artifactId=berilia
* Compile:    Use berilia public api's annotated with [public] (./src/main/scala/com/criteo/dev/cluster/Public.scala).  Non-annotated API's may be removed in subsequent release.
 * Use the Scala object 'fooCLiAction' to run the equivalent to the CLI command 'foo'.
 * Use ConfigLoader object to load configuration for berilia commands, or alternatively construct the configuration manually using Scala map.
 * Use SshAction, SshMultiAction, SshHiveAction, ScpAction, RsyncAction objects with nodes from NodeFactory as helper classes to execute actions on a dev cluster.
 * Check [unit tests](./src/test/scala/com.criteo.dev.cluster) for the latest examples.
* Run:        Set following environment variable "DEV_CLUSTER_HOME" to point to berilia install.
 * This will be used by berilia libraries to find conf and script files.
 * All relative paths referred by conf files will be relative to DEV_CLUSTER_HOME.

# Configuration Guide

## Switch configuration files

Config files can be specified with the command line options:

* `--source=/path/to/source.conf` for source configurations
* `--target=/path/to/target.conf` for target configurations

By default, Berilia will search `source.conf` and `target.conf` in the working directory.

## Configuring AWS
Working with AWS dev clusters require the following configuration.

* AWS accountId/Key:
* AWS region:
 * The scope of the tool's AWS command restricted to this region.  Choose one close to you.
* AWS Subnet and VPC:
  * It must be a public VPC (nodes are publically accessible) by configuring a valid internet gateway and route table.
  * See [http://docs.aws.amazon.com/AmazonVPC/latest/UserGuide/VPC_Scenario1.html].
  * Summary, the route table must have the following entries: vpc_cidr -> local, 0.0.0.0/0 -> Internet_gateway_id.
  * The default VPC/subnet of a region should work, only need to create a new one if the original one is changed to non-public.
* Security group:
  * Make sure to protect your data by configuring firewall that allow inbound access only from IP CIDR blocks
   where tool may run from (eg, office IP CIDR's).
* Key Pair/Key File:
  * Create in AWS for SSH access to your nodes.  Make sure to save the key and configure it in the tool.
* Image:
  * Choose image of base OS that matches configured scripts that installs CDH on (currently Ubuntu Trusty, ie 14.04).
  AWS has AMI's of all the Linux OS's, but they are different ID's across data centers.
* User:
  * Choose root-privileged user set in the specified image.  All the Amazon AMI's will have one.

## Configuring Custom Copy Listener

* For some special file-formats, some special handling is needed after the data is copied.
  * For example, Pail-format tables require the Pail.meta file to be copied as well.
* You may drop a jar under ./lib with custom listener(s) and specify the fully-qualified class-names in [./source.conf] (./source.conf) in "source.copy.listeners".


## Configuring Hadoop Configuration

* The tool provides a default configuration file set located at [./hadoop-resources/hadoop-conf/cluster-default] (./hadoop-resources/hadoop-conf/cluster-default) that is a minimum configuration for working Hadoop/Hive cluster.
* You may choose to override with your own configuration files.
  * Make sure you copy the original configuration files and then refrain from modifying existing properties required for functioning of the cluster.
  * Copy it under [./hadoop-resources/hadoop-conf] (./hadoop-resources//hadoop-conf), and provide the relative location in [./target.conf] (./target.conf) in "target.hadoop.conf.dir".
  * The Hadoop configuration files may be templated.  Existing templates are listed below:
<pre>
$master:    cluster master name
$local:     local node's name (same as master if on master node)
$accessId:  AWS access id (for S3 storage access)
$accessKey: AWS access key (for S3 storage access)
</pre>

## Configuring Hive Aux Jars

* Add the jar(s) to the [./hadoop-resources/aux-jars] (./hadoop-resources/aux-jars) and specify the list of jar short-names in [./target.conf] (./target.conf) in property "target.hive.aux.jars"
* The tool will create clusters that has the jar, and automatically appends this jar path to the HIVE_AUX_JAR_PATH env variable (via generated hive-env.sh)


## Configuring Remote Cluster for Gateway Nodes

* Make sure you are on the same network as the remote cluster to which you want to create an edge node with Hadoop/Hive client.
* Copy your cluster's Hadoop configuration to [./hadoop-resources/hadoop-conf] (./hadoop-resources/hadoop-conf), and specify the relative path in ./gateway.xml as property "gateway.${cluster}.prop".
 You may now run "create-gateway $cluster".

## Configuring Gateway Docker Add-ons

* Add custom docker files in the directory [./docker/contrib-gateway] (./docker/contrib-gateway),
and specify the list of DockerFiles in [./source.conf] (./source.conf) in "gateway.docker.files".
These will be run to finalize the gateway image.
  * Dockerfiles must begin with line "FROM dev_cluster/gateway".
* You may also specify comma-separated list of ports that the gateway will expose under "gateway.docker.ports".
  * Port can be a mapping of $port_description/$exposed_port_num:$portNum
  * If neither port_description or exposed_port_num are provided, both will take the form $portNum.
  * These ports wll be displayed on 'dev-cluster list-gateway' command.

## Configuring Local-Cluster Docker Add-ons

* Add custom docker files in the directory [./docker/contrib-gateway] (./docker/contrib-gateway),
and specify the list of DockerFiles in [./source.conf] (./source.conf) in "target.local.docker.files".
These will be run to finalize the local-cluster image.
  * Dockerfiles must begin with line "FROM dev_cluster/local".
  * Note to set an env variable that will be set upon SSH into the cluster, do not use ENV command.
  Instead use the following:  'RUN echo "export VAR=value" >> /etc/profile'
* You may also specify comma-separated list of ports that the gateway will expose under "target.local.ports".
  * Port can be a mapping of {[Optional] port_description}/{[Optional] exposed_port_num}:{portNum}
  * If neither port_description or exposed_port_num are provided, both will take the form {portNum}.
  * These ports wll be displayed on 'dev-cluster list-local' command.

## Configuring Hadoop Install

* Currently, only Ubuntu trusty (14.04) dev clusters/gateways are created.
* Hadoop components installed include hadoop-hdfs, hadoop-yarn, and hive.
* Scripts are located in [./hadoop-resources/setup-scripts/ubuntu] (./hadoop-resources/setup-scripts/ubuntu) and could be modified by hand to install new services.
* CDH5 is supported, and any CDH5 version may be specified in [./conf/target.conf] (./conf/target.conf) under "target.hadoop.version"
* TODO- make it easier to install new tools or customize different installation strategies like Chef, Bigtop.

# How to Contribute

* Contributions are welcome, many enhancements are possible.
* Dev-cluster is written mostly in Scala, calling a mix of bash scripts and DockerFiles.
* To debug or pass different JVM options to dev-cluster tool:
 * Set JAVA_OPTS environment variable before running tool.  These will be passed in to program's JVM.
* To run unit tests:  mvn test

# Known Issues

* AWS commands fail sporadically:
  * Exception Message: 17:27:54.232 INFO  c.c.dev.cluster.node.AwsUtilities$ - Connecting to AWS in region eu-west-1
  org.jclouds.rest.AuthorizationException: POST http://ec2.eu-west-1.amazonaws.com/HTTP/1.1 -> HTTP/1.1 401 Unauthorized
  * It seems this is due to AWS blocking requests from too many different machines with same AWS secret/key.
  * Wait a little bit (maybe even a few hours) and try again.
  * Use different AWS secret/key per machine, if available.
<br>
* 'create-gateway' command on a production cluster succeeds, but commands in gateway fail talking to production cluster, with hive/hdfs fails with Kerberos errors.
 * Make sure you have run "kinit -f" successfully before running hive/hdfs.
 * You may run 'export HADOOP_OPTS=-Dsun.security.krb5.debug=true' to get more information before running a Hive/HDFS command
 * If you see 'clock skew too great' after enabling debug log, it may be due to docker-on-mac daemon getting clock skew when the mac sleeps:
 [http://stackoverflow.com/questions/22800624/will-docker-container-auto-sync-time-with-the-host-machine](stackoverflow).
 As mentioned in the link, there is no one fix for all the different dockers on macs, so some of these might work for your version.
 * /usr/local/bin/boot2docker ssh sudo ntpclient -s -h pool.ntp.org
 * docker-machine ssh default 'sudo ntpclient -s -h pool.ntp.org'
 * docker-machine ssh dev 'sudo ntpclient -s -h pool.ntp.org'
 * docker-machine restart (should always work, but takes longer)
<br>
* 'create-local' or 'start-local' fails to cluster fails to initialize with: Can't connect to local MySQL server through socket '/var/mysql/mysql.sock' (38)
 * I'm not sure why this happens, there are similar issues mentioned online.  It's intermittent and maybe because mysql-in-docker cannot
  bind to a specific port.  You may log into the container, check and potentially restart mysql and dependent hive-metastore to resolve the issue.
<br>
* 'create-local' fails to start hadoop with: # There is insufficient memory for the Java Runtime Environment to continue.
 * This is because of resource limits on your local docker machine.  See above discussion on local mode resource usage.
 * [http://stackoverflow.com/questions/32834082/how-to-increase-docker-machine-memory-mac](http://stackoverflow.com/questions/32834082/how-to-increase-docker-machine-memory-mac)
<br>
* After ssh into a dev cluster, hdfs/hive commands are failing.
 * Due to errors, hadoop processes may come down.  As there is no monitoring yet, it may be hard to see why.
 * The list of services to check are :  hadoop-hdfs-namenode, hadoop-hdfs-datanode, hadoop-mapreduce-historyserver, hadoop-yarn-resourcemanager,
 hadoop-yarn-nodemanager, hive-metastore, mysql.
 * To check status of a service, type "sudo $service status".  To restart it, type "sudo $service (re)start"
 * To check service logs for errors, go to /var/log/.
 * To check config for the services and make some changes, go to /etc/hadoop/conf or /etc/hive/conf.
<br>
* 'copy-*' command is failing.
 * Make sure you have run 'kinit -f' successfully before this command.
 * Make sure you can ssh into the configured source-gateway without password, and access hdfs/hive commands.  For example,
 run 'ssh -K <source-gateway> hdfs dfs -ls /user' or 'ssh -K <source-gateway> hive -e 'show databases' and make sure they work.
 * If intermittent error, try again as the command is idempotent and does not drop data.
* 'create-aws' fails with "It is required that your private key files are NOT accessible by others.
                           This private key will be ignored.
                           bad permissions: ignore key:"
 * Make sure the AWS public key permission is set to 400.
 * Be aware, git source control does not preserve permissions, if you check in your dev cluster key somewhere.
