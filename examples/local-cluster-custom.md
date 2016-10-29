# Create a local-cluster mounting custom application 'sample-tool'

<pre>

# Create local cluster, choose custom add-on to prepare for tool, mount a directory with local tool
$ dev-cluster create-local -Plangoustine /path/to/local/langoustine/project/dir

# Note that the ports configured in ./gateway.xml are properly displayed, and can be accessed once local tool is running.
$ dev-cluster list-local
Instance id: [a82426189cfc], State: [Running], Created: [2016-09-28 15:54:47 -0700 PDT]
SSH command:        ssh -o StrictHostKeyChecking=no root@192.168.99.100 -p 32811
Ports:
NodeManager:        192.168.99.100:32810
ResourceManager:    192.168.99.100:32809
HistoryServer:      192.168.99.100:32806
NameNode:           192.168.99.100:32805
DataNode:           192.168.99.100:32804
SampleHttp:         192.168.99.100:32808
SampleHttps:        192.168.99.100:32807

# SSH into the instance and start it
$ ssh -o StrictHostKeyChecking=no root@192.168.99.100 -p 32811
Welcome to Ubuntu 14.04.4 LTS (GNU/Linux 4.4.12-boot2docker x86_64)

 * Documentation:  https://help.ubuntu.com/
root@a82426189cfc:~# cd /mount
root@a82426189cfc:/mount# /bin/bash -ex ./bin/run_langoustine.sh

# Run tests

# Destroy local cluster when done
dev-cluster destroy-local a82426189cfc
</pre>

### Minimum Configuration

Configure ./conf/target-local.xml:

<pre>
profile : sample-tool
target.local.docker.files=cluster-sample-tool
target.local.ports=SampleHttp/9400, SampleHttps/9401
</pre>
