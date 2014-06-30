HDFS over Apache Mesos
======================

NOTE: this is a proof of concept that has been written extremely quickly without
any experience of the Mesos scheduler API. There are no tests and most
exceptions are catched in a very ugly way.

How to build
------------

```
mvn clean package
```

Configuration
-------------

All mesos-specific conf goes to `conf/mesos-site.xml`
The regular HDFS config is in `conf/hdfs-site.xml`

The first (and only required) setting is the namenode host that needs to be
manually at `mesos.hdfs.namenode.hostname`. Mesos will look for a slave with
this hostname to launch the namenode.

After that, all datanodes are launched (1 per mesos slave).

Other options for `mesos-site.xml`:
* `mesos.failover.timeout.sec` failover timeout (default: 1 week)
* `mesos.hdfs.cluster.name` the HDFS cluster name in mesos, also used to store
  the state in zookeeper, (default `my-cluster`)
* `mesos.hdfs.conf.hostname` the configuration server to download `hdfs-site.xml` (default: the hostname running hdfs-mesos)
* `mesos.hdfs.conf.port` (default `8283`)
* `mesos.hdfs.container.image` to use for ContainerInto (e.g docker/deimos)
* `mesos.hdfs.container.options` (comma separated) to use for ContainerInto (e.g docker/deimos)
* `mesos.hdfs.executor.uri` (default
  `http://mirrors.ircam.fr/pub/apache/hadoop/common/hadoop-2.4.1/hadoop-2.4.1.tar.gz`)
* `mesos.hdfs.namenode.hostname` hostname of the namenode (see above, default
  `localhost`)
* `mesos.hdfs.namenode.port` (default 8020)
* `mesos.hdfs.state.zk` where to store the scheduler state (default
  `localhost:2181`)
* `mesos.hdfs.state.zk.timeout.ms` zookeeper state session timeout (default
  20000)
* `mesos.native.library` (default `/usr/local/lib/libmesos.so`)
* `mesos.url` url of mesos cluster (default `zk://localhost:2181/mesos`)

Run
---

`bin/hdfs-mesos` should do it once you have configured the namenode hostname
