package brugidou.hdfs;

import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.mesos.MesosNativeLibrary;
import org.apache.mesos.state.ZooKeeperState;

public class Main {

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		conf.addResource(new Path("conf/mesos-site.xml"));

		String clusterName = conf.get("mesos.hdfs.cluster.name", "my-cluster");
		String stateZkServers = conf.get("mesos.hdfs.state.zk", "localhost:2181");
		int stateZkTimeout = conf.getInt("mesos.hdfs.state.zk.timeout.ms", 20000);

		MesosNativeLibrary.load(conf.get("mesos.native.library", "/usr/local/lib/libmesos.so"));

		ZooKeeperState zkState = new ZooKeeperState(stateZkServers, stateZkTimeout, TimeUnit.MILLISECONDS, "/hdfs-mesos/" + clusterName);
		State state = new State(zkState);

		Thread sched = new Thread(new HdfsMesosScheduler(conf, state));
		sched.start();

		new ConfigServer(conf);
	}

}
