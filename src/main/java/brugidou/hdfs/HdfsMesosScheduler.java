package brugidou.hdfs;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.mesos.Protos.CommandInfo;
import org.apache.mesos.Protos.CommandInfo.ContainerInfo;
import org.apache.mesos.Protos.ExecutorID;
import org.apache.mesos.Protos.FrameworkID;
import org.apache.mesos.Protos.FrameworkInfo;
import org.apache.mesos.Protos.MasterInfo;
import org.apache.mesos.Protos.Offer;
import org.apache.mesos.Protos.OfferID;
import org.apache.mesos.Protos.Resource;
import org.apache.mesos.Protos.SlaveID;
import org.apache.mesos.Protos.TaskID;
import org.apache.mesos.Protos.TaskInfo;
import org.apache.mesos.Protos.TaskState;
import org.apache.mesos.Protos.TaskStatus;
import org.apache.mesos.MesosSchedulerDriver;
import org.apache.mesos.Protos.Value.Scalar;
import org.apache.mesos.Protos.Value.Type;
import org.apache.mesos.Scheduler;
import org.apache.mesos.SchedulerDriver;

import brugidou.hdfs.State.Node;

import com.google.protobuf.InvalidProtocolBufferException;

public class HdfsMesosScheduler extends Configured implements Scheduler,
		Runnable {

	private State state;

	public HdfsMesosScheduler(Configuration conf, State state) {
		setConf(conf);
		this.state = state;
	}

	@Override
	public void disconnected(SchedulerDriver arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void error(SchedulerDriver arg0, String arg1) {
		// TODO Auto-generated method stub

	}

	@Override
	public void executorLost(SchedulerDriver arg0, ExecutorID arg1,
			SlaveID arg2, int arg3) {
		// TODO Auto-generated method stub

	}

	@Override
	public void frameworkMessage(SchedulerDriver arg0, ExecutorID arg1,
			SlaveID arg2, byte[] arg3) {
		// TODO Auto-generated method stub

	}

	@Override
	public void offerRescinded(SchedulerDriver arg0, OfferID arg1) {
		// TODO Auto-generated method stub

	}

	@Override
	public void registered(SchedulerDriver driver, FrameworkID frameworkId,
			MasterInfo masterInfo) {
		try {
			state.setFrameworkId(frameworkId);
		} catch (InterruptedException | ExecutionException e) {
			// TODO don't rethrow blindly
			throw new RuntimeException(e);
		}
		System.out.println("Registered framework " + frameworkId.getValue());
		reregistered(driver, masterInfo);

	}

	@Override
	public void reregistered(SchedulerDriver driver, MasterInfo masterInfo) {
		List<TaskStatus> tasks = new ArrayList<TaskStatus>();
		try {
			Node namenode = state.getNamenode();
			if(namenode != null) {
				tasks.add(TaskStatus.newBuilder()
						.setTaskId(TaskID.newBuilder().setValue(namenode.taskId))
						.setSlaveId(SlaveID.newBuilder().setValue(namenode.slaveId))
						.setState(TaskState.TASK_RUNNING)
						.build());
			}
			for (Node n : state.getNodes()) {
				tasks.add(TaskStatus.newBuilder()
						.setTaskId(TaskID.newBuilder().setValue(n.taskId))
						.setSlaveId(SlaveID.newBuilder().setValue(n.slaveId))
						.setState(TaskState.TASK_RUNNING)
						.build());
			}
		} catch (ClassNotFoundException | InterruptedException
				| ExecutionException | IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println("Reregistered framework, reconciling " + tasks.size() + " tasks.");

		driver.reconcileTasks(tasks);
	}

	@Override
	public void resourceOffers(SchedulerDriver driver, List<Offer> offers) {
		Set<Node> nodes;
		Node namenode;
		try {
			nodes = state.getNodes();
			namenode = state.getNamenode();
		} catch (ClassNotFoundException | InterruptedException
				| ExecutionException | IOException e) {
			for(Offer offer : offers) {
				driver.declineOffer(offer.getId());
			}
			return;
		}

		String execUri = getConf().get("mesos.hdfs.executor.uri", "http://mirrors.ircam.fr/pub/apache/hadoop/common/hadoop-2.4.1/hadoop-2.4.1.tar.gz");
		String localhost = "localhost";
		try {
			localhost = InetAddress.getLocalHost().getHostName().toString();
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
		}
		String confServerHostName = getConf().get("mesos.hdfs.conf.hostname", localhost);
		int confServerPort = getConf().getInt("mesos.hdfs.conf.port", 8283);
		String cmdPrefix = "cd hadoop-* && cd etc/hadoop && rm hdfs-site.xml && wget http://" + confServerHostName + ":" + confServerPort + "/hdfs-site.xml && cd ../.. && ";
		cmdPrefix += "export JAVA_HOME=$(dirname $(readlink -f $(which java)))/../.. && ";

		String containerImage = getConf().get("mesos.hdfs.container.image");
		String[] containerOptions = getConf().getStrings("mesos.hdfs.container.options");

		ContainerInfo.Builder container = null;

		if (containerImage != null) {
			container = ContainerInfo.newBuilder()
					.setImage(containerImage);
			if (containerOptions != null){
				for(int i = 0; i < containerOptions.length; i++) {
					container.setOptions(i, containerOptions[i]);
				}
			}
		}

		List<Resource> resources = new ArrayList<Resource>();
		resources.add(Resource.newBuilder().setType(Type.SCALAR).setName("cpus").setScalar(Scalar.newBuilder().setValue(1).build()).build());
		resources.add(Resource.newBuilder().setType(Type.SCALAR).setName("mem").setScalar(Scalar.newBuilder().setValue(256).build()).build());

		if(namenode == null) {

			String namenodeHostname = "localhost";
			try {
				namenodeHostname = getConf().get("mesos.hdfs.namenode.hostname", InetAddress.getLocalHost().getHostName());
			} catch (UnknownHostException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}

			/* TODO: provision namenode port?
			int namenodePort = getConf().getInt("mesos.hdfs.namenode.port", 8020);
			resources.add(
					Resource.newBuilder().setType(Type.RANGES).setName("ports").setRanges(
							Ranges.newBuilder().addRange(Range.newBuilder().setBegin(namenodePort).setEnd(namenodePort))
							).build());
			*/

			CommandInfo.Builder cmd = CommandInfo.newBuilder()
					.addUris(CommandInfo.URI.newBuilder().setValue(execUri));

			if (container != null) {
				cmd.setContainer(container);
			}

			String cmdValue = cmdPrefix + "bin/hdfs namenode -format && bin/hdfs namenode";
			cmd.setValue(cmdValue);

			System.out.println("Looking for offer from slave " + namenodeHostname);

			for(Offer offer : offers) {
				if (offer.getHostname().equals(namenodeHostname)) {
					System.out.println("Found namenode for slave " + offer.getHostname());

					String id = "namenode";
					TaskInfo task = TaskInfo.newBuilder()
							.setCommand(cmd.build())
							.setName(id)
							.setTaskId(TaskID.newBuilder().setValue(id))
							.setSlaveId(offer.getSlaveId())
							.addAllResources(resources)
							.build();

					driver.launchTasks(Arrays.asList(offer.getId()), Arrays.asList(task));
					namenode = new Node();
					namenode.hostname = offer.getHostname();
					namenode.taskId = task.getTaskId().getValue();
					namenode.slaveId = task.getSlaveId().getValue();
					try {
						state.setNamenode(namenode);
					} catch (InterruptedException | ExecutionException
							| IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					break;
				}
			}
		}

		for(Offer offer : offers) {
			boolean alreadyUsed = false;
			for (Node n : nodes) {
				if(n.hostname.equals(offer.getHostname())) {
					alreadyUsed = true;
				}
			}

			if (!alreadyUsed) {
				CommandInfo.Builder cmd = CommandInfo.newBuilder()
						.addUris(CommandInfo.URI.newBuilder().setValue(execUri));

				if (container != null) {
					cmd.setContainer(container);
				}

				String cmdValue = cmdPrefix + "bin/hdfs datanode";
				cmd.setValue(cmdValue);

				String id = "datanode-" + offer.getHostname();
				TaskInfo task = TaskInfo.newBuilder()
						.setCommand(cmd.build())
						.setName(id)
						.setTaskId(TaskID.newBuilder().setValue(id))
						.setSlaveId(offer.getSlaveId())
						.addAllResources(resources)
						.build();

				System.out.println("Launching datanode on slave " + offer.getHostname());
				driver.launchTasks(Arrays.asList(offer.getId()), Arrays.asList(task));
				Node n = new Node();
				n.hostname = offer.getHostname();
				n.taskId = task.getTaskId().getValue();
				n.slaveId = task.getSlaveId().getValue();
				nodes.add(n);
				try {
					state.setNodes(nodes);
				} catch (InterruptedException | ExecutionException
						| IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			} else {
				System.out.println("Declining offer on slave " + offer.getHostname());
				driver.declineOffer(offer.getId());
			}
		}
	}

	@Override
	public void slaveLost(SchedulerDriver arg0, SlaveID arg1) {
		// TODO Auto-generated method stub

	}

	@Override
	public void statusUpdate(SchedulerDriver driver, TaskStatus status) {

		if (status.getState().equals(TaskState.TASK_FAILED)
				|| status.getState().equals(TaskState.TASK_FINISHED)
				|| status.getState().equals(TaskState.TASK_KILLED)
				|| status.getState().equals(TaskState.TASK_LOST)) {

			System.out.println("Task " + status.getTaskId().getValue() + " lost, removing it.");

			Set<Node> nodes;
			Node namenode;
			try {
				nodes = state.getNodes();
				namenode = state.getNamenode();

				if (namenode != null && namenode.taskId.equals(status.getTaskId().getValue())) {
					state.setNamenode(null);
				} else {

					Node node = null;
					for(Node n : nodes) {
						if (n.taskId.equals(status.getTaskId().getValue())) {
							node = n;
							break;
						}
					}

					if (node != null) {
						nodes.remove(node);
						state.setNodes(nodes);
					} else {
						// weird...
					}
				}

			} catch (ClassNotFoundException | InterruptedException
					| ExecutionException | IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		} else if (status.getState().equals(TaskState.TASK_RUNNING)) {
			System.out.println("Task " + status.getTaskId().getValue() + " running.");
		} else if (status.getState().equals(TaskState.TASK_STAGING)) {
			System.out.println("Task " + status.getTaskId().getValue() + " staging.");
		} else {
			//TODO ?
		}
	}

	@Override
	public void run() {

		String clusterName = getConf().get("mesos.hdfs.cluster.name",
				"my-cluster");

		// 1 week by default
		long failoverTimeout = getConf().getLong("mesos.failover.timeout.sec", 7*24*3600);

		FrameworkInfo.Builder frameworkInfo = FrameworkInfo.newBuilder()
				.setName("HDFS " + clusterName)
				.setFailoverTimeout(failoverTimeout) // seconds
				.setUser("");

		try {
			FrameworkID frameworkID = state.getFrameworkID();
			if (frameworkID != null) {
				frameworkInfo.setId(frameworkID);
			}
		} catch (InterruptedException | ExecutionException
				| InvalidProtocolBufferException e) {
			// TODO don't rethrow blindly
			throw new RuntimeException(e);
		}

		String masterUrl = getConf().get("mesos.url",
				"zk://localhost:2181/mesos");

		MesosSchedulerDriver driver = new MesosSchedulerDriver(this,
				frameworkInfo.build(), masterUrl);
		driver.run().getValueDescriptor().getFullName();
	}

}
