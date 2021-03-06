package brugidou.hdfs;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import org.apache.mesos.Protos.FrameworkID;
import org.apache.mesos.state.Variable;
import org.apache.mesos.state.ZooKeeperState;

import com.google.protobuf.InvalidProtocolBufferException;

public class State {
	private static String FRAMEWORK_ID_KEY = "frameworkId";
	private static String NODES_KEY = "nodes";
	private static String NAMENODE_KEY = "namenode";

	public static class Node implements Serializable {
		private static final long serialVersionUID = 1L;

		public String hostname;
		public String taskId;
		public String slaveId;
	}

	private ZooKeeperState zkState;

	public State(ZooKeeperState zkState) {
		this.zkState = zkState;
	}

	/**
	 * return null if no frameworkId found
	 * @throws ExecutionException
	 * @throws InterruptedException
	 * @throws InvalidProtocolBufferException
	 **/
	public FrameworkID getFrameworkID() throws InterruptedException, ExecutionException, InvalidProtocolBufferException {
		byte[] existingFrameworkId = zkState.fetch(FRAMEWORK_ID_KEY).get().value();
		if(existingFrameworkId.length >0){
			return FrameworkID.parseFrom(existingFrameworkId);
		} else {
			return null;
		}
	}

	public void setFrameworkId(FrameworkID frameworkId) throws InterruptedException, ExecutionException {
		Variable value = zkState.fetch(FRAMEWORK_ID_KEY).get();
		value = value.mutate(frameworkId.toByteArray());
		zkState.store(value).get();
	}

	public Node getNamenode() throws ClassNotFoundException, InterruptedException, ExecutionException, IOException {
		return get(NAMENODE_KEY);
	}

	public void setNamenode(Node node) throws InterruptedException, ExecutionException, IOException {
		set(NAMENODE_KEY, node);
	}

	public Set<Node> getNodes() throws InterruptedException, ExecutionException, IOException, ClassNotFoundException {
		Set<Node> nodes = get(NODES_KEY);
		if (nodes == null)
			nodes = new HashSet<Node>();
		return nodes;
	}

	public void setNodes(Set<Node> nodes) throws InterruptedException, ExecutionException, IOException {
		set(NODES_KEY, nodes);
	}


	/**
	 * Get serializable object from store
	 * null if none
	 * @return Object
	 * @throws ExecutionException
	 * @throws InterruptedException
	 * @throws IOException
	 * @throws ClassNotFoundException
	 */
	@SuppressWarnings("unchecked")
	private<T extends Object> T get(String key) throws InterruptedException, ExecutionException, IOException, ClassNotFoundException {
		byte[] existingNodes = zkState.fetch(key).get().value();
		if(existingNodes.length >0){
			ByteArrayInputStream bis = new ByteArrayInputStream(existingNodes);
			ObjectInputStream in = null;
			try {
				in = new ObjectInputStream(bis);
				return (T) in.readObject();
			} finally {
				try {
					bis.close();
				} finally {
					if(in != null) {
						in.close();
					}
				}
			}
		} else {
			return null;
		}
	}

	/**
	 * Set serializable object in store
	 * @throws ExecutionException
	 * @throws InterruptedException
	 * @throws IOException
	 *
	 */
	private<T extends Object> void set(String key, T object) throws InterruptedException, ExecutionException, IOException {
		Variable value = zkState.fetch(key).get();
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		ObjectOutputStream out = null;
		try {
			out = new ObjectOutputStream(bos);
			out.writeObject(object);
			value = value.mutate(bos.toByteArray());
			zkState.store(value).get();
		} finally {
			try {
				if(out != null) {
					out.close();
				}
			} finally {
				bos.close();
			}
		}
	}

}
