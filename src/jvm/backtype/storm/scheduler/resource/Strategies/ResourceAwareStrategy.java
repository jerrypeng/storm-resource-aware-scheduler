package backtype.storm.scheduler.resource.Strategies;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.TreeMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.scheduler.Cluster;
import backtype.storm.scheduler.ExecutorDetails;
import backtype.storm.scheduler.Globals;
import backtype.storm.scheduler.Topologies;
import backtype.storm.scheduler.TopologyDetails;
import backtype.storm.scheduler.resource.Component;
import backtype.storm.scheduler.resource.GetStats;
import backtype.storm.scheduler.resource.GlobalResources;
import backtype.storm.scheduler.resource.GlobalState;
import backtype.storm.scheduler.resource.Node;

public class ResourceAwareStrategy implements IStrategy {
	protected Logger LOG = null;
	protected GlobalState _globalState;
	protected GlobalResources _globalResources;
	protected GetStats _getStats;
	protected Cluster _cluster;
	protected Topologies _topologies;
	protected TopologyDetails _topo;
	protected Collection<Node> _availNodes;
	protected Node refNode = null;

	private final Double CPU_WEIGHT = 1.0;
	private final Double MEM_WEIGHT = 1.0;
	private final Double NETWORK_WEIGHT = 1.0;

	public ResourceAwareStrategy(GlobalState globalState,
			GlobalResources globalResources, GetStats getStats,
			TopologyDetails topo, Cluster cluster, Topologies topologies) {
		this._globalState = globalState;
		this._getStats = getStats;
		this._cluster = cluster;
		this._topologies = topologies;
		this._topo = topo;
		this._availNodes = this.getAvaiNodes();
		this._globalResources = globalResources;

		this.LOG = LoggerFactory.getLogger(this.getClass());
	}

	protected void prepare() {
		LOG.info("Clustering info: {}", this._globalState.clusteringInfo);
	}

	protected TreeMap<Integer, List<ExecutorDetails>> getCompToComponent(
			Queue<Component> comps) {
		TreeMap<Integer, List<ExecutorDetails>> retMap = new TreeMap<Integer, List<ExecutorDetails>>();
		Integer rank = 0;
		for (Component comp : comps) {
			retMap.put(rank, new ArrayList<ExecutorDetails>());
			retMap.get(rank).addAll(comp.execs);
			rank++;
		}
		return retMap;
	}

	private boolean checkDone(TreeMap<Integer, List<ExecutorDetails>> taskPriority) {
		for(Entry<Integer, List<ExecutorDetails>> i : taskPriority.entrySet()) {
			if(i.getValue().isEmpty()== false) {
				return true;
			}
		}
		
		return false;
	}
	public Map<Node, Collection<ExecutorDetails>> schedule(TopologyDetails td,
			Collection<ExecutorDetails> unassignedExecutors) {
		if (this._availNodes.size() <= 0) {
			LOG.warn("No available nodes to schedule tasks on!");
			return null;
		}

		Map<Node, Collection<ExecutorDetails>> taskToNodeMap = new HashMap<Node, Collection<ExecutorDetails>>();
		LOG.debug("ExecutorsNeedScheduling: {}", unassignedExecutors);
		Collection<ExecutorDetails> scheduledTasks = new ArrayList<ExecutorDetails>();
		Component root = this.getSpout();
		if (root == null) {
			LOG.error("Cannot find a Spout!");
			return null;
		}
		Queue<Component> comps = this.bfs(root);
		LOG.info("PriorityQueue: {}", comps);
		TreeMap<Integer, List<ExecutorDetails>> taskPriority = this
				.getCompToComponent(comps);
		LOG.info("taskPriority: {}", taskPriority);

		while (checkDone(taskPriority)==true) {
			
			for (Entry<Integer, List<ExecutorDetails>> entry : taskPriority
					.entrySet()) {
				if (entry.getValue().isEmpty() != true) {
					for (Iterator<ExecutorDetails> it = entry.getValue().iterator(); it
							.hasNext();) {
						ExecutorDetails exec = it.next(); 
						LOG.info("\n\nAttempting to schedule: {} of component {} with rank {}", new Object[]{exec, this._topo.getExecutorToComponent().get(exec), entry.getKey()});
						Node n = this.getNode(exec);
						if (n != null) {
							if (taskToNodeMap.containsKey(n) == false) {
								Collection<ExecutorDetails> newMap = new LinkedList<ExecutorDetails>();
								taskToNodeMap.put(n, newMap);
							}
							taskToNodeMap.get(n).add(exec);
							n.consumeResourcesforTask(exec, td.getId(),
									this._globalResources);
							scheduledTasks.add(exec);
							LOG.info(
									"TASK {} assigned to NODE {} -- AvailMem: {} AvailCPU: {}",
									new Object[] { exec, n,
											n.getAvailableMemoryResources(),
											n.getAvailableCpuResources() });
						} else {
							LOG.error(
									"Not Enough Resources to schedule Task {}",
									exec);
						}
						it.remove();
						break;
					}
				}
			}
		}

		// for(Component comp : comps) {
		// LOG.info("Scheduling component: {}", comp.id);
		// for(ExecutorDetails exec : comp.execs){
		// LOG.info("\n\nAttempting to schedule: {}", exec);
		// Node n = this.getNode(exec);
		// if (n != null) {
		// if (taskToNodeMap.containsKey(n) == false) {
		// Collection<ExecutorDetails> newMap = new
		// LinkedList<ExecutorDetails>();
		// taskToNodeMap.put(n, newMap);
		// }
		// taskToNodeMap.get(n).add(exec);
		// n.consumeResourcesforTask(exec, td.getId(),
		// this._globalResources);
		// scheduledTasks.add(exec);
		// LOG.info(
		// "TASK {} assigned to NODE {} -- AvailMem: {} AvailCPU: {}",
		// new Object[] { exec, n,
		// n.getAvailableMemoryResources(),
		// n.getAvailableCpuResources() });
		// } else {
		// LOG.error("Not Enough Resources to schedule Task {}", exec);
		// }
		// }
		// }

		Collection<ExecutorDetails> tasksNotScheduled = new ArrayList<ExecutorDetails>(
				unassignedExecutors);
		tasksNotScheduled.removeAll(scheduledTasks);
		// schedule left over system tasks
//		for (ExecutorDetails exec : tasksNotScheduled) {
//			Node n = this.getBestNode(exec);
//			if (n != null) {
//				if (taskToNodeMap.containsKey(n) == false) {
//					Collection<ExecutorDetails> newMap = new LinkedList<ExecutorDetails>();
//					taskToNodeMap.put(n, newMap);
//				}
//				taskToNodeMap.get(n).add(exec);
//				n.consumeResourcesforTask(exec, td.getId(),
//						this._globalResources);
//				scheduledTasks.add(exec);
//				LOG.info(
//						"TASK {} assigned to NODE {} -- AvailMem: {} AvailCPU: {}",
//						new Object[] { exec, n,
//								n.getAvailableMemoryResources(),
//								n.getAvailableCpuResources() });
//			} else {
//				LOG.error("Not Enough Resources to schedule Task {}", exec);
//			}
//		}
		
		//round robin system tasks
		Iterator it = this._availNodes.iterator();
		for (ExecutorDetails exec : tasksNotScheduled) {
			if(it.hasNext()==false) {
				it = this._availNodes.iterator();
			} 
			if(it.hasNext()==true) {
				Node n=(Node)it.next();
				if (taskToNodeMap.containsKey(n) == false) {
					Collection<ExecutorDetails> newMap = new LinkedList<ExecutorDetails>();
					taskToNodeMap.put(n, newMap);
				}
				taskToNodeMap.get(n).add(exec);
			}
			
		}
		

		tasksNotScheduled = new ArrayList<ExecutorDetails>(unassignedExecutors);
		tasksNotScheduled.removeAll(scheduledTasks);
		if (tasksNotScheduled.size() > 0) {
			LOG.error("Resources not successfully scheduled: {}",
					tasksNotScheduled);
			taskToNodeMap = null;
		} else {
			LOG.debug("All resources successfully scheduled!");
		}

		if (taskToNodeMap == null) {
			LOG.error("Topology {} not successfully scheduled!", td.getId());
		}

		return taskToNodeMap;
	}

	public String getBestCluster() {
		String bestCluster = null;
		Double mostRes = 0.0;
		for (Entry<String, List<String>> cluster : this._globalState.clusteringInfo
				.entrySet()) {
			Double clusterTotalRes = getTotalClusterRes(cluster.getValue());
			if (clusterTotalRes > mostRes) {
				mostRes = clusterTotalRes;
				bestCluster = cluster.getKey();
			}
		}
		return bestCluster;
	}

	public Double getTotalClusterRes(List<String> cluster) {
		Double res = 0.0;
		for (String node : cluster) {
			LOG.info("node: {}", node);
			res += this._globalState.nodes.get(this.NodeHostnameToId(node))
					.getAvailableMemoryResources()
					+ this._globalState.nodes.get(this.NodeHostnameToId(node))
							.getAvailableMemoryResources();
		}
		return res;
	}

	public Node getNode(ExecutorDetails exec) {
		// first scheduling
		Node n = null;
		if (this.refNode == null) {
			String clus = this.getBestCluster();
			n = this.getBestNodeInCluster_Mem_CPU(clus, exec);
			this.refNode = n;
			LOG.info("refNode: {}", this.refNode.hostname);
		} else {
			n = this.getBestNode(exec);
		}

		return n;
	}

	public Node getBestNode(ExecutorDetails exec) {
		Double taskMem = this._globalResources.getTotalMemReqTask(
				this._topo.getId(), exec);
		Double taskCPU = this._globalResources.getTotalCpuReqTask(
				this._topo.getId(), exec);
		Double shortestDistance = Double.POSITIVE_INFINITY;
		Node closestNode = null;
		for (Node n : this._availNodes) {
			// hard constraint
			if (n.getAvailableMemoryResources() >= taskMem
					&& n.getAvailableCpuResources() >= taskCPU) {
				Double a = Math.pow((taskCPU - n.getAvailableCpuResources())
						* this.CPU_WEIGHT, 2);
				Double b = Math.pow((taskMem - n.getAvailableMemoryResources())
						* this.MEM_WEIGHT, 2);
				Double c = Math.pow(this.distToNode(this.refNode, n)
						* this.NETWORK_WEIGHT, 2);
				Double distance = Math.sqrt(a + b + c);
				if (shortestDistance > distance) {
					shortestDistance = distance;
					closestNode = n;
				}
			}
		}
		return closestNode;
	}

	Double distToNode(Node src, Node dest) {
		if (this.NodeToCluster(src) == this.NodeToCluster(dest)) {
			return 1.0;
		} else {
			return 2.0;
		}
	}

	public String NodeToCluster(Node src) {
		for (Entry<String, List<String>> entry : this._globalState.clusteringInfo
				.entrySet()) {
			if (entry.getValue().contains(src.hostname)) {
				return entry.getKey();
			}
		}
		LOG.error("Node: {} not found in any clusters", src.hostname);
		return null;
	}

	public List<Node> getNodesFromCluster(String clus) {
		List<Node> retList = new ArrayList<Node>();
		for (String node_id : this._globalState.clusteringInfo.get(clus)) {
			retList.add(this._globalState.nodes.get(this
					.NodeHostnameToId(node_id)));
		}
		return retList;
	}

	public Node getBestNodeInCluster_Mem_CPU(String clus, ExecutorDetails exec) {
		Double taskMem = this._globalResources.getTotalMemReqTask(
				this._topo.getId(), exec);
		Double taskCPU = this._globalResources.getTotalCpuReqTask(
				this._topo.getId(), exec);
		Collection<Node> NodeMap = this.getNodesFromCluster(clus);
		Double shortestDistance = Double.POSITIVE_INFINITY;
		String msg = "";
		LOG.info("exec: {} taskMem: {} taskCPU: {}", new Object[] { exec,
				taskMem, taskCPU });
		Node closestNode = null;
		LOG.info("NodeMap.size: {}", NodeMap.size());
		LOG.info("NodeMap: {}", NodeMap);
		for (Node n : NodeMap) {
			if (n.getAvailableMemoryResources() >= taskMem
					&& n.getAvailableCpuResources() >= taskCPU
					&& n.totalSlotsFree() > 0) {
				Double distance = Math
						.sqrt(Math.pow(
								(taskMem - n.getAvailableMemoryResources()), 2)
								+ Math.pow(
										(taskCPU - n.getAvailableCpuResources()),
										2));
				msg = msg + "{" + n.getId() + "-" + distance.toString() + "}";
				if (distance < shortestDistance) {
					closestNode = n;
					shortestDistance = distance;
				}
			}
		}
		if (closestNode != null) {
			LOG.info(msg);
			LOG.info("node: {} distance: {}", closestNode, shortestDistance);
			LOG.info("node availMem: {}",
					closestNode.getAvailableMemoryResources());
			LOG.info("node availCPU: {}",
					closestNode.getAvailableCpuResources());
		}
		return closestNode;
	}

	protected Collection<Node> getAvaiNodes() {
		return this._globalState.nodes.values();
	}

	public Queue<Component> bfs(Component root) {
		// Since queue is a interface
		Queue<Component> retList = new LinkedList<Component>();
		HashMap<String, Component> visited = new HashMap<String, Component>();
		Queue<Component> queue = new LinkedList<Component>();

		if (root == null)
			return null;

		// Adds to end of queue
		queue.add(root);
		visited.put(root.id, root);
		retList.add(root);

		while (!queue.isEmpty()) {
			// removes from front of queue
			Component r = queue.remove();

			// System.out.print(r.getVertex() + "\t");

			// Visit child first before grandchild
			List<String> neighbors = new ArrayList<String>();
			neighbors.addAll(r.children);
			neighbors.addAll(r.parents);
			for (String comp : neighbors) {
				if (visited.containsKey(comp) == false) {
					Component child = this._globalState.components.get(
							this._topo.getId()).get(comp);
					queue.add(child);
					visited.put(child.id, child);
					retList.add(child);
				}
			}

		}
		return retList;
	}

	public Component getSpout() {
		for (Component c : this._globalState.components.get(this._topo.getId())
				.values()) {
			if (c.type == Component.ComponentType.SPOUT) {
				return c;
			}
		}
		return null;
	}

	public String NodeHostnameToId(String hostname) {
		for (Node n : this._globalState.nodes.values()) {
			if (n.hostname.equals(hostname) == true) {
				return n.supervisor_id;
			}
		}
		LOG.error("Cannot find Node with hostname {}", hostname);
		return null;
	}

}
