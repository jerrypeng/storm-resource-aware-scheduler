package backtype.storm.scheduler.resource.Strategies;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.TreeMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.scheduler.Cluster;
import backtype.storm.scheduler.ExecutorDetails;
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

	protected Node strategy(Collection<Node> NodeMap, String topoId,
			ExecutorDetails exec) {
		Double taskMem = this._globalResources.getTotalMemReqTask(topoId, exec);
		Double taskCPU = this._globalResources.getTotalCpuReqTask(topoId, exec);
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

	public Map<Node, Collection<ExecutorDetails>> schedule(TopologyDetails td,
			Collection<ExecutorDetails> unassignedExecutors) {
		if (this._availNodes.size() <= 0) {
			LOG.warn("No available nodes to schedule tasks on!");
			return null;
		}

		Map<Node, Collection<ExecutorDetails>> taskToNodeMap = new HashMap<Node, Collection<ExecutorDetails>>();
		LOG.debug("ExecutorsNeedScheduling: {}", unassignedExecutors);
		Collection<ExecutorDetails> scheduledTasks = new ArrayList<ExecutorDetails>();
		for (ExecutorDetails exec : unassignedExecutors) {
			LOG.info("\n\nAttempting to schedule: {}", exec);
			Node n = this.getBestNode(this._availNodes, td.getId(), exec);
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
				LOG.error("Not Enough Resources to schedule Task {}", exec);
			}
		}
		Collection<ExecutorDetails> tasksNotScheduled = new ArrayList<ExecutorDetails>(
				unassignedExecutors);
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

	public Node getBestNode(Collection<Node> NodeMap, String topoId,
			ExecutorDetails exec) {
		return strategy(NodeMap, topoId, exec);
	}

	protected Collection<Node> getAvaiNodes() {
		return this._globalState.nodes.values();
	}

}
