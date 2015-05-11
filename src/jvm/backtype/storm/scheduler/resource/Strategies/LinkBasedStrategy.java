package backtype.storm.scheduler.resource.Strategies;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.TreeMap;
import java.util.Map.Entry;

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

public class LinkBasedStrategy {
	protected Logger LOG = null;
	protected GlobalState _globalState;
	protected GlobalResources _globalResources;
	protected GetStats _getStats;
	protected Cluster _cluster;
	protected Topologies _topologies;
	protected TopologyDetails _topo;
	public LinkBasedStrategy(GlobalState globalState,
			GlobalResources globalResources, GetStats getStats,
			TopologyDetails topo, Cluster cluster, Topologies topologies) {
		this._globalState = globalState;
		this._getStats = getStats;
		this._cluster = cluster;
		this._topologies = topologies;
		this._topo = topo;
		this._globalResources = globalResources;

		this.LOG = LoggerFactory.getLogger(this.getClass());
	}
	public Map<Node, Collection<ExecutorDetails>> schedule(TopologyDetails td,
			Collection<ExecutorDetails> unassignedExecutors) {
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

		Iterator it_node = this._globalState.nodes.values().iterator();
		while (checkDone(taskPriority)==true) {
			for (Entry<Integer, List<ExecutorDetails>> entry : taskPriority
					.entrySet()) {
				if (entry.getValue().isEmpty() != true) {
					for (Iterator<ExecutorDetails> it = entry.getValue().iterator(); it
							.hasNext();) {
						ExecutorDetails exec = it.next(); 
						LOG.info("\n\nAttempting to schedule: {} of component {} with rank {}", new Object[]{exec, this._topo.getExecutorToComponent().get(exec), entry.getKey()});
						if(it_node.hasNext()==false) {
							it_node =this._globalState.nodes.values().iterator();
						}
						Node n = (Node)it_node.next();
						if (n != null) {
							if (taskToNodeMap.containsKey(n) == false) {
								Collection<ExecutorDetails> newMap = new LinkedList<ExecutorDetails>();
								taskToNodeMap.put(n, newMap);
							}
							taskToNodeMap.get(n).add(exec);
							
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
		
		Collection<ExecutorDetails> tasksNotScheduled = new ArrayList<ExecutorDetails>(
				unassignedExecutors);
		tasksNotScheduled.removeAll(scheduledTasks);
		
		Iterator it = this._globalState.nodes.values().iterator();
		for (ExecutorDetails exec : tasksNotScheduled) {
			if(it.hasNext()==false) {
				it = this._globalState.nodes.values().iterator();
			} 
			if(it.hasNext()==true) {
				Node n=(Node)it.next();
				if (taskToNodeMap.containsKey(n) == false) {
					Collection<ExecutorDetails> newMap = new LinkedList<ExecutorDetails>();
					taskToNodeMap.put(n, newMap);
				}
				taskToNodeMap.get(n).add(exec);
				scheduledTasks.add(exec);
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
	
	private boolean checkDone(TreeMap<Integer, List<ExecutorDetails>> taskPriority) {
		for(Entry<Integer, List<ExecutorDetails>> i : taskPriority.entrySet()) {
			if(i.getValue().isEmpty()== false) {
				return true;
			}
		}
		
		return false;
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

}
