package backtype.storm.scheduler.resource;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.scheduler.Cluster;
import backtype.storm.scheduler.ExecutorDetails;
import backtype.storm.scheduler.Globals;
import backtype.storm.scheduler.SchedulerAssignment;
import backtype.storm.scheduler.SupervisorDetails;
import backtype.storm.scheduler.Topologies;
import backtype.storm.scheduler.TopologyDetails;
import backtype.storm.scheduler.WorkerSlot;

public class GlobalState {

	private static final Logger LOG = LoggerFactory
			.getLogger(GlobalState.class);
	
	private static GlobalState instance = null;
	
	/**
	 * supervisor id -> Node
	 */
	public Map<String, Node> nodes;
	
	/**
	 * topology id -> <component name - > Component>
	 */
	public Map<String, Map<String, Component>>components;
	
	/**
	 * Topology id -> <worker slot -> collection<executors>>
	 */
	public Map <String, Map<WorkerSlot, List<ExecutorDetails>>> schedState;
	
	/**
	 * Topology id -> num of workers
	 */
	public Map<String, Integer> topoWorkers = new HashMap<String, Integer>();

	
	public Map<String, List<String>> clusteringInfo;
	
	private File scheduling_log;
	
	public boolean isBalanced = false;
	
	private GlobalState(String filename) {
		this.schedState = new HashMap<String, Map<WorkerSlot, List<ExecutorDetails>>>();
		this.scheduling_log = new File(Config.LOG_PATH + filename + "_SchedulingInfo");
		this.clusteringInfo = (new GetNetworkInfo()).getClusterInfo();
		try {
			this.scheduling_log.delete();
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}

	public static GlobalState getInstance(String filename) {
		if(instance==null) {
			instance = new GlobalState(filename);
		}
		return instance;
	}
	
	public void storeState(Cluster cluster, Topologies topologies, GlobalResources globalResources) {
		this.storeSchedState(cluster, topologies, globalResources);
	}
	
	public boolean stateEmpty() {
		return this.schedState.isEmpty();
	}
	
	public void storeSchedState(Cluster cluster, Topologies topologies, GlobalResources globalResources) {
		HashMap<String, Map<WorkerSlot, List<ExecutorDetails>>> sched_state = new HashMap<String, Map<WorkerSlot, List<ExecutorDetails>>>();
		for(TopologyDetails topo : topologies.getTopologies()) {
			if(cluster.getAssignmentById(topo.getId())!=null) {
				
				Map<WorkerSlot, List<ExecutorDetails>> topoSched = new HashMap<WorkerSlot, List<ExecutorDetails>>();
				for(Map.Entry<ExecutorDetails, WorkerSlot> entry : cluster.getAssignmentById(topo.getId()).getExecutorToSlot().entrySet()) {
					if(topoSched.containsKey(entry.getValue()) == false) {
						topoSched.put(entry.getValue(), new ArrayList<ExecutorDetails>());
					}
					topoSched.get(entry.getValue()).add(entry.getKey());
				}
				
				sched_state.put(topo.getId(), topoSched);
				
			}
			
		}
		for (Map.Entry<String, Map<WorkerSlot, List<ExecutorDetails>>> i : sched_state
				.entrySet()) {
			if (this.schedState.containsKey(i.getKey()) == false
					|| i.getValue().hashCode() != this.schedState.get(
							i.getKey()).hashCode()) {
				this.logSchedChange(i.getValue(),
						topologies.getById(i.getKey()), globalResources);
			}
		}
		this.schedState = new HashMap<String, Map<WorkerSlot, List<ExecutorDetails>>>();
		this.schedState.putAll(sched_state);
	}
	
	public void logSchedChange(
			Map<WorkerSlot, List<ExecutorDetails>> sched_state,
			TopologyDetails topo, GlobalResources globalResources) {
		Map<String, Map<WorkerSlot, List<ExecutorDetails>>> node_to_worker = new HashMap<String, Map<WorkerSlot, List<ExecutorDetails>>>();
		for (Node n : this.nodes.values()) {
			node_to_worker.put(n.supervisor_id,
					new HashMap<WorkerSlot, List<ExecutorDetails>>());
			for (WorkerSlot ws : n.slots) {
				node_to_worker.get(n.supervisor_id).put(ws,
						new ArrayList<ExecutorDetails>());
			}
		}

		for (Map.Entry<WorkerSlot, List<ExecutorDetails>> k : sched_state
				.entrySet()) {
			node_to_worker.get(k.getKey().getNodeId()).get(k.getKey())
					.addAll(k.getValue());
		}
		
		String data = "\n\n<!---Scheduling Change---!>\n";
		for (Map.Entry<String, Map<WorkerSlot, List<ExecutorDetails>>> i : node_to_worker
				.entrySet()) {
			data += "->hostname: " + this.nodes.get(i.getKey()).hostname
					+ " Supervisor Id: " + i.getKey() +"Cluster: "+HelperFuncs.nodeToCluster(this.nodes.get(i.getKey()).hostname, this.clusteringInfo) +"\n";
			data += "->WorkerToExec: \n";
			TreeMap<String, Integer> componentOnNodeCount = new TreeMap<String, Integer>();
			for (Map.Entry<WorkerSlot, List<ExecutorDetails>> entry : i
					.getValue().entrySet()) {
				data += "-->" + entry.getKey().getPort() + " => "
						+ entry.getValue().toString() + "\n";

				
				TreeMap<String, Integer> count = new TreeMap<String, Integer>();
				for (ExecutorDetails ex : entry.getValue()) {
					String comp = topo.getExecutorToComponent().get(ex);
					// Per Node component count
					if (componentOnNodeCount.containsKey(comp) == false) {
						componentOnNodeCount.put(comp, 0);
					}
					componentOnNodeCount.put(comp,
							componentOnNodeCount.get(comp) + 1);
					// Per Slot component count
					if (count.containsKey(comp) == false) {
						count.put(comp, 0);
					}
					count.put(comp, count.get(comp) + 1);
				}
				
				for(Map.Entry<String, Integer> c : count.entrySet()) {
					data+="{"+c.getKey()
							+"[CPU:"+globalResources.getTotalCpuReqComp(topo.getId(), c.getKey())
							+"&Mem:"+globalResources.getTotalMemReqComp(topo.getId(), c.getKey())+"]-"+c.getValue()+"}";
				}
//				if (count.size() > 0) {
//					data += "        =>" + count.toString() + "\n";
//				}

			}
			data += "->Overall Component Count:"
					+ componentOnNodeCount.toString() + "\n\n";

		}

		HelperFuncs.writeToFile(this.scheduling_log, data);

	}
	
	public void clearStoreState() {
		this.schedState = new HashMap<String, Map<WorkerSlot, List<ExecutorDetails>>>();
	}
	
	public void updateInfo(Cluster cluster, Topologies topologies, GlobalResources globalResources) {
		this.nodes = this.getNodes(cluster, globalResources);
		this.components = this.getComponents(topologies);
		this.checkResourceSet(topologies, globalResources, cluster);
	}

	private  Map<String, Map<String, Component>> getComponents(Topologies topologies) {
		Map<String, Map<String, Component>> retVal = new HashMap<String, Map<String, Component>>();
		this.topoWorkers = new HashMap<String, Integer>();
		GetTopologyInfo gt = new GetTopologyInfo();
		
		for(TopologyDetails topo : topologies.getTopologies()) {
			gt.getTopologyInfo(topo.getId());
			for(Component comp : gt.all_comp.values()) {
				comp.execs = HelperFuncs.compToExecs(topo, comp.id);
			}
			retVal.put(topo.getId(), gt.all_comp);
			
			this.topoWorkers.put(topo.getId(), gt.numWorkers);
		}
		return retVal;
	}
	
	private Map<String, Node> getNodes(Cluster cluster, GlobalResources globalResources) {
		Map<String, Node> retVal = Node.getAllNodesFrom(cluster, globalResources);
		for (Map.Entry<String, SchedulerAssignment> entry : cluster
				.getAssignments().entrySet()) {
			for (Map.Entry<ExecutorDetails, WorkerSlot> exec : entry.getValue()
					.getExecutorToSlot().entrySet()) {
				if (retVal.containsKey(exec.getValue().getNodeId()) == true) {
					if (retVal.get(exec.getValue().getNodeId()).slot_to_exec
							.containsKey(exec.getValue()) == true) {
						retVal.get(exec.getValue().getNodeId()).slot_to_exec
								.get(exec.getValue()).add(exec.getKey());
						retVal.get(exec.getValue().getNodeId()).execs.add(exec
								.getKey());
					} else {
						LOG.info(
								"ERROR: should have node {} should have worker: {}",
								exec.getValue().getNodeId(), exec.getValue());
						return null;
					}
				} else {
					LOG.info("ERROR: should have node {}", exec.getValue()
							.getNodeId());
					return null;
				}
			}
		}
		
		return retVal;
	}
	
	/**
	 * migrate exec to ws
	 * @param exec
	 * @param ws
	 */
	public void migrateTask(ExecutorDetails exec, WorkerSlot ws, TopologyDetails topo) {
		Map<WorkerSlot, List<ExecutorDetails>> schedMap = this.schedState.get(topo.getId());
		
		if(this.execExist(exec, topo) == false) {
			LOG.error("Error: exec {} does not exist!", exec);
			return;
		}
		
		if(schedMap.containsKey(ws)==false) {
			schedMap.put(ws, new ArrayList<ExecutorDetails>());
		}
		
		for(Map.Entry<WorkerSlot, List<ExecutorDetails>> sched : schedMap.entrySet()) {
			if(sched.getValue().contains(exec) == true) {
				sched.getValue().remove(exec);
			}
		}
		
		schedMap.get(ws).add(exec);
	}
	
	public boolean execExist(ExecutorDetails exec, TopologyDetails topo) {
		Map<WorkerSlot, List<ExecutorDetails>> schedMap = this.schedState.get(topo.getId());
		for(Map.Entry<WorkerSlot, List<ExecutorDetails>> sched : schedMap.entrySet()) {
			if(sched.getValue().contains(exec) == true) {
				return true;
			}
		}
		return false;
	}
	
	public List<Node> getNewNode () {
		
		List<Node> retVal = new ArrayList<Node>();
		retVal.addAll(this.nodes.values());
		
		for(Map.Entry<String, Map<WorkerSlot, List<ExecutorDetails>>> i : this.schedState.entrySet()) {
			for(Map.Entry<WorkerSlot, List<ExecutorDetails>> k : i.getValue().entrySet()) {
				if(k.getValue().size() > 0 ){
					for(Node n : this.nodes.values()) {
						if(n.slots.contains(k.getKey())){
							retVal.remove(n);
						}
					}
				}
			}
		}
		/*
		for (Map.Entry<String, Node> n : this.nodes.entrySet()) {
			if(n.getValue().execs.size()==0) {
				retVal.add(n.getValue());
			}
		}
		*/
		return retVal;
	}
	public String ComponentsToString() {
		String str = "";
		str+="\n!--Components--!\n";
		for(Map.Entry<String, Map<String, Component>> entry : this.components.entrySet()) {
			str+="->Topology: "+entry.getKey()+"\n";
			for (Map.Entry<String, Component> comp : entry.getValue().entrySet()) {
				str+="-->Component: "+comp.getValue().id+"=="+entry.getKey()+"\n";
				str+="--->Parents: "+comp.getValue().parents+"\n";
				str+="--->Children: "+comp.getValue().children+"\n";
				str+="--->execs: " + comp.getValue().execs+"\n\n";
			}
			str+="\n";
		}
		return str;
	}
	
	public String NodesToString() {
		String str = "";
		str+="\n!--Nodes--! \n";
		for (Map.Entry<String, Node> n : this.nodes.entrySet()) {
			str+="->hostname: "+n.getValue().hostname+" Supervisor Id: "+n.getValue().supervisor_id+"\n";
			str+="->Execs: "+n.getValue().execs+"\n";
			str+="->WorkerToExec: \n";
			for(Map.Entry<WorkerSlot, List<ExecutorDetails>> entry : n.getValue().slot_to_exec.entrySet()) {
				str+="-->"+entry.getKey().getPort()+" => "+entry.getValue()+"\n";
			}
		str+="\n";
		}
		return str;
	}
	
	public String ClusterInfoToString() {
		String str = "";
		str+="\n!--ClusterInfo--! \n";
		for(Entry<String, List<String>> entry : this.clusteringInfo.entrySet()) {
			str+="\n->Cluster "+entry.getKey()+"\n";
			str+="-->nodes: "+entry.getValue().toString()+"\n";
		}
		return str;
	}
	
	public String StoredStateToString() {
		String str="";
		str+="\n!--Stored Scheduling State--!\n";
		for(Map.Entry<String, Map<WorkerSlot, List<ExecutorDetails>>> entry : this.schedState.entrySet()) {
			str+="->Topology: "+entry.getKey()+"\n";
			for(Map.Entry<WorkerSlot, List<ExecutorDetails>> sched : entry.getValue().entrySet()) {
				str+="-->WorkerSlot: "+sched.getKey().getNodeId()+":"+sched.getKey().getNodeId()+"\n";
				str+=sched.getValue()+"\n\n";
			}
			str+="\n";
		}
		return str;
	}
	
	@Override 
	public String toString(){
		String str="";
		str+=this.NodesToString();
		str+=this.ComponentsToString();
		str+=this.StoredStateToString();
		str+=this.ClusterInfoToString();
		str+="\n topWorkers: "+ this.topoWorkers+"\n";
		
		return str;
	}
	
	private Map<String, Boolean> log_scheduling_info = new HashMap<String, Boolean>();
	public void logTopologyInfo(TopologyDetails topo){
		if(this.components.size()>0) {
			File file= this.scheduling_log;
			if(this.log_scheduling_info.containsKey(topo.getId())==false) {
				this.log_scheduling_info.put(topo.getId(), false);
			}
			if(log_scheduling_info.get(topo.getId())==false) {
				String data = "\n\n<!---Topology Info---!>\n";
				data+=this.ComponentsToString();
				
				HelperFuncs.writeToFile(file, data);
				this.log_scheduling_info.put(topo.getId(), true);
			}
		}
	}
	public void checkResourceSet(Topologies topologies, GlobalResources globalResources, Cluster cluster) {
		for (TopologyDetails td : topologies.getTopologies()) {
			//LOG.info("cluster.getUnassignedExecutors(td): {}", cluster.getUnassignedExecutors(td));
			for (ExecutorDetails exec : cluster.getUnassignedExecutors(td)) {
				//LOG.info("exec: {} comp: {}", exec, td.getExecutorToComponent().get(exec));
				if (!globalResources.hasExecInTopo(td.getId(), exec)) {
					
					if (td.getExecutorToComponent().get(exec)
							.compareTo("__acker") == 0) {
						LOG.warn(
								"Scheduling __acker {} with memory requirement as {} - {} and {} - {} and CPU requirement as {}-{}",
								new Object[] {
										exec,
										Globals.TYPE_MEMORY_ONHEAP,
										Globals.DEFAULT_ONHEAP_MEMORY_REQUIREMENT,
										Globals.TYPE_MEMORY_OFFHEAP,
										Globals.DEFAULT_OFFHEAP_MEMORY_REQUIREMENT,
										Globals.TYPE_CPU_TOTAL,
										Globals.DEFAULT_CPU_REQUIREMENT });
						globalResources.addExecutorResourceReqDefault(
								exec, td.getId());
					} else {
						LOG.warn(
								"Executor {} of Component: {} does not have a set memory resource requirement!",
								exec, td.getExecutorToComponent().get(exec));
						globalResources.addExecutorResourceReqDefault(
								exec, td.getId());
					}
				}
			}
	  }
	}
}