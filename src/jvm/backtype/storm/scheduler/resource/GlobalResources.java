package backtype.storm.scheduler.resource;

import java.util.Map;
import java.util.HashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.scheduler.Cluster;
import backtype.storm.scheduler.ExecutorDetails;
import backtype.storm.scheduler.Globals;
import backtype.storm.scheduler.Topologies;
import backtype.storm.scheduler.TopologyDetails;

/**
 * A storage class for All topology resource requirements.
 */
public class GlobalResources {

  /**
   * Map of all resource requirements
   * Map<String - Toplogy Id, Map<ExecutorDetails - Task, 
   * Map<String - Resource Type i.e memory, cpu & etc, 
   * Map<String - Type of that resource i.e. offheap, onheap, 
   * Double - amount of this resource>>>>.
   */
  private Map<String, Map<ExecutorDetails, Map<String, Map<String, Double>>>> _globalResourceList;


  /**
   * For logging and error messages.
   */
  private static final Logger LOG = LoggerFactory.getLogger(GlobalResources.class);
  
  /**
   * constructor of this class.
   * @param topologies a list of topologies.
   */
  public GlobalResources(Cluster cluster, Topologies topologies) {
    this.getGlobalResourceList(cluster, topologies);
    
    if (_globalResourceList == null) {
      _globalResourceList = new HashMap<String, Map<ExecutorDetails, Map<String, Map<String, Double>>>>();
    }
  }
  
  public void getGlobalResourceList(Cluster cluster, Topologies topologies) {
		this._globalResourceList = topologies.getGlobalResourceReqList();
		for (TopologyDetails td : topologies.getTopologies()) {
			for (ExecutorDetails exec : cluster.getUnassignedExecutors(td)) {
				if (!this.hasExecInTopo(td.getId(), exec)) {
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
						this.addExecutorResourceReqDefault(
								exec, td.getId());
					} else {
						LOG.warn(
								"Executor {} of Component: {} does not have a set memory resource requirement!",
								exec, td.getExecutorToComponent().get(exec));
						this.addExecutorResourceReqDefault(
								exec, td.getId());
					}
				}
			}
	  }
  }

  /**
   * Gets the on heap memory requirement for a
   * certain task within a topology
   * @param topoId the topology in which the task
   * is apart of.
   * @param exec the executor the inquiry is concerning.
   * @return Double the amount of on heap memory
   * requirement for this exec in topology topoId.
   */
  public Double getOnHeapMemoryRequirement(String topoId, ExecutorDetails exec) {
    Double ret = null;
    if (hasExecInTopo(topoId, exec)) {
      ret = _globalResourceList.get(topoId)
          .get(exec).get(Globals.TYPE_MEMORY)
          .get(Globals.TYPE_MEMORY_ONHEAP);
      if (ret == null) {
        LOG.error("{} not set!" + Globals.TYPE_MEMORY_ONHEAP);
      }
    }
    return ret;
  }

  /**
   * Gets the off heap memory requirement for a
   * certain task within a topology
   * @param topoId the topology in which the task
   * is apart of.
   * @param exec the executor the inquiry is concerning.
   * @return Double the amount of off heap memory
   * requirement for this exec in topology topoId.
   */
  public Double getOffHeapMemoryRequirement(String topoId, ExecutorDetails exec) {
    Double ret = null;
    if (hasExecInTopo(topoId, exec)) {
      ret = _globalResourceList.get(topoId)
          .get(exec).get(Globals.TYPE_MEMORY)
          .get(Globals.TYPE_MEMORY_OFFHEAP);
      if (ret == null) {
        LOG.error("{} not set!" + Globals.TYPE_MEMORY_OFFHEAP);
      }
    }
    return ret;
  }

  /**
   * Gets the total memory requirement for a task
   * @param topoId the topology in which the task
   * is apart of.
   * @param exec the executor the inquiry is concerning.
   * @return Double the total memory requirement
   *  for this exec in topology topoId.
   */
  public Double getTotalMemReqTask(String topoId, ExecutorDetails exec) {
    if (hasExecInTopo(topoId, exec)) {
      return getOffHeapMemoryRequirement(topoId, exec)
          + getOnHeapMemoryRequirement(topoId, exec);
    }
    LOG.info("cannot find {} - {}", topoId, exec);
    return null;
  }

  /**
   * Gets the total memory resource list for a 
   * set of tasks that is part of a topology.
   * @param topoId the topology in which inquiry is concerning
   * @return Map<ExecutorDetails, Double> a map of the total memory requirement
   *  for all tasks in topology topoId.
   */
  public Map<ExecutorDetails, Double> getTotalMemoryResourceList(String topoId) {
    if (hasTopology(topoId)) {
      Map<ExecutorDetails, Double> ret = new HashMap<ExecutorDetails, Double>();
      for (ExecutorDetails exec : _globalResourceList.get(topoId).keySet()) {
        ret.put(exec, getTotalMemReqTask(topoId, exec));
      }
      return ret;
    }
    return null;
  }

  public Double getTotalCpuReqTask(String topoId, ExecutorDetails exec) {
    if (hasExecInTopo(topoId, exec)) {
      return _globalResourceList.get(topoId).get(exec).get(Globals.TYPE_CPU).get(Globals.TYPE_CPU_TOTAL);
    }
    LOG.info("cannot find {} - {}", topoId, exec);
    return null;
  }

  public Map<String,Map<String,Double>> getTaskResourceReqList(String topoId, ExecutorDetails exec) {
    if (hasExecInTopo(topoId, exec)) {
      return _globalResourceList.get(topoId).get(exec);
    }
    LOG.info("cannot find {} - {}", topoId, exec);
    return null;
  }
  /**
   * gets the size of the _globalResourceList.
   * @return int the size of _globalResourceList.
   */
  public int size() {
    return _globalResourceList.size();
  }

  /**
   * Does the _globalResourceList have resource information
   * regarding a topology.
   * @param topoId the topology id of the topology.
   * @return Boolean whether or not the topology is included in _globalResourceList.
   */
  public boolean hasTopology(String topoId) {
    if (_globalResourceList.containsKey(topoId)) {
      return true;
    }
    LOG.info("Does not have topo {}", topoId);
    return false;
  }

  /**
   * Does the _globalResourceList have resource information
   * regarding a topology.
   * @param topoId the topology id of the topology.
   * @return Boolean whether or not the topology is included in _globalResourceList.
   */
  public boolean hasExecInTopo(String topoId, ExecutorDetails exec) {
    if (hasTopology(topoId)) {
      return _globalResourceList.get(topoId).containsKey(exec);
    }
    return false;
  }

  public void addExecutorResourceReq(ExecutorDetails exec, String topoId, Map<String, Map<String, Double>> resourceList) {
    if (hasExecInTopo(topoId, exec)) {
      LOG.warn("Executor {} in Topo {} already exisits...ResourceList: {}", new Object[]{exec, topoId, getTaskResourceReqList(topoId, exec)});
      return;
    }
    _globalResourceList.get(topoId).put(exec, resourceList);
  }

  public void addExecutorResourceReqDefault(ExecutorDetails exec, String topoId) {
    Map<String, Map<String, Double>> defaultResourceList = new HashMap<String, Map<String, Double>>();
    defaultResourceList.put(Globals.TYPE_CPU, new HashMap<String, Double>());
    defaultResourceList.get(Globals.TYPE_CPU).put(Globals.TYPE_CPU_TOTAL, Globals.DEFAULT_CPU_REQUIREMENT);

    defaultResourceList.put(Globals.TYPE_MEMORY, new HashMap<String, Double>());
    defaultResourceList.get(Globals.TYPE_MEMORY).put(Globals.TYPE_MEMORY_OFFHEAP, Globals.DEFAULT_OFFHEAP_MEMORY_REQUIREMENT);
    defaultResourceList.get(Globals.TYPE_MEMORY).put(Globals.TYPE_MEMORY_ONHEAP, Globals.DEFAULT_ONHEAP_MEMORY_REQUIREMENT);

    addExecutorResourceReq(exec, topoId, defaultResourceList);
  }

  @Override
  public String toString() {
    return _globalResourceList.toString();
  }

  /**
   * prints _globalResourceList.
   */
  public void print() {
    LOG.info("GlobalResourceList: {}", _globalResourceList);
  }

}
