package backtype.storm.scheduler.resource;

import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;
import java.util.ArrayList;
import java.util.Iterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.scheduler.Cluster;
import backtype.storm.scheduler.ExecutorDetails;
import backtype.storm.scheduler.Globals;
import backtype.storm.scheduler.SchedulerAssignment;
import backtype.storm.scheduler.SupervisorDetails;
import backtype.storm.scheduler.WorkerSlot;

/**
 * Represents a single node in the cluster.
 */
public class Node {
  private static final Logger LOG = LoggerFactory.getLogger(Node.class);
  private Map<String, Set<WorkerSlot>> _topIdToUsedSlots = new HashMap<String,Set<WorkerSlot>>();
  private Set<WorkerSlot> _freeSlots = new HashSet<WorkerSlot>();
  private final String _nodeId;
  private boolean _isAlive;
  public SupervisorDetails sup;
  public String supervisor_id;
  private Double availMemory;
  private Double availCPU;
  public String hostname;
  public List<WorkerSlot> slots;
  public List<ExecutorDetails> execs;
  public Map<WorkerSlot, List<ExecutorDetails>> slot_to_exec;

  public Node(String nodeId, Set<Integer> allPorts, boolean isAlive) {
    _nodeId = nodeId;
    _isAlive = isAlive;
    if (_isAlive && allPorts != null) {
      for (int port: allPorts) {
        _freeSlots.add(new WorkerSlot(_nodeId, port));
      }
    }
  }

  public Node(String nodeId, Set<Integer> allPorts, boolean isAlive,
      SupervisorDetails sup) {
	  this(nodeId, allPorts, isAlive);
	  this.sup = sup;
	  LOG.info("sup-2: {}", sup);
	  this.supervisor_id = sup.getId();
	  this.availMemory = this.getTotalMemoryResources();
	  this.availCPU = this.getTotalCpuResources();
	  this.hostname = this.sup.getHost();
	  this.slots = new ArrayList<WorkerSlot>();
	  this.slots.addAll(this._freeSlots);
	  this.execs = new ArrayList<ExecutorDetails>();
	  slot_to_exec = new HashMap<WorkerSlot, List<ExecutorDetails>>();
	  for (WorkerSlot ws : this.slots) {
		  slot_to_exec.put(ws, new ArrayList<ExecutorDetails>());
	  }

  }

  public String getId() {
    return _nodeId;
  }
  
  public boolean isAlive() {
    return _isAlive;
  }
  
  /**
   * @return a collection of the topology ids currently running on this node
   */
  public Collection<String> getRunningTopologies() {
    return _topIdToUsedSlots.keySet();
  }
  
  public boolean isTotallyFree() {
    return _topIdToUsedSlots.isEmpty();
  }
  
  public int totalSlotsFree() {
    return _freeSlots.size();
  }
  
  public int totalSlotsUsed() {
    int total = 0;
    for (Set<WorkerSlot> slots: _topIdToUsedSlots.values()) {
      total += slots.size();
    }
    return total;
  }
  
  public int totalSlots() {
    return totalSlotsFree() + totalSlotsUsed();
  }
  
  public int totalSlotsUsed(String topId) {
    int total = 0;
    Set<WorkerSlot> slots = _topIdToUsedSlots.get(topId);
    if (slots != null) {
      total = slots.size();
    }
    return total;
  }

  private void validateSlot(WorkerSlot ws) {
    if (!_nodeId.equals(ws.getNodeId())) {
      throw new IllegalArgumentException(
          "Trying to add a slot to the wrong node " + ws + 
          " is not a part of " + _nodeId);
    }
  }
 
  private void addOrphanedSlot(WorkerSlot ws) {
    if (_isAlive) {
      throw new IllegalArgumentException("Orphaned Slots " +
        "only are allowed on dead nodes.");
    }
    validateSlot(ws);
    if (_freeSlots.contains(ws)) {
      return;
    }
    for (Set<WorkerSlot> used: _topIdToUsedSlots.values()) {
      if (used.contains(ws)) {
        return;
      }
    }
    _freeSlots.add(ws);
  }
 
  boolean assignInternal(WorkerSlot ws, String topId, boolean dontThrow) {
    validateSlot(ws);
    if (!_freeSlots.remove(ws)) {
      if (dontThrow) {
        return true;
      }
      throw new IllegalStateException("Assigning a slot that was not free " + ws);
    }
    Set<WorkerSlot> usedSlots = _topIdToUsedSlots.get(topId);
    if (usedSlots == null) {
      usedSlots = new HashSet<WorkerSlot>();
      _topIdToUsedSlots.put(topId, usedSlots);
    }
    usedSlots.add(ws);
    return false;
  }
  
  /**
   * Free all slots on this node.  This will update the Cluster too.
   * @param cluster the cluster to be updated
   */
  public void freeAllSlots(Cluster cluster) {
    if (!_isAlive) {
      LOG.warn("Freeing all slots on a dead node {} ",_nodeId);
    } 
    for (Entry<String, Set<WorkerSlot>> entry : _topIdToUsedSlots.entrySet()) {
      cluster.freeSlots(entry.getValue());
      if (_isAlive) {
        _freeSlots.addAll(entry.getValue());
      }
    }
    _topIdToUsedSlots = new HashMap<String,Set<WorkerSlot>>();
  }
  
  /**
   * Frees a single slot in this node
   * @param ws the slot to free
   * @param cluster the cluster to update
   */
  public void free(WorkerSlot ws, Cluster cluster) {
    if (_freeSlots.contains(ws)) return;
    for (Entry<String, Set<WorkerSlot>> entry : _topIdToUsedSlots.entrySet()) {
      Set<WorkerSlot> slots = entry.getValue();
      if (slots.remove(ws)) {
        cluster.freeSlot(ws);
        if (_isAlive) {
          _freeSlots.add(ws);
        }
        return;
      }
    }
    throw new IllegalArgumentException("Tried to free a slot that was not" +
    		" part of this node " + _nodeId);
  }
   
  /**
   * Frees all the slots for a topology.
   * @param topId the topology to free slots for
   * @param cluster the cluster to update
   */
  public void freeTopology(String topId, Cluster cluster) {
    Set<WorkerSlot> slots = _topIdToUsedSlots.get(topId);
    if (slots == null || slots.isEmpty()) return;
    for (WorkerSlot ws : slots) {
      cluster.freeSlot(ws);
      if (_isAlive) {
        _freeSlots.add(ws);
      }
    }
    _topIdToUsedSlots.remove(topId);
  }
 
  /**
   * Assign a free slot on the node to the following topology and executors.
   * This will update the cluster too.
   * @param topId the topology to assign a free slot to.
   * @param executors the executors to run in that slot.
   * @param cluster the cluster to be updated
   */
  public void assign(String topId, Collection<ExecutorDetails> executors, 
      Cluster cluster) {
    if (!_isAlive) {
      throw new IllegalStateException("Trying to adding to a dead node " + _nodeId);
    }
    if (_freeSlots.isEmpty()) {
      throw new IllegalStateException("Trying to assign to a full node " + _nodeId);
    }
    if (executors.size() == 0) {
      LOG.warn("Trying to assign nothing from " + topId + " to " + _nodeId + " (Ignored)");
    } else {
      WorkerSlot slot = _freeSlots.iterator().next();
      cluster.assign(slot, topId, executors);
      assignInternal(slot, topId, false);
    }
  }
  
  @Override
  public boolean equals(Object other) {
    if (other instanceof Node) {
      return _nodeId.equals(((Node)other)._nodeId);
    }
    return false;
  }
  
  @Override
  public int hashCode() {
    return _nodeId.hashCode();
  }
  
  @Override
  public String toString() {
    return "{Node: " + this.sup.getHost() + ", AvailMem: "+this.availMemory.toString()+", AvailCPU: "+this.availCPU.toString()+"}";
  }

  public static int countSlotsUsed(String topId, Collection<Node> nodes) {
    int total = 0;
    for (Node n: nodes) {
      total += n.totalSlotsUsed(topId);
    }
    return total;
  }
  
  public static int countSlotsUsed(Collection<Node> nodes) {
    int total = 0;
    for (Node n: nodes) {
      total += n.totalSlotsUsed();
    }
    return total;
  }
  
  public static int countFreeSlotsAlive(Collection<Node> nodes) {
    int total = 0;
    for (Node n: nodes) {
      if (n.isAlive()) {
        total += n.totalSlotsFree();
      }
    }
    return total;
  }
  
  public static int countTotalSlotsAlive(Collection<Node> nodes) {
    int total = 0;
    for (Node n: nodes) {
      if (n.isAlive()) {
        total += n.totalSlots();
      }
    }
    return total;
  }

  public static Map<String, Node> getAllNodesFrom(Cluster cluster, GlobalResources globalResources) {
    Map<String, Node> nodeIdToNode = new HashMap<String, Node>();
    for (SupervisorDetails sup : cluster.getSupervisors().values()) {
      //Node ID and supervisor ID are the same.
    	LOG.info("sup-1: {}", sup);
      String id = sup.getId();
      boolean isAlive = !cluster.isBlackListed(id);
      LOG.info("Found a {} Node {} {}", 
          new Object[] {isAlive? "living":"dead", id, sup.getAllPorts()});
      nodeIdToNode.put(sup.getHost(), new Node(id, sup.getAllPorts(), isAlive, sup));
    }

    for (Entry<String, SchedulerAssignment> entry : cluster.getAssignments().entrySet()) {
      String topId = entry.getValue().getTopologyId();
      for (WorkerSlot ws: entry.getValue().getSlots()) {
        String id = ws.getNodeId();
        Node node = nodeIdToNode.get(id);
        if (node == null) {
          LOG.info("Found an assigned slot on a dead supervisor {} with executors {}",
              ws, HelperFuncs.getExecutors(ws, cluster));
          node = new Node(id, null, false, null);
          nodeIdToNode.put(id, node);
        }
        if (!node.isAlive()) {
          //The supervisor on the node down so add an orphaned slot to hold the unsupervised worker 
          node.addOrphanedSlot(ws);
        }
        if (node.assignInternal(ws, topId, true)) {
          LOG.warn("Bad scheduling state, "+ws+" assigned multiple workers, unassigning everything...");
          node.free(ws, cluster);
        }
      }
    }
    if (globalResources != null) {
      Node.updateAvailableResources(cluster,globalResources, nodeIdToNode);
    }

    return nodeIdToNode;
  }

  /**
   * updates the available resources for every node in a cluster
   * by recalculating memory requirements.
   * @param cluster the cluster used in this calculation
   * @param globalResourceList the global resource requirement list
   * @param nodeIdToNode a map between node id and node
   */
  private static void updateAvailableResources(Cluster cluster,
      GlobalResources globalResources,
      Map<String, Node> nodeIdToNode) {
    //recompute memory
    LOG.debug("globalResourceList: {} nodeIdToNode: {}", globalResources,
        nodeIdToNode);
    Map<WorkerSlot, Node> SlotNeedToBeFreed = new HashMap<WorkerSlot, Node>();
    if (globalResources.size() > 0 && cluster.getAssignments().size() > 0) {
      for (Entry<String, SchedulerAssignment> entry : cluster.getAssignments()
          .entrySet()) {
        Map<ExecutorDetails, WorkerSlot> executorToSlot = entry.getValue()
            .getExecutorToSlot();
        Map<ExecutorDetails, Double> topoMemoryResourceList = globalResources.getTotalMemoryResourceList(entry.getKey());
        if (topoMemoryResourceList == null || topoMemoryResourceList.size() <= 0) {
          continue;
        }
        for (Map.Entry<ExecutorDetails, WorkerSlot> execToSlot : executorToSlot
            .entrySet()) {
          WorkerSlot slot = execToSlot.getValue();
          ExecutorDetails exec = execToSlot.getKey();
          Node node = nodeIdToNode.get(slot.getNodeId());
          if (!node.isAlive()) {
            SlotNeedToBeFreed.put(slot, node);
            continue;
          }
          if (topoMemoryResourceList.containsKey(exec) == true) {
            node.consumeResourcesforTask(exec, entry.getKey(), globalResources);
          } else {
            LOG.warn("Resource Req not found...Scheduling Task{} with memory requirement as {} - {} and {} - {} and CPU requirement as {}-{}",
                new Object[] {exec, Globals.TYPE_MEMORY_ONHEAP, 
                Globals.DEFAULT_ONHEAP_MEMORY_REQUIREMENT,
                Globals.TYPE_MEMORY_OFFHEAP, Globals.DEFAULT_OFFHEAP_MEMORY_REQUIREMENT, Globals.TYPE_CPU_TOTAL, Globals.DEFAULT_CPU_REQUIREMENT});           globalResources.addExecutorResourceReqDefault(exec, entry.getKey());
      	    node.consumeResourcesforTask(exec, entry.getKey(), globalResources);
          }
        }
      }
      for (Map.Entry<WorkerSlot, Node> entry : SlotNeedToBeFreed.entrySet()) {
        entry.getValue().free(entry.getKey(), cluster);
      }
    } else {
      for (Node n : nodeIdToNode.values()) {
        n.setAvailableMemory(n.getAvailableMemoryResources());
      }
    }
  }

  /**
   * Sets the Available Memory for a node
   * @param amountthe amount to set as available memory
   */
  public void setAvailableMemory(Double amount) {
    this.availMemory = amount;
  }

  /**
   * Gets the available memory resources for this node
   * @return the available memory for this node
   */
  public Double getAvailableMemoryResources() {
    if (this.availMemory==null) {
      return -1.0;
    }
    return this.availMemory;
  }

  /**
   * Gets the total memory resources for this node
   * @return the total memory for this node
   */
  public Double getTotalMemoryResources() {
    if (sup != null && this.sup.getTotalMemory()!=null) {
      return this.sup.getTotalMemory();
    } else{
      return -1.0;
    }
  }

  /**
   * Consumes a certain amount of memory for this node
   * @param amount is the amount memory to consume from this node
   * @return the current available memory for this node after consumption
   */
  public Double consumeMemory(Double amount) {
    if (amount > this.availMemory) {
      LOG.error("Attempting to consume more memory than available!");
      return null;
    }
    this.availMemory = this.availMemory - amount;
    return this.availMemory;
  }

  /**
   * Gets the available cpu resources for this node
   * @return the available cpu for this node
   */
  public Double getAvailableCpuResources() {
    if (this.availCPU==null) {
      return -1.0;
    }
    return this.availCPU;
  }

  /**
   * Gets the total cpu resources for this node
   * @return the total cpu for this node
   */
  public Double getTotalCpuResources() {
    if (sup != null && this.sup.getTotalCPU()!=null) {
      return this.sup.getTotalCPU();
    } else{
      return -1.0;
    }
  }

  /**
   * Consumes a certain amount of cpu for this node
   * @param amount is the amount cpu to consume from this node
   * @return the current available cpu for this node after consumption
   */
  public Double consumeCPU(Double amount) {
    if (amount > this.availCPU) {
      LOG.error("Attempting to consume more CPU than available!");
      return null;
    }
    this.availCPU = this.availCPU - amount;
    return this.availCPU;
  } 

  /**
   * Consumes a certain amount of resources for a executor in a topology.
   * @param exec is the executor that is consuming resources on this node
   * @param topoId the topology the executor is a part
   * @param globalResources for resource req lookup
   */
  public void consumeResourcesforTask(ExecutorDetails exec, String topoId, GlobalResources globalResources) {
    Double taskMemReq = globalResources.getTotalMemReqTask(topoId, exec);
    Double taskCpuReq = globalResources.getTotalCpuReqTask(topoId, exec);
    this.consumeCPU(taskCpuReq);
    this.consumeMemory(taskMemReq);
  }
}
