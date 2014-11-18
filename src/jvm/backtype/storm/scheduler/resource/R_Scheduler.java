package backtype.storm.scheduler.resource;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.scheduler.Cluster;
import backtype.storm.scheduler.ExecutorDetails;
import backtype.storm.scheduler.Globals;
import backtype.storm.scheduler.TopologyDetails;

public class R_Scheduler {
	private Collection<Node> _availNodes;

    private Cluster _cluster;
    
    private GlobalResources _globalResources;
    
    private static final Logger LOG = LoggerFactory.getLogger(R_Scheduler.class);

    /**
     * Constructor for MemoryAwareScheduler
     * @param pools a list a pools to use Sorts the nodes in the pools by available memory
     */
    public R_Scheduler(Cluster cluster, GlobalState globalState, GlobalResources globalResources) {

      this._availNodes = globalState.nodes.values();
      this._cluster = cluster;
      this._globalResources = globalResources;
    }

    /**
     * Main schedule function for Memory aware scheduler.
     * @param td respresenting a topology that needs to be scheduled
     * @param unassignedExecutors a list of excutors that need to be scheduled
     * @return taskToNodeMap a map between tasks and nodes to schedule them on
     */
    public Map<Node, Collection<ExecutorDetails>> schedule(TopologyDetails td,
        Collection<ExecutorDetails> unassignedExecutors) {
      if(this._availNodes.size()<=0){
        LOG.warn("No available nodes to schedule tasks on!");
        return null;
      }
      Map<Node, Collection<ExecutorDetails>> taskToNodeMap = null; 
      for (ExecutorDetails exec : unassignedExecutors) {
        if (!this._globalResources.hasExecInTopo(td.getId(), exec)) {
          if (td.getExecutorToComponent().get(exec).compareTo("__acker") == 0) {
            LOG.warn("Scheduling __acker {} with memory requirement as {} - {} and {} - {} and CPU requirement as {}-{}",
                new Object[] {exec, Globals.TYPE_MEMORY_ONHEAP, 
                Globals.DEFAULT_ONHEAP_MEMORY_REQUIREMENT,
                Globals.TYPE_MEMORY_OFFHEAP, Globals.DEFAULT_OFFHEAP_MEMORY_REQUIREMENT, Globals.TYPE_CPU_TOTAL, Globals.DEFAULT_CPU_REQUIREMENT});
            this._globalResources.addExecutorResourceReqDefault(exec, td.getId());
          } else {
            LOG.warn(
                "Executor {} of Component: {} does not have a set memory resource requirement!",
                exec, td.getExecutorToComponent().get(exec));
             this._globalResources.addExecutorResourceReqDefault(exec, td.getId());
          }
        }
      }

      taskToNodeMap = defaultScheduler(td, unassignedExecutors);

      if(taskToNodeMap == null){
        LOG.error("Topology {} not successfully scheduled!", td.getId());
      }

      return taskToNodeMap;
    }
    


    /**
     * Default scheduler for memory aware scheduling schedule tasks on nodes
     * with the currently most available memory resources.
     * @param td representing a topology that needs to be scheduled
     * @param unassignedExecutors a list of executors that need to be scheduled
     * @return taskToNodeMap a map between tasks and nodes to schedule them on
     */
    public Map<Node, Collection<ExecutorDetails>> defaultScheduler(
        TopologyDetails td, Collection<ExecutorDetails> ExecutorsNeedScheduling, Collection<Node> nodesToUse) {
      Map<Node, Collection<ExecutorDetails>> taskToNodeMap = new HashMap<Node, Collection<ExecutorDetails>>();
      LOG.debug("ExecutorsNeedScheduling: {}", ExecutorsNeedScheduling);
      Collection<ExecutorDetails> scheduledTasks = new ArrayList<ExecutorDetails>();
      for (ExecutorDetails exec : ExecutorsNeedScheduling) {
        LOG.info("\n\nAttempting to schedule: {}", exec);
        Node n=null;
        //Isolation purposes
        if (nodesToUse!=null && nodesToUse.size()>0) {
          n = this.getBestNode(nodesToUse, td.getId(), exec);
          if (n==null) {
            n = this.getBestNode(this._availNodes, td.getId(), exec);
          }
        } else {
          n = this.getBestNode(this._availNodes, td.getId(), exec);
        }
        if (n!=null) {
          if (taskToNodeMap.containsKey(n) == false) {
            Collection<ExecutorDetails> newMap = new LinkedList<ExecutorDetails>();
            taskToNodeMap.put(n, newMap);
          }
          taskToNodeMap.get(n).add(exec);
          n.consumeResourcesforTask(exec, td.getId(),  this._globalResources);
          scheduledTasks.add(exec);
          LOG.info("TASK {} assigned to NODE {} -- AvailMem: {} AvailCPU: {}"
              , new Object[] {exec, n, n.getAvailableMemoryResources(), n.getAvailableCpuResources()});
        } else {
          LOG.error("Not Enough Resources to schedule Task {}", exec);
        }
      }
      Collection<ExecutorDetails> tasksNotScheduled = new ArrayList<ExecutorDetails>(ExecutorsNeedScheduling);
      tasksNotScheduled.removeAll(scheduledTasks);
      
      if (tasksNotScheduled.size() > 0) {
        LOG.error("Resources not successfully scheduled: {}", tasksNotScheduled);
        return null;
      } else {
        LOG.debug("All resources successfully scheduled!");
      }
      return taskToNodeMap;
    }

    /**
     * Wrapper for default scheduler
     * @param td representing a topology that needs to be scheduled
     * @param unassignedExecutors a list of executors that need to be scheduled
     * @return taskToNodeMap a map between tasks and nodes to schedule them on
     */
    public Map<Node, Collection<ExecutorDetails>> defaultScheduler(
      TopologyDetails td, Collection<ExecutorDetails> ExecutorsNeedScheduling) {
      Collection<Node> nodesToUse = this._availNodes;
      LOG.debug("nodesToUse: {}", nodesToUse);
      return this.defaultScheduler(td, ExecutorsNeedScheduling, nodesToUse);
    }

    /**
     * Get the best nodes for the list of executors based on vector distances
     * get unique nodes.
     * @param nodeList is the list of nodes to choose from.
     * @param execs the list of executors to get nodes from.
     * @param topoId the topology id in which these executors are from.
     * @param blackList list of nodes not to use
     * @return retList a list chosen nodes
     */
    public Collection<Node> getBestNodes(Collection<Node>nodeList, Collection<ExecutorDetails> execs, String topoId, Map<String, Node> blackList) {
      if (nodeList == null) {
        return null;
      }
      Collection<Node> nodeMap = new LinkedList<Node>();
      nodeMap.addAll(nodeList);
      Collection <Node> retList = new ArrayList<Node>();
      if ( blackList!=null) {
        nodeMap.removeAll(blackList.values());
      }
      if (nodeList.size() < nodeList.size()) {
        LOG.error(
            "Not Enough Nodes to Spread! Spread request {} Actual # of Nodes: {}",
            nodeList.size(), nodeList.size());
        return null;
      }
      Iterator<ExecutorDetails> itr = execs.iterator();
      ExecutorDetails exec = null;
      while (itr.hasNext()) {
       if (exec==null) {
         exec = itr.next();
       }
        Node bestNode = this.getBestNode(nodeMap, topoId, exec);
        if (bestNode==null) {
          LOG.error("Couldn't find suitable node to execute Task {}", exec);
          return null;
        } else if(blackList.containsKey(bestNode.getId())) {
          LOG.error("Node: {} was blacklisted", bestNode);
          nodeMap.remove(bestNode);
          continue;
        } else {
          if(itr.hasNext()) {
            exec = itr.next();
          }
        }
        retList.add(bestNode);
      }
      return retList;
    }

    /**
     * gets a list of free nodes
     * @param pools from these pools
     * @param requested number of free nodes
     * @return retList a TreeSet<Node> of nodes sorted by available memory
     */
    public Collection<Node> getFreeNodes(Collection<Node> NodeMap, Number requested){
      Collection<Node> retList = new ArrayList<Node>();
      for (Node n : NodeMap) {
        if (n.isTotallyFree()==true) {
          retList.add(n);
          if (retList.size()>=requested.intValue()) {
            break;
          }
        }
      }
      return retList;
    }

    /**
     * Get a node for the spread scheduling 
     * @param nodeList is the list of nodes to choose from.
     * @param execs the list of executors to get nodes from.
     * @param topoId the topology id in which these executors are from.
     * @param blackList list of nodes not to use
     * @return retList a list chosen nodes
     */
  public Node getBestSpreadNode(Collection<Node>nodeList, ExecutorDetails exec, String topoId, Map<String, Node> blackList) {
    Collection<ExecutorDetails> execs = new ArrayList<ExecutorDetails>();
    execs.add(exec);
    ArrayList<Node>  bestNodes = ((ArrayList<Node>) this.getBestNodes(this._availNodes, execs, topoId, blackList));
    if (bestNodes==null || bestNodes.size()<=0) {
      return null;
    }
    return bestNodes.get(0);
  }

  /**
   * Get a node for a executor
   * @param NodeMap is the list of nodes to choose from.
   * @param exec the executor to get the node for.
   * @param topoId the topology id in which these executors are from.
   * @return Node a chosen node
   */
    public Node getBestNode(Collection<Node> NodeMap, String topoId, ExecutorDetails exec) {
      
      return schedulingStrategy_1(NodeMap, topoId, exec);
      //return schedulingStrategy_2(NodeMap, topoId, exec);
      //return schedulingStrategy_3(NodeMap, topoId, exec);
      //return schedulingStrategy_4(NodeMap, topoId, exec);
    }

    /**
     * A strategy based on proximity of vector distances
     * tends to back executors to as few nodes as possible
     * may not good for spread scheduling.
     * @param NodeMap is the list of nodes to choose from.
     * @param exec the executor to get the node for.
     * @param topoId the topology id in which these executors are from.
     * @return Node a chosen node
     */
     private Node schedulingStrategy_1(Collection<Node> NodeMap, String topoId, ExecutorDetails exec) {
       Double taskMem =  this._globalResources.getTotalMemReqTask(topoId, exec);
       Double taskCPU =  this._globalResources.getTotalCpuReqTask(topoId, exec);
       Double shortestDistance = Double.POSITIVE_INFINITY;
       String msg = "";
     LOG.info("exec: {} taskMem: {} taskCPU: {}", new Object[]{exec, taskMem, taskCPU});
       Node closestNode = null;
       LOG.info("NodeMap.size: {}", NodeMap.size());
       LOG.info("NodeMap: {}", NodeMap);
       for (Node n : NodeMap) {
         if(n.getAvailableMemoryResources() >= taskMem && n.getAvailableCpuResources() >= taskCPU && n.totalSlotsFree()>0) {
           Double distance = Math.sqrt(Math.pow((taskMem - n.getAvailableMemoryResources()), 2) + Math.pow((taskCPU - n.getAvailableCpuResources()), 2));
           msg = msg + "{"+n.getId()+"-"+distance.toString()+"}";
           if (distance < shortestDistance) {
             closestNode = n;
             shortestDistance = distance;
           }
         }
       }
       if (closestNode != null) {
         LOG.info(msg);
         LOG.info("node: {} distance: {}", closestNode, shortestDistance);
         LOG.info("node availMem: {}", closestNode.getAvailableMemoryResources());
         LOG.info("node availCPU: {}", closestNode.getAvailableCpuResources());
       }
       return closestNode;
     }

     /**
      * Uses normalized values... tends to spread executors across different
      * nodes better.
      * @param NodeMap is the list of nodes to choose from.
      * @param exec the executor to get the node for.
      * @param topoId the topology id in which these executors are from.
      * @return Node a chosen node
      */
     private Node schedulingStrategy_2(Collection<Node> NodeMap, String topoId, ExecutorDetails exec) {
       Double taskMem =  this._globalResources.getTotalMemReqTask(topoId, exec);
       Double taskCPU =  this._globalResources.getTotalCpuReqTask(topoId, exec);
       Double mostRes = 0.0;
       Node bestNode = null;
       for (Node n : NodeMap) {
         if(n.getAvailableMemoryResources() >= taskMem && n.getAvailableCpuResources() >= taskCPU && n.totalSlotsFree()>0) {
          Double res = (Math.log10(n.getAvailableCpuResources())*0.5)+(Math.log10(n.getAvailableMemoryResources())*0.5);
           if (res > mostRes) {
             bestNode = n;
             mostRes = res;
           }
         }
       }
       return bestNode;
     }

     /**
      * Uses the resources proportions
      * @param NodeMap is the list of nodes to choose from.
      * @param exec the executor to get the node for.
      * @param topoId the topology id in which these executors are from.
      * @return Node a chosen node
      */
     private Node schedulingStrategy_3(Collection<Node> NodeMap, String topoId, ExecutorDetails exec) {
       Double taskMem =  this._globalResources.getTotalMemReqTask(topoId, exec);
       Double taskCPU =  this._globalResources.getTotalCpuReqTask(topoId, exec);
       Double totalMem = 0.0;
       Double totalCPU=0.0;
       for (Node n : NodeMap) {
         totalMem += n.getAvailableMemoryResources();
         totalCPU += n.getAvailableCpuResources();
       }
       Double mostRes = 0.0;
       Node bestNode = null;
       for (Node n : NodeMap) {
         if(n.getAvailableMemoryResources() >= taskMem && n.getAvailableCpuResources() >= taskCPU && n.totalSlotsFree()>0) {
          Double res = ((n.getAvailableCpuResources()/totalCPU)*0.5)+((n.getAvailableMemoryResources()/totalMem)*0.5);
           if (res > mostRes) {
             bestNode = n;
             mostRes = res;
           }
         }
       }
       return bestNode;
     }

     /**
      * Uses the ratio of resources. Matches task to node 
      * with the closest resources ratio.
      * @param NodeMap is the list of nodes to choose from.
      * @param exec the executor to get the node for.
      * @param topoId the topology id in which these executors are from.
      * @return Node a chosen node
      */
     private Node schedulingStrategy_4 (Collection<Node> NodeMap, String topoId, ExecutorDetails exec) {
       Double taskMem =  this._globalResources.getTotalMemReqTask(topoId, exec);
       Double taskCPU =  this._globalResources.getTotalCpuReqTask(topoId, exec);
       Double taskResourceRatio = taskMem/taskCPU;

       Double closest = Double.POSITIVE_INFINITY;
       Node bestNode = null;
       for (Node n : NodeMap) {
         if(n.getAvailableMemoryResources() >= taskMem && n.getAvailableCpuResources() >= taskCPU && n.totalSlotsFree()>0) {
          Double nodeResourceRatio = n.getAvailableMemoryResources()/n.getAvailableCpuResources();
           if (Math.abs(nodeResourceRatio-taskResourceRatio) < Math.abs(closest)) {
             bestNode = n;
             closest = Math.abs(nodeResourceRatio-taskResourceRatio);
           }
         }
       }
       return bestNode;
     }
}
