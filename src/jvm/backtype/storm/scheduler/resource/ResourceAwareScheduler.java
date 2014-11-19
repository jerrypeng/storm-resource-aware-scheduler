package backtype.storm.scheduler.resource;

import java.util.Collection;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.scheduler.Cluster;
import backtype.storm.scheduler.ExecutorDetails;
import backtype.storm.scheduler.IScheduler;
import backtype.storm.scheduler.Topologies;
import backtype.storm.scheduler.TopologyDetails;

public class ResourceAwareScheduler implements IScheduler {
	private static final Logger LOG = LoggerFactory
			.getLogger(ResourceAwareScheduler.class);
	@SuppressWarnings("rawtypes")
	private Map _conf;

	@Override
	public void prepare(Map conf) {
		_conf = conf;
	}

	@Override
	public void schedule(Topologies topologies, Cluster cluster) {
		LOG.info("\n\n\nRerunning ResourceAwareScheduler...");

		GlobalResources globalResources = new GlobalResources(topologies);
		GlobalState globalState = GlobalState.getInstance("ResourceAwareScheduer");
		globalState.updateInfo(cluster, topologies, globalResources);
		
		resourceAwareScheduling(topologies, cluster, globalState, globalResources);

		Map<String, Node> nodeIdToNode = Node.getAllNodesFrom(cluster,
				globalResources);
		LOG.info("GlobalResources: \n{}\n", globalResources);
		HelperFuncs.printNodeResources(nodeIdToNode);
	}
	
	public void resourceAwareScheduling(Topologies topos, Cluster cluster, GlobalState globalState, GlobalResources globalResources) {
		R_Scheduler resource_aware_scheduler =
	        new R_Scheduler(cluster, globalState, globalResources);
	    for (TopologyDetails td : topos.getTopologies()) {
	      String topId = td.getId();
	      Map<Node, Collection<ExecutorDetails>> taskToNodesMap;
	      if (cluster.needsScheduling(td) && cluster.getUnassignedExecutors(td).size()>0) {
	        LOG.info("/********Scheduling topology {} ************/", topId);
	        int totalTasks = td.getExecutors().size();
	        int executorsNotRunning = cluster.getUnassignedExecutors(td).size();
	        LOG.info(
	            "Total number of executors: {} " +
	            "Total number of Unassigned Executors: {}",
	            totalTasks, executorsNotRunning);
	        LOG.info("executors that need scheduling: {}",
	            cluster.getUnassignedExecutors(td));
	        taskToNodesMap = resource_aware_scheduler.schedule(td,
	            cluster.getUnassignedExecutors(td));
	        if (taskToNodesMap != null) {
	          try {
	            for (Map.Entry<Node, Collection<ExecutorDetails>> entry :
	                taskToNodesMap.entrySet()) {
	                entry.getKey().assign(td.getId(), entry.getValue(),
	                    cluster);
	                LOG.info("ASSIGNMENT    TOPOLOGY: {}  TASKS: {} To Node: "
	                    + entry.getKey().getId() + " Slots left: "
	                    + entry.getKey().totalSlotsFree(), td.getId(),
	                    entry.getValue());
	            }
	            LOG.info("Toplogy: {} assigned to {} nodes", td.getId(), taskToNodesMap.keySet().size());
	            
	            HelperFuncs.setTopoStatus(td.getId(),"Fully Scheduled");
	          } catch (IllegalStateException ex) {
	            LOG.error(ex.toString());
	            LOG.error("Unsuccessfull in scheduling topology {}", td.getId());
	            HelperFuncs.setTopoStatus(td.getId(), "Unsuccessfull in scheduling topology");
	          }
	        } else {
	          LOG.error("Unsuccessfull in scheduling topology {}", td.getId());
	          HelperFuncs.setTopoStatus(td.getId(), "Unsuccessfull in scheduling topology");
	        }
	      } else {
	    	  HelperFuncs.setTopoStatus(td.getId(),"Fully Scheduled");
	      }
	    }
	  }

}
