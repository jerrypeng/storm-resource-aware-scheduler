package backtype.storm.scheduler.resource;

import java.io.IOException;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.scheduler.Cluster;
import backtype.storm.scheduler.ExecutorDetails;
import backtype.storm.scheduler.IScheduler;
import backtype.storm.scheduler.Topologies;
import backtype.storm.scheduler.TopologyDetails;
import backtype.storm.scheduler.resource.ResourceUsageServer.ResourceUsageServer;

public class EvenScheduler implements IScheduler{
	private static final Logger LOG = LoggerFactory
			.getLogger(EvenScheduler.class);
	@SuppressWarnings("rawtypes")
	private Map _conf;

	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map conf) {
		_conf = conf;
	}

	@Override
	public void schedule(Topologies topologies, Cluster cluster) {
		LOG.info("\n\n\nRerunning EvenScheduler...");
		for (TopologyDetails topo : topologies.getTopologies()) {
			LOG.info("ID: {} NAME: {}", topo.getId(), topo.getName());
			LOG.info("Unassigned Executors for {}: ", topo.getName());
			LOG.info("Current Assignment: {}", HelperFuncs.nodeToTask(cluster, topo.getId()));
		}
		ResourceUsageServer rs = ResourceUsageServer.getInstance();
		
		
		GetStats gs = GetStats.getInstance("EvenScheduler");
		GetTopologyInfo gt = new GetTopologyInfo();
		gs.getStatistics();
		GlobalResources globalResources = new GlobalResources(cluster, topologies);
		GlobalState globalState = GlobalState.getInstance("EvenScheduler");
		globalState.updateInfo(cluster, topologies, globalResources);
		HelperFuncs.printNodeResources(globalState.nodes);
		LOG.info("GlobalState:\n{}", globalState);
		for(TopologyDetails topo : topologies.getTopologies()) {
			gt.getTopologyInfo(topo.getId());
			LOG.info("Topology layout: {}", gt.all_comp);
		}
		
		
		LOG.info("running EvenScheduler now...");
		new backtype.storm.scheduler.EvenScheduler().schedule(topologies, cluster);
		
		globalState.storeSchedState(cluster, topologies, globalResources);
	}
}