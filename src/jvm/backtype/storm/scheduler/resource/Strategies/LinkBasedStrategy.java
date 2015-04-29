package backtype.storm.scheduler.resource.Strategies;

import java.util.Collection;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.scheduler.Cluster;
import backtype.storm.scheduler.ExecutorDetails;
import backtype.storm.scheduler.Topologies;
import backtype.storm.scheduler.TopologyDetails;
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
		return null;
	}

}
