package backtype.storm.scheduler.resource.Strategies;

import java.util.Collection;
import java.util.Map;

import backtype.storm.scheduler.ExecutorDetails;
import backtype.storm.scheduler.TopologyDetails;
import backtype.storm.scheduler.resource.Node;

public interface IStrategy {
	
	public Map<Node, Collection<ExecutorDetails>> schedule(TopologyDetails td,
	        Collection<ExecutorDetails> unassignedExecutors);
		
	
}
