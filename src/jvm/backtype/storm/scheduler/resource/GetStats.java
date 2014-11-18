package backtype.storm.scheduler.resource;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.generated.ClusterSummary;
import backtype.storm.generated.ExecutorStats;
import backtype.storm.generated.ExecutorSummary;
import backtype.storm.generated.Nimbus;
import backtype.storm.generated.StormTopology;
import backtype.storm.generated.TopologyInfo;
import backtype.storm.generated.TopologySummary;

public class GetStats {
	public class NodeStats {
		public String hostname;
		public ArrayList<ExecutorSummary> bolts_on_node;
		public ArrayList<ExecutorSummary> spouts_on_node;
		public Integer emit_throughput;
		public Integer transfer_throughput;
		// transfer or emit
		public HashMap<String, Integer> bolts_on_node_throughput;
		public HashMap<String, Integer> spouts_on_node_throughput;

		public NodeStats(String hostname) {
			this.hostname = hostname;
			this.bolts_on_node = new ArrayList<ExecutorSummary>();
			this.spouts_on_node = new ArrayList<ExecutorSummary>();
			this.bolts_on_node_throughput = new HashMap<String, Integer>();
			this.bolts_on_node_throughput.put("transfer", 0);
			this.bolts_on_node_throughput.put("emit", 0);
			this.spouts_on_node_throughput = new HashMap<String, Integer>();
			this.spouts_on_node_throughput.put("transfer", 0);
			this.spouts_on_node_throughput.put("emit", 0);
			this.emit_throughput = 0;
			this.transfer_throughput = 0;
		}
	}

	public class ComponentStats {
		public String componentId;
		public Integer total_emit_throughput;
		public Integer total_transfer_throughput;
		public Integer parallelism_hint;

		public ComponentStats(String id) {
			this.componentId = id;
			this.total_emit_throughput = 0;
			this.total_transfer_throughput = 0;
		}

	}

	/**
	 * Unique Task id hash -> Transfer throughput
	 */
	public HashMap<String, Integer> transferStatsTable;

	/**
	 * Unique Task id hash -> emit throughput
	 */
	public HashMap<String, Integer> emitStatsTable;

	/**
	 * Topology_id->start time
	 */
	public HashMap<String, Long> startTimes;

	/**
	 * hostname -> NodeStats
	 */
	public HashMap<String, NodeStats> nodeStats;

	/**
	 * Topology_id->(Component_Id->List of previous throughputs)
	 */
	public HashMap<String, HashMap<String, ComponentStats>> componentStats;

	/**
	 * Topology_id->(Component_Id->List of previous throughputs)
	 */
	public HashMap<String, HashMap<String, List<Integer>>> transferThroughputHistory;
	/**
	 * Topology_id->(Component_Id->List of previous throughputs)
	 */
	public HashMap<String, HashMap<String, List<Integer>>> emitThroughputHistory;

	/**
	 * File output
	 */
	private File complete_log;
	private File avg_log;
	private File output_bolt_log;
	private String sched_type;
	

	private final static Integer MOVING_AVG_WINDOW = 30;
	private static GetStats instance = null;
	private static final Logger LOG = LoggerFactory.getLogger(GetStats.class);

	protected GetStats(String filename) {
		this.transferStatsTable = new HashMap<String, Integer>();
		this.emitStatsTable = new HashMap<String, Integer>();
		this.emitThroughputHistory = new HashMap<String, HashMap<String, List<Integer>>>();
		this.transferThroughputHistory = new HashMap<String, HashMap<String, List<Integer>>>();
		this.startTimes = new HashMap<String, Long>();
		this.nodeStats = new HashMap<String, NodeStats>();
		this.componentStats = new HashMap<String, HashMap<String, ComponentStats>>();

		// delete old files
		try {
			complete_log = new File(Config.LOG_PATH + filename + "_complete");
			avg_log = new File(Config.LOG_PATH + filename + "_complete");
			output_bolt_log = new File(Config.LOG_PATH + filename + "output_bolt");
			sched_type = filename;

			complete_log.delete();
			avg_log.delete();
			output_bolt_log.delete();
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	public static GetStats getInstanceifInit() {
		if (instance != null) {
			return instance;
		}
		return null;
	}

	public static GetStats getInstance(String filename) {
		if (instance == null) {
			instance = new GetStats(filename);
		}
		return instance;
	}

	public void getStatistics() {
		LOG.info("Getting stats...");

		// reseting values
		this.nodeStats.clear();
		this.componentStats.clear();

		TSocket tsocket = new TSocket("localhost", 6627);
		TFramedTransport tTransport = new TFramedTransport(tsocket);
		TBinaryProtocol tBinaryProtocol = new TBinaryProtocol(tTransport);
		Nimbus.Client client = new Nimbus.Client(tBinaryProtocol);

		try {
			tTransport.open();
			ClusterSummary clusterSummary = client.getClusterInfo();
			List<TopologySummary> topologies = clusterSummary.get_topologies();
			for (TopologySummary topo : topologies) {
				// get start time of topology
				if (this.startTimes.containsKey(topo.get_id()) == false) {
					this.startTimes.put(topo.get_id(),
							(System.currentTimeMillis() / 1000));
				}
				TopologyInfo topologyInfo = null;
				StormTopology stormTopo = null;
				try {
					topologyInfo = client.getTopologyInfo(topo.get_id());
					stormTopo = client.getTopology(topo.get_id());
				} catch (Exception e) {
					LOG.info(e.toString());
					continue;
				}
				// get all executors for topology
				List<ExecutorSummary> executorSummaries = topologyInfo
						.get_executors();
				// iterate all executors
				for (ExecutorSummary executorSummary : executorSummaries) {
					// getting general info
					String host = executorSummary.get_host();
					String port = String.valueOf(executorSummary.get_port());
					String componentId = executorSummary.get_component_id();
					String taskId = Integer.toString(executorSummary
							.get_executor_info().get_task_start());

					// populating data structures
					this.initDataStructs(componentId, host, executorSummary, stormTopo, topo);
					//LOG.info("componentStats: {}", this.componentStats);
					//LOG.info("transferThroughputHistory: {}", this.transferThroughputHistory);
					
					//executor stats
					ExecutorStats executorStats = executorSummary.get_stats();
					if (executorStats == null) {
						continue;
					}
					// get transfer info
					Map<String, Map<String, Long>> transfer = executorStats
							.get_transferred();
					// get emit info
					Map<String, Map<String, Long>> emit = executorStats
							.get_emitted();

					// LOG.info("Transfer: {}", transfer);
					if (transfer.get(":all-time").get("default") != null
							&& emit.get(":all-time").get("default") != null) {
						// getting task hash
						String hash_id = this.getTaskHashId(host, port,
								componentId, topo, taskId);
						// getting total output
						Integer totalTransferOutput = transfer.get(":all-time")
								.get("default").intValue();
						Integer totalEmitOutput = emit.get(":all-time")
								.get("default").intValue();

						if (this.transferStatsTable.containsKey(hash_id) == false) {
							this.transferStatsTable.put(hash_id,
									totalTransferOutput);
						}
						if (this.emitStatsTable.containsKey(hash_id) == false) {
							this.emitStatsTable.put(hash_id, totalEmitOutput);
						}

						// get throughput
						Integer transfer_throughput = totalTransferOutput
								- this.transferStatsTable.get(hash_id);
						Integer emit_throughput = totalEmitOutput
								- this.emitStatsTable.get(hash_id);

						LOG.info((host + ':' + port + ':' + componentId + ":"
								+ topo.get_id() + ":" + taskId + ","
								+ transfer.get(":all-time").get("default")
								+ "," + this.transferStatsTable.get(hash_id)
								+ "," + transfer_throughput + ","
								+ emit.get(":all-time").get("default") + ","
								+ this.emitStatsTable.get(hash_id) + "," + emit_throughput));
						// LOG.info("-->transfered: {}\n -->emmitted: {}",
						// executorStats.get_transferred(),
						// executorStats.get_emitted());

						this.transferStatsTable.put(hash_id,
								totalTransferOutput);
						this.emitStatsTable.put(hash_id, totalEmitOutput);

						// get node stats

						this.nodeStats.get(host).transfer_throughput += transfer_throughput;
						this.nodeStats.get(host).emit_throughput += emit_throughput;

						// get node component stats
						if (stormTopo.get_bolts().containsKey(componentId) == true) {
							this.nodeStats.get(host).bolts_on_node_throughput
									.put("transfer",
											this.nodeStats.get(host).bolts_on_node_throughput
													.get("transfer")
													+ transfer_throughput);
							this.nodeStats.get(host).bolts_on_node_throughput
									.put("emit",
											this.nodeStats.get(host).bolts_on_node_throughput
													.get("emit")
													+ emit_throughput);
						} else if (stormTopo.get_spouts().containsKey(
								componentId) == true) {
							this.nodeStats.get(host).spouts_on_node_throughput
									.put("transfer",
											this.nodeStats.get(host).spouts_on_node_throughput
													.get("transfer")
													+ transfer_throughput);
							this.nodeStats.get(host).spouts_on_node_throughput
									.put("emit",
											this.nodeStats.get(host).spouts_on_node_throughput
													.get("emit")
													+ emit_throughput);
						}

						this.componentStats.get(topo.get_id()).get(componentId).total_transfer_throughput += transfer_throughput;
						this.componentStats.get(topo.get_id()).get(componentId).total_emit_throughput += emit_throughput;

						// write to file
						long unixTime = (System.currentTimeMillis() / 1000)
								- this.startTimes.get(topo.get_id());

						String data = String.valueOf(unixTime) + ':'
								+ this.sched_type + ":" + host + ':' + port
								+ ':' + componentId + ":" + topo.get_id() + ":"
								+ taskId + "," + transfer_throughput + "\n";

						// write log to file
						HelperFuncs.writeToFile(this.complete_log, data);

					}
				}

				// weighted moving avg purposes
				//LOG.info("executorSummaries {}", executorSummaries);
				if(executorSummaries.size()>0) {
					this.updateThroughputHistory(topo);
					// print stats in log
					this.logGeneralStats();
					this.logNodeStats();
					this.logComponentStats(topo);
				}

				
			}
		} catch (TException e) {
			e.printStackTrace();
			LOG.error(e.toString());
		}
	}

	private void updateThroughputHistory(TopologySummary topo) {
		
		HashMap<String, List<Integer>> compTransferHistory = this.transferThroughputHistory
				.get(topo.get_id());
		HashMap<String, List<Integer>> compEmitHistory = this.emitThroughputHistory
				.get(topo.get_id());
		//LOG.info("compTransferHistory: {}", compTransferHistory);
		//LOG.info("componentStats: {}", this.componentStats);
		for (Map.Entry<String, ComponentStats> entry : this.componentStats.get(
				topo.get_id()).entrySet()) {
			if (compTransferHistory.get(entry.getKey()).size() >= MOVING_AVG_WINDOW) {
				compTransferHistory.get(entry.getKey()).remove(0);
			}
			if (compEmitHistory.get(entry.getKey()).size() >= MOVING_AVG_WINDOW) {
				compEmitHistory.get(entry.getKey()).remove(0);
			}

			compTransferHistory.get(entry.getKey()).add(
					entry.getValue().total_transfer_throughput);
			compEmitHistory.get(entry.getKey()).add(
					entry.getValue().total_emit_throughput);
		}
	}

	private String getTaskHashId(String host, String port, String componentId,
			TopologySummary topo, String taskId) {
		String hash_id = host + ':' + port + ':' + componentId + ":"
				+ topo.get_id() + ":" + taskId;
		return hash_id;
	}

	private void logGeneralStats() {
		LOG.info("!!!- GENERAL STATISTICS -!!!");
		LOG.info("OVERALL THROUGHPUT:");
		for (Map.Entry<String, NodeStats> ns : this.nodeStats.entrySet()) {
			LOG.info("{} -> transfer: {}    emit: {}",
					new Object[] { ns.getKey(),
							ns.getValue().transfer_throughput,
							ns.getValue().emit_throughput });
		}
	}

	private void logNodeStats() {
		LOG.info("NODE STATS:");

		for (Map.Entry<String, NodeStats> ns : this.nodeStats.entrySet()) {
			LOG.info("{}:", ns.getKey());
			LOG.info("# of Spouts: {}    # of Bolts: {}",
					ns.getValue().spouts_on_node.size(),
					ns.getValue().bolts_on_node.size());
			LOG.info("total spout throughput (transfer):{} (emit):{}",
					ns.getValue().spouts_on_node_throughput.get("transfer"),
					ns.getValue().spouts_on_node_throughput.get("emit"));
			LOG.info("total bolt throughput (transfer):{} (emit):{}",
					ns.getValue().bolts_on_node_throughput.get("transfer"),
					ns.getValue().bolts_on_node_throughput.get("emit"));

		}
	}

	private void logComponentStats(TopologySummary topo) {
		LOG.info("COMPONENT STATS:");

		int num_output_bolt = 0;
		int total_output_bolt_emit = 0;
		String output_bolts = "";
		for (Map.Entry<String, ComponentStats> cs : this.componentStats.get(
				topo.get_id()).entrySet()) {
			int avg_transfer_throughput = cs.getValue().total_transfer_throughput
					/ cs.getValue().parallelism_hint;
			int avg_emit_throughput = cs.getValue().total_emit_throughput
					/ cs.getValue().parallelism_hint;
			if (cs.getKey().matches(".*_output_.*")) {
				LOG.info(
						"Component: {}(output) total throughput (transfer): {} (emit): {} avg throughput (transfer): {} (emit): {}",
						new Object[] { cs.getKey(),
								cs.getValue().total_transfer_throughput,
								cs.getValue().total_emit_throughput,
								avg_transfer_throughput, avg_emit_throughput });
				num_output_bolt++;
				total_output_bolt_emit += cs.getValue().total_emit_throughput;
				output_bolts += cs.getKey() + ",";
			} else {
				LOG.info(
						"Component: {} total throughput (transfer): {} (emit): {} avg throughput (transfer): {} (emit): {}",
						new Object[] { cs.getKey(),
								cs.getValue().total_transfer_throughput,
								cs.getValue().total_emit_throughput,
								avg_transfer_throughput, avg_emit_throughput });
			}
		}
		if (num_output_bolt > 0) {
			LOG.info("Output Bolts stats: ");

			long unixTime = (System.currentTimeMillis() / 1000)
					- this.startTimes.get(topo.get_id());
			String data = String.valueOf(unixTime) + ':' + this.sched_type
					+ ":" + output_bolts + ":" + topo.get_id() + ":"
					+ total_output_bolt_emit / num_output_bolt + "\n";
			LOG.info(data);
			HelperFuncs.writeToFile(this.output_bolt_log, data);
		}
	}

	public void setRebalanceTime(String topoId) {
		if (this.startTimes.containsKey(topoId) == false) {
			LOG.error("Topology {} does not exist!", topoId);
			return;
		}
		Long rebalanceTime = this.startTimes.get(topoId);

	}
	
	public String printTransferThroughputHistory(){
		String retVal="";
		for( Map.Entry<String, HashMap<String, List<Integer>>> i : this.transferThroughputHistory.entrySet()) {
			retVal+="Topology: "+i.getKey()+"\n";
			for(Map.Entry<String, List<Integer>> k : i.getValue().entrySet()) {
				retVal+="Component: "+k.getKey()+"\n";
				retVal+="Transfer History: "+k.getValue().toString()+"\n";
				retVal+="MvgAvg: "+HelperFuncs.computeMovAvg(k.getValue())+"\n";
			}
		}
		return retVal;
	}
	
	public void initDataStructs(String componentId, String host, ExecutorSummary executorSummary, StormTopology stormTopo, TopologySummary topo) {
		if (this.transferThroughputHistory.containsKey(topo.get_id()) == false) {
			this.transferThroughputHistory.put(topo.get_id(),
					new HashMap<String, List<Integer>>());
		}
		if (this.emitThroughputHistory.containsKey(topo.get_id()) == false) {
			this.emitThroughputHistory.put(topo.get_id(),
					new HashMap<String, List<Integer>>());
		}
		if(this.componentStats.containsKey(topo.get_id())==false) {
			this.componentStats.put(topo.get_id(), new HashMap<String, ComponentStats>());
			
		}
		if (componentId.matches("(__).*") == false) {
			if (this.nodeStats.containsKey(host) == false) {
				this.nodeStats.put(host, new NodeStats(host));
			}
			
			if (this.componentStats.get(topo.get_id()).containsKey(componentId) == false) {
				this.componentStats.get(topo.get_id()).put(
						componentId,
						new ComponentStats(componentId));
			}
			if(this.transferThroughputHistory.get(topo.get_id()).containsKey(componentId) == false) {
				this.transferThroughputHistory.get(topo.get_id()).put(componentId, new ArrayList<Integer>());
			}
			if(this.emitThroughputHistory.get(topo.get_id()).containsKey(componentId) == false) {
				this.emitThroughputHistory.get(topo.get_id()).put(componentId, new ArrayList<Integer>());
			}
			// getting component info
			if (stormTopo.get_bolts().containsKey(componentId) == true) {

				// adding bolt to host
				this.nodeStats.get(host).bolts_on_node
						.add(executorSummary);
				// getting parallelism hint
				this.componentStats.get(topo.get_id()).get(
						componentId).parallelism_hint = stormTopo
						.get_bolts().get(componentId).get_common()
						.get_parallelism_hint();
			} else if (stormTopo.get_spouts().containsKey(
					componentId) == true) {

				// adding spout to host
				this.nodeStats.get(host).spouts_on_node
						.add(executorSummary);
				// getting parallelism hint
				this.componentStats.get(topo.get_id()).get(
						componentId).parallelism_hint = stormTopo
						.get_spouts().get(componentId).get_common()
						.get_parallelism_hint();
			} else {
				LOG.info("ERROR: type of component not determined!");
			}
		}
	}

}
