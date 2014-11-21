package backtype.storm.scheduler.resource;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GetNetworkInfo {
	private static final Logger LOG = LoggerFactory
			.getLogger(GetNetworkInfo.class);
	File networkDataFile;
	Map<String, List<String>> clusteringInfo;

	public GetNetworkInfo() {
		this.networkDataFile = new File(Config.NETWORK_DATA_FILE);
		this.clusteringInfo = new HashMap<String, List<String>>();
	}

	public void getClusterInfo() {
		try {
			// read the json file
			FileReader reader = new FileReader("NetworkData");

			JSONParser jsonParser = new JSONParser();
			JSONObject jsonObject = (JSONObject) jsonParser.parse(reader);

			JSONObject networkData = (JSONObject) jsonObject.get("NetworkData");
			JSONArray clusterInfo = (JSONArray) networkData.get("ClusterInfo");

			for (int i = 0; i < clusterInfo.size(); i++) {
				JSONObject cluster = (JSONObject) clusterInfo.get(i);
				String cluster_num = (String) cluster.get("cluster");
				JSONArray nodes = (JSONArray) cluster.get("nodes");
				ArrayList<String> clus = new ArrayList<String>();
				for (int j = 0; j < nodes.size(); j++) {
					String canonicalName = HelperFuncs.hostnameToCanonicalName((String) nodes.get(j));
					clus.add(canonicalName);
				}
				this.clusteringInfo.put(cluster_num, clus);

			}
		} catch (ParseException e) {
			LOG.error(e.toString());
		} catch (FileNotFoundException e) {
			LOG.error(e.toString());
		} catch (IOException e) {
			LOG.error(e.toString());
			e.printStackTrace();
		}
	}

	public String getNetworkData() {
		String retVal = "";
		try {
			BufferedReader in = new BufferedReader(new FileReader(
					Config.NETWORK_DATA_FILE));
			String line = null;
			while ((line = in.readLine()) != null) {
				retVal += line;
			}

		} catch (FileNotFoundException e) {
			LOG.error(e.toString());
		} catch (IOException e) {
			LOG.error(e.toString());
		}
		return retVal;
	}
}
