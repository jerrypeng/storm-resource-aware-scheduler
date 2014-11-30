package backtype.storm.scheduler.resource;

import java.util.ArrayList;
import java.util.List;

import backtype.storm.scheduler.ExecutorDetails;

public class Component {
	public enum ComponentType{
		SPOUT(1), BOLT(2);
		
		private int value;
		private ComponentType(int value) {
			this.value = value;
		}
	}
	public String id;
	public List<String> parents = null;
	public List<String> children = null;
	public List<ExecutorDetails> execs = null;
	public ComponentType type = null;
	public Component(String id) {
		this.parents = new ArrayList<String>();
		this.children = new ArrayList<String>();
		this.execs = new ArrayList<ExecutorDetails>();
		this.id = id;
	}
	@Override public String toString() {
		String retVal = "id: "+this.id+" Parents: "+this.parents.toString()+" Children: "+this.children.toString() + " Execs: "+this.execs;
		return retVal;
	}
	
	
}
