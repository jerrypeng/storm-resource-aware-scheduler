package backtype.storm.scheduler.resource.ResourceUsageServer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Serializable;



public class Profile implements Serializable{
	
	public String ip;
	private double cpu_usage;
	private double bandwidth_in;
	private double bandwidth_out;
	private double current_inoctets;
	private double current_outoctets;
	
	
	public Profile(String ip) throws IOException{
		this.ip=ip;
		this.cpu_usage=0.0;
		this.bandwidth_in=0.0;
		this.bandwidth_out=0.0;
		this.current_inoctets=0.0;
		this.current_outoctets=0.0;
	}
	
	public double getCpu_usage() {
		return cpu_usage;
	}
	public void setCpu_usage(double cpu_usage) {
		this.cpu_usage = cpu_usage;
	}
	public double getBandwidth_in() {
		return bandwidth_in;
	}
	public void setBandwidth_in(double bandwidth_in) {
		this.bandwidth_in = bandwidth_in;
	}
	public double getBandwidth_out() {
		return bandwidth_out;
	}
	public void setBandwidth_out(double bandwidth_out) {
		this.bandwidth_out = bandwidth_out;
	}
	public double getCurrent_inoctets() {
		return current_inoctets;
	}
	public void setCurrent_inoctets(double Current_inoctets) {
		this.current_inoctets = Current_inoctets;
	}
	public double getCurrent_outoctets() {
		return current_outoctets; 
	}
	public void setCurrent_outoctets(double Current_outoctets) {
		this.current_inoctets = Current_outoctets;
	}
	

}