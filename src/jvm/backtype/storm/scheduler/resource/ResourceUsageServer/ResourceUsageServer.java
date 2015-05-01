package backtype.storm.scheduler.resource.ResourceUsageServer;


import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.scheduler.resource.HelperFuncs;


public class ResourceUsageServer {
	
	private static final Logger LOG = LoggerFactory
			.getLogger(ResourceUsageServer.class);
	public static HashMap<String, Profile> profile_map;
	private static ResourceUsageServer instance;
	String schedulerType=null;
	
	private ResourceUsageServer(String schedulerType){
		this.schedulerType = schedulerType;
		try {
		File log = new File("/tmp/"+this.schedulerType+"_cpu_usage");
			

			log.delete();
		} catch (Exception e) {

			e.printStackTrace();

		}
		profile_map = new HashMap<String, Profile>();
		try{
			this.start();
		} catch (IOException ex) {
			
		}
	}
	
	public static ResourceUsageServer getInstance(String schedulerType) {
		if(instance==null) {
			instance=new ResourceUsageServer(schedulerType);
		}
		return instance;
	}
	
	private void start() throws IOException{
	//public static void start() throws IOException{
		LOG.info("Cluster Stats Monitoring Server started...");
		Thread t=new Thread(new ServerThread(this.schedulerType));
		t.start();
	}

}

class ServerThread implements Runnable{

	private static final Logger LOG = LoggerFactory
			.getLogger(ResourceUsageServer.class);
	
	String schedulerType = null;
	public ServerThread(String schedulerType) {
		this.schedulerType = schedulerType;
	}
	@Override
	public void run() {
		// TODO Auto-generated method stub
		
		int port = 6789;
		ServerSocket socket;
		try {
			socket = new ServerSocket(port, 10);
			Socket connection;
			while(true){
				//LOG.info("Waiting for connection...");
				connection=socket.accept();
				
				//LOG.info("Connection received from " + connection.getInetAddress().getHostName());
				ServerWorker worker=new ServerWorker(connection, this.schedulerType);
				worker.run();			
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
	}
	
}

class ServerWorker implements Runnable{
	
	private Socket connection;
	private ObjectInputStream in;
	private ObjectOutputStream out;
	String schedulerType=null;
	private static final Logger LOG = LoggerFactory
			.getLogger(ResourceUsageServer.class);

	public ServerWorker(Socket connection, String schedulerType) throws IOException {
		// TODO Auto-generated constructor stub
		this.connection=connection;
		this.in=new ObjectInputStream(this.connection.getInputStream());
		this.out=new ObjectOutputStream(this.connection.getOutputStream());
		this.schedulerType = schedulerType;
	}

	@Override
	public void run() {
		// TODO Auto-generated method stub
		//System.out.println("master in run...");
		try {
			this.out.flush();
			
			//receive profile
			
			Object obj=this.in.readObject();
			String ip=obj.toString();
			obj=this.in.readObject();
			double cpu=Double.valueOf(obj.toString());
			obj=this.in.readObject();
			double bandwidth_in=Double.valueOf(obj.toString());
			obj=this.in.readObject();
			double bandwidth_out=Double.valueOf(obj.toString());
			this.out.flush();

			Profile prf=new Profile(ip);
			prf.setBandwidth_in(bandwidth_in);
			prf.setBandwidth_out(bandwidth_out);
			prf.setCpu_usage(cpu);
			 InetAddress addr = InetAddress.getByName(prf.ip);
			  String host = addr.getHostName();
			  Double cpu_usage=prf.getCpu_usage();
			
			ResourceUsageServer.profile_map.put(prf.ip, prf);
			//print out information
			//System.out.println("hostname: "+host+" IP address: "+prf.ip);
			//System.out.println(prf.ip+"-Bandwidth_in: "+prf.getBandwidth_in());
			//System.out.println(prf.ip+"-Bandwidth_out: "+prf.getBandwidth_out());
			//System.out.println(prf.ip+"-cpu_usage: "+cpu_usage);
			
			String data=host+","+cpu_usage+"\n";
			
			HelperFuncs.writeToFile(new File("/tmp/"+this.schedulerType+"_cpu_usage"), data);
			
			
			ResourceUsageServer.profile_map.put(prf.ip, prf);
			//print out information
//			LOG.info("host IP address: "+prf.ip);
//			LOG.info(prf.ip+"-Bandwidth_in: "+prf.getBandwidth_in());
//			LOG.info(prf.ip+"-Bandwidth_out: "+prf.getBandwidth_out());
//			LOG.info(prf.ip+"-cpu_usage: "+prf.getCpu_usage());
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
}