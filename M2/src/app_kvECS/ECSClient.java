package app_kvECS;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.File; 
import java.io.FileNotFoundException; 
import java.util.Scanner;
import java.util.Map;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.zookeeper.*;

import logger.LogSetup;

import ecs.*;
import zookeeper.*;
import shared.metadata.*;

// Server Config Class, read from config file
class ServerConfig {
	String serverName;
	String serverIP;
	int serverPort;
	public ServerConfig(String name, String IP, int port){
		serverName = name;
		serverIP = IP;
		serverPort = port;
	}
  }

public class ECSClient implements IECSClient {
	private final String SERVICE_CONFIG_PATH = "ecs.config";

	private static Logger logger = Logger.getLogger("ECSClient");
    private boolean stop = false;
    private static final String PROMPT = "ECSClient> ";
	private BufferedReader stdin;
	public ZK zk;
	private HashRing hashRing;

	private ECSClient client =  null;
	private HashMap<String, IECSNode> availableMachines; // Nodes that are still available
	private HashMap<String, IECSNode> serversOn; // All nodes that are online
    

    public ECSClient(){
		hashRing = new HashRing();
		// Establish a connection to the zookeeper
        zk = new ZK();
		logger.info("Connecting to zookeeper...");
		zk.connect(zk.ZOOKEEPER_HOST);

		// Read config file, set available machines
		logger.info("Loading configs...");
		availableMachines = readMachineConfigs(SERVICE_CONFIG_PATH);
		logger.info(availableMachines.size() + " available machines in total");
		serversOn = new HashMap();

		// Add /ActiveServers directory
		zk.create(zk.ZOOKEEPER_ACTIVESERVERS_PATH, "", true);

		// Add /WaitingServers directory
		zk.create(zk.ZOOKEEPER_WAITINGSERVERS_PATH, "", true);

		// Add /Metadata directory
		zk.create(zk.ZOOKEEPER_METADATA_PATH, "", false);
    }

	private HashMap<String, IECSNode> readMachineConfigs(String ConfigFilePath){
		HashMap<String, IECSNode> machines = new HashMap<String, IECSNode>(); 
		try {
			File ecsFile = new File(ConfigFilePath);
			Scanner myReader = new Scanner(ecsFile);
			while (myReader.hasNextLine()) {
				String data = myReader.nextLine();
				String[] entries = data.trim().split("\\s+");
				String serverName = entries[0];
				String serverIP = entries[1];
				int serverPort = Integer.parseInt(entries[2]);

				machines.put(serverName, new ECSNode(serverName, serverIP, serverPort));
			}
			myReader.close();
		  } catch (FileNotFoundException e) {
			logger.error("File '"+ConfigFilePath+"' not found!");
		  }
		  return machines;
	}

    public void run() throws Exception {
		while(!stop) {
			stdin = new BufferedReader(new InputStreamReader(System.in));
			System.out.print(PROMPT);
			
			try {
				String cmdLine = stdin.readLine();
				this.handleCommand(cmdLine);
			} catch (IOException e) {
				logger.error("CLI does not respond - Application terminated ");
			} catch (NumberFormatException e){
				logger.error("Invalid number");
            } catch (Exception e){
				shutdown();
				zk.close();
				stop = true;
				logger.error("Error when running ECS Application", e);
			}
		}
		System.out.println(PROMPT + "Application exit!");
    }

	/**
	 * Launch a server using SSH call
	 * @param IP
	 * @param port
	 * @param cacheSize
	 * @param cacheStrategy
	 */
	private void launchServer(String IP, int port, int cacheSize, String cacheStrategy){
		Process proc;
		String script = "initialize_KVserver.sh";
		String cmd = String.join(" ", "bash", script, Integer.toString(port), Integer.toString(cacheSize), cacheStrategy);
		Runtime run = Runtime.getRuntime();
		try{
			logger.debug("Initializing server at IP:host = " + IP + ":" + port);
			proc = run.exec(cmd);
		} catch (IOException e) {
			logger.error("Failed in initalizing server ", e);
		}
	}

	/***********************************ECS-KVServer Communication Controls***************************************** */

	public void shutDownAllServersInZookeeper(){
		for (String IpPort : zk.getChildren(zk.ZOOKEEPER_ACTIVESERVERS_PATH, false)){
			zk.setData(zk.ZOOKEEPER_ACTIVESERVERS_PATH + "/" + IpPort, "SHUTDOWN");
		}
	}

	//Leave messeges to a list of active server znodes in zookeeper
	private void leaveMessages(String msg, Collection<IECSNode> nodes) {
		Iterator<IECSNode> it = nodes.iterator();
		while (it.hasNext()) {
			IECSNode node = it.next();
			String host = node.getNodeHost();
			int port = node.getNodePort();
			logger.debug("Leave a msg: " + msg + " at " + host + ":" + port);
			zk.setData(zk.ZOOKEEPER_ACTIVESERVERS_PATH + "/" + host + ":" + port, msg);
		}
	}

	// Check if the active server znode has the message
	private boolean checkMessage(String msg, String host, int port){
		String data = zk.getData(zk.ZOOKEEPER_ACTIVESERVERS_PATH + "/" + host + ":" + port);
		if (data.equals(msg)){
			return true;
		} else{
			return false;
		}
	}

	// Wait for a message in the zookeeper.
	// Return false if message does not show up before timeout
	private boolean waitMessages(long timeout, String msg, Collection<IECSNode> nodes){
		logger.info("Waiting for msg " + msg);
		// Wait for all nodes to complete the job
        long startTime = System.nanoTime();

		Iterator<IECSNode> it = nodes.iterator();
		while(it.hasNext() && System.nanoTime() - startTime <= timeout*1000000){
			IECSNode node = it.next();
			// logger.debug("  Waiting for server " + node.getNodeName());
			while (!checkMessage(msg, node.getNodeHost(), node.getNodePort())){
				if (System.nanoTime() - startTime > timeout*1000000){
					break;
				}
			}
		}

		if (System.nanoTime() - startTime > timeout*1000000){
			logger.error("Time out in waiting msg: " + msg);
			return false;
		}

        return true;
	}

    @Override
    public boolean start() {
		// Tell servers to start
		leaveMessages("START", serversOn.values());

		// Wait for all nodes to complete the job
		long timeout = 1000;
		boolean success = waitMessages(timeout*serversOn.size(), "DONE", serversOn.values());

		return success;
    }

    @Override
    public boolean stop() {
		// Tell servers to start
		leaveMessages("STOP", serversOn.values());

		// Wait for all nodes to complete the job
		long timeout = 1000;
		boolean success = waitMessages(timeout*serversOn.size(), "DONE", serversOn.values());

		return success;
    }

    @Override
    public boolean shutdown() {
		ArrayList <String> nodeNames = new ArrayList <String>();
		for(IECSNode node : serversOn.values()){
			nodeNames.add(node.getNodeName());
		}
		
		for (String nodeName : nodeNames){
			if (!serversOn.containsKey(nodeName)){
				logger.error("Node " + nodeName + " does not exist");
				return false;
			}
			IECSNode node = serversOn.get(nodeName);
			// delete node in zookeeper
			ArrayList<IECSNode> nodes = new ArrayList<IECSNode>();
			nodes.add(node);
			leaveMessages("SHUTDOWN", nodes);
		}


		// Wait until nodes no longer exists
        long startTime = System.nanoTime();
		long timeout = 1000 * serversOn.size();
		Iterator<String> it = nodeNames.iterator();
		while(it.hasNext() && System.nanoTime() - startTime > timeout*1000000){
			String nodeName = it.next();
			IECSNode node = serversOn.get(nodeName);
			while (zk.exists(zk.ZOOKEEPER_ACTIVESERVERS_PATH + "/" + node.getNodeHost() + ":" + node.getNodePort(), false)){
				if (System.nanoTime() - startTime > timeout*1000000){
					break;
				}
			}
		}
		if (System.nanoTime() - startTime > timeout*1000000){
			logger.error("Time out in waiting servers to shutown");
			return false;
		}

		// change status in ecs
		for (String nodeName : nodeNames){
			IECSNode node = serversOn.get(nodeName);
			availableMachines.put(nodeName, node);
			serversOn.remove(nodeName);
		}

        return true;
    }

	// Used to scale up, the server starts it self after it has received data from it successor
    @Override
    public IECSNode addNode(String cacheStrategy, int cacheSize) {
		logger.info("adding node...");
		if (availableMachines == null || availableMachines.size() == 0){
			logger.error("No machines available!");
			return null;
		}
		if (serversOn.size() == 0){
			logger.error("There is no node running. addNode is only used for scaling up server sizes.");
			return null;
		}
		int timeout = 5000;

		// Pick a machine
		HashMap.Entry<String, IECSNode> entry = (Map.Entry)(availableMachines.entrySet().iterator()).next();
		IECSNode node = entry.getValue();

		// Initialize Server through SSH calls
		launchServer(node.getNodeHost(), node.getNodePort(), cacheSize, cacheStrategy);
		
		// await
		try{
			awaitNodes(1, timeout);
		} catch (Exception e){
			logger.error(e);
			return null;
		}

		// Update new hash ring
		ArrayList<IECSNode> currentNodes = new ArrayList<IECSNode>();
		for (IECSNode existingNode : serversOn.values()){
			currentNodes.add(existingNode);
		}
		currentNodes.add(node);
		hashRing.updateMatadata(currentNodes);
		zk.setData(zk.ZOOKEEPER_METADATA_PATH, Serialization.serialize((Object) hashRing));

		String successorName = hashRing.getSuccessor(node.getNodeName());
		ArrayList<IECSNode> oneNodeList = new ArrayList<IECSNode>();
		oneNodeList.add(serversOn.get(successorName));

		// Let server know it has been recognized
		String node_path = zk.ZOOKEEPER_WAITINGSERVERS_PATH + "/" + node.getNodeHost() + ":" + node.getNodePort();
		zk.setData(node_path, zk.I_SEE_YOU);

		// lock
		leaveMessages("LOCKWRITE", oneNodeList);
		if (!waitMessages(timeout*serversOn.size(), "DONE", oneNodeList)){
			logger.error("Error in LOCKWRITE servers");
		}

		// Get new hash range of the current server
		String range = hashRing.getServerHashRange(node.getNodeName());

		// transfer data from successor
		String msg = "MOVE_DATA" + ":" + node.getNodeHost() + ":" + node.getNodePort() + ":" + range;
		node_path = zk.ZOOKEEPER_ACTIVESERVERS_PATH + "/" + node.getNodeHost() + ":" + node.getNodePort();
		while(!zk.exists(node_path, false)){
			// Wait for new node to come up in the active list
			// zookeeper is sometimes slow
			;
		}
		leaveMessages(msg, oneNodeList);
		boolean success = waitMessages(timeout*10, "DONE", oneNodeList);

		if (success){
			logger.info("Data transfer completed");
		} else {
			logger.error("Failed when adding node");
		}

		// Update metadata
		leaveMessages("UPDATE_METADATA", serversOn.values());
		if (!waitMessages(timeout*serversOn.size(), "DONE", serversOn.values())){
			logger.error("Error in updating all metadata");
		}

		// Unlock
		leaveMessages("UNLOCKWRITE", oneNodeList);
		if (!waitMessages(timeout*serversOn.size(), "DONE", oneNodeList)){
			logger.error("Error in UNLOCKWRITE servers");
		}

		// start
		ArrayList<IECSNode> addedNodeList = new ArrayList<IECSNode>();
		addedNodeList.add(node);
		leaveMessages("START", addedNodeList);
		if (!waitMessages(timeout*serversOn.size(), "DONE", addedNodeList)){
			logger.error("Error in START servers");
		}

		// Set node online
		((ECSNode) node).setOnline();
		availableMachines.remove(node.getNodeName());
		serversOn.put(node.getNodeName(), node);

        return node;
    }


	// Used to initialize only, need to call start to start all the servers
    @Override
    public Collection<IECSNode> addNodes(int count, String cacheStrategy, int cacheSize) {
		logger.debug("adding " + count + " nodes with cacheStrategy, cacheSize = "+cacheStrategy+", "+cacheSize);
        // Check count range(1 - N)
        if (count < 0 || count > availableMachines.size()){
            logger.error("Requiring " + count + " machines, but can only request 1 - " + availableMachines.size() + " machines!");
            return null;
        }
		if (serversOn.size() != 0){
			logger.error("There are servers already running");
			return null;
		}

		// Pick machines & update metadata
		ArrayList<IECSNode> list = (ArrayList<IECSNode>) setupNodes(count, cacheStrategy, cacheSize);

        // Initialize Servers through SSH calls
        for(IECSNode node : list){
			launchServer(node.getNodeHost(), node.getNodePort(), cacheSize, cacheStrategy);
        }

		// await
		try{
			awaitNodes(count, 5000*count);
		} catch (Exception e){
			logger.error(e);
			return null;
		}
		
		// if success: Set nodes online
		for(IECSNode node : list){
			// Let server know it has been recognized
			String node_path = zk.ZOOKEEPER_WAITINGSERVERS_PATH + "/" + node.getNodeHost() + ":" + node.getNodePort();
			zk.setData(node_path, zk.I_SEE_YOU);

			int timeout = 2000;
			long startTime = System.nanoTime();
			node_path = zk.ZOOKEEPER_ACTIVESERVERS_PATH + "/" + node.getNodeHost() + ":" + node.getNodePort();
			while(!zk.exists(node_path, false) && System.nanoTime() - startTime <= timeout*1000000){
				;
			}
			if (System.nanoTime() - startTime > timeout*1000000) {
				logger.error("Timeout in moving node " + node.getNodeName() + " from waiting list to active list");
				return null;
			}

			// Set node online
			((ECSNode) node).setOnline();
			availableMachines.remove(node.getNodeName());
			serversOn.put(node.getNodeName(), node);
        }
        return list;
    }

	// Select available machines
    @Override
    public Collection<IECSNode> setupNodes(int count, String cacheStrategy, int cacheSize) {
		ArrayList<IECSNode> list=new ArrayList<IECSNode>();
		// Select randome nodes from availabe server list, assume there are sufficient machines
		Iterator it = availableMachines.entrySet().iterator();
		for(int i=0; i != count; i++){
			HashMap.Entry<String, IECSNode> pair = (Map.Entry)it.next();
			list.add(pair.getValue());
		}
		// Calculate new metadata directly from the list
		hashRing.updateMatadata(list);
		// Update metadata in zookeeper
		zk.setData(zk.ZOOKEEPER_METADATA_PATH, Serialization.serialize((Object) hashRing));
        return list;
    }

    @Override
    public boolean awaitNodes(int count, int timeout_int) throws Exception {
		logger.debug("Awaiting " + count + " nodes.");
        long startTime = System.nanoTime();
		long timeout = (long) timeout_int;
		ArrayList<String> reported_children_path = null;
		while(true){
			if (System.nanoTime() - startTime > timeout*1000000){
				logger.error("Time out in waiting nodes");
				throw new Exception("Time out in awaiting nodes.");
			}
			ArrayList<String> children = zk.getChildren(zk.ZOOKEEPER_WAITINGSERVERS_PATH, true);

			if (children.size() > count){
				logger.error("More servers are in waiting list then expected.");
				break;
			}
			if (children.size() == count){
				String listString = "";
				for (String s : children)
				{
					listString += s + ";";
				}
				logger.info("Servers initialized:  " + listString);
				break;
			}
		}
        return true;
    }

    @Override
    public boolean removeNodes(Collection<String> nodeNames) {
		for(String nodeName : nodeNames){
			logger.debug("Removing node " + nodeName);
			IECSNode node = serversOn.get(nodeName);

			// Get hash range and predeceser of the server to be deleted
			// Need to be done before hash ring update
			String range = hashRing.getServerHashRange(nodeName);
			String successorName = hashRing.getSuccessor(nodeName);

			// Update hash ring
			ArrayList<IECSNode> list=new ArrayList<IECSNode>();
			Iterator it = serversOn.entrySet().iterator();
			while(it.hasNext()){
				HashMap.Entry<String, IECSNode> pair = (Map.Entry)it.next();
				if(!pair.getKey().equals(nodeName)){
					list.add(pair.getValue());
				}
			}
			// Calculate new metadata directly from the list
			hashRing.updateMatadata(list);

			hashRing.printAllServer();
			// Update metadata in zookeeper
			zk.setData(zk.ZOOKEEPER_METADATA_PATH, Serialization.serialize((Object) hashRing));

			// Lock the server
			int timeout = 2000;
			ArrayList<IECSNode> oneNodeList = new ArrayList<IECSNode>();
			oneNodeList.add(node);
			leaveMessages("LOCKWRITE", oneNodeList);
			if (!waitMessages(timeout, "DONE", oneNodeList)){
				logger.error("Error in locking write on " + nodeName);
			}

			// Transfer data to successor
			String msg = "MOVE_DATA" + ":" + hashRing.getServerIpPort(successorName) + ":" + range;
			leaveMessages(msg, oneNodeList);
			if (!waitMessages(timeout, "DONE", oneNodeList)){
				logger.error("Error in MOVE_DATA " + nodeName);
			}

			// Update metadata on on servers
			leaveMessages("UPDATE_METADATA", serversOn.values());
			if (!waitMessages(timeout*serversOn.size(), "DONE", serversOn.values())){
				logger.error("Error in updating all metadata");
			}

			// Shutdown server
			leaveMessages("SHUTDOWN", oneNodeList);

			// Wait until node shutdown
			long startTime = System.nanoTime();
			while (zk.exists(zk.ZOOKEEPER_ACTIVESERVERS_PATH + "/" + node.getNodeHost() + ":" + node.getNodePort(), false)){
				if (System.nanoTime() - startTime > 2000*1000000){
					logger.error("Timeout in shutting down the server " + nodeName);
					return false;
				}
			}

			availableMachines.put(nodeName, node);
			serversOn.remove(nodeName);
		}
		return true;
    }

    @Override
    public Map<String, IECSNode> getNodes() {
        return serversOn;
    }

    @Override
    public IECSNode getNodeByKey(String Key) {
        return serversOn.get(Key);
    }

	public boolean clearAllServersStorage(){
		// Tell servers to clear storage
		leaveMessages("CLEAR_STORAGE", serversOn.values());

		// Wait for all nodes to complete the job
		long timeout = 1000;
		boolean success = waitMessages(timeout*serversOn.size(), "DONE", serversOn.values());

		return success;
	}

	/***********************Other functions************************** */

	// Print machine status on zookeeper
	private void printMachineStatus(){
		ArrayList<String> waitingServers = zk.getChildren(zk.ZOOKEEPER_WAITINGSERVERS_PATH, true);
		ArrayList<String> activeServers = zk.getChildren(zk.ZOOKEEPER_ACTIVESERVERS_PATH, true);

		String activeServersString = "";
		for (String s : activeServers)
		{
			activeServersString += s + "; ";
		}
		logger.info("\nActive Servers on zookeeper:\n " + activeServersString);

		String waitingServersString = "";
		for (String s : waitingServers)
		{
			waitingServersString += s + "; ";
		}
		logger.info("\nWaiting Servers on zookeeper:\n " + waitingServersString);
	}

	private String setLevel(String levelString) {
		if(levelString.equals(Level.ALL.toString())) {
			logger.setLevel(Level.ALL);
			return Level.ALL.toString();
		} else if(levelString.equals(Level.DEBUG.toString())) {
			logger.setLevel(Level.DEBUG);
			return Level.DEBUG.toString();
		} else if(levelString.equals(Level.INFO.toString())) {
			logger.setLevel(Level.INFO);
			return Level.INFO.toString();
		} else if(levelString.equals(Level.WARN.toString())) {
			logger.setLevel(Level.WARN);
			return Level.WARN.toString();
		} else if(levelString.equals(Level.ERROR.toString())) {
			logger.setLevel(Level.ERROR);
			return Level.ERROR.toString();
		} else if(levelString.equals(Level.FATAL.toString())) {
			logger.setLevel(Level.FATAL);
			return Level.FATAL.toString();
		} else if(levelString.equals(Level.OFF.toString())) {
			logger.setLevel(Level.OFF);
			return Level.OFF.toString();
		} else {
			return LogSetup.UNKNOWN_LEVEL;
		}
	}

	private void printPossibleLogLevels() {
		System.out.println(PROMPT 
				+ "Possible log levels are:");
		System.out.println(PROMPT 
				+ "ALL | DEBUG | INFO | WARN | ERROR | FATAL | OFF");
	}

	private void printHelp() {
		StringBuilder sb = new StringBuilder();
		sb.append("ECS CLIENT HELP (Usage):\n");
		sb.append("::::::::::::::::::::::::::::::::");
		sb.append("::::::::::::::::::::::::::::::::\n");
		sb.append("logLevel");
		sb.append("\t\t\t changes the logLevel \n");
		sb.append("\t\t\t\t ");
		sb.append("ALL | DEBUG | INFO | WARN | ERROR | FATAL | OFF \n");
		sb.append("quit ");
		sb.append("\t\t\t exits the program");
		System.out.println(sb.toString());
	}


	private void handleCommand(String cmdLine) throws Exception {
		String[] tokens = cmdLine.trim().split("\\s+");

		if(tokens[0].equals("quit")) {	
			stop = true;
			if (shutdown()){
				logger.info("Successfully shutdown all nodes");
			}
			else{
				logger.error("Failed to shutdown all nodes");
			}
			zk.close();
        } else if (tokens[0].equals("addNodes")) {
            if(tokens.length == 4) {
                int count;
                String cacheStrategy = tokens[2];
                int cacheSize;
                cacheSize = Integer.parseInt(tokens[3]);
                count = Integer.parseInt(tokens[1]);
				Collection<IECSNode> nodes = addNodes(count, cacheStrategy, cacheSize);
				if(nodes == null){
					logger.error("Error in adding nodes");
				}
                else if(nodes.size() == count){
					logger.info("Nodes successfully added");
				} else{
					logger.info("Failed to add all nodes");
				}
				
			} else {
				logger.error("Invalid number of parameters!");
			}
		} else if (tokens[0].equals("addNode")) {
            if(tokens.length == 3) {
                String cacheStrategy = tokens[1];
                int cacheSize;
                cacheSize = Integer.parseInt(tokens[2]);
				IECSNode node = addNode(cacheStrategy, cacheSize);
                if(node != null){
					logger.info("Node successfully added");
				} else{
					logger.info("Failed to add node");
				}
			} else {
				logger.error("Invalid number of parameters!");
			}
		}  else if (tokens[0].equals("removeNode")) {
            if(tokens.length == 2) {
                String nodeName = tokens[1];
                ArrayList<String> nodeNames = new ArrayList<String> ();
				nodeNames.add(nodeName);
				if (removeNodes(nodeNames)){
					logger.info("Successfully removed node: " + nodeName);
				}
				else{
					logger.error("Failed to remove node: " + nodeName);
				}
			} else {
				logger.error("Invalid number of parameters!");
			}
		} else if (tokens[0].equals("start")) {
			if (start()){
				logger.info("Successfully start all nodes");
			}
			else{
				logger.error("Failed to start all nodes");
			}
		} else if (tokens[0].equals("stop")) {
			if (stop()){
				logger.info("Successfully stop all nodes");
			}
			else{
				logger.error("Failed to stop all nodes");
			}
		} else if (tokens[0].equals("shutdown")) {
			if (shutdown()){
				logger.info("Successfully shutdown all nodes");
			}
			else{
				logger.error("Failed to shutdown all nodes");
			}
		} else if (tokens[0].equals("machineStatus")) {
			printMachineStatus();
		} else if (tokens[0].equals("shutdownAllServers")){
			shutDownAllServersInZookeeper();
		} else if (tokens[0].equals("clearStorage")){
			clearAllServersStorage();
		} else if (tokens[0].equals("logLevel")) {
			if(tokens.length == 2) {
				String level = setLevel(tokens[1]);
				if(level.equals(LogSetup.UNKNOWN_LEVEL)) {
					logger.error("No valid log level!");
					printPossibleLogLevels();
				} else {
					System.out.println(PROMPT + 
							"Log level changed to level " + level);
				}
			} else {
				logger.error("Invalid number of parameters!");
			}
			
		} else if(tokens[0].equals("help")) {
			printHelp();
		} else {
			logger.error("Unknown command");
			printHelp();
		}
	}

	public HashMap<String, IECSNode> getServersOn(){
		return serversOn;
	}

    public static void main(String[] args) {
    	try {
			new LogSetup("logs/ECSClient.log", Level.ALL);
			ECSClient ecsclient = new ECSClient();
			ecsclient.run();
		} catch (IOException e) {
			System.out.println("Error! Unable to initialize logger!");
			e.printStackTrace();
			System.exit(1);
		} catch (Exception e) {
			e.printStackTrace();
		}
    }
}
