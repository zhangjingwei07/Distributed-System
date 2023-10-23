package app_kvServer;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.zookeeper.*;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.json.simple.JSONObject;

import client.TextMessage;
import logger.LogSetup;
import server.ClientConnection;
import shared.metadata.HashRing;
import shared.metadata.SubConnectionTable;
import shared.metadata.Serialization;
import shared.messages.KVMessage;
import shared.messages.KVMessageImple;
import shared.messages.KVMessage.StatusType;
import storage.KVStorage;
import zookeeper.ZK;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.File;
import java.net.BindException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.*;


public class KVServer extends Thread implements IKVServer {
	private static Logger logger;
	private static final String PROMPT = "KVServer> ";

	// Traditional Client - Server Connection info
	private final String HOST_NAME = "127.0.0.1";	
	private final int port;	
	private boolean running;
	private String serverName = "DefaultServerName"; // Default server name;	
	private ServerSocket serverSocket = null;
    private ServerStatus status = ServerStatus.ACTIVE;

	// Server (acting as client) - Server connection info
	private Socket clientSocket;
	private OutputStream output = null;
	private InputStream input = null;
	private static final int BUFFER_SIZE = 1024;
	private static final int DROP_SIZE = 1024 * BUFFER_SIZE;
	// Reserved key for data transfer. Currently a place holder
	private static final String DATA_TRANSFER_KEY = "DATA_TRANSFER_KEY";	 

	// Storage & Metadata info
	private CacheStrategy strategy;
	private HashRing metadata = new HashRing();
	private int cacheSize;
	private KVStorage storage;

	// ECS info
	private static boolean admin_invoked;
	private ZK zk = null;
	private Watcher watcherNodeDataChange = null;

	// Subscription info
	private SubConnectionTable subConnectionTable = new SubConnectionTable();
	// store the mapping relationship of clientname & corresponding subscription connection
	private HashMap<String, ClientConnection> clientName_subConnection = new HashMap<String, ClientConnection>();
	// If this server currently has a subcription thread with a client, this will have a value
	// Remember to turn this name back to null when the subscription thread is terminated.
	private String subscriptionClientName = null;

	/**
	 * Start KV Server at given port
	 * @param port given port for storage server to operate
	 * @param cacheSize specifies how many key-value pairs the server is allowed
	 *           to keep in-memory
	 * @param strategy specifies the cache replacement strategy in case the cache
	 *           is full and there is a GET- or PUT-request on a key that is
	 *           currently not contained in the cache. Options are "FIFO", "LRU",
	 *           and "LFU".
	 */
	public KVServer(final int port, int cacheSize, String strategy) {
		this.port = port;
		this.cacheSize = cacheSize;
		this.strategy = CacheStrategy.valueOf(strategy);;
		this.storage = new KVStorage(this.cacheSize, this.strategy, this.port);

		int temp_port = port % 10;
		if (temp_port == 0) {
			temp_port = 10;
		} 

		logger = Logger.getLogger("server" + temp_port);
		this.start();
		if (admin_invoked){
			status = ServerStatus.WAITING;
			// Create znode on zookeeper
			zk = new ZK();
			zk.connect(zk.ZOOKEEPER_HOST);
			String node_path = zk.ZOOKEEPER_WAITINGSERVERS_PATH + "/" + HOST_NAME + ":" + port;
			zk.create(node_path, "", false);

			// Once ecs sees the server, Change server to acitive in zookeeper
			while (true) {
				if (zk.getData(node_path).equals(zk.I_SEE_YOU)){
					// Set up metadata
					logger.debug("Set up metadata from zookeeper");
					updateMetadata();
					serverName = metadata.getServerNameFromIpPort(HOST_NAME + ":" + port);
					logger.debug("ECS sees the server, move server node under " + zk.ZOOKEEPER_ACTIVESERVERS_PATH);
					zk.delete(node_path, -1);
					node_path = zk.ZOOKEEPER_ACTIVESERVERS_PATH + "/" + HOST_NAME + ":" + port;
					zk.create(node_path, "", false);
					status = ServerStatus.STOP;
					// Watch the event of data change
					// To communicate with ecs
					watcherNodeDataChange = new Watcher() {
						public void process(WatchedEvent we) {
							watchNodedataChange(we.getPath());
							if(we.getType() == Watcher.Event.EventType.NodeDataChanged){
								// logger.info("Watcher triggered on data changed");
								String ECSCommand = checkECSMessage();
								handleECSCommand(ECSCommand);
							}
						}
					};
					watchNodedataChange(node_path);
					break;
				}
			}
		}
	}

	private void watchNodedataChange(String node_path){
		zk.setUpWatcher(node_path, watcherNodeDataChange);
	}


    private boolean initializeServer() {
    	logger.info("Initialize server ...");
    	try {
            serverSocket = new ServerSocket(port);
            logger.info("Server listening on port: " 
            		+ port);    
            return true;
        
        } catch (IOException e) {
        	logger.error("Error! Cannot open server socket:");
            if(e instanceof BindException){
            	logger.error("Port " + port + " is already bound!");
            }
            return false;
        }
	}	
	
    private boolean isRunning() {
        return this.running;
	}	
	
	@Override
	public int getPort(){
		return this.port;
	}

	@Override
	public String getHostname() {
		// TODO Auto-generated method stub
		return null;
	}
	
	public String getServerName() {
		return serverName;
	}

	public void setServerName(String _serverName) {
		serverName = _serverName;
	}

	@Override
    public CacheStrategy getCacheStrategy(){
		return strategy;
	}

	@Override
    public int getCacheSize(){
		return cacheSize;
	}

	@Override
	public boolean valueInStorage(String key) {
		return storage.get(key) != null;
	}

	public int subscriberInStorage(String key, String client) {
		return storage.didSub(key, client);
	}
	
	public KVStorage getStorage() {
		return storage;
	}

	@Override
    public boolean inCache(String key){
		return storage.cache.inCache(key);
	}

	@Override
    public String getKV(String key) throws Exception{
		return storage.get(key);
	}

	@Override
	public void putKV(String key, String value) throws Exception {
		// First, store the old value before PUT command, as we need the old value (it can be null).
		String oldVal = getKV(key);

		if (value != null) {
			storage.put(key, value);
		} else {
			storage.delete(key);
		}
		// If data is in range, the curent server is the coordinator. Or else the server is one of the successor. We need to do 2 things:
		if (metadata.inRange(key, serverName)) {
			// (1) Send replicated PUT / DELETE commands
			if (putOrDeleteReplicatedData(key, value)) {
				logger.debug("Successfully send all replication to successors, if possible.");
			} else {
				logger.error("Failed to send all replication to successors. One or two replication action has failed.");
			}
			// (2) Trigger subscription update 

			HashSet<String> subscribers = storage.getSubscribersWithKey(key);

			if (subUpdateToAllResponsibleServers(key, oldVal, value, subscribers)) {
				logger.debug("send subscription update of key = " + key + " to all subscription servers, if possible.");
			} else {
				logger.error("Some subscription update of key = " + key + " to responsible subscription servers are failed.");
			}
			
		}
	}
	
	public int sub(String key, String client) throws Exception {
		int returnResult = storage.sub(key, client);

		// Same reason as putKV
		if (metadata.inRange(key, serverName)) {
			if (subOrUnsubReplicatedData(key, client, true)) {
				logger.debug("Successfully sending all subscription replication to successors, if possible.");
			} else {
				logger.error("Some subscription replications to successors are failed.");
			}
		}
		return returnResult;
	}
	
	public int unsub(String key, String client) throws Exception {
		int returnResult = storage.unsub(key, client);

		// Same reason as putKV
		if (metadata.inRange(key, serverName)) {
			if (subOrUnsubReplicatedData(key, client, false)) {
				logger.debug("Successfully sending all unsubscription replication to successors, if possible.");
			} else {
				logger.error("Some unsubscription replications to successors are failed.");
			}
		}
		return returnResult;
	}

	public int didSub(String key, String client) throws Exception {
		return storage.didSub(key, client);
	}

	public HashRing getMetadata(){
		return metadata;
	}

	public void setMetadata(HashRing new_metadata){
		metadata = new_metadata;
	}

	public HashMap<String, ClientConnection> getClientName_subConnection() {
		return clientName_subConnection;
	}

	public void printAllSubConnections() {
		logger.debug("Following are all the subscription threads of server = " + serverName + ": ");
		for (Map.Entry pair : clientName_subConnection.entrySet()) {
			logger.debug(pair.getKey() + "_" + serverName);
		}
	}

	@Override
    public void clearCache(){
		storage.cache.clearCache();
	}

	@Override
	public void clearStorage() {
		storage.clearStorage();
	}
	
    /**
     * Merge the original storage with newly received data
     */
	public boolean mergeStorage(JSONObject JSONData) {
		return storage.mergeStorage(JSONData);
	}

	@Override
    public void run(){
		running = initializeServer();
        
        if(serverSocket != null) {
	        while(isRunning()){
	            try {
					Socket client = serverSocket.accept();

					System.out.print("client socket address: " + client.getRemoteSocketAddress());

	                ClientConnection connection = 
	                		new ClientConnection(this, client);
					new Thread(connection).start();

	                logger.info("Connected to " 
	                		+ client.getInetAddress().getHostName() 
	                		+  " on port " + client.getPort());
	            } catch (IOException e) {
					if (isRunning()){
						logger.error("Error! Unable to establish connection. \n", e);
					}
	            }
	        }
        }
        logger.info("Server stopped.");
	}

	public String checkECSMessage(){
		return zk.getData(zk.ZOOKEEPER_ACTIVESERVERS_PATH + "/" + HOST_NAME + ":" + port);
	}

	public void handleECSCommand(String ECSCommand){
		String tokens[] = ECSCommand.trim().split(":");
		if (ECSCommand == null || ECSCommand.equals("")){
			return;
		} else if (tokens[0].equals("SHUTDOWN")){
			logger.debug("Received SHUTDOWN command from ecs");
			close();
		} else if (tokens[0].equals("LOCKWRITE")){
			logger.debug("Received LOCKWRITE command from ecs");
			lockWrite();
		} else if (tokens[0].equals("UNLOCKWRITE")){
			logger.debug("Received UNLOCKWRITE command from ecs");
			unlockWrite();
		} else if (tokens[0].equals("SEND_DATA")){
			if (tokens.length != 5){
				logger.error("Expected 4 arguments in message: " + ECSCommand);
				return;
			}
			sendData(tokens[3], tokens[4], tokens[1] + ":" + tokens[2]);
			logger.debug("Received MOVE_DATA command from ecs");
		} else if (tokens[0].equals("DELETE_DATA")){
			if (tokens.length != 3){
				logger.error("Expected 2 arguments in message: " + ECSCommand);
				return;
			}
			deleteData(tokens[1], tokens[2]);
			logger.debug("Received DELETE_DATA command from ecs");
		} else if (tokens[0].equals("MOVE_DATA")) {
			// Get "private" new metadata from zookeeper
			// Get the serverName from corresponding Znode
			if (tokens.length != 5){
				logger.error("Expected 4 arguments in message: " + ECSCommand);
				return;
			}
			moveData(tokens[3], tokens[4], tokens[1] + ":" + tokens[2]);
			logger.debug("Received MOVE_DATA command from ecs");
		} else if (tokens[0].equals("UPDATE_METADATA")) {
			// Get "public" new metadata from zookeeper
			logger.debug("Received UPDATE_METADATA command from ecs");
			updateMetadata();
		} else if (tokens[0].equals("START")) {
			logger.debug("Received START command from ecs");
			start_all();
		} else if (tokens[0].equals("STOP")) {
			logger.debug("Received STOP command from ecs");
			stop_all();
		} else if (tokens[0].equals("CLEAR_STORAGE")) {
			logger.debug("Received CLEAR_STORAGE command from ecs");
			storage.clearStorage();
		} else if (tokens[0].equals("CAST_SUBCONNECTIONTABLE")){
			logger.debug("Received CAST_SUBCONNECTIONTABLE command from ecs");
			if (!unicastSubConnectionTableToNewlyAddedServer(tokens[1] + ":" + tokens[2])){
				logger.debug("CAST fail");
			}

		}else if (tokens[0].equals("DONE")) {
			return;
		} else{
			logger.error("Unrecognized ECSCommand: " + ECSCommand);
			return;
		}

		// Tell server the task is completed
		String nodePath = zk.ZOOKEEPER_ACTIVESERVERS_PATH + "/" + HOST_NAME + ":" + port;
		zk.setData(nodePath, "DONE");
		logger.debug("Done handling the command " + ECSCommand);
	}

	@Override
    public void kill(){
		try{
			if(serverSocket != null){
				this.serverSocket.close();
			}
		} catch (IOException e){
			logger.error(PROMPT + "Unable to close socket on port: " + this.port, e);
		} 
		this.running = false;
		// 问题 Can add more implementation, e.g, save data in cache into files.
	}

	@Override
    public void close() {
		kill();

		// Close node on zookeeper
		if (zk != null){
			String node_path = zk.ZOOKEEPER_ACTIVESERVERS_PATH + "/" + HOST_NAME + ":" + port;
			zk.delete(node_path, -1);
		}

		// Can add more actions, e.g, clear cache.
	}

	/************************** ECS Command *****************************/
	// The server is only allowed to call LOCKWRITE and UNLOCKWRITE by itself
	// Other functions need to be invoked by ECS
	@Override
	public void lockWrite() {
		logger.info(this.serverName + ": server status is changed to LOCKED");
		this.status = ServerStatus.LOCKED;
	}

	@Override
	public void unlockWrite() {
		logger.info(this.serverName + ": server status is changed to ACTIVE");
		this.status = ServerStatus.ACTIVE;
	}

	@Override
	public void start_all() {
		logger.info(this.serverName + ": server status is changed to ACTIVE");
		this.status = ServerStatus.ACTIVE;
	}

	@Override
	public void stop_all() {
		logger.info(this.serverName + ": server status is changed to STOP");
		this.status = ServerStatus.STOP;
	}
  
  public ServerStatus getStatus() {
		return status;
	}

	public void updateMetadata(){
		metadata = (HashRing) Serialization.deserialize(zk.getDataBtye(zk.ZOOKEEPER_METADATA_PATH));
		metadata.printAllServer();
	}

	public ServerStatus getServerStatus(){
		return status;
	}

	/**
	 * Send the data in a given range to targeted server + merge data in the targeted server.
	 */
	public boolean sendData(String leftBound, String rightBound, String ip_port_pair) {
		String[] ip_port_array = ip_port_pair.trim().split(":");
		String address = ip_port_array[0];
		int port = Integer.parseInt(ip_port_array[1]);

		String stringData = this.storage.preSendData(leftBound, rightBound);
		if (stringData == null || stringData.isEmpty()) {
			return true;
		}
		String secretKey = KVMessageImple.DEFAULT_SECRET;
		KVMessage message = new KVMessageImple(DATA_TRANSFER_KEY, stringData, KVMessage.StatusType.MERGE, secretKey);
		// Response Object placeholder.
		KVMessageImple received_message = new KVMessageImple(DATA_TRANSFER_KEY, null, KVMessage.StatusType.MERGE, secretKey);
		try 
		{
			// Establish connection to targeted server
			connectToTargetedServer(address, port);
			// Send message to targeted server
			sendMessage(new TextMessage(((KVMessageImple) message).encode()), KVMessageImple.DEFAULT_SECRET);
			// Wait for targeted server response
			TextMessage latestMsg = receiveMessage(KVMessageImple.DEFAULT_SECRET);		
			received_message.decode(latestMsg.getMsg());
			DisconnectToTargetedServer();
		} catch (IOException e){
			logger.error("Fail to move data: " + e);
		}
		// Handle the received message from targeted server regarding the file merging
		if (received_message.getStatus().equals(KVMessage.StatusType.MERGE_SUCCESS)) {
			logger.info("File merging is successful.");
			return true;
		} else if(received_message.getStatus().equals(KVMessage.StatusType.MERGE_ERROR)) {
			logger.error("File merging is failed!");
			return false;
		} else {
			logger.error("Received unknown status regarding file merging!");
			return false;
		}		
	} 	
	
	/**
	 * Call the storage to delete the data in the given range
	 */
	public boolean deleteData(String leftBound, String rightBound) {
		return this.storage.deleteData(leftBound, rightBound);
	}

	/**
	 * Send the PUT command value update to the range of subscription-responsible servers.
	 */
	public boolean subUpdateToAllResponsibleServers(String key, String oldVal, String newVal, HashSet<String> subscribers) {
		boolean areAllUpdatesSuccessful = true;

		for (String subscriber : subscribers) {
			// Special case: if the subscriber (aka, client) does not connect to any server. Say, the subscriber 
			// has already ***actively*** disconnected from the whole service.
			if (!subConnectionTable.connectionExists(subscriber)) {
				logger.debug("Skip SUBUPDATETOSERVER: subscriber = " + subscriber + " does not connect to the service. No need to send subscription update.");
				continue;
			}


			String serverName = subConnectionTable.getConnectedServerName(subscriber);
			logger.debug("SubUpdateToServer: " + serverName + ".");

			String ip_port_pair = metadata.getAllServerInfo().get(serverName);
			logger.debug("SubUpdateToAllResponsibleServers: ip_port_pair = " + ip_port_pair);

			String[] ip_port_array = ip_port_pair.trim().split(":");
			String address = ip_port_array[0];
			int port = Integer.parseInt(ip_port_array[1]);

			String stringData = null;
			if (oldVal == null && newVal == null) {
				logger.error("Failed SUBUPDATETOSERVER: old value and new value should not both be null.");
			} else if (oldVal == null) {
				// Insert case
				stringData = "Subscription Update: key - " + key + ", new value - " + newVal + " has been inserted.";
			} else if (newVal == null) {
				// Delete case
				stringData = "Subscription Update: key - " + key + ", old value - " + oldVal + " has been deleted.";
			} else if (oldVal != null && oldVal.equals(newVal)) {
				logger.debug("The old value = " + oldVal + " is the same as the new value = " + newVal
						+ ". No subscription update will happen.");
				return true;
			} else {
				// Update case
				stringData = "Subscription Update: key - " + key + ", old value - " + oldVal
						+ " has been updated to new value - " + newVal + ".";
			}
			// Key is the subscriber name, so that in the subscription server, it knows which subscription clientconnection
			// should be used to forward the subscription update
			String secretKey = KVMessageImple.DEFAULT_SECRET;
			KVMessage message = new KVMessageImple(subscriber, stringData, KVMessage.StatusType.SUBUPDATETOSERVER, secretKey);
			// Response Object placeholder.
			// By default, set to SUBUPDATETOSERVER_ERROR. If response has been successfully received, the status
			// will be changed to KVMessage.StatusType.SUBUPDATETOSERVER_SUCCESS
			KVMessageImple received_message = new KVMessageImple(subscriber, null, KVMessage.StatusType.SUBUPDATETOSERVER, secretKey);
			try {
				// Establish connection to targeted server
				connectToTargetedServer(address, port);
				// Send message to targeted server
				sendMessage(new TextMessage(((KVMessageImple) message).encode()), KVMessageImple.DEFAULT_SECRET);
				// Wait for subscription-responsible server response
				TextMessage latestMsg = receiveMessage(KVMessageImple.DEFAULT_SECRET);
				received_message.decode(latestMsg.getMsg());
				DisconnectToTargetedServer();
			} catch (IOException e) {
				// This can happen if the responsible server is crashed while the subConnection table has not yet been updated and broadcasted
				logger.error("Failed SUBUPDATETOSERVER: key = " + key + ", server = " + serverName + ". " + e);
				areAllUpdatesSuccessful = false;
			}
			// Handle the received message from targeted server regarding the file merging
			if (received_message.getStatus().equals(KVMessage.StatusType.SUBUPDATETOSERVER_SUCCESS)) {
				logger.info("Successful SUBUPDATETOSERVER: key = " + key + ", server = " + serverName + ".");
			} else if (received_message.getStatus().equals(KVMessage.StatusType.SUBUPDATETOSERVER_ERROR)) {
				logger.error("Failed SUBUPDATETOSERVER: key = " + key + ", server = " + serverName + ".");
				areAllUpdatesSuccessful = false;
			} else {
				logger.error("Received unknown status regarding SUBUPDATETOSERVER: key = " + key + ", server = "
						+ serverName + ".");
				areAllUpdatesSuccessful = false;
			}				
		}
		
		// All SUBUPDATETOSERVER requests are successful
		return areAllUpdatesSuccessful;
	}

	/* Following are the helper functions related to data transfer */

	/**
	 * Function to connect the current server with the targeted server for data transfer
	 * @param targetHost the targeted server host
	 * @param targetPort the targeted server port
	 */
	public void connectToTargetedServer(String targetHost, int targetPort) throws IOException, UnknownHostException {
		clientSocket = new Socket(targetHost, targetPort);
		output = clientSocket.getOutputStream();
		input = clientSocket.getInputStream();

		logger.info("Connecting to targeted server");
	}

	/**
	 * Function to disconnect with the targeted server after moving data
	 * @throws IOException
	 */
	private void DisconnectToTargetedServer() throws IOException {
		logger.debug("tearing down the connection with targeted server ...");
		if (clientSocket != null) {
			// input.close();
			// output.close();
			clientSocket.close();
			clientSocket = null;
			logger.info("Server - Server connection closed!");
		}
	}
	
	/**
	 * Method sends a TextMessage using this socket.
	 * @param msg the message that is to be sent.
	 * @param key the key to encryt msg.
	 * @throws IOException some I/O error regarding the output stream 
	 */
	public synchronized void sendMessage(TextMessage msg, String key) throws IOException {
		byte[] msgBytes = msg.getMsgBytes();
		output.write(msgBytes, 0, msgBytes.length);
		output.flush();
		KVMessageImple KVmsgTemp = new KVMessageImple(key);
		String decryptedMsg = KVmsgTemp.decryptString(msg.getMsg());
		logger.debug("Send message:\t '" + decryptedMsg + "'");
	}
	
	/**
	 * Method receive the response from receiver server
	 * @param key the key to decrypt msg
	 * @return msg The response text message from receiver server
	 */
	public TextMessage receiveMessage(String key) throws IOException {

		int index = 0;
		byte[] msgBytes = null, tmp = null;
		byte[] bufferBytes = new byte[BUFFER_SIZE];

		/* read first char from stream */
		byte read = (byte) input.read();
		boolean reading = true;

		while (read != 13 && reading) {/* carriage return */
			/* if buffer filled, copy to msg array */
			if (index == BUFFER_SIZE) {
				if (msgBytes == null) {
					tmp = new byte[BUFFER_SIZE];
					System.arraycopy(bufferBytes, 0, tmp, 0, BUFFER_SIZE);
				} else {
					tmp = new byte[msgBytes.length + BUFFER_SIZE];
					System.arraycopy(msgBytes, 0, tmp, 0, msgBytes.length);
					System.arraycopy(bufferBytes, 0, tmp, msgBytes.length, BUFFER_SIZE);
				}
				msgBytes = tmp;
				bufferBytes = new byte[BUFFER_SIZE];
				index = 0;
			}

			/* only read valid characters, i.e. letters and numbers */
			if ((read > 31 && read < 127)) {
				bufferBytes[index] = read;
				index++;
			}

			/* stop reading is DROP_SIZE is reached */
			if (msgBytes != null && msgBytes.length + index >= DROP_SIZE) {
				reading = false;
			}
			/* read next char from stream */
			read = (byte) input.read();
		}

		if (msgBytes == null) {
			tmp = new byte[index];
			System.arraycopy(bufferBytes, 0, tmp, 0, index);
		} else {
			tmp = new byte[msgBytes.length + index];
			System.arraycopy(msgBytes, 0, tmp, 0, msgBytes.length);
			System.arraycopy(bufferBytes, 0, tmp, msgBytes.length, index);
		}

		msgBytes = tmp;

		/* build final String */
		TextMessage msg = new TextMessage(msgBytes);
		KVMessageImple KVmsgTemp = new KVMessageImple(key);
		String decryptedMsg = KVmsgTemp.decryptString(msg.getMsg());
		logger.debug("Received message:\t '" + decryptedMsg + "'");
		return msg;
	}
	
	/**
	 * Function to send replicated PUT or DELETE command to successive servers
	 * @param key 
	 * @param value if null, then is delete; Else is put.
	 */
	public boolean putOrDeleteReplicatedData(String key, String value) {
		boolean areAllReplicationsSuccessful = true;
		
		String successorName = metadata.getSuccessor(serverName);
		ArrayList<String> targetedServersNames = new ArrayList<String>();
		// If only one server existed in total, then no need for replication
		if (serverName.equals(successorName)) {
			logger.debug("Only one server in hashRing. No need for replicated PUT / DELETE.");
			return true;
		}
		targetedServersNames.add(successorName);

		String suc_successorName = metadata.getSuccessor(successorName);
		// If 2 servers existed in total
		if (serverName.equals(suc_successorName)) {
			logger.debug("Will do 1 replicated PUT / DELETE to " + successorName + ".");
		} else {
			// If 3 or more servers
			targetedServersNames.add(suc_successorName);
			logger.debug("Will do 2 replicated PUT / DELETE to " + successorName + " and " + suc_successorName + ".");
		}

		// logger.debug("test_debug: " + serverName + " " + successorName + " " + suc_successorName + " " +  metadata.getSuccessor(suc_successorName));
		// logger.debug("test_debug: " + targetedServersNames.size());
		String secretKey = KVMessageImple.DEFAULT_SECRET;
		for (int i = 0; i < targetedServersNames.size(); i++) {
			// Transform servers name to ip and port
			String[] ip_port_array = metadata.getServerIpPort(targetedServersNames.get(i)).trim().split(":");
			String address = ip_port_array[0];
			int port = Integer.parseInt(ip_port_array[1]);
			KVMessage message = new KVMessageImple(key, value, KVMessage.StatusType.PUT, true, secretKey);
			// Response Object placeholder.

			KVMessageImple received_message = new KVMessageImple(key, null, KVMessage.StatusType.PUT_ERROR, true, secretKey);
			try 
			{
				// Establish connection to targeted server
				connectToTargetedServer(address, port);
				// Send message to targeted server
				logger.debug(serverName + " is sending message to " + port);
				sendMessage(new TextMessage(((KVMessageImple) message).encode()), KVMessageImple.DEFAULT_SECRET);
				// Wait for targeted server response

				TextMessage latestMsg = receiveMessage(KVMessageImple.DEFAULT_SECRET);	
				logger.debug("receivinging message....");	
				received_message.decode(latestMsg.getMsg());
				DisconnectToTargetedServer();
			} catch (IOException e) {
				logger.error("Fail to do replicated PUT / DELETE: " + e);
				areAllReplicationsSuccessful = false;
			}
			
			if (!received_message.getIsReplicated()) {
				logger.error("Received response is not for replicated PUT / DELETE!");
				areAllReplicationsSuccessful = false;
			}

			// Handle the received message from targeted server regarding the replicated PUT or DELETE
			if (received_message.getStatus().equals(KVMessage.StatusType.PUT_SUCCESS) ||
			received_message.getStatus().equals(KVMessage.StatusType.DELETE_SUCCESS) || 
			received_message.getStatus().equals(KVMessage.StatusType.PUT_UPDATE)) {
				logger.info("Replicated PUT / DELETE is successful.");
			} else if(received_message.getStatus().equals(KVMessage.StatusType.PUT_ERROR) ||
			received_message.getStatus().equals(KVMessage.StatusType.DELETE_ERROR)) {
				logger.error("Replicated PUT / DELETE is failed!");
				areAllReplicationsSuccessful = false;
			} else {
				logger.error(
						"Received unknown status regarding replicated PUT / DELETE: " + received_message.getStatus());
				areAllReplicationsSuccessful = false;
			}			
		}
		return areAllReplicationsSuccessful;
	}
	

	/**
	 * Function to send replicated subscribe or unsubscribe command to successive servers
	 * @param key 
	 * @param value if null, then is delete; Else is put.
	 */
	public boolean subOrUnsubReplicatedData(String key, String subscriber, boolean isSub) {
		boolean areAllReplicationsSuccessful = true;

		String successorName = metadata.getSuccessor(serverName);
		ArrayList<String> targetedServersNames = new ArrayList<String>();
		// If only one server existed in total, then no need for replication
		if (serverName.equals(successorName)) {
			logger.debug("Only one server in hashRing. No need for replicated SUB / UNSUB.");
			return true;
		}
		targetedServersNames.add(successorName);

		String suc_successorName = metadata.getSuccessor(successorName);
		// If 2 servers existed in total
		if (serverName.equals(suc_successorName)) {
			logger.debug("Will do 1 replicated SUB / UNSUB to " + successorName + ".");
		} else {
			// If 3 or more servers
			targetedServersNames.add(suc_successorName);
			logger.debug("Will do 2 replicated SUB / UNSUB to " + successorName + " and " + suc_successorName + ".");
		}

		String secretKey = KVMessageImple.DEFAULT_SECRET;
		for (int i = 0; i < targetedServersNames.size(); i++) {
			// Transform servers name to ip and port
			String[] ip_port_array = metadata.getServerIpPort(targetedServersNames.get(i)).trim().split(":");
			String address = ip_port_array[0];
			int port = Integer.parseInt(ip_port_array[1]);

			StatusType subType = isSub ? KVMessage.StatusType.SUB : KVMessage.StatusType.UNSUB;
      		StatusType defaultReceivedSubType = isSub ? KVMessage.StatusType.SUB_ERROR : KVMessage.StatusType.UNSUB_ERROR;
			KVMessage message = new KVMessageImple(key, subscriber, subType, true, secretKey);

			// Response Object placeholder.
			KVMessageImple received_message = new KVMessageImple(key, null, defaultReceivedSubType, true, secretKey);

			try 
			{
				// Establish connection to targeted server
				connectToTargetedServer(address, port);
				// Send message to targeted server
				logger.debug(serverName + " is sending message to " + port);
				sendMessage(new TextMessage(((KVMessageImple) message).encode()), KVMessageImple.DEFAULT_SECRET);
				// Wait for targeted server response
				TextMessage latestMsg = receiveMessage(KVMessageImple.DEFAULT_SECRET);	
				logger.debug("receivinging message....");	
				received_message.decode(latestMsg.getMsg());
				DisconnectToTargetedServer();
			} catch (IOException e) {
				logger.error("Fail to do replicated SUB / UNSUB: " + e);
				areAllReplicationsSuccessful = false;
			}
			
			if (!received_message.getIsReplicated()) {
				logger.error("Received response is not for replicated SUB / UNSUB!");
			}

			// Handle the received message from targeted server regarding the replicated PUT or DELETE
			if (received_message.getStatus().equals(KVMessage.StatusType.SUB_SUCCESS) ||
			received_message.getStatus().equals(KVMessage.StatusType.UNSUB_SUCCESS)) {
				logger.info("Replicated SUB / UNSUB is successful.");
			} else if(received_message.getStatus().equals(KVMessage.StatusType.SUB_ERROR) ||
			received_message.getStatus().equals(KVMessage.StatusType.UNSUB_ERROR)) {
				logger.error("Replicated SUB / UNSUB is failed!");
				areAllReplicationsSuccessful = false;
			} else {
				logger.error("Received unknown status regarding replicated SUB / UNSUB: " + received_message.getStatus());
				areAllReplicationsSuccessful = false;
			}			
		}
		return areAllReplicationsSuccessful;
	}	

	/**
	 * Function to unicast the up-to-date subConnectionTable to the newly added server
	 */
	public boolean unicastSubConnectionTableToNewlyAddedServer(String ip_port_pair) {
		// boolean areAllUpdatesSuccessful = true;

		if (subConnectionTable == null) {
			logger.error("The subConnectionTable in server = " + serverName + " is null. Should not be.");
			return false;
		}

		// First, serialize the subConnectionTable
		byte[] subConnectionTable_byte = Serialization.serialize((Object) subConnectionTable);
		String subConnectionTable_string = Base64.getEncoder().encodeToString(subConnectionTable_byte);

		String[] ip_port_array = ip_port_pair.trim().split(":");
		String address = ip_port_array[0];
		int port = Integer.parseInt(ip_port_array[1]);
		String targetedServerName = metadata.getServerNameFromIpPort(ip_port_pair);

		String secretKey = KVMessageImple.DEFAULT_SECRET;
		KVMessage message = new KVMessageImple("", subConnectionTable_string, KVMessage.StatusType.UNICASTSUBCONTABLETOSERVER, secretKey);
		KVMessageImple received_message = new KVMessageImple(secretKey);
		try 
		{
			// Establish connection to targeted server
			connectToTargetedServer(address, port);
			// Send message to targeted server
			logger.debug(serverName + " is sending message to " + targetedServerName);
			sendMessage(new TextMessage(((KVMessageImple) message).encode()), KVMessageImple.DEFAULT_SECRET);
			// Wait for targeted server response
			TextMessage latestMsg = receiveMessage(KVMessageImple.DEFAULT_SECRET);	
			logger.debug("receivinging message....");	
			received_message.decode(latestMsg.getMsg());
			DisconnectToTargetedServer();
		} catch (IOException e) {
			logger.error("Fail to unicast up-to-date subConnectionTable to server " + targetedServerName + ": " + e);
		}

		// Handle the received message from targeted server regarding the replicated broadcast request
		if (received_message.getStatus().equals(KVMessage.StatusType.UNICASTSUBCONTABLETOSERVER_FINISH)) {
			logger.debug("Successful unicast up-to-date subConnectionTable to server = " + targetedServerName + ".");
			return true;
		} else {
			logger.error("Unknown state regarding uncasting up-to-date subConnectionTable to server = " + targetedServerName
					+ ". Check the logger!");
			return false;
		}

		// Should not reach here.
	}	


	/**
	 * Function to broadcast up-to-date subConnectionTable to all other servers
	 */
	public boolean broadcastSubConnectionTableToAllOtherServers() {
		boolean areAllUpdatesSuccessful = true;

		if (subConnectionTable == null) {
			logger.error("The subConnectionTable in server = " + serverName + " is null. Should not be.");
			return false;
		}

		// First, serialize the subConnectionTable
		byte[] subConnectionTable_byte = Serialization.serialize((Object) subConnectionTable);
		String subConnectionTable_string = Base64.getEncoder().encodeToString(subConnectionTable_byte);

		// Start sending the up-to-date subConnectionTable info to all other servers
		String tempServerName = metadata.getSuccessor(serverName);
		while (!tempServerName.equals(serverName)) {
			String[] ip_port_array = metadata.getServerIpPort(tempServerName).trim().split(":");
			String address = ip_port_array[0];
			int port = Integer.parseInt(ip_port_array[1]);
			String secretKey = KVMessageImple.DEFAULT_SECRET;
			KVMessage message = new KVMessageImple("", subConnectionTable_string, KVMessage.StatusType.BROADCASTSUBCONTABLETOSERVER, secretKey);
			KVMessageImple received_message = new KVMessageImple(secretKey);
			try 
			{
				// Establish connection to targeted server
				connectToTargetedServer(address, port);
				// Send message to targeted server
				logger.debug(serverName + " is sending message to " + tempServerName);
				sendMessage(new TextMessage(((KVMessageImple) message).encode()), KVMessageImple.DEFAULT_SECRET);
				// Wait for targeted server response
				TextMessage latestMsg = receiveMessage(KVMessageImple.DEFAULT_SECRET);	
				logger.debug("receivinging message....");	
				received_message.decode(latestMsg.getMsg());
				DisconnectToTargetedServer();
			} catch (IOException e) {
				logger.error("Fail to broadcast up-to-date subConnectionTable to server " + tempServerName + ": " + e);
				areAllUpdatesSuccessful = false;
			}

			// Handle the received message from targeted server regarding the replicated broadcast request
			if (received_message.getStatus().equals(KVMessage.StatusType.BROADCASTSUBCONTABLETOSERVER_FINISH)) {
				logger.debug("Successful broadcast up-to-date subConnectionTable to server = " + tempServerName + ".");
			} else {
				logger.error("Unknown state regarding broadcasting up-to-date subConnectionTable to server = " + tempServerName
						+ ". Check the logger!");
				areAllUpdatesSuccessful = false;
			}

			// iterate to the next successor
			tempServerName = metadata.getSuccessor(tempServerName);
		}

		return areAllUpdatesSuccessful;
	}	

	public void replaceSubConnectionTable(SubConnectionTable newSubConnectionTable) {
		subConnectionTable = newSubConnectionTable;
		logger.debug("The subConnectionTable has been replaced with the new one.");
		subConnectionTable.printSubConnectionTable();
	}

	public SubConnectionTable getSubConnectionTable() {
		return subConnectionTable;
	}

    /**
     * Main entry point for the echo server application. 
     * @param args contains the port number at args[0].
     */
	public static void main(String[] args) {
		try {
			String jarDirectroy = new File(KVServer.class.getProtectionDomain().getCodeSource().getLocation().toURI())
					.getParent();
			String log_directory = jarDirectroy + "/logs/KVServer.log";
			new LogSetup(log_directory, Level.DEBUG);

			if (args.length >= 3) {
				if (args.length == 4) {
					if (args[3].equals("-admin")) {
						System.out.println("Invoking admin mode");
						admin_invoked = true;
					} else {
						System.out.println("Error! Invalid 4th argument!");
						System.out.println("Usage: Server <port> <cache size> <strategy> [-admin]!");
					}
				} else {
					admin_invoked = false;
				}
				int port = Integer.parseInt(args[0]);
				int cacheSize = Integer.parseInt(args[1]);
				String strategy = args[2];
				new KVServer(port, cacheSize, strategy);
			} else {
				System.out.println("Error! Invalid number of arguments!");
				System.out.println("Usage: Server <port> <cache size> <strategy> [--admin]!");
			}
		} catch (IOException e) {
			System.out.println("Error! Unable to initialize logger!");
			e.printStackTrace();
			System.exit(1);
		} catch (IllegalArgumentException iae) {
			System.out.println(
					"Error! Invalid argument <port: number>, or invalid <cache size: number>, or invalid <strategy: {None, FIFO, LRU, LFU}>");
			System.exit(1);
		} catch (Exception e) {
			System.out.println("Error: " + e);
			System.exit(1);
		}
	}
	
	/********************Following are deprecated functions ***********************/
	/**
	 * Move the data from the current server to the targeted server (This function is deprecated in Milestone 3)
	 * @param leftBound leftbound of the hash values to be moved (exclusive)
	 * @param rightBound rightbound (inclusive)
	 * @param ip_port_pair the targeted server's address:port
	 * @return
	 */
	@Deprecated
	public boolean moveData(String leftBound, String rightBound, String ip_port_pair) {
		logger.error("Function moveData is deprecated in midlestone 3, please check sendData() and deleteData()");
		String[] ip_port_array = ip_port_pair.trim().split(":");
		String address = ip_port_array[0];
		int port = Integer.parseInt(ip_port_array[1]);

		String stringData = this.storage.prevMoveData(leftBound, rightBound);
		if (stringData == null || stringData.isEmpty()) {
			return true;
		}
		String secretKey = KVMessageImple.DEFAULT_SECRET;
		KVMessage message = new KVMessageImple(DATA_TRANSFER_KEY, stringData, KVMessage.StatusType.MERGE, secretKey);
		// Response Object placeholder.
		KVMessageImple received_message = new KVMessageImple(DATA_TRANSFER_KEY, null, KVMessage.StatusType.MERGE_ERROR, secretKey);
		try 
		{
			// Establish connection to targeted server
			connectToTargetedServer(address, port);
			// Send message to targeted server
			sendMessage(new TextMessage(((KVMessageImple) message).encode()), KVMessageImple.DEFAULT_SECRET);
			// Wait for targeted server response
			TextMessage latestMsg = receiveMessage(KVMessageImple.DEFAULT_SECRET);		
			received_message.decode(latestMsg.getMsg());
			DisconnectToTargetedServer();
		} catch (IOException e){
			logger.error("Fail to move data: " + e);
		}
		// Handle the received message from targeted server regarding the file merging
		if (received_message.getStatus().equals(KVMessage.StatusType.MERGE_SUCCESS)) {
			logger.info("File merging is successful.");
		} else if(received_message.getStatus().equals(KVMessage.StatusType.MERGE_ERROR)) {
			logger.error("File merging is failed!");
			return false;
		} else {
			logger.error("Received unknown status regarding file merging!");
			return false;
		}

		this.storage.postMoveData();
		return true;
	}
}