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
import shared.metadata.Serialization;
import shared.messages.KVMessage;
import shared.messages.KVMessageImple;
import storage.KVStorage;
import zookeeper.ZK;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.File;
import java.util.concurrent.CountDownLatch;
import java.net.BindException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;

public class KVServer extends Thread implements IKVServer {
	private static Logger logger;
	private static final String PROMPT = "KVServer> ";

	// Traditional Client - Server Connection info
	private final String HOST_NAME = "127.0.0.1";	
	private final int port;	
	private boolean running;
	public String serverName = "Server1"; // Default server name;	
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


	/// Testing purpose only
	//ArrayList<IECSNode> nodes= new ArrayList<IECSNode>();

	//nodes.add();
	//metadata.updateMatadata(nodes);


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
		logger = Logger.getLogger(port+"_Server");
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
							String node_path = zk.ZOOKEEPER_ACTIVESERVERS_PATH + "/" + HOST_NAME + ":" + port;
							watchNodedataChange(node_path);
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
    public String getHostname(){
		// TODO Auto-generated method stub
		return null;
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
	public boolean inStorage(String key) {
		return storage.get(key) != null;
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
    public void putKV(String key, String value) throws Exception{
		if (value != null) {
			storage.put(key, value);
		} else {
			storage.delete(key);

		}
	}

	public HashRing getMetadata(){
		return metadata;
	}

	public void setMetadata(HashRing new_metadata){
		metadata = new_metadata;
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
	                ClientConnection connection = 
	                		new ClientConnection(this, client);
	                new Thread(connection).start();
					// System.out.println(serverName+" connected to "
					//		+ client.getInetAddress().getHostName()
					//		+  " on port " + client.getPort());
	                logger.info("Connected to " 
	                		+ client.getInetAddress().getHostName() 
	                		+  " on port " + client.getPort());
	            } catch (IOException e) {
	            	logger.error("Error! " +
	            			"Unable to establish connection. \n", e);
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
		} else if (tokens[0].equals("DONE")) {
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
		String node_path = zk.ZOOKEEPER_ACTIVESERVERS_PATH + "/" + HOST_NAME + ":" + port;
		zk.delete(node_path, -1);

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
	}

	public ServerStatus getServerStatus(){
		return status;
	}

	/**
	 * Move the data from the current server to the targeted server
     * @param leftBound leftbound of the hash values to be moved (exclusive)
     * @param rightBound rightbound (inclusive)
	 * @param ip_port_pair the targeted server's address:port
	 * @return
	 */
	@Override
	public boolean moveData(String leftBound, String rightBound, String ip_port_pair) {
		String[] ip_port_array = ip_port_pair.trim().split(":");
		String address = ip_port_array[0];
		int port = Integer.parseInt(ip_port_array[1]);

		String stringData = this.storage.prevMoveData(leftBound, rightBound);
		if (stringData == null || stringData.isEmpty()) {
			return true;
		}
		KVMessage message = new KVMessageImple(DATA_TRANSFER_KEY, stringData, KVMessage.StatusType.MERGE);
		// Response Object placeholder.
		KVMessageImple received_message = new KVMessageImple(DATA_TRANSFER_KEY, null, KVMessage.StatusType.MERGE);
		try 
		{
			// Establish connection to targeted server
			connectToTargetedServer(address, port);
			// Send message to targeted server
			sendMessage(new TextMessage(((KVMessageImple) message).encode()));
			// Wait for targeted server response
			TextMessage latestMsg = receiveMessage();		
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
	 * @throws IOException some I/O error regarding the output stream 
	 */
	public synchronized void sendMessage(TextMessage msg) throws IOException {
		byte[] msgBytes = msg.getMsgBytes();
		output.write(msgBytes, 0, msgBytes.length);
		output.flush();
		logger.debug("Send message:\t '" + msg.getMsg() + "'");
	}
	
	/**
	 * Method receive the response from receiver server
	 * @return msg The response text message from receiver server
	 */
	public TextMessage receiveMessage() throws IOException {
		
		int index = 0;
		byte[] msgBytes = null, tmp = null;
		byte[] bufferBytes = new byte[BUFFER_SIZE];

		/* read first char from stream */
		byte read = (byte) input.read();	
		boolean reading = true;

		while(read != 13 && reading) {/* carriage return */
			/* if buffer filled, copy to msg array */
			if(index == BUFFER_SIZE) {
				if(msgBytes == null){
					tmp = new byte[BUFFER_SIZE];
					System.arraycopy(bufferBytes, 0, tmp, 0, BUFFER_SIZE);
				} else {
					tmp = new byte[msgBytes.length + BUFFER_SIZE];
					System.arraycopy(msgBytes, 0, tmp, 0, msgBytes.length);
					System.arraycopy(bufferBytes, 0, tmp, msgBytes.length,
							BUFFER_SIZE);
				}
				msgBytes = tmp;
				bufferBytes = new byte[BUFFER_SIZE];
				index = 0;
			} 

			/* only read valid characters, i.e. letters and numbers */
			if((read > 31 && read < 127)) {
				bufferBytes[index] = read;
				index++;
			}
			
			/* stop reading is DROP_SIZE is reached */
			if(msgBytes != null && msgBytes.length + index >= DROP_SIZE) {
				reading = false;
			}
			/* read next char from stream */
			read = (byte) input.read();
		}

		if(msgBytes == null){
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
		logger.debug("Receive message:\t '" + msg.getMsg() + "'");
		return msg;
    }

    /**
     * Main entry point for the echo server application. 
     * @param args contains the port number at args[0].
     */
    public static void main(String[] args) {
    	try {
			String jarDirectroy = new File(KVServer.class.getProtectionDomain().getCodeSource().getLocation().toURI()).getParent();
			String log_directory = jarDirectroy + "/logs/KVServer.log";
			new LogSetup(log_directory, Level.ALL);

			if(args.length >= 3) {
				if (args.length == 4){
					if (args[3].equals("-admin")){
						System.out.println("Invoking admin mode");
						admin_invoked = true;
					} else{
						System.out.println("Error! Invalid 4th argument!");
						System.out.println("Usage: Server <port> <cache size> <strategy> [-admin]!");
					}
				} else{
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
			System.out.println("Error! Invalid argument <port: number>, or invalid <cache size: number>, or invalid <strategy: {None, FIFO, LRU, LFU}>");
			System.exit(1);
		} catch (Exception e){
			System.out.println("Error: " + e);
			System.exit(1);
		}
    }

}