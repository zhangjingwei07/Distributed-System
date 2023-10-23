package app_kvServer;

import java.net.BindException;
import java.net.ServerSocket;
import java.net.Socket;
import java.io.IOException;

import logger.LogSetup;
import server.ClientConnection;
import storage.KVStorage;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class KVServer extends Thread implements IKVServer {

	private static Logger logger = Logger.getLogger("Server");
	private static final String PROMPT = "KVServer> ";

	
	private int port;
	private ServerSocket serverSocket;
	private KVStorage storage;
	private boolean running;
	private int cacheSize;
	private CacheStrategy strategy;

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
	public KVServer(int port, int cacheSize, String strategy) {
		this.port = port;
		this.cacheSize = cacheSize;
		this.strategy = CacheStrategy.valueOf(strategy);;
		storage = new KVStorage(this.cacheSize, this.strategy);
		this.start();
	}

    private boolean initializeServer() {
    	logger.info("Initialize server ...");
    	try {
            serverSocket = new ServerSocket(port);
            logger.info("Server listening on port: " 
            		+ serverSocket.getLocalPort());    
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

	@Override
    public void clearCache(){
		storage.cache.clearCache();
	}

	@Override
    public void clearStorage(){
		storage.clearStorage();
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
    public void close(){
		kill();
		// Can add more actions, e.g, clear cache.
	}

    /**
     * Main entry point for the echo server application. 
     * @param args contains the port number at args[0].
     */
    public static void main(String[] args) {
    	try {
			new LogSetup("logs/server.log", Level.ALL);
			if(args.length != 3) {
				System.out.println("Error! Invalid number of arguments!");
				System.out.println("Usage: Server <port> <cache size> <strategy>!");
			} else {
				int port = Integer.parseInt(args[0]);
				int cacheSize = Integer.parseInt(args[1]);
				String strategy = args[2];
				new KVServer(port, cacheSize, strategy);
			}
		} catch (IOException e) {
			System.out.println("Error! Unable to initialize logger!");
			e.printStackTrace();
			System.exit(1);
		} catch (IllegalArgumentException iae) {
			System.out.println("Error! Invalid argument <port: number>, or invalid <cache size: number>, or invalid <strategy: {None, FIFO, LRU, LFU}>");
			System.exit(1);
		} 
    }
}
