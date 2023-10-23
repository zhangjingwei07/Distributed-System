package client;

import client.ClientSocketListener.SocketStatus;
import org.apache.log4j.Logger;
import shared.messages.KVMessage;
import shared.messages.KVMessageImple;
import shared.metadata.HashRing;
import shared.metadata.Serialization;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;


public class KVStore extends Thread implements KVCommInterface {

	private Logger logger = Logger.getLogger("NormalKVClient");
	private Set<ClientSocketListener> listeners;
	private boolean running;
	protected String address;
	protected int port;
	private HashRing metadata = null;
	private String clientName = null;
	public String secretKey = KVMessageImple.DEFAULT_SECRET;

	protected Socket clientSocket;
	protected OutputStream output=null;
	protected InputStream input=null;
	private final String SERVICE_CONFIG_PATH = "ecs.config";

	private static final int BUFFER_SIZE = 1024;
	private static final int DROP_SIZE = 1024 * BUFFER_SIZE;

	/**
	 * Initialize KVStore with address and port of KVServer
	 * @param address the address of the KVServer
	 * @param port the port of the KVServer
	 */
	public KVStore(String address, int port) {
		this.address = address;
		this.port = port;
		listeners = new HashSet<ClientSocketListener>();
	}

	public KVStore(String address, int port, String name) {
		this.address = address;
		this.port = port;
		listeners = new HashSet<ClientSocketListener>();
		this.clientName = name;
	}

	public KVStore() {};


	@Override
	public void connect() throws IOException,UnknownHostException {
		System.out.println("Success: Connecting to server: "+address+":"+port);
		this.clientSocket = new Socket(this.address, this.port);
		output = clientSocket.getOutputStream();
		input = clientSocket.getInputStream();
		this.setRunning(true);
		logger.info("Connecting to server");

		// this.start();

		requestSecretKeyFromServer();

		if (metadata == null){
			fetchMetadata();
		}

		// TextMessage latestMsg = receiveMessage();
		// for(ClientSocketListener listener : listeners) {
		// 	listener.handleNewMessage(latestMsg);
		// }
	}

	// Reconnect: Try to connect to any server in local metadata
	synchronized public void reconnect() throws IOException,UnknownHostException {
		HashMap<String, String> server_ip_port = metadata.getAllServerInfo();
		Iterator it = server_ip_port.entrySet().iterator();
		while (it.hasNext()){
			Map.Entry pair = (Map.Entry)it.next();
			String ip_port_pair = pair.getValue().toString();
			// System.out.println("Sending info to "+server_name);
			String[] ip_port = ip_port_pair.trim().split(":");
			port = Integer.parseInt(ip_port[1]);
			address = ip_port[0];

			System.out.println("Reconnecting to server: "+address+":"+port);
			try {
				this.clientSocket = new Socket(this.address, this.port);
				output = clientSocket.getOutputStream();
				input = clientSocket.getInputStream();
			} catch (IOException ioe){
				logger.debug("Server down");
				it.remove(); // avoids a ConcurrentModificationException
				continue;
			}

			// Connected to new server
			this.setRunning(true);
			secretKey = KVMessageImple.DEFAULT_SECRET;
			logger.info("Reconnected to server");
			requestSecretKeyFromServer();
			fetchMetadata();
			return;
		}
		System.out.println("Error: All known servers are down, please update local info");
	}

	@Override
	public synchronized void disconnect() throws IOException {
		try {
			tearDownConnection();
			if (listeners != null) {
				for (ClientSocketListener listener : listeners) {
					listener.handleStatus(SocketStatus.DISCONNECTED);
				}
			}
		} catch (IOException ioe) {
			logger.error("Unable to close connection!");
		}
	}

	private void tearDownConnection() throws IOException {
		setRunning(false);
		secretKey = KVMessageImple.DEFAULT_SECRET; // Everytime disconnect, reset the secretKey to default
		logger.debug("tearing down the connection ...");
		if (clientSocket != null) {
			// input.close();
			// output.close();
			clientSocket.close();
			clientSocket = null;
			logger.info("connection closed!");
		}
	}

	public boolean isRunning() {
		return running;
	}

	public void setRunning(boolean run) {
		running = run;
	}

	public void addListener(ClientSocketListener listener){
		listeners.add(listener);
	}

	/**
	 * Method sends a TextMessage using this socket.
	 * @param msg the message that is to be sent.
	 * @param key the secretKey to encode the msg
	 * @throws IOException some I/O error regarding the output stream 
	 */
	public synchronized void sendMessage(TextMessage msg, String key) throws IOException {
		byte[] msgBytes = msg.getMsgBytes();
		output.write(msgBytes, 0, msgBytes.length);
		output.flush();
		KVMessageImple KVMsgTemp = new KVMessageImple(key);
		String decryptedMsg = KVMsgTemp.decryptString(msg.getMsg());
		logger.debug("Send message:\t '" + decryptedMsg + "'");
	}

	public synchronized void sendHeartbeatMessage(TextMessage msg) throws IOException {
		byte[] msgBytes = msg.getMsgBytes();
		output.write(msgBytes, 0, msgBytes.length);
		output.flush();
		KVMessageImple KVMsgTemp = new KVMessageImple(secretKey);
		String decryptedMsg = KVMsgTemp.decryptString(msg.getMsg());
		// logger.debug("Send message:\t '" + decryptedMsg + "'");
	}

	/* Method receive the response from receiver server
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
		KVMessageImple KVMsgTemp = new KVMessageImple(key);
		String decryptedMsg = KVMsgTemp.decryptString(msg.getMsg());
		logger.debug("Received message:\t '" + decryptedMsg + "'");
		return msg;
	}
	
	// Request a random secret Key from server, used for communication encryption between current client and server.
	public void requestSecretKeyFromServer() throws IOException {
		KVMessage message = new KVMessageImple("", "", KVMessage.StatusType.AUTHENTICATION_SETUP, KVMessageImple.DEFAULT_SECRET);
		sendMessage(new TextMessage(((KVMessageImple) message).encode()), KVMessageImple.DEFAULT_SECRET);
		logger.debug("Secret Key request sent...");
		TextMessage latestMsg = receiveMessage(KVMessageImple.DEFAULT_SECRET);
		KVMessageImple received_message = new KVMessageImple("", null, KVMessage.StatusType.AUTHENTICATION_SETUP, KVMessageImple.DEFAULT_SECRET);
		received_message.decode(latestMsg.getMsg());
		handleServerResponse(received_message);
	}

	// Get Metadata for the first time from any indicated server
	public void fetchMetadata() throws IOException{
		KVMessage message = new KVMessageImple("", "", KVMessage.StatusType.METADATA_REQUEST, secretKey);
		sendMessage(new TextMessage(((KVMessageImple) message).encode()), secretKey);
		logger.debug("First metadata request sent...");
		TextMessage latestMsg = receiveMessage(secretKey);
		KVMessageImple received_message = new KVMessageImple("", null, KVMessage.StatusType.METADATA_UPDATE, secretKey);
		logger.debug("First metadata received...");
		received_message.decode(latestMsg.getMsg());
		handleServerResponse(received_message);
	}


	public String getHashValue(String key) {
		try {
			byte[] keyInBytes = key.getBytes("UTF-8");
			MessageDigest md = MessageDigest.getInstance("MD5");
			byte[] md5Val_byte = md.digest(keyInBytes);

			BigInteger md5Val = new BigInteger(1, md5Val_byte);
			return md5Val.toString(16);

		} catch (UnsupportedEncodingException UEE){
			logger.info("Unsupported Encoding");
		} catch (NoSuchAlgorithmException NSAE){
			logger.info("Cannot get MD5 Algorithm");
		}
		return "";
	}

	// Send the message to the responsible server according to key
	public void sendToCorrectServer(KVMessage message, String key) throws Exception{
		fetchMetadata();
		boolean reconnect = false;
		if (running){
			reconnect = true;
		}
		// send
		String server_name = metadata.getCorrectServer(key);
		String ip_port_pair = metadata.getServerIpPort(server_name);
		// System.out.println("Sending info to "+server_name);
		String[] ip_port = ip_port_pair.trim().split(":");
		if (message.getStatus() != KVMessage.StatusType.GET || metadata.inReplicaRange(key, server_name)){
			if (!this.address.equals(ip_port[0]) || this.port != Integer.parseInt(ip_port[1])) {
				this.address = ip_port[0];
				this.port = Integer.parseInt(ip_port[1]);
				disconnect();
				if (reconnect){
					connect();
				}
			}
		}
		System.out.println("Sending message to port: "+ this.port);
		// Update secretKey
		((KVMessageImple) message).setSecretKey(secretKey);
		sendMessage(new TextMessage(((KVMessageImple) message).encode()), secretKey);
	}

	// Handle all server responses
	public void handleServerResponse(KVMessageImple message) {
		KVMessage.StatusType status = message.getStatus();
		if (status == null){
			// logger.error("Received message with null status");
			return;
		}
		switch (status) {
			case PUT_ERROR:
			case DELETE_ERROR:
			case INPUT_ERROR:
			case GET_ERROR:
			case UNKNOWN_STATUS_ERROR:
			case DIDSUB_ERROR:
			case DIDSUB_SUCCESS:
			case SUB_SUCCESS:
			case UNSUB_SUCCESS:
			case SUB_ERROR:
			case UNSUB_ERROR:
			case GETSUBCONNECTIONTABLE_SUCCESS:
			case GETSUBCONNECTIONTABLE_ERROR:
				System.out.println(message.getValue());
				break;
			case AUTHENTICATION_SETUP:
				System.out.println("Success: Received secretKey from server!");
				secretKey = message.getValue();
				break;
			case PUT_SUCCESS:
				System.out.println("Success: The key has been inserted!");
				break;
			case PUT_UPDATE:
				System.out.println("Success: The value associated with this key has been updated!");
				break;
			case DELETE_SUCCESS:
				System.out.println("Success: The value associated with this key has been deleted!");
				break;
			case GET_SUCCESS:
				System.out.println("Success: The requested key: "+message.getKey()+" has value: "+message.getValue());
				break;
			case SERVER_NOT_RESPONSIBLE:
			case METADATA_UPDATE:
				metadata = (HashRing) Serialization.deserialize(Base64.getDecoder().decode(message.getValue().getBytes()));
				// System.out.println("Printing metadata servers");
				metadata.printAllServer();

				System.out.println("Success: Server Info updated");
				break;
			case METADATA_ERROR:
				System.out.println("Error: Unknown Error occurred at server, cannot fetch metadata");
				break;
			case SERVER_STOPPED:
				System.out.println("Error: The server is not in a working state");
				break;
		}
	}

	@Override
	public KVMessage put(String key, String value) throws Exception {
		if (!this.running){

		}
		// treat String 'null' as a null value
		if (value != null && value.equals("null")){
			value = null;
		}
		// Send message to the correct server
		KVMessage message = new KVMessageImple(key, value, KVMessage.StatusType.PUT, secretKey);
		sendToCorrectServer(message, key);

		// Receive message
		TextMessage latestMsg = receiveMessage(secretKey);
		KVMessageImple received_message = new KVMessageImple(key, null, KVMessage.StatusType.PUT, secretKey);
		received_message.decode(latestMsg.getMsg());
		handleServerResponse(received_message);

		// Metadata just updated, send again
		if (received_message.getStatus() == KVMessage.StatusType.METADATA_UPDATE ||
				received_message.getStatus() == KVMessage.StatusType.SERVER_NOT_RESPONSIBLE){
			sendToCorrectServer(message, key);
			latestMsg = receiveMessage(secretKey);
			// Secret key is updated. Need to reload the KVmsg object
			received_message = new KVMessageImple(key, null, KVMessage.StatusType.PUT, secretKey);
			received_message.decode(latestMsg.getMsg());
			handleServerResponse(received_message);
		}

		/*if (received_message.getStatus() == KVMessage.StatusType.PUT_ERROR){
			System.out.println(received_message.getValue());
		} else if (received_message.getStatus() == KVMessage.StatusType.PUT_SUCCESS){
			System.out.println("Success: The key has been inserted!");
		} else if (received_message.getStatus() == KVMessage.StatusType.PUT_UPDATE){
			System.out.println("Success: The value associated with this key has been updated!");
		} else if (received_message.getStatus() == KVMessage.StatusType.DELETE_SUCCESS){
			System.out.println("Success: The value associated with this key has been deleted!");
		} else if (received_message.getStatus() == KVMessage.StatusType.DELETE_ERROR){
			System.out.println(received_message.getValue());
		} else if (received_message.getStatus() == KVMessage.StatusType.INPUT_ERROR){
			System.out.println(received_message.getValue());
		}*/

		return received_message;
	}

	@Override
	public KVMessage get(String key) throws Exception {
		// Send message to the correct server
		KVMessage message = new KVMessageImple(key, null, KVMessage.StatusType.GET, secretKey);
		sendToCorrectServer(message, key);
		TextMessage latestMsg = receiveMessage(secretKey);

		// Receive message from server
		KVMessageImple received_message = new KVMessageImple(key, "", KVMessage.StatusType.GET, secretKey);
		received_message.decode(latestMsg.getMsg());
		handleServerResponse(received_message);

		// Metadata just updated, send again
		if (received_message.getStatus() == KVMessage.StatusType.METADATA_UPDATE ||
				received_message.getStatus() == KVMessage.StatusType.SERVER_NOT_RESPONSIBLE){
			sendToCorrectServer(message, key);
			latestMsg = receiveMessage(secretKey);
			received_message.decode(latestMsg.getMsg());
			handleServerResponse(received_message);
		}
		if (received_message.getStatus() == KVMessage.StatusType.METADATA_UPDATE){
			System.out.println("Fatal Error");
			return null;
		}
		/*if (received_message.getStatus() == KVMessage.StatusType.GET_ERROR){
			System.out.println(received_message.getValue());
		} else if (received_message.getStatus() == KVMessage.StatusType.GET_SUCCESS){
			System.out.println("Success: The requested key: "+received_message.getKey()+" has value: "+received_message.getValue());
		} else if (received_message.getStatus() == KVMessage.StatusType.UNKNOWN_STATUS_ERROR){
			System.out.println(received_message.getValue());
		}*/
		// check if wrong server, load new metadata
		// if (wrongServer) resend

		return received_message;
	}

	public KVMessage subscribe(String key) throws Exception{
		KVMessage message = new KVMessageImple(key, clientName, KVMessage.StatusType.SUB, secretKey);
		sendToCorrectServer(message, key);

		// Receive message
		TextMessage latestMsg = receiveMessage(secretKey);
		KVMessageImple received_message = new KVMessageImple(null, null, null, secretKey);
		received_message.decode(latestMsg.getMsg());
		handleServerResponse(received_message);

		// Metadata just updated, send again
		if (received_message.getStatus() == KVMessage.StatusType.METADATA_UPDATE ||
				received_message.getStatus() == KVMessage.StatusType.SERVER_NOT_RESPONSIBLE){
			sendToCorrectServer(message, key);
			latestMsg = receiveMessage(secretKey);
			received_message.decode(latestMsg.getMsg());
			handleServerResponse(received_message);
		}

		return received_message;
	}

	public KVMessage unsubscribe(String key) throws Exception{
		KVMessage message = new KVMessageImple(key, clientName, KVMessage.StatusType.UNSUB, secretKey);
		sendToCorrectServer(message, key);

		// Receive message
		TextMessage latestMsg = receiveMessage(secretKey);
		KVMessageImple received_message = new KVMessageImple(null, null, null, secretKey);
		received_message.decode(latestMsg.getMsg());
		handleServerResponse(received_message);

		// Metadata just updated, send again
		if (received_message.getStatus() == KVMessage.StatusType.METADATA_UPDATE ||
				received_message.getStatus() == KVMessage.StatusType.SERVER_NOT_RESPONSIBLE){
			sendToCorrectServer(message, key);
			latestMsg = receiveMessage(secretKey);
			received_message.decode(latestMsg.getMsg());
			handleServerResponse(received_message);
		}

		return received_message;
	}

	public KVMessage checkSubscription(String key) throws Exception {
		KVMessage message = new KVMessageImple(key, clientName, KVMessage.StatusType.DIDSUB, secretKey);
		sendToCorrectServer(message, key);

		// Receive message
		TextMessage latestMsg = receiveMessage(secretKey);
		KVMessageImple received_message = new KVMessageImple(null, null, null, secretKey);
		received_message.decode(latestMsg.getMsg());
		handleServerResponse(received_message);

		// Metadata just updated, send again
		if (received_message.getStatus() == KVMessage.StatusType.METADATA_UPDATE
				|| received_message.getStatus() == KVMessage.StatusType.SERVER_NOT_RESPONSIBLE) {
			sendToCorrectServer(message, key);
			latestMsg = receiveMessage(secretKey);
			received_message.decode(latestMsg.getMsg());
			handleServerResponse(received_message);
		}

		return received_message;
	}
	
	public KVMessage getSubconnectionTable(String key) throws Exception{
		KVMessage message = new KVMessageImple(null, null, KVMessage.StatusType.GETSUBCONNECTIONTABLE, secretKey);
		sendToCorrectServer(message, key);

		// Receive message
		TextMessage latestMsg = receiveMessage(secretKey);
		KVMessageImple received_message = new KVMessageImple(null, null, null, secretKey);
		received_message.decode(latestMsg.getMsg());
		handleServerResponse(received_message);

		// Metadata just updated, send again
		if (received_message.getStatus() == KVMessage.StatusType.METADATA_UPDATE ||
				received_message.getStatus() == KVMessage.StatusType.SERVER_NOT_RESPONSIBLE){
			sendToCorrectServer(message, key);
			latestMsg = receiveMessage(secretKey);
			received_message.decode(latestMsg.getMsg());
			handleServerResponse(received_message);
		}

		return received_message;
	}

}
