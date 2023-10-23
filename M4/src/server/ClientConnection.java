package server;

import app_kvServer.IKVServer;
import com.google.gson.Gson;
import org.apache.log4j.Logger;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

import app_kvServer.KVServer;
import shared.messages.KVMessage;
import shared.messages.KVMessageImple;
import shared.messages.KVMessage.StatusType;
import shared.metadata.Serialization;
import shared.metadata.SubConnectionTable;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.Base64;
import java.util.Random;


/**
 * Represents a connection end point for a particular client that is
 * connected to the server. This class is responsible for message reception
 * and sending.
 * The class also implements the echo functionality. Thus whenever a message
 * is received it is going to be echoed back to the client.
 */
public class ClientConnection implements Runnable {

	private Logger logger = Logger.getLogger("ClientConnectionDefaultName");

	private boolean isOpen;
	private static final int BUFFER_SIZE = 1024;
	private static final int MAX_KEY_LEN = 20;
	private static final int MAX_VAL_LEN = 120 * 1024;
	private static final int DROP_SIZE = 128 * BUFFER_SIZE;

	private KVServer kvServer;
	private Socket clientSocket;
	private String secretKey = KVMessageImple.DEFAULT_SECRET;
	private InputStream input;
	private OutputStream output;

	// This variable is interesting
	// It is used to identify whether this ClientConnection is:
	// (1) server-client. Then clientName should have value
	// (2) server-server. Then clientName should be null
	private String clientName;

	/**
	 * Constructs a new CientConnection object for a given TCP socket.
	 * @param clientSocket the Socket object for the client connection.
	 */
	public ClientConnection(KVServer kvServer, Socket clientSocket) {
		this.kvServer = kvServer;
		this.clientSocket = clientSocket;
		this.isOpen = true;
		logger = Logger.getLogger("NormalConnection_" + kvServer.getServerName());
	}

	/**
	 * Initializes and starts the client connection.
	 * Loops until the connection is closed or aborted by the client.
	 */
	public void run() {
		try {
			output = clientSocket.getOutputStream();
			input = clientSocket.getInputStream();

			while (getIsOpen()) {
				String keyForThisRound = secretKey;
				try {
					// Decode Byte message into KVMessageImple java object
					KVMessageImple request = new KVMessageImple(keyForThisRound);
					TextMessage request_text_mess = receiveMessage(keyForThisRound);
					String string_mess = request_text_mess.getMsg();
					TextMessage response_text_mess = new TextMessage("");

					// To tackle disconnect situation when client will send "" to server
					if (!string_mess.equals("")) {
						request.decode(string_mess);
						if(request.getStatus() == null && 
							request.getValue() == null && 
							request.getKey() == null){
							// If the message is not in a valid form (byzantine behavior)
							continue;
						}

						// Handle the request java object and send back message
						KVMessageImple response = handleMessage(request);
						// if status is update, metadata already sent
						if (response.getStatus() == KVMessage.StatusType.SERVER_NOT_RESPONSIBLE
								|| response.getStatus() == KVMessage.StatusType.METADATA_UPDATE
								|| response.getStatus() == KVMessage.StatusType.METADATA_REQUEST) {
							continue;
						}
						response_text_mess = new TextMessage(response.encode());
					}
					sendMessage(response_text_mess, keyForThisRound);
					

					/* connection either terminated by the client or lost due to
					 * network problems*/
				} catch (IOException ioe) {
					logger.error("Error! Connection lost!");
					setIsOpen(false);
				}
			}
		} catch (IOException ioe) {
			logger.error("Error! Connection could not be established!", ioe);
		} finally {
			try {
				if (clientSocket != null) {
					logger.info("Tearing down connection!");

					// If this clientConnection is server-client, which means subscription clientConnection
					if (clientName != null) {
						// Remove the subscription clientConnection from clientName_subConnections
						if (!kvServer.getClientName_subConnection().containsKey(clientName)) {
							logger.error("Before removal, the ****subscription***** clientConnection: " + clientName + "_"
									+ kvServer.getServerName() + " does not exist in record.");
						}
						kvServer.getClientName_subConnection().remove(clientName);
						logger.debug("The ****subscription***** clientConnection: " + clientName
								+ "_" + kvServer.getServerName() + " has been removed from server = " + kvServer.getServerName() + " records.");

						// Need to remove current subscription thread from subConnectionTable
						// and broadcast the newest subConnectionTable information to all other servers before exit.
						kvServer.getSubConnectionTable().removeConnection(clientName);
						if (kvServer.broadcastSubConnectionTableToAllOtherServers()) {
							logger.debug("Successful broadcast up-to-date subConnectionTable to all other servers.");
						} else {
							logger.error("Some broadcast of up-to-date subConnectionTable to servers are failed.");
						}
						
						clientName = null;										
					} else {
						logger.debug("The ****normal**** clientConnection: xxx_" + kvServer.getServerName() + " has been removed.");
					}

					input.close();
					output.close();
					clientSocket.close();
				}
			} catch (IOException ioe) {
				logger.error("Error! Unable to tear down connection!", ioe);
			}
		}
	}

	/**
	 * Method sends a TextMessage using this socket.
	 * @param msg the message that is to be sent.
	 * @throws IOException some I/O error regarding the output stream
	 */
	public void sendMessage(TextMessage msg, String key) throws IOException {
		byte[] msgBytes = msg.getMsgBytes();
		output.write(msgBytes, 0, msgBytes.length);
		output.flush();
		KVMessageImple KVMsgTemp = new KVMessageImple(key);
		String decryptedMsg = KVMsgTemp.decryptString(msg.getMsg());
		logger.debug("SEND <" + clientSocket.getInetAddress().getHostAddress() + ":" + clientSocket.getPort() + ">: '"
				+ decryptedMsg + "'");
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

		//		logger.info("First Char: " + read);
		//		Check if stream is closed (read returns -1)
		//		if (read == -1){
		//			TextMessage msg = new TextMessage("");
		//			return msg;
		//		}

		while (/*read != 13  && */ read != 10 && read != -1 && reading) {/* CR, LF, error */
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

			/* only read valid characters, i.e. letters and constants */
			bufferBytes[index] = read;
			index++;

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
		KVMessageImple KVMsgTemp = new KVMessageImple(key);
		String decryptedMsg = KVMsgTemp.decryptString(msg.getMsg());
		if (decryptedMsg == null){
			// Return an empty string if cannot decode string
			return new TextMessage("");
		}
		logger.debug(kvServer.getServerName() + ": RECEIVE \t<" + clientSocket.getInetAddress().getHostAddress() + ":" + clientSocket.getPort()
				+ ">: '" + decryptedMsg.trim() + "'");
		return msg;
	}

	// Send latest metadata to client
	private void sendMetadataToClient() throws IOException {
		KVMessageImple response = new KVMessageImple(secretKey);
		if (kvServer.getMetadata() == null) {
			response.setKey("");
			response.setStatus(KVMessage.StatusType.METADATA_ERROR);
			response.setValue("Error: Internal server error, no metadata found.");
			sendMessage(new TextMessage(response.encode()), secretKey);
			return;
		}
		response.setKey("New metadata");
		response.setStatus(KVMessage.StatusType.METADATA_UPDATE);
		byte[] metadata_byte = Serialization.serialize((Object) kvServer.getMetadata());
		response.setValue(Base64.getEncoder().encodeToString(metadata_byte));
		sendMessage(new TextMessage(response.encode()), secretKey);
	}

	// Send the subscription update to subscriber
	public void sendSubUpdateToClient(String stringData) throws IOException {
		KVMessageImple response = new KVMessageImple(secretKey);
		response.setKey("");
		response.setStatus(KVMessage.StatusType.SUBUPDATETOCLIENT);
		response.setValue(stringData);
		logger.debug("Before: subscription server = " + kvServer.getServerName()
				+ " send subscription update to client = " + clientName + ".");
		
		sendMessage(new TextMessage(response.encode()), secretKey);
		logger.debug("After: subscription server = " + kvServer.getServerName()
				+ " send subscription update to client = " + clientName + ".");
	}

	private KVMessageImple handleMessage(KVMessageImple obj_mess) {
		KVMessageImple response = new KVMessageImple(secretKey);
		// By default, set the same key to the response.
		response.setKey(obj_mess.getKey());

		/* Handle the data move case */
		if (obj_mess.getStatus().equals(KVMessage.StatusType.MERGE)) {
			String stringData = obj_mess.getValue();
			boolean mergeSuccess;
			if (stringData.length() == 2) {
				logger.debug("There is also no incoming data, don't do anything.");
				mergeSuccess = true;
			} else {
				kvServer.lockWrite();
				JSONObject JSONData = (JSONObject) JSONValue.parse(stringData);
				logger.debug("Merging incoming data, " + stringData);
				mergeSuccess = kvServer.mergeStorage(JSONData);
			}

			if (mergeSuccess) {
				logger.debug("Merging Successfull");
				response.setValue("Successful MERGE");
				response.setStatus(KVMessage.StatusType.MERGE_SUCCESS);
			} else {
				logger.debug("Merging Failed");
				response.setStatus(KVMessage.StatusType.MERGE_ERROR);
				logger.error("Fail to merge the newly received file!");
			}
			kvServer.unlockWrite();
			return response;
		}

		/* Handle Subscription Update */

		// Helper Command for debugging: get current subconnectionTable on this server
		if (obj_mess.getStatus().equals(KVMessage.StatusType.GETSUBCONNECTIONTABLE)) {
			Gson gsonObj = new Gson();
			try {
				String stringData = gsonObj.toJson(kvServer.getSubConnectionTable());
				response.setStatus(KVMessage.StatusType.GETSUBCONNECTIONTABLE_SUCCESS);
				response.setValue("The current SubconnectionTable: " + stringData);
				logger.debug("Successful GETSUBCONNECTIONTABLE of server = " + kvServer.getServerName() + ".");
			} catch (Exception e){
				response.setStatus(KVMessage.StatusType.GETSUBCONNECTIONTABLE_ERROR);
				response.setValue("Failed to get SubconnectionTable: " + e);
				logger.debug("Failed GETSUBCONNECTIONTABLE of server = " + kvServer.getServerName() + ".");				
			}

			return response;
		}

	
		// SUBUPDATETOSERVER request
		/** Standing at the view of one subscription server.
		 * It will handle the subscription update notification **from** the server storing the key. 
		 * Specifically, it will send the received update from the server storing the key, and forward it to the subscriber (client).
		*/
		if (obj_mess.getStatus().equals(KVMessage.StatusType.SUBUPDATETOSERVER)) {
			String stringData = obj_mess.getValue();
			// The client name with respect to the current subscription update message.
			String receivedClientName = obj_mess.getKey();

			if (!kvServer.getClientName_subConnection().containsKey(receivedClientName)) {
				logger.error("The client = " + receivedClientName
						+ " is not recorded in the current server = " + kvServer.getServerName() + ".");
			}
			ClientConnection subConnection = kvServer.getClientName_subConnection().get(receivedClientName);

			try {
				subConnection.sendSubUpdateToClient(stringData);
				response.setStatus(KVMessage.StatusType.SUBUPDATETOSERVER_SUCCESS);
				response.setValue("Successful SUBUPDATETOCLIENT to client - " + receivedClientName + ".");
				logger.debug("Successful SUBUPDATETOCLIENT to client = " + receivedClientName + ".");
			} catch (IOException e) {
				response.setStatus(KVMessage.StatusType.SUBUPDATETOSERVER_ERROR);
				response.setValue("Failed SUBUPDATETOCLIENT to client - " + receivedClientName + ".");
				logger.error("Failed SUBUPDATETOCLIENT to client = " + receivedClientName + ".");
			}

			return response;
		}

		// Handle BROADCASTSUBCONTABLETOSERVER request
		/** This means the current server will receive the up-to-date subConnectionTable info. This request is triggered when a client's 
		 * subscription thread connects or reconnects to a new server (subConnectionTable info will be updated).
		 */
		if (obj_mess.getStatus().equals(KVMessage.StatusType.BROADCASTSUBCONTABLETOSERVER)) {
			// Transform the string back to subConnectionTable object
			SubConnectionTable newSubConnectionTable = (SubConnectionTable) Serialization
					.deserialize(Base64.getDecoder().decode(obj_mess.getValue().getBytes()));
			kvServer.replaceSubConnectionTable(newSubConnectionTable);
			response.setStatus(KVMessage.StatusType.BROADCASTSUBCONTABLETOSERVER_FINISH);
			return response;
		}
		
		// Similar to above, handle or UNICASTSUBCONTABLETOSERVER request
		if (obj_mess.getStatus().equals(KVMessage.StatusType.UNICASTSUBCONTABLETOSERVER)) {
			// Transform the string back to subConnectionTable object
			SubConnectionTable newSubConnectionTable = (SubConnectionTable) Serialization.deserialize(Base64.getDecoder().decode(obj_mess.getValue().getBytes()));
			kvServer.replaceSubConnectionTable(newSubConnectionTable);
			response.setStatus(KVMessage.StatusType.UNICASTSUBCONTABLETOSERVER_FINISH);
			return response;
		}

		// Secret Key Set Up
		if (obj_mess.getStatus().equals(KVMessage.StatusType.AUTHENTICATION_SETUP)){
			// generate a random alphanumeric secret key for this connection
			String alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
			StringBuilder sb = new StringBuilder();
			Random random = new Random();
			int length = 10;
			for(int i = 0; i < length; i++) {
				int index = random.nextInt(alphabet.length());
				char randomChar = alphabet.charAt(index);
				sb.append(randomChar);
			}
			String generatedSecret = sb.toString();
			// Set up secret key for follow up communication
			secretKey = generatedSecret;
			logger.info("secret key is set up");
			response.setValue((generatedSecret));
			response.setStatus(KVMessage.StatusType.AUTHENTICATION_SETUP);
			return response;
		}

		/* Handle the normal client request */

		// Handle 'any' error cases, like message size exceeded, message format unknown, etc
		// By default, set return status to be PUT_ERROR.
		response.setStatus(null);
		String error_mess = null;
		if (obj_mess.getKey() == null) {
			error_mess = "Key = null, should not be null";
			response.setStatus(KVMessage.StatusType.INPUT_ERROR);
			response.setValue(error_mess);
			logger.error(error_mess);
		} else if (obj_mess.getKey().length() > MAX_KEY_LEN) {
			error_mess = "Key exceeds max length: key length = " + obj_mess.getKey().length() + ", should < 20 Bytes";
			response.setStatus(KVMessage.StatusType.INPUT_ERROR);
			response.setValue(error_mess);
			logger.error(error_mess);
		} else if (obj_mess.getValue() != null && obj_mess.getValue().length() > MAX_VAL_LEN) {
			error_mess = "Value exceeds max length: value length = " + obj_mess.getValue().length()
					+ ", should < 120 KBytes";
			response.setStatus(KVMessage.StatusType.INPUT_ERROR);
			response.setValue(error_mess);
			logger.error(error_mess);
		}

		// Process the command only when there is no 'any' error detected.
		if (error_mess == null) {
			switch (obj_mess.getStatus()) {
				case METADATA_REQUEST: {
					// This means the connection is subscription thread
					// Then record this thread in the server.
					if (obj_mess.getKey().equals("SUBCONNECTION")) {
						// Also, give the client name to the local variable clientName, to identify this thread as a server-client clientConnection.
						clientName = obj_mess.getValue();

						// Change the title of logger for debugging.
						logger = Logger.getLogger("SubclientConnection_" + clientName + "_" + kvServer.getServerName());

						kvServer.getClientName_subConnection().put(clientName, this);
						logger.debug("ClientConnection = " + clientName + "_" + kvServer.getServerName()
								+ " is successfully recorded in server = " + kvServer.getServerName() + ".");
						
						// (2) insert the current client - server into subConnectionTable 
						response.setStatus(StatusType.SUBCONTABLEINSERTTOSERVER_SUCCESS);
						kvServer.getSubConnectionTable().addConnection(clientName, kvServer.getServerName());
						logger.debug("client_server pair = " + clientName + "_" + kvServer.getServerName()
								+ "is successfully inserted in server = " + kvServer.getServerName()
								+ "'s subConnectionTable.");		

						kvServer.printAllSubConnections();

						// Broadcast the current subConnectionTable to all other servers, one by one.
						if (kvServer.broadcastSubConnectionTableToAllOtherServers()) {
							logger.debug("Successful broadcast up-to-date subConnectionTable to all other servers.");
						} else {
							logger.error("Some broadcast of up-to-date subConnectionTable to servers are failed.");
						}
						
					}
					// Else, the connection is the normal connection
					// Send back the up-to-date metadata to client.
					try {
						sendMetadataToClient();
						response.setStatus(StatusType.METADATA_UPDATE);
					} catch (IOException ioe) {
						logger.error("Failed to send metadata to client");
					}
					break;
				}
				case GET: {
					logger.info("GET " + obj_mess.getKey());
					String value = null;
					try {
						if (kvServer.getStatus() == IKVServer.ServerStatus.STOP) {
							response.setStatus(KVMessage.StatusType.SERVER_STOPPED);
							break;
						}
						// If the requested key not in range, send latest metadata
						if (!kvServer.getMetadata().inRange(obj_mess.getKey(), kvServer.getServerName())
								&& !kvServer.getMetadata().inReplicaRange(obj_mess.getKey(), kvServer.getServerName())) {
							sendMetadataToClient();
							response.setStatus(KVMessage.StatusType.SERVER_NOT_RESPONSIBLE);
							System.out.println("metadata wrong");
							break;
						}

						value = kvServer.getKV(obj_mess.getKey());
						if (value == null) {
							response.setValue("Failed GET: Key = " + obj_mess.getKey() + " not found");
							response.setStatus(KVMessage.StatusType.GET_ERROR);
							logger.error("Failed GET: key = " + response.getKey());
						} else {
							response.setValue(value);
							response.setStatus(KVMessage.StatusType.GET_SUCCESS);
							logger.info("Successful GET: key = " + response.getKey() + ", value = " + response.getValue());
						}
					} catch (Exception e) {
						response.setValue("Failed GET: key = " + obj_mess.getKey());
						response.setStatus(KVMessage.StatusType.GET_ERROR);
						logger.error("Failed GET: key = " + obj_mess.getKey(), e);
					}
					break;
				}
				case PUT: {
					logger.info("PUT " + obj_mess.getKey() + ":" + obj_mess.getValue());
					response.setKey(obj_mess.getKey());
					response.setValue(obj_mess.getValue());

					boolean valueInStorage = kvServer.valueInStorage(obj_mess.getKey());

					try {
						if (kvServer.getStatus() == IKVServer.ServerStatus.STOP) {
							response.setStatus(KVMessage.StatusType.SERVER_STOPPED);
							break;
						}
						if (kvServer.getStatus() == IKVServer.ServerStatus.LOCKED) {
							response.setStatus(KVMessage.StatusType.SERVER_WRITE_LOCK);
							break;
						}
						// If the response is PUT / DELETE replicated response
						// Else if data is not in range
						if (obj_mess.getIsReplicated()) {
							response.setIsReplicated(true);
						} else if (!kvServer.getMetadata().inRange(obj_mess.getKey(), kvServer.getServerName())) {
							System.out.println(
									"Correct Server: " + kvServer.getMetadata().getCorrectServer(obj_mess.getKey()));
							System.out.println("Current Server: " + kvServer.getServerName());
							sendMetadataToClient();
							response.setStatus(KVMessage.StatusType.SERVER_NOT_RESPONSIBLE);
							break;
						}
						kvServer.putKV(obj_mess.getKey(), obj_mess.getValue());
						if (!valueInStorage) {
							if (obj_mess.getValue() == null) {
								response.setStatus(KVMessage.StatusType.DELETE_ERROR);
								response.setValue("Failed DELETE: Key = " + obj_mess.getKey() + " does not exist");
								logger.info("Failed DELETE: key = " + obj_mess.getKey());
							} else {
								response.setStatus(KVMessage.StatusType.PUT_SUCCESS);
								logger.info("Successful INSERT: key = " + obj_mess.getKey() + ", value = "
										+ obj_mess.getValue());
							}
						} else {
							if (obj_mess.getValue() == null) {
								response.setStatus(KVMessage.StatusType.DELETE_SUCCESS);
								logger.info("Successful DELETE: key = " + obj_mess.getKey() + ", value = "
										+ obj_mess.getValue());
							} else {
								response.setStatus(KVMessage.StatusType.PUT_UPDATE);
								logger.info("Successful UPDATE: key = " + obj_mess.getKey() + ", value = "
										+ obj_mess.getValue());
							}
						}

					} catch (Exception e) {
						logger.error("Exception: " + e);
						if (obj_mess.getValue() == null) {
							response.setStatus(KVMessage.StatusType.DELETE_ERROR);
							response.setValue("Failed DELETE: key = " + obj_mess.getKey());
							logger.error(
									"Failed DELETE: key = " + obj_mess.getKey() + ", value = " + obj_mess.getValue(),
									e);
						} else {
							response.setStatus(KVMessage.StatusType.PUT_ERROR);
							response.setValue("Failed UPDATE/INSERT: key = " + obj_mess.getKey() + ", value = "
									+ obj_mess.getValue());
							logger.error("Failed UPDATE/INSERT: key = " + obj_mess.getKey() + ", value = "
									+ obj_mess.getValue(), e);
						}
					}
					break;
				}
				case SUB: {
					logger.info("SUB " + obj_mess.getKey() + ":" + obj_mess.getValue());
					response.setKey(obj_mess.getKey());
					response.setValue(obj_mess.getValue());

					try {
						if (kvServer.getStatus() == IKVServer.ServerStatus.STOP) {
							response.setStatus(KVMessage.StatusType.SERVER_STOPPED);
							break;
						}
						if (kvServer.getStatus() == IKVServer.ServerStatus.LOCKED) {
							response.setStatus(KVMessage.StatusType.SERVER_WRITE_LOCK);
							break;
						}

						// If the response is SUB / UNSUB replicated response
						// Else if data is not in range
						if (obj_mess.getIsReplicated()) {
							response.setIsReplicated(true);
						} else if (!kvServer.getMetadata().inRange(obj_mess.getKey(), kvServer.getServerName())) {
							System.out.println(
									"Correct Server: " + kvServer.getMetadata().getCorrectServer(obj_mess.getKey()));
							System.out.println("Current Server: " + kvServer.getServerName());
							sendMetadataToClient();
							response.setStatus(KVMessage.StatusType.SERVER_NOT_RESPONSIBLE);
							break;
						}

						int result = kvServer.sub(obj_mess.getKey(), obj_mess.getValue());

						if (result == 1) {
							response.setStatus(KVMessage.StatusType.SUB_SUCCESS);
							response.setValue("Successful SUB: the client - " + obj_mess.getValue() + " subscribes the key - " + obj_mess.getKey() + ".");
							logger.info("The client = " + obj_mess.getValue() + " successfully subscribes the key = " + obj_mess.getKey() + ".");
						} else if (result == -1) {
							response.setStatus(KVMessage.StatusType.SUB_SUCCESS);
							response.setValue("Redundant SUB: the client - " + obj_mess.getValue() + " has already subscribed the key - " + obj_mess.getKey() + ".");
							logger.info("The client = " + obj_mess.getValue() + " has already subscribed the key = " + obj_mess.getKey() + " before. So do nothing.");
						} else {
							// result = 0
							response.setStatus(KVMessage.StatusType.SUB_ERROR);
							response.setValue("Failed SUB: key - " + obj_mess.getKey() + ", client - " + obj_mess.getValue() + ".");
							logger.error("Failed SUB: key = " + obj_mess.getKey() + ", client = " + obj_mess.getValue() + ".");						
						}

					} catch (Exception e) {
						response.setStatus(KVMessage.StatusType.SUB_ERROR);
						response.setValue("Failed SUB: key - " + obj_mess.getKey() + ", client - " + obj_mess.getValue() + ".");
						logger.error("Failed SUB: key = " + obj_mess.getKey() + ", client = " + obj_mess.getValue() + ".", e);
					}
					break;
				}
				case UNSUB: {
					logger.info("UNSUB " + obj_mess.getKey() + ":" + obj_mess.getValue());
					response.setKey(obj_mess.getKey());
					response.setValue(obj_mess.getValue());

					try {
						if (kvServer.getStatus() == IKVServer.ServerStatus.STOP) {
							response.setStatus(KVMessage.StatusType.SERVER_STOPPED);
							break;
						}
						if (kvServer.getStatus() == IKVServer.ServerStatus.LOCKED) {
							response.setStatus(KVMessage.StatusType.SERVER_WRITE_LOCK);
							break;
						}

						// If the response is SUB / UNSUB replicated response
						// Else if data is not in range
						if (obj_mess.getIsReplicated()) {
							response.setIsReplicated(true);
						} else if (!kvServer.getMetadata().inRange(obj_mess.getKey(), kvServer.getServerName())) {
							System.out.println(
									"Correct Server: " + kvServer.getMetadata().getCorrectServer(obj_mess.getKey()));
							System.out.println("Current Server: " + kvServer.getServerName());
							sendMetadataToClient();
							response.setStatus(KVMessage.StatusType.SERVER_NOT_RESPONSIBLE);
							break;
						}

						int result = kvServer.unsub(obj_mess.getKey(), obj_mess.getValue());

						if (result == 1) {
							response.setStatus(KVMessage.StatusType.UNSUB_SUCCESS);
							response.setValue("Successful UNSUB: the client - " + obj_mess.getValue()
									+ " unsubscribes the key - " + obj_mess.getKey() + ".");
							logger.info("The client - " + obj_mess.getValue() + " successfully unsubscribes the key - "
									+ obj_mess.getKey() + ".");
						} else if (result == -1) {
							response.setStatus(KVMessage.StatusType.UNSUB_SUCCESS);
							response.setValue("Redundant UNSUB: the client - " + obj_mess.getValue()
									+ " hasn't subscribed the key - " + obj_mess.getKey() + " yet.");
							logger.info("The client - " + obj_mess.getValue() + " hasn't subscribed the key - "
									+ obj_mess.getKey() + " before. So do nothing.");
						} else {
							// result = 0
							response.setStatus(KVMessage.StatusType.UNSUB_ERROR);
							response.setValue("Failed UNSUB: key - " + obj_mess.getKey() + ", client - " + obj_mess.getValue() + ".");
							logger.error("Failed UNSUB: key = " + obj_mess.getKey() + ", client = " + obj_mess.getValue() + ".");				
						}

					} catch (Exception e) {
						response.setStatus(KVMessage.StatusType.UNSUB_ERROR);
						response.setValue("Failed UNSUB: key - " + obj_mess.getKey() + ", client - " + obj_mess.getValue() + ".");
						logger.error("Failed UNSUB: key = " + obj_mess.getKey() + ", client = " + obj_mess.getValue() + ".", e);
					}
					break;
				}
				case DIDSUB: {
					logger.info("DIDSUB " + obj_mess.getKey() + ":" + obj_mess.getValue());
					response.setKey(obj_mess.getKey());
					response.setValue(obj_mess.getValue());

					try {
						if (kvServer.getStatus() == IKVServer.ServerStatus.STOP) {
							response.setStatus(KVMessage.StatusType.SERVER_STOPPED);
							break;
						}

						// If the requested key not in range, send latest metadata
						if (!kvServer.getMetadata().inRange(obj_mess.getKey(), kvServer.getServerName())
								&& !kvServer.getMetadata().inReplicaRange(obj_mess.getKey(), kvServer.getServerName())) {
							sendMetadataToClient();
							response.setStatus(KVMessage.StatusType.SERVER_NOT_RESPONSIBLE);
							System.out.println("metadata wrong");
							break;
						}

						int result = kvServer.didSub(obj_mess.getKey(), obj_mess.getValue());

						if (result == 1) {
							response.setStatus(KVMessage.StatusType.DIDSUB_SUCCESS);
							response.setValue("Successful DIDSUB: the client - " + obj_mess.getValue() + " has subscribed the key - " + obj_mess.getKey() + " before.");
							logger.info("The client = " + obj_mess.getValue() + " has subscribed the key = " + obj_mess.getKey() + " before.");
						} else if (result == -1) {
							response.setStatus(KVMessage.StatusType.SUB_SUCCESS);
							response.setValue("Successful DIDSUB: the client - " + obj_mess.getValue() + " hasn't subscribed the key - " + obj_mess.getKey() + " before.");
							logger.info("The client - " + obj_mess.getValue() + " hasn't subscribed the key - " + obj_mess.getKey() + " before.");
						} else {
							// result = 0
							response.setStatus(KVMessage.StatusType.DIDSUB_ERROR);
							response.setValue("Failed DIDSUB: key - " + obj_mess.getKey() + ", client - " + obj_mess.getValue() + ".");
							logger.error("Failed DIDSUB: key = " + obj_mess.getKey() + ", client = " + obj_mess.getValue() + ".");
						}

					} catch (Exception e) {
						response.setStatus(KVMessage.StatusType.DIDSUB_ERROR);
						response.setValue("Failed DIDSUB: key = " + obj_mess.getKey() + ", client = " + obj_mess.getValue() + ".");
						logger.error("Failed DIDSUB: key = " + obj_mess.getKey() + ", client = " + obj_mess.getValue() + ".", e);
					}
					break;					
				}
				default: {
					response.setKey("---");
					response.setValue("Error: An unknown error has occurred, please contact tech support!");
					response.setStatus(KVMessage.StatusType.UNKNOWN_STATUS_ERROR);
				}
			}
		}
		return response;
	}

	public boolean getIsOpen() {
		return this.isOpen;
	}

	public void setIsOpen(boolean isOpen) {
		this.isOpen = isOpen;
	}

}
