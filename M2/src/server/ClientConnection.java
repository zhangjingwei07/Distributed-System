package server;

import app_kvServer.IKVServer;
import org.apache.log4j.Logger;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

import app_kvServer.KVServer;
import shared.messages.KVMessage;
import shared.messages.KVMessageImple;
import shared.metadata.Serialization;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.Base64;


/**
 * Represents a connection end point for a particular client that is 
 * connected to the server. This class is responsible for message reception 
 * and sending. 
 * The class also implements the echo functionality. Thus whenever a message 
 * is received it is going to be echoed back to the client.
 */
public class ClientConnection implements Runnable {

	private static Logger logger = Logger.getLogger("Server");
	
	private boolean isOpen;
	private static final int BUFFER_SIZE = 1024;
	private static final int MAX_KEY_LEN = 20;
	private static final int MAX_VAL_LEN = 120 * 1024;
	private static final int DROP_SIZE = 128 * BUFFER_SIZE;
	
	private KVServer kvServer;
	private Socket clientSocket;
	private InputStream input;
	private OutputStream output;
	
	/**
	 * Constructs a new CientConnection object for a given TCP socket.
	 * @param clientSocket the Socket object for the client connection.
	 */
	public ClientConnection(KVServer kvServer, Socket clientSocket) {
		this.kvServer = kvServer;
		this.clientSocket = clientSocket;
		this.isOpen = true;
	}
	
	/**
	 * Initializes and starts the client connection. 
	 * Loops until the connection is closed or aborted by the client.
	 */
	public void run() {
		try {
			output = clientSocket.getOutputStream();
			input = clientSocket.getInputStream();
			
			while(getIsOpen()) {
				try {
					// Decode Byte message into KVMessageImple java object
					KVMessageImple request = new KVMessageImple();
					TextMessage request_text_mess = receiveMessage();
					String string_mess = request_text_mess.getMsg();
					TextMessage response_text_mess = new TextMessage("");

					// To tackle disconnect situation when client will send "" to server
					if(!string_mess.equals("")){
						request.decode(string_mess);
					
						// Handle the request java object and send back message
						KVMessageImple response = handleMessage(request);
						// if status is update, metadata already sent
						if (response.getStatus() == KVMessage.StatusType.SERVER_NOT_RESPONSIBLE ||
						response.getStatus() == KVMessage.StatusType.METADATA_UPDATE){
							continue;
						}
						response_text_mess = new TextMessage(response.encode());
					}
					sendMessage(response_text_mess);
					
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
	public void sendMessage(TextMessage msg) throws IOException {
		byte[] msgBytes = msg.getMsgBytes();
		output.write(msgBytes, 0, msgBytes.length);
		output.flush();
		logger.debug("SEND <" 
				+ clientSocket.getInetAddress().getHostAddress() + ":" 
				+ clientSocket.getPort() + ">: '" 
				+ msg.getMsg() +"'");
    }
	
	
	public TextMessage receiveMessage() throws IOException {
		
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

		while(/*read != 13  && */ read != 10 && read !=-1 && reading) {/* CR, LF, error */
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
			
			/* only read valid characters, i.e. letters and constants */
			bufferBytes[index] = read;
			index++;
			
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
		logger.debug("RECEIVE \t<" 
				+ clientSocket.getInetAddress().getHostAddress() + ":" 
				+ clientSocket.getPort() + ">: '" 
				+ msg.getMsg().trim() + "'");
		return msg;
	}
    // Send latest metadata to client
	private void sendMetadataToClient() throws IOException{
		KVMessageImple response = new KVMessageImple();
		if (kvServer.getMetadata() == null){
			response.setKey("");
			response.setStatus(KVMessage.StatusType.METADATA_ERROR);
			response.setValue("Error: Internal server error, no metadata found.");
			sendMessage(new TextMessage(response.encode()));
			return;
		}
		response.setKey("New metadata");
		response.setStatus(KVMessage.StatusType.METADATA_UPDATE);
		byte[] metadata_byte = Serialization.serialize((Object) kvServer.getMetadata());
		response.setValue(Base64.getEncoder().encodeToString(metadata_byte));
		sendMessage(new TextMessage(response.encode()));
	}
	
	private KVMessageImple handleMessage(KVMessageImple obj_mess) {
		KVMessageImple response = new KVMessageImple();
		// By default, set the same key to the response.
		response.setKey(obj_mess.getKey());

		/* Handle the data move case */
		if (obj_mess.getStatus().equals(KVMessage.StatusType.MERGE)) {
			String stringData = obj_mess.getValue();
			boolean mergeSuccess;
			if (stringData.length() == 2){
				logger.debug("There is also no incoming data, don't do anything.");
				mergeSuccess = true;
			} else{
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

		/* Handle the normal client request */
		
		// Handle 'any' error cases, like message size exceeded, message format unknown, etc
		// By default, set return status to be PUT_ERROR.
		response.setStatus(KVMessage.StatusType.INPUT_ERROR);
		String error_mess = null;
		if (obj_mess.getKey() == null) {
			error_mess = "Key = null, should not be null";
			logger.error(error_mess);	
		} else if (obj_mess.getKey().length() > MAX_KEY_LEN) {
			error_mess = "Key exceeds max length: key length = " + obj_mess.getKey().length() + ", should < 20 Bytes";
			logger.error(error_mess);
		} else if (obj_mess.getValue() != null && obj_mess.getValue().length() > MAX_VAL_LEN) {
			error_mess = "Value exceeds max length: value length = " + obj_mess.getValue().length() + ", should < 120 KBytes";
			logger.error(error_mess);
		}	
		response.setValue(error_mess);

		// Process the command only when there is no 'any' error detected.
		if (error_mess == null) {
			switch(obj_mess.getStatus()) {
				case METADATA_REQUEST:{
					try {
						sendMetadataToClient();
					} catch (IOException ioe){
						logger.error("Failed to send metadata to client");
					}
					break;
				}
				case GET: {
					String value = null;
					try {
						if (kvServer.getStatus() == IKVServer.ServerStatus.STOP){
							response.setStatus(KVMessage.StatusType.SERVER_STOPPED);
							break;
						}
						// If the requested key not in range, send latest metadata
						if (!kvServer.getMetadata().inRange(obj_mess.getKey(), kvServer.serverName)){
							sendMetadataToClient();
							response.setStatus(KVMessage.StatusType.SERVER_NOT_RESPONSIBLE);
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
					response.setKey(obj_mess.getKey());
					response.setValue(obj_mess.getValue());
					
					boolean inStorage = kvServer.inStorage(obj_mess.getKey());

					try {
						if (kvServer.getStatus() == IKVServer.ServerStatus.STOP){
							response.setStatus(KVMessage.StatusType.SERVER_STOPPED);
							break;
						}
						if (kvServer.getStatus() == IKVServer.ServerStatus.LOCKED){
							response.setStatus(KVMessage.StatusType.SERVER_WRITE_LOCK);
							break;
						}
						// If the requested key not in range, send latest metadata
						if (!kvServer.getMetadata().inRange(obj_mess.getKey(), kvServer.serverName)){
							System.out.println("Correct Server: "+ kvServer.getMetadata().getCorrectServer(obj_mess.getKey()));
							System.out.println("Current Server: "+kvServer.serverName);
							sendMetadataToClient();
							response.setStatus(KVMessage.StatusType.SERVER_NOT_RESPONSIBLE);
							break;
						}
						kvServer.putKV(obj_mess.getKey(), obj_mess.getValue());

						if (!inStorage) {
							if (obj_mess.getValue() == null) {
								response.setStatus(KVMessage.StatusType.DELETE_ERROR);
								response.setValue("Failed DELETE: Key = " + obj_mess.getKey() + " does not exist");
								logger.info("Failed DELETE: key = " + obj_mess.getKey());
							}
							else{
								response.setStatus(KVMessage.StatusType.PUT_SUCCESS);
								logger.info("Successful INSERT: key = " + obj_mess.getKey() + ", value = " + obj_mess.getValue());
							}
						} else {
							if (obj_mess.getValue() == null) {
								response.setStatus(KVMessage.StatusType.DELETE_SUCCESS);
								logger.info("Successful DELETE: key = " + obj_mess.getKey() + ", value = " + obj_mess.getValue());
							} else {
								response.setStatus(KVMessage.StatusType.PUT_UPDATE);
								logger.info("Successful UPDATE: key = " + obj_mess.getKey() + ", value = " + obj_mess.getValue());
							}
						}
					
					} catch (Exception e) {
						if (obj_mess.getValue() == null) {
							response.setStatus(KVMessage.StatusType.DELETE_ERROR);
							response.setValue("Failed DELETE: key = " + obj_mess.getKey());
							logger.error("Failed DELETE: key = " + obj_mess.getKey() + ", value = " + obj_mess.getValue(), e);
						} else {
							response.setStatus(KVMessage.StatusType.PUT_ERROR);
							response.setValue("Failed UPDATE/INSERT: key = " + obj_mess.getKey() + ", value = " + obj_mess.getValue());
							logger.error("Failed UPDATE/INSERT: key = " + obj_mess.getKey() + ", value = " + obj_mess.getValue(), e);
						}
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
