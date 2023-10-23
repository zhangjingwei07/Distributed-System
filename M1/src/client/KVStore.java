package client;

import shared.messages.KVMessage;
import shared.messages.KVMessageImple;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.HashSet;
import java.util.Set;

import org.apache.log4j.Logger;

import client.ClientSocketListener.SocketStatus;

public class KVStore extends Thread implements KVCommInterface {
	
	private Logger logger = Logger.getLogger("Client");
	private Set<ClientSocketListener> listeners;
	private boolean running;
	protected String address;
	protected int port;

	protected Socket clientSocket;
	protected OutputStream output=null;
 	protected InputStream input=null;
	
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
		// try {
			// clientSocket = new Socket(address, port);
		listeners = new HashSet<ClientSocketListener>();
			//setRunning(true);
			//logger.info("Connection established");
		//} catch (UnknownHostException e) {
		//	logger.error("Unknown host name: " + address, e);
		//} catch (IOException ioe) {
		//	logger.error(ioe);
		//}
	}

	public KVStore() {};
	
	// /**
	//  * Initializes and starts the client connection. 
	//  * Loops until the connection is closed or aborted by the client.
	//  */
	// public void run() {		
	// 		while(isRunning()) {		
	// 			;		
	// 		}
	// }

	@Override
	public void connect() throws IOException,UnknownHostException {
		this.clientSocket = new Socket(this.address, this.port);
		output = clientSocket.getOutputStream();
		input = clientSocket.getInputStream();
		this.setRunning(true);
		logger.info("Connecting to server");
		// this.start();

		System.out.println("Success: Connected to server!");

		// TextMessage latestMsg = receiveMessage();
		// for(ClientSocketListener listener : listeners) {
		// 	listener.handleNewMessage(latestMsg);
	    // }
	}

	@Override
	public synchronized void disconnect() throws IOException {
		logger.debug("try to close connection ...");
		
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
	 * @throws IOException some I/O error regarding the output stream 
	 */
	public synchronized void sendMessage(TextMessage msg) throws IOException {
		byte[] msgBytes = msg.getMsgBytes();
		output.write(msgBytes, 0, msgBytes.length);
		output.flush();
		logger.debug("Send message:\t '" + msg.getMsg() + "'");
	}
	
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

	@Override
	public KVMessage put(String key, String value) throws Exception {
		// treat String 'null' as a null value
		if (value != null && value.equals("null")){
			value = null;
		}
		KVMessage message = new KVMessageImple(key, value, KVMessage.StatusType.PUT);
		sendMessage(new TextMessage(((KVMessageImple) message).encode()));
		TextMessage latestMsg = receiveMessage();

		KVMessageImple received_message = new KVMessageImple(key, null, KVMessage.StatusType.PUT);
		received_message.decode(latestMsg.getMsg());
		if (received_message.getStatus() == KVMessage.StatusType.PUT_ERROR){
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
		}

		return received_message;
	}

	@Override
	public KVMessage get(String key) throws Exception {
		KVMessage message = new KVMessageImple(key, null, KVMessage.StatusType.GET);
		sendMessage(new TextMessage(((KVMessageImple) message).encode()));
		TextMessage latestMsg = receiveMessage();

		KVMessageImple received_message = new KVMessageImple(key, "", KVMessage.StatusType.GET);

		received_message.decode(latestMsg.getMsg());
		if (received_message.getStatus() == KVMessage.StatusType.GET_ERROR){
			System.out.println(received_message.getValue());
		} else if (received_message.getStatus() == KVMessage.StatusType.GET_SUCCESS){
			System.out.println("Success: The requested key: "+received_message.getKey()+" has value: "+received_message.getValue());
		} else if (received_message.getStatus() == KVMessage.StatusType.UNKNOWN_STATUS_ERROR){
			System.out.println(received_message.getValue());
		}
		return received_message;
	}
}
