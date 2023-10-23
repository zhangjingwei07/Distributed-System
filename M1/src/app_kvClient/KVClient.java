package app_kvClient;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.UnknownHostException;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import logger.LogSetup;

import client.*;
import shared.messages.KVMessage;
import shared.messages.KVMessageImple;

public class KVClient implements IKVClient, ClientSocketListener {

	private static Logger logger = Logger.getLogger("Client");
    private boolean stop = false;
    private static final String PROMPT = "KVClient> ";
	private BufferedReader stdin;
	private KVStore client =  null;

    private String serverAddress;
	private int serverPort;

    public void run() throws Exception {
		while(!stop) {
			stdin = new BufferedReader(new InputStreamReader(System.in));
			System.out.print(PROMPT);
			
			try {
				String cmdLine = stdin.readLine();
				this.handleCommand(cmdLine);
			} catch (IOException e) {
				stop = true;
				printError("CLI does not respond - Application terminated ");
			}
		}
    }
    
    private void handleCommand(String cmdLine) throws Exception {
		String[] tokens = cmdLine.trim().split("\\s+");

		if(tokens[0].equals("quit")) {	
			stop = true;
			if (client != null) {
				disconnect();
			}
			System.out.println(PROMPT + "Application exit!");
		
		} else if (tokens[0].equals("connect")){
			if(checkCommandLength(tokens.length, 3, 3)) {
				try{
					serverAddress = tokens[1];
					serverPort = Integer.parseInt(tokens[2]);
					newConnection(serverAddress, serverPort);
				} catch (NumberFormatException nfe) {
					printError("No valid address. Port must be a number!");
					logger.info("Unable to parse argument <port>", nfe);
				} catch (UnknownHostException e) {
					printError("Unknown Host!");
					logger.info("Unknown Host!", e);
				} catch (IOException e) {
					printError("Could not establish connection!");
					logger.warn("Could not establish connection!", e);
				} catch (Exception e) {
					printError("Unknow Error in connect");
					logger.warn("Unknow Error in connect", e);
					System.out.println(e);
				}
			}

		}  else if(tokens[0].equals("disconnect")) {
			if (client != null){
				disconnect();
			} else{
				printError("No valid connection with server. Please connect to a server!");
			}
			
		} else if (tokens[0].equals("put")) {
			if(client != null && client.isRunning()) {
				if (checkCommandLength(tokens.length, 2, 3)) {
					if (tokens.length < 3 || tokens[2].equals("null")) {
						logger.info("Delete: key = " + tokens[1]);
					} else{
						logger.info("Put: key = " + tokens[1] + ", value = " + tokens[2]);
					}
					if(tokens.length < 3) {
						client.put(tokens[1], null);
					} else{
						client.put(tokens[1], tokens[2]);
					}
				}
			} else {
				printError("No valid connection with server. Please connect to a server!");
			}
		} else if (tokens[0].equals("get")){
			if (client!= null && client.isRunning()){
				if (checkCommandLength(tokens.length, 2, 2)) {
					logger.info("Get " + tokens[1]);
					client.get(tokens[1]);
				}
			} else {
				printError("No valid connection with server. Please connect to a server!");
			}

	    } else if (tokens[0].equals("logLevel")) {
			if(tokens.length == 2) {
				String level = setLevel(tokens[1]);
				if(level.equals(LogSetup.UNKNOWN_LEVEL)) {
					printError("No valid log level!");
					printPossibleLogLevels();
				} else {
					System.out.println(PROMPT + 
							"Log level changed to level " + level);
				}
			} else {
				printError("Invalid number of parameters!");
			}
			
		} else if(tokens[0].equals("help")) {
			printHelp();
		} else {
			printError("Unknown command");
			printHelp();
		}
	}

    @Override
    public void newConnection(String hostname, int port) throws Exception{
        client = new KVStore(hostname, port);
		client.addListener(this);
		client.connect();
	}
	
	private void disconnect() {
		try {
			if(client != null) {
				client.disconnect();
				client = null;
			}
		} catch (IOException ioe) {
			logger.error("Unable to close connection!");
		}
	}

/*	private void sendMessage(String msg){
		try {
			client.sendMessage(new TextMessage(msg));
		} catch (IOException e) {
			printError("Unable to send message!");
			disconnect();
		}
	} */

	/**
	 * Check the command line input parameter length
	 * @param actual length of the input
	 * @param minbound min length of the arguments
	 * @param maxbound min length of the arguments
	 * @return if the parameter length is [minbound, maxbound] inclusive
	 */
	public boolean checkCommandLength(int actual, int minbound, int maxbound){
    	if (minbound > actual || actual > maxbound){
    		printError("Required [" + (minbound-1) + " - " + (maxbound-1)+ "] parameters, but you have "+(actual-1));
    		return false;
		}
    	else return true;
	}

    @Override
    public KVCommInterface getStore(){
        // TODO Auto-generated method stub
        return null;
	}
	
	@Override
	public void handleNewMessage(TextMessage msg) {
		if(!stop) {
			System.out.println(msg.getMsg());
			System.out.print(PROMPT);
		}
	}
	
	@Override
	public void handleStatus(SocketStatus status) {
		if(status == SocketStatus.CONNECTED) {

		} else if (status == SocketStatus.DISCONNECTED) {
			System.out.print(PROMPT);
			System.out.println("Connection terminated: " 
					+ serverAddress + " / " + serverPort);
			
		} else if (status == SocketStatus.CONNECTION_LOST) {
			System.out.println("Connection lost: " 
					+ serverAddress + " / " + serverPort);
			System.out.print(PROMPT);
		}
	}

	private void printError(String error){
		System.out.println(PROMPT + "Error! " +  error);
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
		sb.append("KV CLIENT HELP (Usage):\n");
		sb.append("::::::::::::::::::::::::::::::::");
		sb.append("::::::::::::::::::::::::::::::::\n");
		sb.append("connect <host> <port>");
		sb.append("\t establishes a connection to a server\n");
		// sb.append("send <text message>");
		// sb.append("\t\t sends a text message to the server \n");
		sb.append("disconnect");
		sb.append("\t\t\t disconnects from the server \n");
		sb.append("put");
		sb.append("\t\t\t store key and value pair into the server \n");
		sb.append("get");
		sb.append("\t\t\t get value of a key stored in server \n");
		sb.append("logLevel");
		sb.append("\t\t\t changes the logLevel \n");
		sb.append("\t\t\t\t ");
		sb.append("ALL | DEBUG | INFO | WARN | ERROR | FATAL | OFF \n");
		
		sb.append("quit ");
		sb.append("\t\t\t exits the program");
		System.out.println(sb.toString());
	}

    /**
     * Main entry point for the echo server application. 
     * @param args contains the port number at args[0].
     */
    public static void main(String[] args) {
    	try {
			new LogSetup("logs/client.log", Level.INFO);
			KVClient kvclient = new KVClient();
			kvclient.run();
		} catch (IOException e) {
			System.out.println("Error! Unable to initialize logger!");
			e.printStackTrace();
			System.exit(1);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
