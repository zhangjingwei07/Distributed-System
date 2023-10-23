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



public class ConnectionMonitor extends Thread{

    private KVStore client;
    private String clientName;
    private KVSubscription sub_client;
    private int TIME_INT = 2;
    public boolean stop = false;
    private Logger logger = Logger.getLogger("DefaultClientName");

    public ConnectionMonitor(KVStore client_in, KVSubscription sub_client_in, String clientName) {
        this.client = client_in;
        this.sub_client = sub_client_in;
        this.clientName = clientName;
	}

    public void run() {
		try {
            logger = Logger.getLogger(clientName);
            
			while(!stop) {
                Thread.sleep(TIME_INT * 1000);
                if (stop){
                    break;
                }
                
                try {
                    KVMessage message = new KVMessageImple(null, null, null, sub_client.secretKey);
               
                    sub_client.sendHeartbeatMessage(new TextMessage(((KVMessageImple) message).encode()));
                } catch (IOException ioe) {
                    System.out.println("Subscription disconnection detected");
                    Thread.sleep(8 * 1000);
                    logger.debug("Subscription Start reconnecting***");
                    sub_client.disconnect();
                    sub_client.reconnect();
                }

                // if (stop){
                //     break;
                // }

			    // try {
                //          KVMessage message1 = new KVMessageImple(null, null, null, client.secretKey);
                    
                //          client.sendHeartbeatMessage(new TextMessage(((KVMessageImple) message1).encode()));
				// } catch (IOException ioe) {
                //          System.out.println("Client disconnection detected");
                //          client.reconnect();
				// }

                
                		
			}
		} catch (IOException ioe) {
			logger.error("Monitor Error");
			
		} finally {
			return;
		}
	}


}