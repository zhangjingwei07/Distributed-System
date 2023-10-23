package shared.metadata;

import ecs.IECSNode;
import org.apache.log4j.Logger;

import java.io.Serializable;
import java.util.*;

public class SubConnectionTable implements Serializable {
    private static Logger logger = Logger.getLogger("Server");

    // Client Name - Server Name
    private HashMap<String, String> subConnectionTable;

    public SubConnectionTable(){
        subConnectionTable = new HashMap<String, String>();
    }

    /**
     * Add a new connection pair
     * @param clientName
     * @param serverName
     * @return True 
     */
    public boolean addConnection(String clientName, String serverName){
        // if (connectionExists(clientName)){
        //     // logger.info("Client " + clientName + " is already connected to " + subConnectionTable.get(clientName));
        //     return false;
        // }
        subConnectionTable.put(clientName, serverName);
        return true;
    }

    /**
     * Remove a connection
     * @param clientName
     * @return Return true on success. False when the connection does not exist.
     */
    public boolean removeConnection(String clientName){
        if (!connectionExists(clientName)){
            return false;
        }
        subConnectionTable.remove(clientName);
        return true;
    }

    /**
     * Check if the client has connection or not
     * @param clientName
     * @return
     */
    public boolean connectionExists(String clientName){
        if (subConnectionTable.containsKey(clientName)){
            return true;
        }
        else{
            return false;
        }
    }

    /**
     * Get the connected server of a client.
     * @param clientName
     * @return The name of the connected server. Return NULL if not found.
     */
    public String getConnectedServerName(String clientName) {
        if (!connectionExists(clientName)) {
            return null;
        }
        return subConnectionTable.get(clientName);
    }
    
    /**
     * Print all Client - server pairs
     */
    public void printSubConnectionTable(){
        logger.debug("In subConnectionTable, the current client_server pairs:");
        for (Map.Entry pair : subConnectionTable.entrySet()) {
            logger.debug(" client = " + pair.getKey() + ", subServer = " + pair.getValue() + ".");
        }
    }
}
