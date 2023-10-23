package shared.metadata;

import ecs.IECSNode;
import org.apache.log4j.Logger;

import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;

// TODO - need to cover the cases of differernt server mapping into the same key

// Store {server_name - hash_value} info
public class HashRing implements Serializable {
    private static Logger logger = Logger.getLogger("HashRing");
    public final static String HASHRING_START = "00000000000000000000000000000000";
    public final static String HASHRING_END = "ffffffffffffffffffffffffffffffff";

    private HashMap<String, String> server_hash;
    private ArrayList<String> serverNamesRing; // ordered server name, from smallest to biggest
    private HashMap<String, Integer> server_index; // Server_name - index in the list
    private HashMap<String, String> server_ip_port; // Server_name - ip_address:port
    private HashMap<String, String> ip_port_server; // ip_address:port - Server_name


    public HashRing(){
        server_hash = new HashMap<String, String>();
        serverNamesRing = new ArrayList<String>();
        server_index = new HashMap<String, Integer>();
        server_ip_port = new HashMap<String, String>();
        ip_port_server = new HashMap<String, String>();
    }

    public void printAllServer(){
        logger.debug("Servers in the Hash Ring:");
        for (String serverName:serverNamesRing){
            logger.debug(" - " + serverName + " at " + server_hash.get(serverName));
        }
    }

    /**
     * Recalculate and update the entire metadata with new set of nodes.
     * @param nodes ECSNodes that is in the ring.
     */
    public void updateMatadata(ArrayList<IECSNode> nodes) {
        server_hash.clear();
        serverNamesRing.clear();
        server_index.clear();
        server_ip_port.clear();
        ip_port_server.clear();

        for (IECSNode node : nodes) {
            String serverName = node.getNodeName();
            String hashKey = getKeyFromECSNode(node);
            String hashValue = getHashValue(hashKey);
            String ip = node.getNodeHost();
            String port = String.valueOf(node.getNodePort());

            server_hash.put(serverName, hashValue);
            serverNamesRing.add(serverName);
            server_ip_port.put(serverName, ip+":"+port);
            ip_port_server.put(ip+":"+port, serverName);
        }

        Collections.sort(serverNamesRing, new Comparator<String>() {
            @Override
            public int compare(String lhs, String rhs) {
                // -1 - less than, 1 - greater than, 0 - equal, all inversed for descending
                int compResult = server_hash.get(lhs).compareTo(server_hash.get(rhs));
                return compResult < 0 ? -1 : compResult > 0 ? 1 : 0;
            }
        });

        for (int i = 0; i != serverNamesRing.size(); i++) {
            server_index.put(serverNamesRing.get(i), i);
        }
    }

    /**
     * Get hashvalue associated to a ket value based on MD5 hashing
     * @param key
     * @return
     */
    public static String getHashValue(String key) {
        try {
            byte[] keyInBytes = key.getBytes("UTF-8");
            MessageDigest md = MessageDigest.getInstance("MD5");
            byte[] md5Val_byte = md.digest(keyInBytes);
            BigInteger md5Val = new BigInteger(1, md5Val_byte);
            return md5Val.toString(16);
        } catch (UnsupportedEncodingException UEE) {
            logger.error("Unsupported Encoding");
        } catch (NoSuchAlgorithmException NSAE) {
            logger.error("Cannot get MD5 Algorithm");
        }
        return "";
    }

    /**
     * Check if the data associated with dataKey is in the range of (prev server, current server]
     * @param dataKey
     * @param serverName: current server
     * @return
     */
    public boolean inRange(String dataKey, String serverName){
        String dataValue = getHashValue(dataKey);
        String self_hash_value = server_hash.get(serverName);
        if (self_hash_value == null){
            logger.error("server name " + serverName + " is not in the hash ring!");
            return false;
        }
        // Get index of prev server
        int self_server_index = server_index.get(serverName);
        int prev_index = (self_server_index == 0) ? (server_hash.size()-1) : (self_server_index - 1);

        // corner case: only 1 server in the hash ring
        if (server_hash.size() == 1){
            return true;
        }

        String prev_server_hash = server_hash.get(serverNamesRing.get(prev_index));

        if (prev_server_hash.compareTo(self_hash_value) < 0){
            return dataValue.compareTo(prev_server_hash) > 0 && dataValue.compareTo(self_hash_value) <= 0;
        }
        else if (prev_server_hash.compareTo(self_hash_value) > 0){
            return dataValue.compareTo(prev_server_hash) > 0 || dataValue.compareTo(self_hash_value) <= 0;
        }
        else{
            logger.error("Prev server has the same hash value as the current server!");
            return false;
        }
    }

    /**
     * Check if the data associated with dataKey is in the range of [current server, current server+2]
     * @param dataKey
     * @param serverName: current server
     * @return
     */
    public boolean inReplicaRange(String dataKey, String serverName){
        String dataValue = getHashValue(dataKey);
        String self_hash_value = server_hash.get(serverName);
        if (self_hash_value == null){
            logger.error("server name " + serverName + " is not in the hash ring!");
            return false;
        }
        // Get index of prev 3 servers
        int self_server_index = server_index.get(serverName);
        int prev_index3 = 0;
        if (server_hash.size() <= 3){
            if (inRange(dataKey, serverName)){
                return false;
            } else {
                return true;
            }
        }

        if (self_server_index == 0){
            prev_index3 = server_hash.size()-3;
        } else if (self_server_index == 1){
            prev_index3 = server_hash.size()-2;
        } else if (self_server_index == 2){
            prev_index3 = server_hash.size()-1;
        } else {
            prev_index3 = self_server_index - 3;
        }

        String prev3_server_hash = server_hash.get(serverNamesRing.get(prev_index3));

        // System.out.println("Data Hash Value: "+dataValue + "CurrentS: " + self_hash_value + "Next2: "+prev3_server_hash);

        if (prev3_server_hash.compareTo(self_hash_value) < 0){
            return dataValue.compareTo(prev3_server_hash) > 0 && dataValue.compareTo(self_hash_value) <= 0;
        }
        else if (prev3_server_hash.compareTo(self_hash_value) > 0){
            return dataValue.compareTo(prev3_server_hash) > 0 || dataValue.compareTo(self_hash_value) <= 0;
        }
        else{
            logger.error("Prev server has the same hash value as the current server!");
            return false;
        }

    }

    /**
     * Check if the data is in range of (leftBound, rightBound]. Note that left bound might > right bound
     * @param dataKey
     * @param leftBound
     * @param rightBound
     * @return
     */
    public static boolean inRange(String dataKey, String leftBound, String rightBound){
        String dataValue = getHashValue(dataKey);
        if (leftBound.compareTo(rightBound) < 0){
            return dataValue.compareTo(leftBound) > 0 && dataValue.compareTo(rightBound) <= 0;
        }
        else if (leftBound.compareTo(rightBound) > 0){
            return dataValue.compareTo(leftBound) > 0 || dataValue.compareTo(rightBound) <= 0;
        } else{
            return true;
        }
    }

    // Return the responsible server name according to given key
    public String getCorrectServer(String key){
        for (String server_name:serverNamesRing){
            if (inRange(key, server_name)) {
                return server_name;
            }
        }
        return "";
    }

    // Get server ip and port from server name
    public String getServerIpPort(String server_name){
        return server_ip_port.get(server_name);
    }

    public String getServerNameFromIpPort(String IpPort){
        return ip_port_server.get(IpPort);
    }

    public String getKeyFromECSNode(IECSNode node){
        return node.getNodeHost() + ":" + node.getNodePort();
    }

    // get predecessor name
    public String getPredecessor(String serverName){
        int selfServerIndex = server_index.get(serverName);
        int prevIndex = (selfServerIndex == 0) ? (server_hash.size()-1) : (selfServerIndex - 1);
        return serverNamesRing.get(prevIndex);
    }

    // get Successor name
    public String getSuccessor(String serverName){
        int selfServerIndex = server_index.get(serverName);
        int successorIndex = (selfServerIndex == server_hash.size()-1) ? (0) : (selfServerIndex + 1);
        return serverNamesRing.get(successorIndex);
    }

    //  get hash range
    public String getServerHash(String serverName1, String serverName2){
        String leftbound = server_hash.get(serverName1);
        String rightbound = server_hash.get(serverName2);
        return leftbound + ":" + rightbound;
    }

    /**
     * get Server's Range in the hash ring 
     * @param serverName Current server name
     * @return the server's hash range in the format of HASH_PRE:HASH_CURR
     */
    public String getServerHashRange(String serverName){
        String predesesorName = getPredecessor(serverName);
        String hashPre = server_hash.get(predesesorName);
        String hashCurr = server_hash.get(serverName);
        return hashPre + ":" + hashCurr;
    }

    // Return all server/ip/port info list
    public HashMap<String, String> getAllServerInfo(){
        return server_ip_port;
    }

    // size of the hash ring
    public int getSize(){
        return server_hash.size();
    }
}

