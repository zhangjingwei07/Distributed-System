package storage;

import app_kvServer.IKVServer;
import shared.metadata.HashRing;
import shared.metadata.Serialization;
import shared.StorageValue;
import logger.LogSetup;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.json.simple.parser.JSONParser;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken; 

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.*; 
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;


public class KVStorage {
    private static Logger logger;
    public ICache cache;
    private final String DATA_INFIX = "_data";
    private String ROOT_DATA_DIRECTORY = null;
    private String dataFileName = null;

    public KVStorage(int port){
        logger = Logger.getLogger(port + "_storage");
        // Get current directroy of runnin jar file
        try{
            ROOT_DATA_DIRECTORY = new File(KVStorage.class.getProtectionDomain().getCodeSource().getLocation().toURI()).getParent() + "/data/";
        } catch (Exception e){
            logger.error("error in getting jar file path, "+ e);
        }
        cache = null;
        setdataFileName(Integer.toString(port) + DATA_INFIX);
    }

    public KVStorage(int cacheSize, IKVServer.CacheStrategy cacheStrategy, int port){
        logger = Logger.getLogger(port + "_storage");
        // Get current directroy of runnin jar file
        try{
            ROOT_DATA_DIRECTORY = new File(KVStorage.class.getProtectionDomain().getCodeSource().getLocation().toURI()).getParent() + "/data/";
        } catch (Exception e){
            logger.error("error in getting jar file path, "+ e);
        }
        setdataFileName(Integer.toString(port) + DATA_INFIX);

        if (cacheStrategy==IKVServer.CacheStrategy.None){
            cache = null;
        }
        else if (cacheStrategy==IKVServer.CacheStrategy.FIFO){
            cache = new FIFOCache(cacheSize);
        }
        else if (cacheStrategy==IKVServer.CacheStrategy.LFU){
            System.out.println("LFU not implemented, use NONE cache instead");
            cache = null;
        }
        else if (cacheStrategy==IKVServer.CacheStrategy.LRU){
            cache = new LRUCache(cacheSize);
        }
        else{
            System.out.println("Unrecognized Cache");
            logger.error("Unrecognized Cache");
            System.exit(1);
        }
    }

    /**
     * Update or add a value to a key
     * @param key
     * @param value 
     * @return return 1 if success, 0 if error
     */
    public synchronized int put(String key, String value) {
        logger.debug("PUT: key = " + key + ", value = " + value);

        Gson gson = new Gson();
        HashMap<String, StorageValue> key_dataValue_pairs = null;
        try {
            //Read json file
            FileReader reader = new FileReader(getStorageFileWritingPath(getDataFileName()));
            BufferedReader br = new BufferedReader(reader);
            key_dataValue_pairs = gson.fromJson(br, new TypeToken<HashMap<String, StorageValue>>() {
            }.getType());

        } catch (FileNotFoundException e) {
            logger.info("PUT: File does not exist, creating json file.");
            key_dataValue_pairs = new HashMap<String, StorageValue>();

        } catch (Exception e) {
            logger.info("PUT: End of file.");
            key_dataValue_pairs = new HashMap<String, StorageValue>();
        }

        if (key_dataValue_pairs.get(key) == null) {
            // The key does not exist. Need to create the new key.
            logger.debug("PUT: The key = " + key + " does not exist. Create it.");
            key_dataValue_pairs.put(key, new StorageValue(null, new HashSet<String>()));
        }

        // Update value only
        key_dataValue_pairs.get(key).setValue(value);
        logger.debug("PUT: the key = " + key + " is successfully set to be the value = " + value + ".");


         // Write updated JSON file
        try (FileWriter writer = new FileWriter(getStorageFileWritingPath(getDataFileName()))) {
            gson.toJson(key_dataValue_pairs, writer);
            writer.close();

        } catch (IOException e) {
            e.printStackTrace();
            logger.error("PUT: Unable to write json file.", e);
            return 0;
        }

        logger.debug("Return from PUT: key = " + key + ", value = " + value);
        return 1;
    }
    
    /**
     * Delete a key's value in the storage
     * @param key 
     * @return return 1 if success, 0 if error, 0 if key does not exist, 0 if value does not exist.
     */
    public synchronized int delete(String key) {
        logger.debug("Start DELETE: key = " + key);

        Gson gson = new Gson();
        HashMap<String, StorageValue> key_dataValue_pairs = null;
        try {
            //Read json file
            FileReader reader = new FileReader(getStorageFileWritingPath(getDataFileName()));
            BufferedReader br = new BufferedReader(reader);
            key_dataValue_pairs = gson.fromJson(br, new TypeToken<HashMap<String, StorageValue>>() {
            }.getType());

        } catch (FileNotFoundException e) {
            logger.debug("DELETE: File does not exist, don't do anything.");
            return 0;
        } catch (Exception e) {
            logger.debug("DELETE: End of file, don't do anything.");
            return 0;
        }

        if (key_dataValue_pairs.get(key) == null) {
            // The key does not exist. return 0
            logger.debug("DELETE: the key = " + key + " to be deleted does not exist. Return -1.");
            return 0;
        } else if (key_dataValue_pairs.get(key).getValue() == null) {
            // The key exist, but the value is null. This means the key has been subscribed but hasn't put and value yet.
            logger.debug("DELETE: the key = " + key + " does not have any value.");
            return 0;
        } else {
            // Delete the value associated to the key.
            String value = key_dataValue_pairs.get(key).getValue();
            key_dataValue_pairs.get(key).setValue(null);
            logger.debug("DELETE: the key = " + key + ", value = " + value + " has been successfully deleted.");
        }       
        
        // Write updated JSON file
        try (FileWriter writer = new FileWriter(getStorageFileWritingPath(getDataFileName()))) {
            gson.toJson(key_dataValue_pairs, writer);
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
            logger.error("DELETE: Unable to write json file.");
            return 0;
        }

        logger.debug("Return from DELETE: key = " + key + ".");
        return 1;
    }
    

    /**
     * Subscribe a key, regardless of its existence in the storage
     * @param key 
     * @param client 
     * @return return 1 if success, 0 if error, -1 if the client already subbed.
     */
    public synchronized int sub(String key, String client) {
        logger.debug("Start SUB: key = " + key + ", client = " + client);

        Gson gson = new Gson();
        HashMap<String, StorageValue> key_dataValue_pairs = null;

        try {
            //Read json file
            FileReader reader = new FileReader(getStorageFileWritingPath(getDataFileName()));
            BufferedReader br = new BufferedReader(reader);
            key_dataValue_pairs = gson.fromJson(br, new TypeToken<HashMap<String, StorageValue>>() {
            }.getType());

        } catch (FileNotFoundException e) {
            logger.info("SUB: File does not exist, creating json file.");
            key_dataValue_pairs = new HashMap<String, StorageValue>();

        } catch (Exception e) {
            logger.info("SUB: End of file.");
            key_dataValue_pairs = new HashMap<String, StorageValue>();
        }

        if (key_dataValue_pairs.get(key) == null) {
            // The key does not exist. Need to create the new key.
            logger.debug("SUB: the key = " + key + " does not exist. Create it.");
            key_dataValue_pairs.put(key, new StorageValue(null, new HashSet<String>()));
        }
        
        if (key_dataValue_pairs.get(key).getSubscribers() != null && 
            key_dataValue_pairs.get(key).getSubscribers().contains(client)) {
            logger.debug("SUB: the client = " + client + " has already subbed the key = " + key + ".");
            return -1;
        }
        // Update subscribers only
        key_dataValue_pairs.get(key).addSubscriber(client);
        logger.debug("SUB: client = " + client + " successfully subs the key = " + key + ".");

        // Write updated JSON file
        try (FileWriter writer = new FileWriter(getStorageFileWritingPath(getDataFileName()))) {
            gson.toJson(key_dataValue_pairs, writer);
            writer.close();

        } catch (IOException e) {
            e.printStackTrace();
            logger.error("SUB: Unable to write json file.", e);
            return 0;
        }

        logger.debug("Return from SUB: key = " + key + ", subscribers = "
                + key_dataValue_pairs.get(key).getSubscribers());
        return 1;
    }
    
    /**
     * Unsubscribe a key, regardless of its existence in the storage
     * @param key 
     * @param client 
     * @return return 1 if success, 0 if error, -1 if client haven't subbed the key.
     */
    public synchronized int unsub(String key, String client) {
        logger.debug("Start UNSUB: key = " + key + ", client = " + client);

        Gson gson = new Gson();
        HashMap<String, StorageValue> key_dataValue_pairs = null;

        try {
            //Read json file
            FileReader reader = new FileReader(getStorageFileWritingPath(getDataFileName()));
            BufferedReader br = new BufferedReader(reader);
            key_dataValue_pairs = gson.fromJson(br, new TypeToken<HashMap<String, StorageValue>>() {}.getType());

        } catch (FileNotFoundException e) {
            logger.info("UNSUB: File does not exist, creating json file.");
            key_dataValue_pairs = new HashMap<String, StorageValue>();

        } catch (Exception e) {
            logger.info("UNSUB: End of file.");
            key_dataValue_pairs = new HashMap<String, StorageValue>();
        }

        if (key_dataValue_pairs.get(key) == null || 
            key_dataValue_pairs.get(key).getSubscribers() == null|| 
            !key_dataValue_pairs.get(key).getSubscribers().contains(client)) {
            logger.debug("UNSUB: The client = " + client + " did not sub the key = " + key + ". Do Nothing.");
            return -1;
        } else {
            key_dataValue_pairs.get(key).removeSubscriber(client);
            logger.debug("UNSUB: The client = " + client + " successfully unsubs the key = " + key + ".");
        }

        // Write updated JSON file
        try (FileWriter writer = new FileWriter(getStorageFileWritingPath(getDataFileName()))) {
            gson.toJson(key_dataValue_pairs, writer);
            writer.close();

        } catch (IOException e) {
            e.printStackTrace();
            logger.error("UNSUB: Unable to write json file.", e);
            return 0;
        }

        logger.debug("Return from UNSUB: key = " + key + ", subscribers = " + key_dataValue_pairs.get(key).getSubscribers());
        return 1;
    }


    /**
     * Get a value from a key in the storage
     * @param key key in key-value pair
     * @return return value of associated key if exists
     *         return null otherwise
     */
    public synchronized String get(String key) {
        logger.debug("GET: key = " + key);

        Gson gson = new Gson();
        HashMap<String, StorageValue> key_dataValue_pairs = null;

        JSONParser jsonParser = new JSONParser();
        try (FileReader reader = new FileReader(getStorageFileWritingPath(getDataFileName()))) {
            // Read json file
            BufferedReader br = new BufferedReader(reader);
            key_dataValue_pairs = gson.fromJson(br, new TypeToken<HashMap<String, StorageValue>>() {
            }.getType());

            // Check if the key's value exists        
            if (!key_dataValue_pairs.containsKey(key)) {
                logger.debug("GET: The key = " + key + " does not exist.");
            } else if (key_dataValue_pairs.get(key).getValue() == null) {
                logger.debug("GET: The key = " + key + " does not have associated value.");
            } else {
                String value = key_dataValue_pairs.get(key).getValue();
                logger.debug("GET: Successfully get the key = " + key + ", value = " + value + ".");
                return value;
            }

        } catch (FileNotFoundException e) {
            // Treat file does not exist as key not found
            logger.debug("GET: File does not exist.");
        } catch (IOException e) {
            e.printStackTrace();
        } catch (Exception e) {
            logger.debug("GET: End of file.");
        }
        return null;
    }  

    // Check whether the client has subscribed the given key.
    // return 1 if the client has subbed, 0 if error, -1 if client hasn't subbed.
    public synchronized int didSub(String key, String client){
        logger.debug("DIDSUB: key = " + key + ". client = " + client + ".");

        Gson gson = new Gson();
        HashMap<String, StorageValue> key_dataValue_pairs = null;

        try (FileReader reader = new FileReader(getStorageFileWritingPath(getDataFileName()))) {
            // Read json file
            BufferedReader br = new BufferedReader(reader);
            key_dataValue_pairs = gson.fromJson(br, new TypeToken<HashMap<String, StorageValue>>() {}.getType());
            
            // Check if the key's value exists        
            if (!key_dataValue_pairs.containsKey(key) || key_dataValue_pairs.get(key).getSubscribers() == null
            || !key_dataValue_pairs.get(key).getSubscribers().contains(client)) {
                logger.debug("DIDSUB: The client = " + client + " did not subscribe the key = " + key + ".");
                return -1;
            } else {
                logger.debug("DIDSUB: The client = " + client + " did subscribe the key = " + key + " before.");
                return 1;
            }
            
        } catch (FileNotFoundException e) {
            // Treat file does not exist as key not found
            logger.debug("DIDSUB: File does not exist.");
        } catch (IOException e) {
            e.printStackTrace();
        } catch (Exception e) {
            logger.debug("DIDSUB: End of file.");
        }
        return 0;
    } 

    /**
     * Get the subscribers given the key
     */
    public HashSet<String> getSubscribersWithKey(String key) {

        Gson gson = new Gson();
        HashMap<String, StorageValue> key_dataValue_pairs = null;

        try {
            //Read json file
            FileReader reader = new FileReader(getStorageFileWritingPath(getDataFileName()));
            BufferedReader br = new BufferedReader(reader);
            key_dataValue_pairs = gson.fromJson(br, new TypeToken<HashMap<String, StorageValue>>() {
            }.getType());

        } catch (FileNotFoundException e) {
            logger.info("getSubscribersWithKey: File does not exist, creating json file.");
            key_dataValue_pairs = new HashMap<String, StorageValue>();

        } catch (Exception e) {
            logger.info("getSubscribersWithKey: End of file.");
            key_dataValue_pairs = new HashMap<String, StorageValue>();
        }

        if (key_dataValue_pairs.get(key) == null) {
            // The key does not exist. This should not happen, as PUT command should have been finished before.
            logger.error("getSubscribersWithKey: the key = " + key + " should not happen, as PUT command should have been done before this function");
        }
        
        // subscribers can be null
        return key_dataValue_pairs.get(key).getSubscribers();
    }

    /**
     * Clear the storage of KV database
     */
    public synchronized void clearStorage(){
        logger.info("Clearing KV Storage...");

        if (hasCache()){
            cache.clearCache();
        }
        try{
            new FileWriter(getStorageFileWritingPath(getDataFileName())).close();
            try {
                boolean fileDeleteSuccess = Files.deleteIfExists(Paths.get(getStorageFileWritingPath(getDataFileName())));
                if (fileDeleteSuccess) {
				    logger.debug("Successfully delete the original storage file.");
                } else {
                    logger.debug("The original storage file does not exist. No need to delete.");
                }
			} catch (IOException e) {
                e.printStackTrace(); 
				logger.error("Unable to delete the original storage file!");
			}
        }
        catch( IOException e) {
            e.printStackTrace();
        }
    }

    private boolean hasCache() {
        return cache != null;
    }

    public String getStorageFileWritingPath(String dataFileName) {
        return ROOT_DATA_DIRECTORY + dataFileName + ".json";
    }

    public String getRemainDataFileName() {
        return this.dataFileName + "_remain";
    }

    public String getDataFileName() {
        return this.dataFileName;
    }

    public void setdataFileName(String dataFileName) {
        this.dataFileName = dataFileName;
    }

    /* Following are the functions for moving data */

    /**
     * Read the data to be sent.
     */
    public synchronized String preSendData(String leftBound, String rightBound) {
        JSONObject ori_value_pairs = null;
        try {   
            //Read json file
            JSONParser jsonParser = new JSONParser();
            FileReader reader = new FileReader(getStorageFileWritingPath(getDataFileName()));
            ori_value_pairs = (JSONObject)jsonParser.parse(reader);
            // System.out.println("Key value pairs = " + value_pairs);
        } catch (FileNotFoundException e) {
            logger.debug("File does not exist, meaning no data store in this server. No need for sending data.");
            return null;
        } catch (IOException e) {
            e.printStackTrace();
            logger.error("Unable to read json file.");
            return null;
        } catch (Exception e) {
            logger.debug("End of file, don't do anything.");
            return null;
        }
        Iterator iterator = ori_value_pairs.keySet().iterator();
        // json objects stores the KVs to be sent
        JSONObject send_value_pairs = new JSONObject();

        while(iterator.hasNext()) {
            String key = (String) iterator.next();
            if (HashRing.inRange(key, leftBound, rightBound)) {
                send_value_pairs.put(key, ori_value_pairs.get(key));
            }
        }
        // Turn the send_JSON_file into String form
        return send_value_pairs.toString();
    }

    /**
     * Delete the data in the given range in the current server.
     */
	public boolean deleteData(String leftBound, String rightBound) {
        JSONObject ori_value_pairs = null;
        try {   
            //Read json file
            JSONParser jsonParser = new JSONParser();
            FileReader reader = new FileReader(getStorageFileWritingPath(getDataFileName()));
            ori_value_pairs = (JSONObject)jsonParser.parse(reader);
            // System.out.println("Key value pairs = " + value_pairs);
        } catch (FileNotFoundException e) {
            logger.debug("File does not exist. No data to be deleted");
            return true;
        } catch (IOException e) {
            e.printStackTrace();
            logger.error("Unable to read json file.");
            return false;
        } catch (Exception e) {
            logger.debug("End of file, don't do anything.");
            return true;
        }
        Iterator iterator = ori_value_pairs.keySet().iterator();
        // json objects stores the KVs to be remained
		JSONObject remain_value_pairs = new JSONObject();	
		while(iterator.hasNext()) {
            String key = (String) iterator.next();
            if (!HashRing.inRange(key, leftBound, rightBound)) {
                remain_value_pairs.put(key, ori_value_pairs.get(key));
            }
		}

		if (remain_value_pairs.isEmpty()) {
            logger.debug("No data should be kept. Delete the original storage file.");
            
			try {
                boolean fileDeleteSuccess = Files.deleteIfExists(Paths.get(getStorageFileWritingPath(getDataFileName())));
                if (fileDeleteSuccess) {
				    logger.debug("Successfully delete the original storage file.");
                } else {
                    logger.debug("The original storage file does not exist. No need to delete.");
                }
			} catch (IOException e) {
                e.printStackTrace(); 
				logger.error("Unable to delete the original storage file!");
				return false;
			}
        } else {
            // Write remain data back to original storage file
            try (FileWriter storageFileWriter = new FileWriter(getStorageFileWritingPath(getDataFileName()))) {
                storageFileWriter.write(remain_value_pairs.toJSONString());
                storageFileWriter.flush();
                logger.debug("Successfully write the remain data into remain file.");
            } catch (IOException e) {
                e.printStackTrace();
                logger.error("Unable to write to the remain storage file!");
                return false;
            }
		}
		return true;
	} 
    
    
    /**
     * Function to merge the newly received data with the current storage file
     * @param JSONData
     * @return
     */
    public boolean mergeStorage(JSONObject JSONData) {
        JSONObject ori_value_pairs = null;
        try {   
            //Read json file
            JSONParser jsonParser = new JSONParser();
            FileReader reader = new FileReader(getStorageFileWritingPath(getDataFileName()));
            ori_value_pairs = (JSONObject)jsonParser.parse(reader);
            // System.out.println("Key value pairs = " + value_pairs);
        } catch (FileNotFoundException e) {
            logger.debug("Merge Opearation: Original storage File does not exist, don't do anything.");
            ori_value_pairs = new JSONObject();
        } catch (IOException e) {
            e.printStackTrace();
            logger.error("Merge Opearation: Unable to read the original storage file.");
            return false;
        } catch (Exception e) {
            ori_value_pairs = new JSONObject();
            logger.debug("Merge Opearation: End of original storage file, don't do anything.");
        }

        // Merge the original and received JSON Object
        if (ori_value_pairs.isEmpty()){
            logger.error("Original data is empty");
            ori_value_pairs = JSONData;
        } else{
            logger.error("Original data is not empty");
            ori_value_pairs.putAll(JSONData);
        } 

        // Write updated JSON file
        try (FileWriter file = new FileWriter(getStorageFileWritingPath(getDataFileName()))) {
            file.write(ori_value_pairs.toJSONString());
            file.flush();
        } catch (IOException e) {
            e.printStackTrace();
            logger.error("Merge Opearation: Unable to write to original storage json file.");
            return false;
        }

        return true;
    }

    public static void main(String[] args) {
        try{
            String jarDirectroy = new File(KVStorage.class.getProtectionDomain().getCodeSource().getLocation().toURI()).getParent();
			String log_directory = jarDirectroy + "/logs/KVStorage.log";
			new LogSetup(log_directory, Level.DEBUG);
        }
        catch (IOException e) {
            System.out.println("Error! Unable to initialize logger!");
			e.printStackTrace();
			System.exit(1);
        } catch (Exception e){
			System.out.println("Error: " + e);
			System.exit(1);
		}
    }

    /********************following are deprecated functions ***********************/
    /**
     * Action after sender has successfully moved the data
     */
    @Deprecated
    public synchronized void postMoveData() {
        File oriStorageFile = new File(getStorageFileWritingPath(getDataFileName()));
        if (!oriStorageFile.exists()) {
            logger.debug("The original storage file does not exist.");
            return;
        } 

        // Delete the original file
        if (oriStorageFile.delete()) {
            logger.debug("The original storage file has been deleted.");
        } else {
            logger.error("Unable to delete the original storage file!");
            return;
        }

        // Rename the remain storage file to the original storage file
        File remainStorageFile = new File(getStorageFileWritingPath(getRemainDataFileName()));
        if (!remainStorageFile.exists()) {
            logger.debug("The remain storage file does not exist, meaning no remain data.");
            return;
        }

        if (remainStorageFile.renameTo(new File(getStorageFileWritingPath(getDataFileName())))) {
            logger.debug("Successfully rename the remain storage file.");
        } else {
            logger.error("Unable to rename the remain storage file!");
        }

        return;
    }  

    /**
     * Preparation before sender moving data operation
     * @param leftBound leftbound of the hash values to be moved (exclusive)
     * @param rightBound rightbound (inclusive)
     * @return String that contains the JSON-form data needed to be moved
     */
    @Deprecated
    public synchronized String prevMoveData(String leftBound, String rightBound) {
        JSONObject ori_value_pairs = null;
        try {   
            //Read json file
            JSONParser jsonParser = new JSONParser();
            FileReader reader = new FileReader(getStorageFileWritingPath(getDataFileName()));
            ori_value_pairs = (JSONObject)jsonParser.parse(reader);
            // System.out.println("Key value pairs = " + value_pairs);
        } catch (FileNotFoundException e) {
            logger.debug("File does not exist, meaning no data store in this server. No need for moving data.");
            return null;
        } catch (IOException e) {
            e.printStackTrace();
            logger.error("Unable to read json file.");
            return null;
        } catch (Exception e) {
            logger.debug("End of file, don't do anything.");
            return null;
        }
        Iterator iterator = ori_value_pairs.keySet().iterator();
        // json objects stores the KVs to be sent
        JSONObject send_value_pairs = new JSONObject();
        // Json object to store the KVs to be remained
        JSONObject remain_value_pairs = new JSONObject();

        while(iterator.hasNext()) {
            String key = (String) iterator.next();
            if (HashRing.inRange(key, leftBound, rightBound)) {
                send_value_pairs.put(key, ori_value_pairs.get(key));
            } else {
                remain_value_pairs.put(key, ori_value_pairs.get(key));
            }
        }

        if (remain_value_pairs.isEmpty()) {
            logger.debug("No data should be kept. The remain file should not be created.");
        } else {
            // Write to newly created remain_JSON_file
            try (FileWriter remainStorageFileWriter = new FileWriter(getStorageFileWritingPath(getRemainDataFileName()))) {
                remainStorageFileWriter.write(remain_value_pairs.toJSONString());
                remainStorageFileWriter.flush();
                logger.debug("successfully write the remain data into remain file.");
            } catch (IOException e) {
                e.printStackTrace();
                logger.error("Unable to write to the remain storage file");
                return null;
            }
        }

        // Turn the send_JSON_file into String form
        return send_value_pairs.toString();
    }
}

