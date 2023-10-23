package storage;

import app_kvServer.IKVServer;
import shared.metadata.HashRing;
import shared.metadata.Serialization;
import logger.LogSetup;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.json.simple.parser.JSONParser;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.*; 
import java.nio.file.Paths;
import java.util.Iterator;

// TO DO: Refactor JSON Read
// TO DO: Consider cache..?

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
     * Update or add a key-value pair in the storage
     * @param key key in key-value pair
     * @param value value in key-value pair
     * @return return 1 if success, 0 if error
     */
    public synchronized int put(String key, String value) {
        logger.debug("Put: key = " + key + ", value = " + value);

        JSONObject value_pairs = null;
        try {
            //Read json file
            JSONParser jsonParser = new JSONParser();
            FileReader reader = new FileReader(getStorageFileWritingPath(getDataFileName()));
            value_pairs = (JSONObject) jsonParser.parse(reader);
            // System.out.println("Key value pairs = " + value_pairs);

        } catch (FileNotFoundException e) {
            logger.info("File does not exist, creating json file.");
            value_pairs = new JSONObject();
        } catch (IOException e) {
            e.printStackTrace();
            logger.error("Unable to read json file.");
            return 0;
        } catch (Exception e) {
            logger.info("End of file.");
            value_pairs = new JSONObject();
        }

        // Update
        value_pairs.put(key, value);

        // Write updated JSON file
        try (FileWriter file = new FileWriter(getStorageFileWritingPath(getDataFileName()))) {
            file.write(value_pairs.toJSONString());
            file.flush();
        } catch (IOException e) {
            e.printStackTrace();
            logger.error("Unable to write json file.", e);
            return 0;
        }

        // Update cache
        if (hasCache()) {
            cache.putInCache(key, value);
        }

        logger.debug("Return from Put: key = " + key + ", value = " + value);
        return 1;
    }
    
    /**
     * Delete a key-value pair in the storage
     * @param key key in key-value pair
     * @return return 1 if success, 0 if error
     */
    public synchronized int delete(String key){
        logger.debug("Delete: key = " + key);
            
        JSONObject value_pairs = null;
        try {   
            //Read json file
            JSONParser jsonParser = new JSONParser();
            FileReader reader = new FileReader(getStorageFileWritingPath(getDataFileName()));
            value_pairs = (JSONObject)jsonParser.parse(reader);
            // System.out.println("Key value pairs = " + value_pairs);
 
        } catch (FileNotFoundException e) {
            logger.debug("File does not exist, don't do anything.");
            return 0;
        } catch (IOException e) {
            e.printStackTrace();
            logger.error("Unable to read json file.");
            return 0;
        } catch (Exception e) {
            logger.debug("End of file, don't do anything.");
            return 0;
        }

        // Delete
        String temp = (String) value_pairs.remove(key);
        if(temp == null){
            logger.info("Key does not exist.");
            return 0;
        }

        // Write updated JSON file
        try (FileWriter file = new FileWriter(getStorageFileWritingPath(getDataFileName()))) {
            file.write(value_pairs.toJSONString());
            file.flush();
        } catch (IOException e) {
            e.printStackTrace();
            logger.error("Unable to write json file.");
            return 0;
        }
        
        // Update cache
        if (hasCache()){
            cache.deleteInCache(key);
        }
        return 1;
    }

    /**
     * Get a value from a key in the storage
     * @param key key in key-value pair
     * @return return value of associated key if exists
     *         return null otherwise
     */
    public synchronized String get(String key){
        logger.debug("Get: key = " + key);

        // Check cache
        if (hasCache()){
            String cache_result = cache.getFromCache(key);
            if (cache_result!=null){
                return cache_result;
            }
        }

        JSONObject value_pairs;
        String value;

        JSONParser jsonParser = new JSONParser();
        try (FileReader reader = new FileReader(getStorageFileWritingPath(getDataFileName()))) {
            //Read json file
            value_pairs = (JSONObject)jsonParser.parse(reader);
            
            // Check if key exist
            if(value_pairs.containsKey(key)){
                value = (String) value_pairs.get(key);
                return value;
            }
            else{
                logger.debug("Key: " + key + " does not exist!");
            }
            
        } catch (FileNotFoundException e) {
            // Treat file does not exist as key not found
            logger.debug("File does not exist.");
        } catch (IOException e) {
            e.printStackTrace();
        } catch (Exception e) {
            logger.debug("End of file.");
        }
        return null;
    }

    /* Following are the helper functions */

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
     * Preparation before sender moving data operation
     * @param leftBound leftbound of the hash values to be moved (exclusive)
     * @param rightBound rightbound (inclusive)
     * @return String that contains the JSON-form data needed to be moved
     */
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
    
    /**
     * Action after sender has successfully moved the data
     */
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
			new LogSetup(log_directory, Level.ALL);
            // new LogSetup("logs/testing/storage.log", Level.ALL);
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
}

