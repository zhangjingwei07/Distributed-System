package storage;

import java.io.FileWriter;
import java.io.FileReader;
import java.io.File;
import java.io.IOException;
import java.io.FileNotFoundException;

import org.apache.log4j.Logger;
import org.apache.log4j.Level;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import logger.LogSetup;
import app_kvServer.IKVServer;

public class KVStorage {
    private static Logger logger = Logger.getLogger("storage");
    public String json_path = "key_value_pairs.json";
    public ICache cache;

    public KVStorage(){
        cache = null;
    }

    public KVStorage(int cacheSize, IKVServer.CacheStrategy cacheStrategy){
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
    public synchronized int put(String key, String value){
        logger.debug("Put: key = " + key + ", value = "+ value);
            
        JSONObject value_pairs = null;
        try 
        {   
            //Read json file
            JSONParser jsonParser = new JSONParser();
            FileReader reader = new FileReader(json_path);
            value_pairs = (JSONObject)jsonParser.parse(reader);
            // System.out.println("Key value pairs = " + value_pairs);
 
        } catch (FileNotFoundException e) {
            logger.info("File does not exit, creating json file");
            value_pairs = new JSONObject();
        } catch (IOException e) {
            e.printStackTrace();
            logger.error("Unable to read json file");
            return 0;
        } catch (Exception e) {
            logger.info("End of file");
            value_pairs = new JSONObject();
        }

        // Update
        value_pairs.put(key, value);

        // Write updated JSON file
        try (FileWriter file = new FileWriter(json_path)) {
            file.write(value_pairs.toJSONString());
            file.flush();
        } catch (IOException e) {
            e.printStackTrace();
            logger.error("Unable to write json file");
            return 0;
        }
        
        // Update cache
        if (hasCache()){
            cache.putInCache(key, value);
        }

        logger.debug("Return from Put: key = " + key + ", value = "+ value);
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
        try 
        {   
            //Read json file
            JSONParser jsonParser = new JSONParser();
            FileReader reader = new FileReader(json_path);
            value_pairs = (JSONObject)jsonParser.parse(reader);
            // System.out.println("Key value pairs = " + value_pairs);
 
        } catch (FileNotFoundException e) {
            logger.debug("File does not exit, don't do anything.");
            return 0;
        } catch (IOException e) {
            e.printStackTrace();
            logger.error("Unable to read json file");
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
        try (FileWriter file = new FileWriter(json_path)) {
            file.write(value_pairs.toJSONString());
            file.flush();
        } catch (IOException e) {
            e.printStackTrace();
            logger.error("Unable to write json file");
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
        try (FileReader reader = new FileReader(json_path))
        {
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
            logger.debug("File does not exit");
        } catch (IOException e) {
            e.printStackTrace();
        } catch (Exception e) {
            logger.debug("End of file");
        }
        return null;
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
            new FileWriter(json_path).close();
        }
        catch( IOException e) {
            e.printStackTrace();
        }
    }

    private boolean hasCache(){
        return cache!=null;
    }

    public static void main(String[] args) {
        try{
            new LogSetup("logs/testing/storage.log", Level.INFO);
        }
        catch (IOException e) {
            System.out.println("Error! Unable to initialize logger!");
			e.printStackTrace();
			System.exit(1);
        }
        
        // For purpose of testing the code
        KVStorage storage = new KVStorage(3, IKVServer.CacheStrategy.FIFO);
        storage.put("key1", "value1");
        storage.put("key2", "value2");
        storage.put("key3", "value3");
        storage.put("key1", "value_updated");

        storage.get("key1");
        storage.get("key2");
        storage.get("key3");
        storage.get("key4");

        storage.put("key4", "value4");
        System.out.println(storage.cache.inCache("key4"));
        System.out.println(storage.cache.inCache("key3"));
        System.out.println(storage.cache.inCache("key1"));
    }
}

