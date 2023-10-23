package storage;

import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.log4j.Logger;

public class LRUCache implements ICache {
    private static Logger logger = Logger.getLogger("LRUCache");
    private LinkedHashMap<String, String> map; 
    private final int cacheSize; // Number of key value pairs allowed in the cache

    public LRUCache(int _cacheSize){
        logger.info("Initializing LRU cache...");
        cacheSize = _cacheSize;
        int capacity = (int)Math.ceil(cacheSize / 0.75) + 1;
        map = new LinkedHashMap<String, String>(capacity, 0.75f, true){
            @Override
            protected boolean removeEldestEntry(Map.Entry<String, String> eldest){
                return size() > cacheSize;
            }
        };
    }

    @Override
    public synchronized String getFromCache(String key){
        logger.debug("Get: key = " + key);
        return map.get(key);
    }

    @Override
    public synchronized void putInCache(String key, String value){
        logger.debug("Put: key = " + key + ", value = " + value);
        map.put(key, value);
    }

    @Override
    public void deleteInCache(String key){
        map.remove(key);
        logger.debug("Delete: key = " + key);
    }

    @Override
    public synchronized boolean inCache(String key){
        return map.containsKey(key);
    }

    @Override
    public synchronized void clearCache(){
        map.clear();
    }

    @Override
    public int getCacheSize(){
        return this.cacheSize;
    }
}
