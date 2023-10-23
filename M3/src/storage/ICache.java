package storage;

public interface ICache {

    public String getFromCache(String key);

    public void putInCache(String key, String value);

    public void deleteInCache(String key);
    
    public boolean inCache(String key);

    public void clearCache();

    public int getCacheSize();
}
