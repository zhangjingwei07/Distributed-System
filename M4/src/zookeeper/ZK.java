package zookeeper;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.concurrent.CountDownLatch;
import java.util.ArrayList;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.graalvm.compiler.lir.StandardOp.NullCheck;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;


public class ZK {
    // declare zookeeper instance to access ZooKeeper ensemble
    public final String ZOOKEEPER_HOST = "127.0.0.1:21815";
	public final String ZOOKEEPER_ACTIVESERVERS_PATH = "/ActiveServers";
	public final String ZOOKEEPER_WAITINGSERVERS_PATH = "/WaitingServers";
	public final String ZOOKEEPER_METADATA_PATH = "/Metadata";
    public final String I_SEE_YOU = "I_SEE_YOU";
    private ZooKeeper zoo = null;
    final CountDownLatch connectedSignal = new CountDownLatch(1);
    private static Logger logger = Logger.getLogger("Zookeeper");
    private static Logger zklogger = Logger.getLogger("org.apache.zookeeper");

    // Method to connect zookeeper ensemble.
    public ZooKeeper connect(String host) {
        zklogger.setLevel(Level.WARN);
        int sessionTimeout = 5000;
        try{
            zoo = new ZooKeeper(host,sessionTimeout,new Watcher() {
                
                public void process(WatchedEvent we) {

                    if (we.getState() == Watcher.Event.KeeperState.SyncConnected) {
                    connectedSignal.countDown();
                    }
                }
            });
            connectedSignal.await();
        }catch (IOException e){
			logger.error("Cannot connect to zookeeper! " + e);
            return null;
        } catch (InterruptedException e){
			logger.error("InterruptedException on connection to zookeeper! " + e);
            return null;
        }
        return zoo;
    }

    public void create(String path, String data, boolean if_persistent) {
        byte[] data_byte = data.getBytes();
        CreateMode createMode = if_persistent ? CreateMode.PERSISTENT : CreateMode.EPHEMERAL;
        try {
            if (!exists(path, false)){
                zoo.create(
                    path, 
                    data_byte, 
                    ZooDefs.Ids.OPEN_ACL_UNSAFE, 
                    createMode);
            }
        } catch (KeeperException e){
            logger.error("Cannot create node <" + path +">, " + e);
        } catch (InterruptedException e){
            logger.error("InterruptedException when creating node! " + e);
        }
    }

    public boolean exists(String path, boolean watch){
        Stat r = null;
        try {
            r = zoo.exists(path, watch);
        } catch (KeeperException e){
            logger.error("Cannot check the existence of the node! " + e);
        } catch (InterruptedException e){
            logger.error("InterruptedException when checking the existence of node! " + e);
        }
        return r != null;
    }

    public boolean exists(String path, Watcher watcher){
        Stat r = null;
        try {
            r = zoo.exists(path, watcher);
        } catch (KeeperException e){
            logger.error("Cannot check the existence of the node! " + e);
        } catch (InterruptedException e){
            logger.error("InterruptedException when checking the existence of node! " + e);
        }
        return r != null;
    }


    public void setUpWatcher(String node_path, Watcher watcher){
        exists(node_path, watcher);
    }


    public void setData(String path, byte[] data_byte){
        try{
            zoo.setData(path, data_byte, -1);
        } catch (Exception e) {
            logger.error("Failed to set data at "+ path + ", " + e);
        }
    }

    public void setData(String path, String data){
        byte[] data_byte = data.getBytes();
        setData(path, data_byte);
    }

    public String getData(String path){
        byte[] b = getDataBtye(path);
        String data = null;
        try {
            data = new String(b, "UTF-8");
        } catch (UnsupportedEncodingException e){
            logger.error(e);
        }
        return data;
    }

    public byte[] getDataBtye(String path){
        byte[] b = null;
        try{
            b = zoo.getData(path, null, null);
        } catch (Exception e) {
            logger.error("Failed to get "+ path + ", " + e);
        }
        return b;
    }

    public ArrayList<String> getChildren(String path, boolean watch) {
        ArrayList<String> list = null;
        try{
            list = (ArrayList<String>) zoo.getChildren(path, watch);
        } catch (KeeperException e){
            logger.error(e);
        } catch (InterruptedException e){
            logger.error(e);
        }
        return list;
    }

    public ArrayList<String> getChildren(String path, Watcher watcher) {
        ArrayList<String> list = null;
        try{
            list = (ArrayList<String>) zoo.getChildren(path, watcher);
        } catch (KeeperException e){
            logger.error(e);
        } catch (InterruptedException e){
            logger.error(e);
        }
        return list;
    }

    public void delete(String path, int version){
        try{
            zoo.delete(path, version);
        } catch (KeeperException e){
            logger.error(e);
        } catch (InterruptedException e){
            logger.error(e);
        }
    }

    // Method to disconnect from zookeeper server
    public void close() {
        try{
            zoo.close();
        } catch (InterruptedException e){
            logger.error("Cannot close connection." + e);
        }
    }
     
}
