package testing;

import app_kvServer.KVServer;
import client.KVStore;
import client.KVSubscription;
import client.TextMessage;
import ecs.ECSNode;
import ecs.IECSNode;
import junit.framework.TestCase;
import org.junit.Test;
import shared.messages.*;
import shared.metadata.*;
import storage.KVStorage;
import zookeeper.ZK;

import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class M4Test extends TestCase {
    Lock sequential = new ReentrantLock();

    protected void setUp() throws Exception {
        // pause(500);
        super.setUp();
        sequential.lock();
        System.out.println("**********" + getName() + "**********");
    }

    protected void tearDown() throws Exception {
        sequential.unlock();
        super.tearDown();
    }

    public void testNewPutAndGetData() {
    	KVStorage storage = new KVStorage(0);
    	storage.put("key1", "value1");
    	storage.put("key2", "value2");
    	assertEquals("value1", storage.get("key1"));

    	storage.put("key1", "value11");
    	assertEquals("value11", storage.get("key1"));

    	storage.put("key2", null);
    	assertEquals(null, storage.get("key2"));

    	storage.clearStorage();
    }

    public void testSubConnectionTable() {
    	SubConnectionTable subConnTable = new SubConnectionTable();
    	// Add some connections
    	assertTrue(subConnTable.addConnection("client1", "server1"));
    	assertTrue(subConnTable.addConnection("client2", "server1"));
    	assertTrue(subConnTable.addConnection("client3", "server2"));
    	// Check existance
    	assertTrue(subConnTable.connectionExists("client1"));
    	assertTrue(subConnTable.connectionExists("client2"));
    	assertTrue(subConnTable.connectionExists("client3"));
    	assertFalse(subConnTable.connectionExists("client4"));
    	// Get connection name
    	assertEquals("server1", subConnTable.getConnectedServerName("client1"));
    	assertEquals("server1", subConnTable.getConnectedServerName("client2"));
    	assertEquals("server2", subConnTable.getConnectedServerName("client3"));
    	assertEquals(null, subConnTable.getConnectedServerName("client4"));
    	// Remove Connections
    	assertTrue(subConnTable.removeConnection("client1"));
    	assertTrue(subConnTable.removeConnection("client2"));
    	assertTrue(subConnTable.removeConnection("client3"));
    	assertFalse(subConnTable.removeConnection("client4"));
    	assertFalse(subConnTable.connectionExists("client1"));
    	assertFalse(subConnTable.connectionExists("client2"));
    	assertFalse(subConnTable.connectionExists("client3"));
    	assertFalse(subConnTable.connectionExists("client4"));
    }

    public void testSubData() {
        KVStorage storage = new KVStorage(0);

        assertEquals(1, storage.sub("key1", "client1"));
        assertEquals(1, storage.sub("key1", "client11"));
        assertEquals(1, storage.sub("key1", "client111"));
        assertEquals(1, storage.sub("key2", "client2"));
        // Test redundant sub
        assertEquals(-1, storage.sub("key1", "client1"));
        assertEquals(-1, storage.sub("key2", "client2"));

        storage.clearStorage();
    }

    public void testUnsubData() {
        KVStorage storage = new KVStorage(0);
        storage.sub("key1", "client1");
        storage.sub("key1", "client11");
        storage.sub("key1", "client111");

        assertEquals(1, storage.unsub("key1", "client1"));
        // Test redundant unsub
        assertEquals(-1, storage.unsub("key1", "client1"));
        // Test unsub the key hasn't subbed
        assertEquals(-1, storage.unsub("key2", "client1"));

        storage.clearStorage();
    }

    public void testDidSubData() {
        KVStorage storage = new KVStorage(0);
        storage.sub("key1", "client1");
        storage.sub("key1", "client11");
        storage.sub("key1", "client111");

        assertEquals(1, storage.didSub("key1", "client1"));
        assertEquals(-1, storage.didSub("key1", "client2"));
        assertEquals(-1, storage.didSub("key2", "client2"));

        storage.clearStorage();
    }

    public void testBroadcastSubConnectionTable() {
        KVServer server1 = new KVServer(41001, 100, "None");
        KVServer server2 = new KVServer(41002, 100, "None");
        KVServer server3 = new KVServer(41003, 100, "None");
        KVServer server4 = new KVServer(41004, 100, "None");

        try {
            Thread.currentThread().sleep(2000); // Wait for server to be fully established
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        // Without initializing ECS, remember set up server name manually.
        server1.setServerName("server1");
        server2.setServerName("server2");
        server3.setServerName("server3");
        server4.setServerName("server4");

        HashRing hashRing = new HashRing();
        ArrayList<IECSNode> nodes = new ArrayList<IECSNode>();
        nodes.add(new ECSNode("server1", "127.0.0.1", 41001));
        nodes.add(new ECSNode("server2", "127.0.0.1", 41002));
        nodes.add(new ECSNode("server3", "127.0.0.1", 41003));
        nodes.add(new ECSNode("server4", "127.0.0.1", 41004));

        hashRing.updateMatadata(nodes);
        server1.setMetadata(hashRing);
        server2.setMetadata(hashRing);
        server3.setMetadata(hashRing);
        server4.setMetadata(hashRing);

        // set up subConnectionTable
        SubConnectionTable subConnectionTable = new SubConnectionTable();
        subConnectionTable.addConnection("client1", "server1");
        server1.replaceSubConnectionTable(subConnectionTable);

        Exception e = null;
        boolean r = false;

        try {
            r = server1.broadcastSubConnectionTableToAllOtherServers();
            assertEquals(true, r);
            assertEquals(true, server2.getSubConnectionTable().connectionExists("client1"));
            assertEquals(true, server3.getSubConnectionTable().connectionExists("client1"));
            assertEquals(true, server4.getSubConnectionTable().connectionExists("client1"));
        } catch (Exception ee) {
            e = ee;
        }
        assertEquals(null, e);
        server1.clearStorage();
        server2.clearStorage();
        server3.clearStorage();
        server4.clearStorage();
        server1.close();
        server2.close();
        server3.close();
        server4.close();
    }

    public void testSubReplicatedData() {
        KVServer server1 = new KVServer(41001, 100, "None");
        KVServer server2 = new KVServer(41002, 100, "None");
        KVServer server3 = new KVServer(41003, 100, "None");
        KVServer server4 = new KVServer(41004, 100, "None");

        try {
            Thread.currentThread().sleep(2000); // Wait for server to be fully established
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        // Without initializing ECS, remember set up server name manually.
        server1.setServerName("server1");
        server2.setServerName("server2");
        server3.setServerName("server3");
        server4.setServerName("server4");

        HashRing hashRing = new HashRing();
        ArrayList<IECSNode> nodes = new ArrayList<IECSNode>();
        nodes.add(new ECSNode("server1", "127.0.0.1", 41001));
        nodes.add(new ECSNode("server2", "127.0.0.1", 41002));
        nodes.add(new ECSNode("server3", "127.0.0.1", 41003));
        nodes.add(new ECSNode("server4", "127.0.0.1", 41004));

        hashRing.updateMatadata(nodes);
        server1.setMetadata(hashRing);
        server2.setMetadata(hashRing);
        server3.setMetadata(hashRing);
        server4.setMetadata(hashRing);

        String key1 = "will";
    	boolean r = false;
    	Exception e = null;
    	// will is in the range of server1
    	assertEquals(true, hashRing.inRange(key1, "server1"));
    	assertEquals(false, hashRing.inRange(key1, "server2"));
    	assertEquals(false, hashRing.inRange(key1, "server3"));
    	assertEquals(false, hashRing.inRange(key1, "server4"));

        try {
            r = server1.subOrUnsubReplicatedData(key1, "client1", true);
            assertEquals(true, r);
            assertEquals(1, server2.didSub(key1, "client1"));
            assertEquals(0, server3.didSub(key1, "client1"));
            assertEquals(1, server4.didSub(key1, "client1"));
        } catch (Exception ee) {
            e = ee;
        }
        assertEquals(null, e);
        server1.clearStorage();
        server2.clearStorage();
        server3.clearStorage();
        server4.clearStorage();
        server1.close();
        server2.close();
        server3.close();
        server4.close();
    }

    public void testUnSubReplicatedData() {
        KVServer server1 = new KVServer(41001, 100, "None");
        KVServer server2 = new KVServer(41002, 100, "None");
        KVServer server3 = new KVServer(41003, 100, "None");
        KVServer server4 = new KVServer(41004, 100, "None");

        try {
            Thread.currentThread().sleep(2000); // Wait for server to be fully established
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        // Without initializing ECS, remember set up server name manually.
        server1.setServerName("server1");
        server2.setServerName("server2");
        server3.setServerName("server3");
        server4.setServerName("server4");

        HashRing hashRing = new HashRing();
        ArrayList<IECSNode> nodes = new ArrayList<IECSNode>();
        nodes.add(new ECSNode("server1", "127.0.0.1", 41001));
        nodes.add(new ECSNode("server2", "127.0.0.1", 41002));
        nodes.add(new ECSNode("server3", "127.0.0.1", 41003));
        nodes.add(new ECSNode("server4", "127.0.0.1", 41004));

        hashRing.updateMatadata(nodes);
        server1.setMetadata(hashRing);
        server2.setMetadata(hashRing);
        server3.setMetadata(hashRing);
        server4.setMetadata(hashRing);

        String key1 = "will";
        boolean r = false;
        Exception e = null;
        // will is in the range of server1
        assertEquals(true, hashRing.inRange(key1, "server1"));
        assertEquals(false, hashRing.inRange(key1, "server2"));
        assertEquals(false, hashRing.inRange(key1, "server3"));
        assertEquals(false, hashRing.inRange(key1, "server4"));

        try {
            server1.sub(key1, "client1");
            r = server1.subOrUnsubReplicatedData(key1, "client1", false);

            assertEquals(true, r);
            assertEquals(1, server1.didSub(key1, "client1"));
            assertEquals(-1, server2.didSub(key1, "client1"));
            assertEquals(0, server3.didSub(key1, "client1"));
            assertEquals(-1, server4.didSub(key1, "client1"));
        } catch (Exception ee) {
            e = ee;
        }
        assertEquals(null, e);
        server1.clearStorage();
        server2.clearStorage();
        server3.clearStorage();
        server4.clearStorage();
        server1.close();
        server2.close();
        server3.close();
        server4.close();
    }
    
    public void testSubscriptionThread() {
        KVServer server1 = new KVServer(41014, 100, "None");
        server1.setServerName("server1");

        HashRing hashRing = new HashRing();
        ArrayList<IECSNode> nodes = new ArrayList<IECSNode>();
        nodes.add(new ECSNode("server1", "localhost", 41014));

        hashRing.updateMatadata(nodes);
        server1.setMetadata(hashRing);

        KVSubscription kvClient = new KVSubscription("localhost", 41014, "client1");
		try {
			kvClient.connect();
        } catch (Exception e) {
            System.out.println(e);
        }

        pause(1000);

        assertEquals(true, server1.getSubConnectionTable().connectionExists("client1"));
        assertEquals(true, server1.getClientName_subConnection().containsKey("client1"));

 		try {
			kvClient.disconnect();
        } catch (Exception e) {
            System.out.println(e);
        } 

        pause(1000);

        assertEquals(false, server1.getSubConnectionTable().connectionExists("client1"));
        assertEquals(false, server1.getClientName_subConnection().containsKey("client1"));

        server1.clearStorage();
        server1.close();
    }

    public void testSubUpdateFlow() {
        /* Set up servers */
        KVServer server1 = new KVServer(41001, 100, "None");
        KVServer server2 = new KVServer(41002, 100, "None");
        KVServer server3 = new KVServer(41003, 100, "None");
        KVServer server4 = new KVServer(41004, 100, "None");

        pause(1000);

        // Without initializing ECS, remember set up server name manually.
        server1.setServerName("server1");
        server2.setServerName("server2");
        server3.setServerName("server3");
        server4.setServerName("server4");

        HashRing hashRing = new HashRing();
        ArrayList<IECSNode> nodes = new ArrayList<IECSNode>();
        nodes.add(new ECSNode("server1", "127.0.0.1", 41001));
        nodes.add(new ECSNode("server2", "127.0.0.1", 41002));
        nodes.add(new ECSNode("server3", "127.0.0.1", 41003));
        nodes.add(new ECSNode("server4", "127.0.0.1", 41004));

        hashRing.updateMatadata(nodes);
        server1.setMetadata(hashRing);
        server2.setMetadata(hashRing);
        server3.setMetadata(hashRing);
        server4.setMetadata(hashRing);

        // set up subConnectionTable
        SubConnectionTable subConnectionTable = new SubConnectionTable();
        subConnectionTable.addConnection("client1", "server1");
        server3.replaceSubConnectionTable(subConnectionTable);

        /* Set up client */
        KVSubscription kvClient = new KVSubscription("localhost", 41001, "client1");
		try {
			kvClient.connect();
        } catch (Exception e) {
            System.out.println(e);
        }

        pause(1000);

        // Server3 will send sub update to server1, and server 1 will send update info to client1
        HashSet<String> subscribers = new HashSet<String>();
        subscribers.add("client1");
        server3.subUpdateToAllResponsibleServers("key1", "oldValue", "newValue", subscribers);

        pause(1000);

        server1.clearStorage();
        server2.clearStorage();
        server3.clearStorage();
        server4.clearStorage();
        server1.close();
        server2.close();
        server3.close();
        server4.close();
        
        // Check the logger to confirm the subscription update message
    }

    /* Following are testings for encryption */
    public void testByzantineFailuresToServers(){
		KVServer server1 = null;
		KVStore kvClient = null;
		try {
			server1 = new KVServer(41041, 100, "None");
			kvClient = new KVStore("localhost", 41041);
			pause(100); // Wait for server to establish
			kvClient.connect();
		} catch (Exception e) {
			fail("Exception:" + e.getMessage());
		}

		// 1. Send message with invalid encrytion (Should not cause exception)
		try {
			kvClient.sendMessage(new TextMessage(AESEncryption.encrypt("Attack", "INVALIDKEY!")), "INVALIDKEY");
		} catch (Exception e){
			fail("Exception:" + e);
		}
		pause(1000); // Wait for server to receive the msg

		// 2. Send server valid message for encrytion but invalid format (Should not cause exception)
		try {
			// Valid for encrytion but invalid message
			kvClient.sendMessage(new TextMessage(AESEncryption.encrypt("Attack", "MY_DEFAULT_KEY")), "MY_DEFAULT_KEY!");
		} catch (Exception e){
			fail("Exception:" + e);
		}
		pause(1000); // Wait for server to receive the msg

		// Close connections
        try {
            kvClient.disconnect();
			server1.clearStorage();
			server1.close();
		} catch (Exception e){
			fail("Exception:" + e);
		}
	}

	public static void pause(int ms) {
		try {
			Thread.sleep(ms);
		} catch (InterruptedException e) {
			System.err.format("IOException: %s%n", e);
		}
	}

}
