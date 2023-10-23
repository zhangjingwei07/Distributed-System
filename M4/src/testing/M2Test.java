package testing;

import app_kvServer.KVServer;
import client.KVStore;
import ecs.ECSNode;
import ecs.IECSNode;
import jdk.jfr.Timestamp;
import junit.framework.TestCase;
import org.junit.Test;
import org.apache.zookeeper.*;
import shared.messages.KVMessage;
import shared.metadata.HashRing;
import shared.metadata.Serialization;
import zookeeper.ZK;
import app_kvECS.*;

import java.util.ArrayList;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.junit.*;



public class M2Test extends TestCase {
	Lock sequential = new ReentrantLock();

	protected void setUp() throws Exception {
		// pause(1000);
		super.setUp();
		sequential.lock();
		System.out.println("**********" + getName() + "**********");
	}

	protected void tearDown() throws Exception {
		sequential.unlock();
		super.tearDown();
	}

	// Test hash ring functionality by using inRange functions
	@Test
	public void testHashRing() {
		HashRing hashRing = new HashRing();
		// Some sample Hash values
		// Tiger - 454c9843110686bf6f67ce5115b66617
		// I dont kown what to write - 87d389ca3d19ef2f559289f368542895
		// Cat - fa3ebd6742c360b2d9652b7f78d9bd7d
		// Banana - e6f9c347672daae5a2557ae118f44a1e
		// "127.0.0.1:1" - 9b5cdbb1c3e1a06da4bf0789b25fe38a
		// "127.0.0.1:2" - abf7a7b4e5522108bc46650d856ec400
		// "127.0.0.1:3" - 73c418f48cafa3c2b9f36efeb19875f8

		assertEquals(hashRing.getHashValue("Tiger"),                      "454c9843110686bf6f67ce5115b66617");
		assertEquals(hashRing.getHashValue("I dont kown what to write"),  "87d389ca3d19ef2f559289f368542895");
		assertEquals(hashRing.getHashValue("Cat"),                        "fa3ebd6742c360b2d9652b7f78d9bd7d");
		assertEquals(hashRing.getHashValue("Banana"),                     "e6f9c347672daae5a2557ae118f44a1e");
		assertEquals(hashRing.getHashValue("127.0.0.1:1"),                "9b5cdbb1c3e1a06da4bf0789b25fe38a");
		assertEquals(hashRing.getHashValue("127.0.0.1:2"),                "abf7a7b4e5522108bc46650d856ec400");
		assertEquals(hashRing.getHashValue("127.0.0.1:3"),                "73c418f48cafa3c2b9f36efeb19875f8");
	}


	@Test
	public void testHashRingRange() {
		HashRing hashRing = new HashRing();
		ArrayList<IECSNode> nodes = new ArrayList<IECSNode>();
		nodes.add(new ECSNode("Server1", "127.0.0.1", 1));
		hashRing.updateMatadata(nodes);

		assertTrue(hashRing.inRange("Tiger", "Server1"));

		nodes.add(new ECSNode("Server2", "127.0.0.1", 2));
		nodes.add(new ECSNode("Server3", "127.0.0.1", 3));
		hashRing.updateMatadata(nodes);

		assertTrue(hashRing.inRange("Tiger", "Server3"));
		assertTrue(hashRing.inRange("Banana", "Server3"));
		assertTrue(hashRing.inRange("Cat", "Server3"));
		assertTrue(hashRing.inRange("I dont kown what to write", "Server1"));
	}

	// // Serialization test basic
	@Test
	public void testSerialization() {
		// Create a HashRing object
		HashRing hashRing = new HashRing();
		ArrayList<IECSNode> nodes= new ArrayList<IECSNode>();
		nodes.add(new ECSNode("Server1", "127.0.0.1", 50001));
		hashRing.updateMatadata(nodes);

		// Serialize it 
		byte[] hashRingBytes = Serialization.serialize((Object) hashRing);

		// deserialize the byte array and check if it is the same
		HashRing hashRingRebuild = (HashRing) Serialization.deserialize(hashRingBytes);

		assertEquals(hashRingRebuild.getServerIpPort("Server1"), "127.0.0.1:50001");
	}

	// // Zookeeper connection test
	@Test
	public void testZookeeperConnection(){
		ZK zk = new ZK();
		ZooKeeper zoo = zk.connect(zk.ZOOKEEPER_HOST);

		assertNotNull(zoo);
	}

	@Test
	public void testServerStopped() throws Exception {
		ECSClient ecsClient = new ECSClient();
		ecsClient.shutDownAllServersInZookeeper();

		// Initialize server with nodes
		int num = 2;
		ArrayList<IECSNode> nodes = (ArrayList<IECSNode>) ecsClient.addNodes(num, "None", 5);
		assertEquals(num, nodes.size());
		long startTime = System.nanoTime();
		ZK zk = ecsClient.zk;

		KVStore kvClient = new KVStore("localhost", nodes.get(0).getNodePort());
		try {
			kvClient.connect();
		} catch (Exception e) {
			System.out.println(e);
		}

		String key1 = "apple";
		String value1 = "fruit";

		KVMessage response = null;
		response = kvClient.put(key1, value1);
		assertEquals(KVMessage.StatusType.SERVER_STOPPED, response.getStatus());

		// clean up
		ecsClient.clearAllServersStorage();
		ecsClient.shutDownAllServersInZookeeper();
		zk.close();
	}
	
	// @Test
	// public void testMoveData(){
	// 	KVServer server1 = new KVServer(41001, 100, "None");
	// 	KVServer server2 = new KVServer(41002, 100, "None");

	// 	server1.serverName = "Server1";
	// 	HashRing hashRing = new HashRing();
	// 	ArrayList<IECSNode> nodes= new ArrayList<IECSNode>();
	// 	nodes.add(new ECSNode("Server1", "127.0.0.1", 41001));
	// 	nodes.add(new ECSNode("Server2", "127.0.0.1", 41002));
	// 	hashRing.updateMatadata(nodes);
	// 	server1.setMetadata(hashRing);
	// 	server2.setMetadata(hashRing);

	// 	String key1 = "moveDataKey";
	// 	String value1 = "moveDataValue";
	// 	boolean r = false;
	// 	Exception e = null;
	// 	String getValue = null;

	// 	try{
	// 		server1.putKV(key1, value1);
	// 		r = server1.moveData(HashRing.HASHRING_START, HashRing.HASHRING_END, "127.0.0.1:41002");
	// 		getValue =  server1.getKV(key1);

	// 		assertTrue(r);
	// 		assertEquals(null, getValue);
	// 	} catch (Exception ee){
	// 		e = ee;
	// 	}
	// 	assertEquals(null, e);

	// 	server1.clearStorage();
	// 	server2.clearStorage();
	// }

	/******************************ECS Control Functionality Test********************************* */
	// Test if ecs can correctly stop all servers after calling shutdown
	@Test
	public void testECSShutdown(){
		ECSClient ecsClient = new ECSClient();
		ecsClient.shutDownAllServersInZookeeper();
		
		// Add nodes
		int num = 4;
		ArrayList<IECSNode> nodes = (ArrayList<IECSNode>) ecsClient.addNodes(num, "LRU", 1000);
		assertEquals(num, nodes.size());
		ZK zk = ecsClient.zk;
		assertEquals(num, zk.getChildren(zk.ZOOKEEPER_ACTIVESERVERS_PATH, false).size());
		assertEquals(0, zk.getChildren(zk.ZOOKEEPER_WAITINGSERVERS_PATH, false).size());

		// Shutdown
		boolean r = ecsClient.shutdown();
		assertTrue(r);
		assertEquals(0, zk.getChildren(zk.ZOOKEEPER_ACTIVESERVERS_PATH, false).size());

		// clean up
		ecsClient.shutDownAllServersInZookeeper();
		zk.close();
	}

	@Test
	public void testECSStartStop(){
		ECSClient ecsClient = new ECSClient();
		ecsClient.shutDownAllServersInZookeeper();

		// Add nodes
		int num = 3;
		ArrayList<IECSNode> nodes = (ArrayList<IECSNode>) ecsClient.addNodes(num, "LRU", 1000);
		assertEquals(num, nodes.size());
		long startTime = System.nanoTime();
		ZK zk = ecsClient.zk;
		assertEquals(num, zk.getChildren(zk.ZOOKEEPER_ACTIVESERVERS_PATH, false).size());
		assertEquals(0, zk.getChildren(zk.ZOOKEEPER_WAITINGSERVERS_PATH, false).size());


		// Start servers, wait for 0.05s to make sure watcher starts
		pause(50);
		boolean r = ecsClient.start();
		assertTrue(r);
		for (String server: zk.getChildren(zk.ZOOKEEPER_ACTIVESERVERS_PATH, false)) {
			assertEquals("DONE", zk.getData(zk.ZOOKEEPER_ACTIVESERVERS_PATH + "/" + server));
		}

		// Stop 
		r = ecsClient.stop();
		assertTrue(r);
		for (String server: zk.getChildren(zk.ZOOKEEPER_ACTIVESERVERS_PATH, false)) {
			assertEquals("DONE", zk.getData(zk.ZOOKEEPER_ACTIVESERVERS_PATH + "/" + server));
		}

		// clean up
		ecsClient.shutDownAllServersInZookeeper();
		zk.close();
	}

	// Test ECS functionality on adding nodes incrementally, and randomly remove a set of nodes 
	@Test
	public void testECSAddNode() {
		ECSClient ecsClient = new ECSClient();
		ecsClient.shutDownAllServersInZookeeper();

		// Add nodes
		int num = 1;
		ArrayList<IECSNode> nodes = (ArrayList<IECSNode>) ecsClient.addNodes(num, "LRU", 1000);
		assertEquals(num, nodes.size());
		long startTime = System.nanoTime();
		ZK zk = ecsClient.zk;
		assertEquals(num, zk.getChildren(zk.ZOOKEEPER_ACTIVESERVERS_PATH, false).size());
		assertEquals(0, zk.getChildren(zk.ZOOKEEPER_WAITINGSERVERS_PATH, false).size());

		// Add 1 node
		IECSNode node_result = ecsClient.addNode("FIFO", 1000);
		assertNotNull(node_result);
		assertEquals(num + 1, zk.getChildren(zk.ZOOKEEPER_ACTIVESERVERS_PATH, false).size());

		// Add 1 node
		node_result = ecsClient.addNode("LRU", 1000);
		assertNotNull(node_result);
		assertEquals(num + 2, zk.getChildren(zk.ZOOKEEPER_ACTIVESERVERS_PATH, false).size());

		// Add 1 node
		node_result = ecsClient.addNode("None", 1000);
		assertNotNull(node_result);
		assertEquals(num + 3, zk.getChildren(zk.ZOOKEEPER_ACTIVESERVERS_PATH, false).size());

		// clean up
		ecsClient.shutDownAllServersInZookeeper();
		zk.close();
	}

	@Test
	public void testECSRemoveNode() {
		ECSClient ecsClient = new ECSClient();
		ecsClient.shutDownAllServersInZookeeper();

		// Add nodes
		int num = 4;
		ArrayList<IECSNode> nodes = (ArrayList<IECSNode>) ecsClient.addNodes(num, "LRU", 1000);
		assertEquals(num, nodes.size());
		long startTime = System.nanoTime();
		ZK zk = ecsClient.zk;
		assertEquals(num, zk.getChildren(zk.ZOOKEEPER_ACTIVESERVERS_PATH, false).size());
		assertEquals(0, zk.getChildren(zk.ZOOKEEPER_WAITINGSERVERS_PATH, false).size());

		// Remove 1 node
		ArrayList<String> cleanNode = new ArrayList<String>();
		cleanNode.add(nodes.get(0).getNodeName());
		boolean r = ecsClient.removeNodes(cleanNode);
		assertTrue(r);
		assertFalse(zk.exists(zk.ZOOKEEPER_ACTIVESERVERS_PATH + "/" + nodes.get(0).getNodeHost() + ":" + nodes.get(0).getNodePort(), false));

		// Remove all 3 nodes
		cleanNode = new ArrayList<String>();
		for (IECSNode node: ecsClient.getServersOn().values()){
			cleanNode.add(node.getNodeName());
		}
		r = ecsClient.removeNodes(cleanNode);
		assertTrue(r);
		assertEquals(0, zk.getChildren(zk.ZOOKEEPER_ACTIVESERVERS_PATH, false).size());

		// clean up
		ecsClient.shutDownAllServersInZookeeper();
		zk.close();
	}

	// ------------------------- Client Server Interaction --------------------------------
	// test multi clients to multi servers Update Delete operations
	@Test
	public void testMultiClientMultiServerUpdateDelete(){
		ECSClient ecsClient = new ECSClient();
		ecsClient.shutDownAllServersInZookeeper();

		// Initialize server with nodes
		int num = 2;
		ArrayList<IECSNode> nodes = (ArrayList<IECSNode>) ecsClient.addNodes(num, "None", 5);
		assertEquals(num, nodes.size());
		long startTime = System.nanoTime();
		ZK zk = ecsClient.zk;
		pause(500); // Wait a bit to ensure that the watcher on the node is armed
		ecsClient.start();

		KVStore kvClient1 = new KVStore("localhost", 9995);
		try {
			kvClient1.connect();
		} catch (Exception e) {
			System.out.println(e);
		}

		KVStore kvClient2 = new KVStore("localhost", 9995);
		try {
			kvClient2.connect();
		} catch (Exception e) {
			System.out.println(e);
		}

		String key1 = "apple"; // Goes to server6
		String value1 = "fruit";
		String key2 = "carrot"; // Goes to server6
		String value2 = "vegetable";
		String key3 = "1111111"; // Goes to server5
		String value3 = "number";
		String key4 = "io"; // Goes to server10
		String value4 = "io";

		try{
			KVMessage response = null;
			kvClient1.put(key1, value1);
			kvClient1.put(key2, value2);
			kvClient2.put(key3, value3);
			kvClient2.put(key4, value4);

			response = kvClient1.put(key1, value2);
			assertEquals(KVMessage.StatusType.PUT_UPDATE, response.getStatus());
			response = kvClient1.put(key2, value3);
			assertEquals(KVMessage.StatusType.PUT_UPDATE, response.getStatus());
			response = kvClient2.put(key3, value1);
			assertEquals(KVMessage.StatusType.PUT_UPDATE, response.getStatus());
			response = kvClient2.put(key4, value2);
			assertEquals(KVMessage.StatusType.PUT_UPDATE, response.getStatus());

			response = kvClient2.get(key1);
			assertTrue(response.getValue().equals(value2));
			response = kvClient2.get(key2);
			assertTrue(response.getValue().equals(value3));
			response = kvClient1.get(key3);
			assertTrue(response.getValue().equals(value1));
			response = kvClient1.get(key4);
			assertTrue(response.getValue().equals(value2));

			response = kvClient1.put(key1, "null");
			assertEquals(KVMessage.StatusType.DELETE_SUCCESS, response.getStatus());
			response = kvClient1.put(key2, "null");
			assertEquals(KVMessage.StatusType.DELETE_SUCCESS, response.getStatus());
			response = kvClient1.put(key3, "null");
			assertEquals(KVMessage.StatusType.DELETE_SUCCESS, response.getStatus());
			response = kvClient1.put(key4, "null");
			assertEquals(KVMessage.StatusType.DELETE_SUCCESS, response.getStatus());

		} catch (Exception e){
			fail("Exception: " + e);
		}

		// clean up
		ecsClient.clearAllServersStorage();
		ecsClient.shutDownAllServersInZookeeper();
		zk.close();	
	}

	// test multi clients to multi servers put get operations
	@Test
	public void testMultiClientMultiServerPutGet(){
		ECSClient ecsClient = new ECSClient();
		ecsClient.shutDownAllServersInZookeeper();

		// Initialize server with nodes
		int num = 2;
		ArrayList<IECSNode> nodes = (ArrayList<IECSNode>) ecsClient.addNodes(num, "None", 5);
		assertEquals(num, nodes.size());
		long startTime = System.nanoTime();
		ZK zk = ecsClient.zk;
		pause(500); // Wait a bit to ensure that the watcher on the node is armed
		ecsClient.start();

		KVStore kvClient1 = new KVStore("localhost", 9995);
		try {
			kvClient1.connect();
		} catch (Exception e) {
			System.out.println(e);
		}

		KVStore kvClient2 = new KVStore("localhost", 9995);
		try {
			kvClient2.connect();
		} catch (Exception e) {
			System.out.println(e);
		}

		String key1 = "apple"; // Goes to server6
		String value1 = "fruit";
		String key2 = "carrot"; // Goes to server6
		String value2 = "vegetable";
		String key3 = "1111111"; // Goes to server5
		String value3 = "number";
		String key4 = "io"; // Goes to server10
		String value4 = "io";

		try{
			KVMessage response = null;
			response = kvClient1.put(key1, value1);
			assertEquals(KVMessage.StatusType.PUT_SUCCESS, response.getStatus());
			response = kvClient1.put(key2, value2);
			assertEquals(KVMessage.StatusType.PUT_SUCCESS, response.getStatus());
			response = kvClient2.put(key3, value3);
			assertEquals(KVMessage.StatusType.PUT_SUCCESS, response.getStatus());
			response = kvClient2.put(key4, value4);
			assertEquals(KVMessage.StatusType.PUT_SUCCESS, response.getStatus());

			response = kvClient2.get(key1);
			assertTrue(response.getValue().equals(value1));
			response = kvClient2.get(key2);
			assertTrue(response.getValue().equals(value2));
			response = kvClient1.get(key3);
			assertTrue(response.getValue().equals(value3));
			response = kvClient1.get(key4);
			assertTrue(response.getValue().equals(value4));
		} catch (Exception e){
			fail("Exception: " + e);
		}

		// clean up
		ecsClient.clearAllServersStorage();
		ecsClient.shutDownAllServersInZookeeper();
		zk.close();
	}

	// Test client server put get functionalities while servers keep scaling up
	@Test
	public void testClientDynamicServer(){
		ECSClient ecsClient = new ECSClient();
		ecsClient.shutDownAllServersInZookeeper();

		// Initialize server with nodes
		int num = 2;
		ArrayList<IECSNode> nodes = (ArrayList<IECSNode>) ecsClient.addNodes(num, "LRU", 5);
		assertNotNull(nodes);
		assertEquals(num, nodes.size());
		long startTime = System.nanoTime();
		ZK zk = ecsClient.zk;
		pause(500); // Wait a bit to ensure that the watcher on the node is armed
		ecsClient.start();

		KVStore kvClient = new KVStore("localhost", nodes.get(0).getNodePort());
		try {
			kvClient.connect();
		} catch (Exception e) {
			System.out.println(e);
		}

		String key1 = "apple"; // Goes to server6 -> 3
		String value1 = "fruit";
		String key2 = "carrot"; // Goes to server6
		String value2 = "vegetable";
		String key3 = "1111111"; // Goes to server5
		String value3 = "number";
		String key4 = "io"; // Goes to server10
		String value4 = "io";
		KVMessage response = null;
		try{
			response = kvClient.put(key1, value1);
			assertEquals(KVMessage.StatusType.PUT_SUCCESS, response.getStatus());
			response = kvClient.put(key2, value2);
			assertEquals(KVMessage.StatusType.PUT_SUCCESS, response.getStatus());
		} catch (Exception e){
			fail("Exception: " + e);
		}
		try{
			// Add a node
			IECSNode node_result = ecsClient.addNode("None", 1000);
			assertNotNull(node_result);

			response = kvClient.put(key3, value3);
			assertEquals(KVMessage.StatusType.PUT_SUCCESS, response.getStatus());
			response = kvClient.put(key4, value4);
			assertEquals(KVMessage.StatusType.PUT_SUCCESS, response.getStatus());
		} catch (Exception e){
			fail("Exception: " + e);
		}
		try{
			// Add a node
			IECSNode node_result = ecsClient.addNode("None", 1000);
			assertNotNull(node_result);
			// Add a node
			node_result = ecsClient.addNode("None", 1000);
			assertNotNull(node_result);
		} catch (Exception e){
			fail("Exception: " + e);
		}
		try{
			response = kvClient.get(key1);
			assertEquals(value1, response.getValue());
			response = kvClient.get(key2);
			assertEquals(value2, response.getValue());
			response = kvClient.get(key3);
			assertEquals(value3, response.getValue());
			response = kvClient.get(key4);
			assertEquals(value4, response.getValue());
		} catch (Exception e){
			e.printStackTrace();
			fail("Exception: " + e);
		}

		// clean up
		ecsClient.clearAllServersStorage();
		ecsClient.shutDownAllServersInZookeeper();
		zk.close();	
	}

    public static void pause(int ms) {
		try {
			Thread.sleep(ms);
		} catch (InterruptedException e) {
			System.err.format("IOException: %s%n", e);
		}
	}

}
