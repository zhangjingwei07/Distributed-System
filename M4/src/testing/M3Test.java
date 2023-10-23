package testing;

import app_kvECS.ECSClient;
import app_kvServer.KVServer;
import client.KVStore;
import ecs.ECSNode;
import ecs.IECSNode;
import junit.framework.TestCase;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.junit.Test;
import shared.metadata.HashRing;
import zookeeper.ZK;

import java.io.FileReader;
import java.util.ArrayList;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class M3Test extends TestCase {
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

	@Test
	public void testHashRingReplicaRange() {
		HashRing hashRing = new HashRing();
		ArrayList<IECSNode> nodes = new ArrayList<IECSNode>();
		nodes.add(new ECSNode("Server1", "127.0.0.1", 1));
		hashRing.updateMatadata(nodes);

		assertFalse(hashRing.inReplicaRange("Tiger", "Server1"));

		nodes.add(new ECSNode("Server2", "127.0.0.1", 2));
		hashRing.updateMatadata(nodes);

		assertTrue(hashRing.inReplicaRange("Tiger", "Server2"));
	}

	@Test
	public void testHashRingReplicaRangeHard() {
		HashRing hashRing = new HashRing();
		ArrayList<IECSNode> nodes = new ArrayList<IECSNode>();
		nodes.add(new ECSNode("Server3", "127.0.0.1", 9993));
		nodes.add(new ECSNode("Server4", "127.0.0.1", 9994));
		nodes.add(new ECSNode("Server5", "127.0.0.1", 9995));
		nodes.add(new ECSNode("Server6", "127.0.0.1", 9996));
		nodes.add(new ECSNode("Server10", "127.0.0.1", 9910));
		hashRing.updateMatadata(nodes);

		assertFalse(hashRing.inReplicaRange("apple", "Server10"));

		assertTrue(hashRing.inReplicaRange("apple", "Server4"));
		assertTrue(hashRing.inReplicaRange("apple", "Server6"));
	}

	@Test
	public void testCorrectServer() {
		HashRing hashRing = new HashRing();
		ArrayList<IECSNode> nodes = new ArrayList<IECSNode>();
		nodes.add(new ECSNode("Server1", "127.0.0.1", 1));
		hashRing.updateMatadata(nodes);

		nodes.add(new ECSNode("Server2", "127.0.0.1", 2));
		hashRing.updateMatadata(nodes);

		assertTrue(hashRing.getCorrectServer("Tiger").equals("Server1"));
	}

	@Test
	public void testPutReplicatedData() {
		KVServer server1 = new KVServer(41001, 100, "None");
		KVServer server2 = new KVServer(41002, 100, "None");
		KVServer server3 = new KVServer(41003, 100, "None");
		KVServer server4 = new KVServer(41004, 100, "None");
		pause(500); // Wait for server to be fully established

		// Without initializing ECS, remember set up server name manually.
		server1.setServerName("Server1");
		server2.setServerName("Server2");
		server3.setServerName("Server3");
		server4.setServerName("Server4");

		HashRing hashRing = new HashRing();
		ArrayList<IECSNode> nodes= new ArrayList<IECSNode>();
		nodes.add(new ECSNode("Server1", "127.0.0.1", 41001));
		nodes.add(new ECSNode("Server2", "127.0.0.1", 41002));
		nodes.add(new ECSNode("Server3", "127.0.0.1", 41003));
		nodes.add(new ECSNode("Server4", "127.0.0.1", 41004));

		hashRing.updateMatadata(nodes);
		server1.setMetadata(hashRing);
		server2.setMetadata(hashRing);
		server3.setMetadata(hashRing);
		server4.setMetadata(hashRing);

		String key1 = "will";
		String value1 = "stanford";
		boolean r = false;
		Exception e = null;
		// will is in the range of server1
		assertEquals(true, hashRing.inRange(key1, "Server1"));		
		assertEquals(false, hashRing.inRange(key1, "Server2"));				
		assertEquals(false, hashRing.inRange(key1, "Server3"));
		assertEquals(false, hashRing.inRange(key1, "Server4"));

		try{
			r = server1.putOrDeleteReplicatedData(key1, value1);
			assertTrue(r);
			assertEquals(null, server1.getKV(key1));
			assertEquals(value1, server2.getKV(key1));
			assertEquals(null, server3.getKV(key1));
			assertEquals(value1, server4.getKV(key1));
		} catch (Exception ee){
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

	@Test
	public void testDeleteReplicatedData() {
		KVServer server1 = new KVServer(41001, 100, "None");
		KVServer server2 = new KVServer(41002, 100, "None");
		KVServer server3 = new KVServer(41003, 100, "None");
		KVServer server4 = new KVServer(41004, 100, "None");
		pause(500); // Wait for server to be fully established

		// Without initializing ECS, remember set up server name manually.
		server1.setServerName("Server1");
		server2.setServerName("Server2");
		server3.setServerName("Server3");
		server4.setServerName("Server4");

		HashRing hashRing = new HashRing();
		ArrayList<IECSNode> nodes= new ArrayList<IECSNode>();
		nodes.add(new ECSNode("Server1", "127.0.0.1", 41001));
		nodes.add(new ECSNode("Server2", "127.0.0.1", 41002));
		nodes.add(new ECSNode("Server3", "127.0.0.1", 41003));
		nodes.add(new ECSNode("Server4", "127.0.0.1", 41004));

		hashRing.updateMatadata(nodes);
		server1.setMetadata(hashRing);
		server2.setMetadata(hashRing);
		server3.setMetadata(hashRing);
		server4.setMetadata(hashRing);

		String key1 = "will";
		String value1 = "stanford";
		boolean r = false;
		Exception e = null;
		// will is in the range of server1
		assertEquals(true, hashRing.inRange(key1, "Server1"));
		assertEquals(false, hashRing.inRange(key1, "Server2"));
		assertEquals(false, hashRing.inRange(key1, "Server3"));
		assertEquals(false, hashRing.inRange(key1, "Server4"));

		try{
			r = server1.putOrDeleteReplicatedData(key1, value1);
			assertTrue(r);
			assertEquals(null, server1.getKV(key1));
			assertEquals(value1, server2.getKV(key1));
			assertEquals(null, server3.getKV(key1));
			assertEquals(value1, server4.getKV(key1));

			r = server1.putOrDeleteReplicatedData(key1, null);
			assertTrue(r);
			assertEquals(null, server1.getKV(key1));
			assertEquals(null, server2.getKV(key1));
			assertEquals(null, server3.getKV(key1));
			assertEquals(null, server4.getKV(key1));
		} catch (Exception ee){
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

	@Test
	public void testPreSendData() {
		KVServer server1 = new KVServer(41001, 100, "None");

		// Without initializing ECS, remember set up server name manually.
		server1.setServerName("Server1");

		HashRing hashRing = new HashRing();
		ArrayList<IECSNode> nodes= new ArrayList<IECSNode>();
		nodes.add(new ECSNode("Server1", "127.0.0.1", 41001));

		hashRing.updateMatadata(nodes);
		server1.setMetadata(hashRing);

		String key1 = "cat";
		String value1 = "Columbia";
		String key2 = "Banana";
		String value2 = "Aaron";
		Exception e = null;

		// Both key1 and key2 belong to server1
		assertEquals(true, hashRing.inRange(key1, "Server1"));
		assertEquals(true, hashRing.inRange(key2, "Server1"));


		try {
			server1.putKV(key1, value1);
			server1.putKV(key2, value2);

			// Confirm successful insertion
			assertEquals(value1, server1.getKV(key1));
			assertEquals(value2, server1.getKV(key2));

			assertEquals("d077f244def8a70e5ea758bd8352fcd8", hashRing.getHashValue("cat"));

			// Will send cat. Change the last character.
			String stringData = server1.getStorage().preSendData("d077f244def8a70e5ea758bd8352fcd7", "d077f244def8a70e5ea758bd8352fcd9");
			assertEquals(value1, server1.getKV(key1));
			assertEquals(value2, server1.getKV(key2));
			assertEquals("{\"cat\":{\"subscribers\":[],\"value\":\"Columbia\"}}", stringData);

		} catch (Exception ee){
			e = ee;
		}
		assertEquals(null, e);
		server1.clearStorage();
		server1.close();
	}


	// Only send data to targeted server
	@Test
	public void testSendData() {
		KVServer server1 = new KVServer(41001, 100, "None");
		KVServer server2 = new KVServer(41002, 100, "None");

		// Without initializing ECS, remember set up server name manually.
		server1.setServerName("Server1");
		server2.setServerName("Server2");

		HashRing hashRing = new HashRing();
		ArrayList<IECSNode> nodes= new ArrayList<IECSNode>();
		nodes.add(new ECSNode("Server1", "127.0.0.1", 41001));
		nodes.add(new ECSNode("Server2", "127.0.0.1", 41002));

		hashRing.updateMatadata(nodes);
		server1.setMetadata(hashRing);
		server2.setMetadata(hashRing);

		String key1 = "will";
		String value1 = "stanford";
		String key2 = "cat";
		String value2 = "Columbia";
		String key3 = "Banana";
		String value3 = "Aaron";
		boolean r = false;
		Exception e = null;
		// All data belong to server1
		assertEquals(true, hashRing.inRange(key1, "Server1"));
		assertEquals(false, hashRing.inRange(key1, "Server2"));
		assertEquals(true, hashRing.inRange(key2, "Server1"));
		assertEquals(false, hashRing.inRange(key2, "Server2"));
		assertEquals(true, hashRing.inRange(key3, "Server1"));
		assertEquals(false, hashRing.inRange(key3, "Server2"));

		try{

			// put key in server2 to avoid sending replicated data to server1
			// (put in server1 will trigger replicated put request to server2);
			server2.putKV(key1, value1);
			server2.putKV(key2, value2);
			server2.putKV(key3, value3);

			// No data in server1
			assertEquals(null, server1.getKV(key1));
			assertEquals(null, server1.getKV(key2));
			assertEquals(null, server1.getKV(key3));
			// Successful put to server2
			assertEquals(value1, server2.getKV(key1));
			assertEquals(value2, server2.getKV(key2));
			assertEquals(value3, server2.getKV(key3));

			// Send cat (key2) to server1
			r = server2.sendData("d077f244def8a70e5ea758bd8352fcd7", "d077f244def8a70e5ea758bd8352fcd9", "127.0.0.1:41001");
			assertTrue(r);

			// Cat exist in server1
			assertEquals(null, server1.getKV(key1));
			assertEquals(value2, server1.getKV(key2));
			assertEquals(null, server1.getKV(key3));
			// Cat still exist in server2
			assertEquals(value1, server2.getKV(key1));
			assertEquals(value2, server2.getKV(key2));
			assertEquals(value3, server2.getKV(key3));

		} catch (Exception ee){
			e = ee;
		}
		assertEquals(null, e);
		server1.clearStorage();
		server2.clearStorage();
		server1.close();
		server2.close();
	}

	@Test
	public void testDeleteData() {
		KVServer server1 = new KVServer(41001, 100, "None");

		// Without initializing ECS, remember set up server name manually.
		server1.setServerName("Server1");

		HashRing hashRing = new HashRing();
		ArrayList<IECSNode> nodes= new ArrayList<IECSNode>();
		nodes.add(new ECSNode("Server1", "127.0.0.1", 41001));

		hashRing.updateMatadata(nodes);
		server1.setMetadata(hashRing);

		String key1 = "cat";
		String value1 = "Columbia";
		String key2 = "Banana";
		String value2 = "Aaron";
		Exception e = null;

		// Both key1 and key2 belong to server1
		assertEquals(true, hashRing.inRange(key1, "Server1"));
		assertEquals(true, hashRing.inRange(key2, "Server1"));

		try {
			server1.putKV(key1, value1);
			server1.putKV(key2, value2);

			// Confirm successful insertion
			assertEquals(value1, server1.getKV(key1));
			assertEquals(value2, server1.getKV(key2));

			assertEquals("d077f244def8a70e5ea758bd8352fcd8", hashRing.getHashValue("cat"));

			// Delete cat. Change the last character.
			server1.deleteData("d077f244def8a70e5ea758bd8352fcd7", "d077f244def8a70e5ea758bd8352fcd9");
			assertEquals(null, server1.getKV(key1));
			assertEquals(value2, server1.getKV(key2));
		} catch (Exception ee){
			e = ee;
		}

		assertEquals(null, e);
		server1.clearStorage();
		server1.close();
	}

	@Test
	public void testNodeCrashHandling(){
		ECSClient ecsClient = new ECSClient();
		ecsClient.shutDownAllServersInZookeeper();

		// Initialize server with nodes
		int num = 2;
		ArrayList<IECSNode> nodes = (ArrayList<IECSNode>) ecsClient.addNodes(num, "None", 1);
		assertEquals(num, nodes.size());
		long startTime = System.nanoTime();
		ZK zk = ecsClient.zk;
		pause(500); // Wait a bit to ensure that the watcher on the node is armed
		ecsClient.start();

		// shutdown a server
		zk.setData(zk.ZOOKEEPER_ACTIVESERVERS_PATH + "/" + "127.0.0.1:9996", "SHUTDOWN");

		// wait for server to re-establish
		pause(1000);
		int activeServersNum = zk.getChildren(zk.ZOOKEEPER_ACTIVESERVERS_PATH, false).size();

		// clean up
		ecsClient.clearAllServersStorage();
		ecsClient.shutDownAllServersInZookeeper();
		zk.close();

		// Check if server is reestablished
		assertEquals(num, activeServersNum);
	}

	@Test
	public void testAddNodeDataHandling(){
		ECSClient ecsClient = new ECSClient();
		ecsClient.shutDownAllServersInZookeeper();

		// Initialize server with nodes
		int num = 1;
		ArrayList<IECSNode> nodes = (ArrayList<IECSNode>) ecsClient.addNodes(num, "None", 1);
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

		String key1 = "apple";
		String value1 = "fruit";
		String key2 = "io";
		String value2 = "io";

		try {
			kvClient.put(key1, value1);
			kvClient.put(key2, value2);
		}  catch (Exception e) {
			fail("Exception = " + e);
		}

		// Add 2 more nodes
		IECSNode new_node1 = ecsClient.addNode("None", 1);
		IECSNode new_node2 = ecsClient.addNode("None", 1);

		// Check if the new node contains both key-value pairs
		JSONParser jsonParser = new JSONParser();
		JSONObject value_pairs;
		String value;
		try (FileReader reader = new FileReader("data/9910_data.json")) {
			//Read json file
			value_pairs = (JSONObject)jsonParser.parse(reader);

			// Check if key exist
			boolean contains_key1 = value_pairs.containsKey(key1);
			boolean contains_key2 = value_pairs.containsKey(key2);

			assertTrue(contains_key1);
			assertTrue(contains_key2);
		} catch (Exception e) {
			fail("Exception = " + e);
		}

		// clean up
		ecsClient.clearAllServersStorage();
		ecsClient.shutDownAllServersInZookeeper();
		zk.close();
	}

	@Test
	public void testRemoveNodeDataHandling(){
		ECSClient ecsClient = new ECSClient();
		ecsClient.shutDownAllServersInZookeeper();

		// Initialize server with nodes
		int num = 4;
		ArrayList<IECSNode> nodes = (ArrayList<IECSNode>) ecsClient.addNodes(num, "None", 1);
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

		// hashring [server10 - server6 - server4 - server5]
		String key1 = "apple"; // goes to server 6
		String value1 = "fruit";
		String key2 = "io"; // goes to server 10
		String value2 = "io";

		try {
			kvClient.put(key1, value1);
			kvClient.put(key2, value2);
		}  catch (Exception e) {
			fail("Exception = " + e);
		}

		// Remove 1 node
		ArrayList<String> cleanNode = new ArrayList<String>();
		cleanNode.add("server6");
		String nodePath = zk.ZOOKEEPER_ACTIVESERVERS_PATH + "/" + "127.0.0.1:9996";
		zk.setData(nodePath, "CLEAR_STORAGE"); // Clear storage
		// Wait until storage is cleared
		int timeout = 2000;
		while (!ecsClient.checkMessage("DONE", "127.0.0.1", 9996)){
			if (System.nanoTime() - startTime > timeout*1000000){
				fail("Timeout in clearing data in server6");
				break;
			}
		}
		// Remove
		boolean r = ecsClient.removeNodes(cleanNode);

		// Server 10 should now have key apple
		JSONParser jsonParser = new JSONParser();
		JSONObject value_pairs;
		String value;
		try (FileReader reader = new FileReader("data/9910_data.json")) {
			//Read json file
			value_pairs = (JSONObject)jsonParser.parse(reader);

			// Check if key exist
			boolean contains_key1 = value_pairs.containsKey(key1);

			assertTrue(contains_key1);
		} catch (Exception e) {
			fail("Exception = " + e);
		}

		// Server 5 should now have key io
		try (FileReader reader = new FileReader("data/9995_data.json")) {
			//Read json file
			value_pairs = (JSONObject)jsonParser.parse(reader);

			// Check if key exist
			boolean contains_key2 = value_pairs.containsKey(key2);

			assertTrue(contains_key2);
		} catch (Exception e) {
			fail("Exception = " + e);
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
