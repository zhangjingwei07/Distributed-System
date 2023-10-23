package testing;

import app_kvServer.KVServer;
import ecs.ECSNode;
import ecs.IECSNode;
import junit.framework.Test;
import junit.framework.TestSuite;
import logger.LogSetup;
import org.apache.log4j.Level;
import shared.metadata.HashRing;

import java.io.IOException;
import java.util.ArrayList;


public class AllTests {

	static {
		try {
			new LogSetup("logs/testing/test.log", Level.DEBUG);
			KVServer server = new KVServer(43210, 10, "None");
			server.getStorage().clearStorage();
			HashRing hashRing = new HashRing();
			ArrayList<IECSNode> nodes= new ArrayList<IECSNode>();
			nodes.add(new ECSNode("Server1", "127.0.0.1", 43210));
			hashRing.updateMatadata(nodes);
			server.setMetadata(hashRing);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public static Test suite() {
		TestSuite clientSuite = new TestSuite("Basic Storage ServerTest-Suite");
		clientSuite.addTestSuite(ConnectionTest.class);
		clientSuite.addTestSuite(InteractionTest.class);
		clientSuite.addTestSuite(AdditionalTest.class);
		// clientSuite.addTestSuite(M2Test.class); 
		clientSuite.addTestSuite(M3Test.class);
		clientSuite.addTestSuite(M4Test.class);
		return clientSuite;
	}
}
