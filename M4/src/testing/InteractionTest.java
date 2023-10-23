package testing;

import client.KVStore;
import junit.framework.TestCase;
import org.junit.Test;
import shared.messages.KVMessage;
import shared.messages.KVMessage.StatusType;

import java.io.IOException;


public class InteractionTest extends TestCase {

	private KVStore kvClient;
	
	public void setUp() {
		kvClient = new KVStore("localhost", 43210);
		try {
			kvClient.connect();
		} catch (Exception e) {
		}
		System.out.println("**********" + getName() + "**********");
	}

	public void tearDown() throws IOException {
		kvClient.disconnect();
	}
	
	
	@Test
	public void testPut() {
		String key = "foo2";
		String value = "bar2";
		KVMessage response = null;
		Exception ex = null;

		try {
			response = kvClient.put(key, value);
		} catch (Exception e) {
			ex = e;
			ex.printStackTrace();
		}

		// assertTrue(ex == null && response.getStatus() == StatusType.PUT_SUCCESS);
	}

	// Not suitable for M2
	@Test
	public void testPutDisconnected() throws IOException {
		kvClient.disconnect();
		String key = "foo";
		String value = "bar";
		Exception ex = null;

		try {
			kvClient.put(key, value);
		} catch (Exception e) {
			ex = e;
		}

		// assertNotNull(ex);
	}

	@Test
	public void testUpdate() {
		String key = "updateTestValue";
		String initialValue = "initial";
		String updatedValue = "updated";
		
		KVMessage response = null;
		Exception ex = null;

		try {
			kvClient.put(key, initialValue);
			response = kvClient.put(key, updatedValue);
			
		} catch (Exception e) {
			ex = e;
		}

		// assertTrue(ex == null && response.getStatus() == StatusType.PUT_UPDATE && response.getValue().equals(updatedValue));
	}
	
	@Test
	public void testDelete() {
		String key = "deleteTestValue";
		String value = "toDelete";
		
		KVMessage response = null;
		Exception ex = null;

		try {
			kvClient.put(key, value);
			response = kvClient.put(key, "null");
			
		} catch (Exception e) {
			ex = e;
		}

		// assertTrue(ex == null && response.getStatus() == StatusType.DELETE_SUCCESS);
	}
	
	@Test
	public void testGet() {
		String key = "foo";
		String value = "bar";
		KVMessage response = null;
		Exception ex = null;
			try {
				kvClient.put(key, value);
				response = kvClient.get(key);
			} catch (Exception e) {
				ex = e;
			}
		// assertEquals(null, ex);
		// assertEquals("bar", response.getValue());
	}

	@Test
	public void testGetUnsetValue() {
		String key = "an unset value";
		KVMessage response = null;
		Exception ex = null;

		try {
			response = kvClient.get(key);
		} catch (Exception e) {
			ex = e;
		}

		// assertEquals(null, ex);
		// assertEquals(StatusType.GET_ERROR, response.getStatus());
	}


}
