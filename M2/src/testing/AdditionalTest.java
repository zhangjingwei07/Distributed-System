package testing;

import app_kvServer.*;
import client.*;
import ecs.*;
import storage.*;
import shared.messages.KVMessageImple;
import shared.messages.KVMessage;

import java.io.*;
import java.util.*;	

import jdk.jfr.Timestamp;	
import junit.framework.TestCase;	
import org.apache.log4j.*;	
import org.junit.Test;
import org.junit.After;	
import org.junit.Before;	
import org.junit.Test;


public class AdditionalTest extends TestCase {
	/********* KVClient unit test **********/
	@Test
	public void testDeleteError() throws Exception {

		KVStore kvClient = new KVStore("localhost", 43210);
		try {
			kvClient.connect();
		} catch (Exception e) {
		}
		String key = "deleteTestValue";

		KVMessage response = null;
		Exception ex = null;
		response = kvClient.put(key, "null");


		assertTrue(response.getStatus() == KVMessage.StatusType.DELETE_ERROR);
	}

	/********* KVServer unit test **********/
	@Test
	public void testSuccessfulEncode() {
		String expected_response = "{\"key\":\"key1\",\"value\":\"value1\",\"status\":\"GET_SUCCESS\"}";
		KVMessageImple response = new KVMessageImple("key1", "value1", KVMessage.StatusType.GET_SUCCESS);
		TextMessage response_text_mess = new TextMessage(response.encode());
		assertTrue(response_text_mess.getMsg().equals(expected_response));
	}	

	/********* KVStorage unit test *********/
	@Test
	public void testKVStorageBasic(){
		KVStorage storage = new KVStorage(0);
		String temp;
		int temp_int;
		// Simple key value store
		temp_int = storage.put("key1", "value1");
		assertTrue(temp_int==1);
		temp_int = storage.put("key2", "value2");
		assertTrue(temp_int==1);
		temp_int = storage.put("key3", "value3");
		assertTrue(temp_int==1);

		temp = storage.get("key1");
		assertTrue(temp.equals("value1"));
		temp = storage.get("key2");
		assertTrue(temp.equals("value2"));
		temp = storage.get("key3");
		assertTrue(temp.equals("value3"));

		// Key value update
		temp_int = storage.put("key1", "value1_updated");
		assertTrue(temp_int==1);
		temp_int = storage.put("key2", "value2_updated");
		assertTrue(temp_int==1);
		temp_int = storage.put("key3", "value3_updated");
		assertTrue(temp_int==1);

		temp = storage.get("key1");
		assertTrue(temp.equals("value1_updated"));
		temp = storage.get("key2");
		assertTrue(temp.equals("value2_updated"));
		temp = storage.get("key3");
		assertTrue(temp.equals("value3_updated"));

		storage.clearStorage();
	}

	@Test
	public void testKVStorageClear(){
		KVStorage storage = new KVStorage(0);
		storage.put("key1", "value1");
		storage.clearStorage();
		String temp = storage.get("key1");
		assertTrue(temp == null);
	}

	@Test
	public void testKVStorageMore(){
		KVStorage storage = new KVStorage(0);
		storage.clearStorage();
		String temp;
		int temp_int;

		// Key does not exist
		temp = storage.get("key");
		assertTrue(temp == null);

		// Delete key
		storage.put("key1", "value1");
		temp_int = storage.delete("key1");
		temp = storage.get("key1");
		assertTrue(temp == null && temp_int == 1);

		temp_int = storage.delete("key1"); // Return 0 on failure
		assertTrue(temp_int == 0);

	}

	/********* Storage FIFO cache test *********/
	@Test
	public void testFIFOCache(){
		int size = 3;
		KVStorage fifoStorage = new KVStorage(size, IKVServer.CacheStrategy.FIFO, 0);
		fifoStorage.clearStorage();
		String temp;
		String key1 = "key1", key2 = "key2", key3 = "key3", key4 = "key4";
		String value1 = "value1", value2 = "value2", value3 = "value3", value4 = "value4";
		fifoStorage.put(key1, value1);
		fifoStorage.put(key2, value2);
		fifoStorage.put(key3, value3);
		// Simple get
		temp = fifoStorage.cache.getFromCache(key2);
		assertTrue(temp == value2);
		// Update an element
		fifoStorage.put(key1, "value1_updated");
		temp = fifoStorage.cache.getFromCache(key1);
		assertTrue(temp == "value1_updated");
		// Evict first element
		assertTrue(fifoStorage.cache.inCache(key1)==true);
		fifoStorage.put(key4, value4);
		assertTrue(fifoStorage.cache.inCache(key1)==false);
		assertTrue(fifoStorage.cache.inCache(key2)==true);
		fifoStorage.put(key1, value1);
		assertTrue(fifoStorage.cache.inCache(key2)==false);
		// Clear cache
		fifoStorage.clearStorage();
		assertTrue(fifoStorage.cache.inCache(key1)==false && fifoStorage.cache.inCache(key1)==false &&
					fifoStorage.cache.inCache(key3)==false && fifoStorage.cache.inCache(key4)==false);
	}

	/********* Storage LRU cache test *********/
	@Test
	public void testLRUCache(){
		int size = 3;
		KVStorage lruStorage = new KVStorage(size, IKVServer.CacheStrategy.LRU, 0);
		String temp;
		String key1 = "key1", key2 = "key2", key3 = "key3", key4 = "key4";
		String value1 = "value1", value2 = "value2", value3 = "value3", value4 = "value4";
		lruStorage.put(key1, value1);
		lruStorage.put(key2, value2);
		lruStorage.put(key3, value3);
		// Simple get
		temp = lruStorage.cache.getFromCache(key2);
		assertTrue(temp == value2);
		// Update an element
		lruStorage.put(key1, "value1_updated");
		temp = lruStorage.cache.getFromCache(key1);
		assertTrue(temp == "value1_updated");
		// Evict first element
		assertTrue(lruStorage.cache.inCache(key3)==true); // Should evict key3
		lruStorage.put(key4, value4);
		assertTrue(lruStorage.cache.inCache(key3)==false);
		assertTrue(lruStorage.cache.inCache(key2)==true); //should evict key2
		lruStorage.put(key3, value3);
		assertTrue(lruStorage.cache.inCache(key2)==false);
		// Clear cache
		lruStorage.clearStorage();
		assertTrue(lruStorage.cache.inCache(key1)==false && lruStorage.cache.inCache(key1)==false &&
					lruStorage.cache.inCache(key3)==false && lruStorage.cache.inCache(key4)==false);
	}
}
