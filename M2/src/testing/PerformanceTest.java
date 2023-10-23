package testing;

import app_kvServer.KVServer;
import client.KVStore;
import ecs.ECSNode;
import ecs.IECSNode;
import junit.framework.TestCase;
import org.apache.commons.io.FileUtils;
import org.junit.Test;
import shared.metadata.HashRing;

import java.io.*;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;


public class PerformanceTest extends TestCase {

    public static List<String> getDataFrom(String dir) {
        String DATA_ROOT = "~/Downloads/ECE419/M2";
        Collection<File> files = FileUtils.listFiles(new File(dir),
                null,
                false);
        System.out.println(files.size());
        List<String> test_data = new ArrayList<>(0);

        int count = 1;
        for (File file : files) {

            try {
                count++;
                StringBuilder content = new StringBuilder();
                FileReader fileReader = new FileReader(file);
                BufferedReader reader = new BufferedReader(fileReader);
                String line;
                while ((line = reader.readLine()) != null) {
                    content.append(line);
                }
                test_data.add(content.toString());

                reader.close();
                fileReader.close();
            } catch (FileNotFoundException e) {
                System.out.println(file.getAbsolutePath() + " doesn't exist");
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return test_data;
    }

    @Test
    public void testPutPerformance() throws Exception {

        KVServer server1 = new KVServer(40001, 100, "FIFO");
        KVServer server2 = new KVServer(40002, 100, "FIFO");
        KVServer server3 = new KVServer(40003, 100, "FIFO");
        KVServer server4 = new KVServer(40004, 100, "FIFO");
        KVServer server5 = new KVServer(40005, 100, "FIFO");
        KVServer server6 = new KVServer(40006, 100, "FIFO");
        KVServer server7 = new KVServer(40007, 100, "FIFO");
        KVServer server8 = new KVServer(40008, 100, "FIFO");
        KVServer server9 = new KVServer(40009, 100, "FIFO");
        server1.serverName = "Server1";
        server2.serverName = "Server2";
        server3.serverName = "Server3";
        server4.serverName = "Server4";
        server5.serverName = "Server5";
        server6.serverName = "Server6";
        server7.serverName = "Server7";
        server8.serverName = "Server8";
        server9.serverName = "Server9";

        HashRing hashRing = new HashRing();
        ArrayList<IECSNode> nodes= new ArrayList<IECSNode>();
        nodes.add(new ECSNode("Server1", "127.0.0.1", 40001));
        nodes.add(new ECSNode("Server2", "127.0.0.1", 40002));
        nodes.add(new ECSNode("Server3", "127.0.0.1", 40003));
        nodes.add(new ECSNode("Server4", "127.0.0.1", 40004));
        nodes.add(new ECSNode("Server5", "127.0.0.1", 40005));
        nodes.add(new ECSNode("Server6", "127.0.0.1", 40006));
        nodes.add(new ECSNode("Server7", "127.0.0.1", 40007));
        nodes.add(new ECSNode("Server8", "127.0.0.1", 40008));
        nodes.add(new ECSNode("Server9", "127.0.0.1", 40009));


        hashRing.updateMatadata(nodes);
        server1.setMetadata(hashRing);
        server2.setMetadata(hashRing);
        server3.setMetadata(hashRing);
        server4.setMetadata(hashRing);
        server5.setMetadata(hashRing);
        server6.setMetadata(hashRing);
        server7.setMetadata(hashRing);
        server8.setMetadata(hashRing);
        server9.setMetadata(hashRing);


        long startTime = System.nanoTime();
        KVStore kvClient = new KVStore("localhost", 40001);
        try {
            kvClient.connect();
        } catch (IOException ioe) {
            System.out.println(ioe);
            return;
        }


        List<String> test_data = getDataFrom("test_data/");

        Integer count = 0;
        for (String data:test_data) {
            count ++;
            kvClient.put(Integer.toString(count), data);
        }
        System.out.println("Put count: "+Integer.toString(count));
        long time = System.nanoTime() - startTime;
        System.out.println("Time spent in msec: "+Long.toString(time/1000000));
    }

    @Test
    public void testGetPerformance() throws Exception {

        KVServer server1 = new KVServer(40001, 100, "FIFO");
        KVServer server2 = new KVServer(40002, 100, "FIFO");
        KVServer server3 = new KVServer(40003, 100, "FIFO");
        KVServer server4 = new KVServer(40004, 100, "FIFO");
        KVServer server5 = new KVServer(40005, 100, "FIFO");
        KVServer server6 = new KVServer(40006, 100, "FIFO");
        KVServer server7 = new KVServer(40007, 100, "FIFO");
        KVServer server8 = new KVServer(40008, 100, "FIFO");
        KVServer server9 = new KVServer(40009, 100, "FIFO");
        server1.serverName = "Server1";
        server2.serverName = "Server2";
        server3.serverName = "Server3";
        server4.serverName = "Server4";
        server5.serverName = "Server5";
        server6.serverName = "Server6";
        server7.serverName = "Server7";
        server8.serverName = "Server8";
        server9.serverName = "Server9";

        HashRing hashRing = new HashRing();
        ArrayList<IECSNode> nodes= new ArrayList<IECSNode>();
        nodes.add(new ECSNode("Server1", "127.0.0.1", 40001));
        nodes.add(new ECSNode("Server2", "127.0.0.1", 40002));
        nodes.add(new ECSNode("Server3", "127.0.0.1", 40003));
        nodes.add(new ECSNode("Server4", "127.0.0.1", 40004));
        nodes.add(new ECSNode("Server5", "127.0.0.1", 40005));
        nodes.add(new ECSNode("Server6", "127.0.0.1", 40006));
        nodes.add(new ECSNode("Server7", "127.0.0.1", 40007));
        nodes.add(new ECSNode("Server8", "127.0.0.1", 40008));
        nodes.add(new ECSNode("Server9", "127.0.0.1", 40009));


        hashRing.updateMatadata(nodes);
        server1.setMetadata(hashRing);
        server2.setMetadata(hashRing);
        server3.setMetadata(hashRing);
        server4.setMetadata(hashRing);
        server5.setMetadata(hashRing);
        server6.setMetadata(hashRing);
        server7.setMetadata(hashRing);
        server8.setMetadata(hashRing);
        server9.setMetadata(hashRing);


        long startTime = System.nanoTime();
        KVStore kvClient = new KVStore("localhost", 40001);
        try {
            kvClient.connect();
        } catch (IOException ioe) {
            System.out.println(ioe);
            return;
        }


        List<String> test_data = getDataFrom("test_data/");

        Integer count = 0;
        for (String data:test_data) {
            count ++;
            kvClient.get(Integer.toString(count));
        }
        System.out.println("Get count: "+Integer.toString(count));
        long time = System.nanoTime() - startTime;
        System.out.println("Time spent in msec: "+Long.toString(time/1000000));
    }
}
