package shared.metadata;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class Serialization {
    private static Logger logger = Logger.getLogger("Serialization");

    public static byte[] serialize(Object obj){
        ByteArrayOutputStream bos = null;
        try{
            bos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(bos);
            oos.writeObject(obj);
            oos.flush();
        } catch (IOException e){
            logger.error("Failed in serialization, "+ e);
        }
        byte [] data = bos.toByteArray();
        return data;
    }

    public static Object deserialize(byte[] data) {
        Object obj = null;
        try{
            ByteArrayInputStream in = new ByteArrayInputStream(data);
            ObjectInputStream is = new ObjectInputStream(in);
            obj = is.readObject();        
        } catch (IOException e) {
            logger.error("Failed in deserialization, "+ e);
        } catch (ClassNotFoundException e) {
            logger.error("ClassNotFoundException in deserialization, "+ e);
        }

        return obj;
    }

}
