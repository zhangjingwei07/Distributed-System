package shared.messages;

import com.google.gson.Gson;
import org.apache.log4j.Logger;


public class KVMessageImple implements KVMessage, EncodingDecoding {
    private static Logger logger = Logger.getLogger("KVMessage");

    private String key;
    private String value;
    private StatusType status;

    public KVMessageImple(){
    }

    public KVMessageImple(String key, String value, StatusType status) {
        this.key = key;
        this.value = value;
        this.status = status;
    }

    @Override
    public String getKey(){
        return key;
    }

    @Override
    public String getValue(){
        return value;
    }

    @Override
    public StatusType getStatus(){
        return status;
    }

    @Override
    public void setKey(String key){
        this.key = key;
    }

    @Override
    public void setValue(String value){
        this.value = value;
    }

    @Override
    public void setStatus(StatusType status){
        this.status = status;
    }
    
    @Override
    public String encode(){
        return new Gson().toJson(this);
    }

    @Override
    public void decode(String data){
        KVMessageImple obj_mess = new Gson().fromJson(data, this.getClass());
        this.setKey(obj_mess.key);
        this.setValue(obj_mess.value);
        // System.out.println(this.value);
        this.setStatus(obj_mess.status);

    }
    
}
